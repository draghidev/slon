using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pools;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Assembly-scoped, BOUNDED, multiplexing pool of PgClientProtocol instances for low-level Pg tests
// that complete their flows cleanly. Backed by the real ConnectionPool<T> (over a thin IPoolConnection
// wrapper), deliberately small so concurrent test methods contend for a handful of wires and actually
// exercise the pipelining/multiplexing machinery - the bag pool this replaced handed every test its own
// exclusive wire, so pipelining went unstressed. Bounded by MaxConnections (no longer unbounded) and
// disposable (DrainAsync closes every wire), so the suite can't exhaust the server's max_connections.
//
// Tests that intentionally fault the wire or destroy the protocol (RecoveryTests, ProtocolCompletionTests)
// MUST still use NewIsolatedAsync - a destroyed protocol on the shared pool would poison the next lessee.
//
// Lease semantics: GetAsync hands out a protocol. When one goes idle (depth -> 0) it publishes itself to
// the pool's idle channel - an O(1) handout fast path so GetAsync needn't scan the striped set (>O(1)) for
// a good candidate. Multiplexing is a SEPARATE path: when concurrent demand exceeds MaxConnections and no
// wire is idle, GetAsync schedules the flow onto the best-scored BUSY wire (LoadScore/CompareTo), putting
// two outstanding flows on one wire = pipelining. The small MaxConnections is what forces that path - the
// point of this pool. An exclusive scope keeps its wire non-idle while held, so it stays exclusive.
static class PgTestPool
{
    // Core count by default - matches the pool's internal per-core striping, and with the test workers
    // contending it still drives multiplexing while leaving headroom so exclusive-scope holders don't
    // starve. Override via PG_TEST_POOL_MAX for a deliberate soak or a tighter pipelining squeeze.
    static readonly int MaxConnections =
        int.TryParse(Environment.GetEnvironmentVariable("PG_TEST_POOL_MAX"), out var m) && m > 0 ? m : Environment.ProcessorCount;
    static readonly TimeSpan LeaseTimeout = TimeSpan.FromSeconds(30);

    static readonly ConnectionPool<PooledProtocol> _pool =
        new(new Factory(), new ConnectionPoolOptions { MaxConnections = MaxConnections, HeartbeatInterval = TimeSpan.FromSeconds(1) });

    internal static PgClientOptions NewOptions() => new()
    {
        EndPoint = TestEndPoint.Default,
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
    };

    // Lease a clean protocol from the bounded shared pool. Use ONLY in tests that complete their flows
    // cleanly. The protocol auto-returns when idle, so the Lease's DisposeAsync has nothing to do.
    internal static async ValueTask<Lease> LeaseAsync()
        => new(await _pool.GetAsync(LeaseTimeout).ConfigureAwait(false));

    // Construct a fresh, non-pooled protocol the caller owns end to end. Use in tests that fault the wire,
    // destroy the protocol, or need custom heartbeat/timeout settings. Standalone heartbeat (no onIdle),
    // so flow activation timeouts work without a pool driving the tick.
    internal static async Task<PgClientProtocol> NewIsolatedAsync(Action<PgClientProtocolOptions>? configureOptions = null)
    {
        var options = NewOptions();
        var transport = await SocketStreamConnection.ConnectAsync(options.EndPoint);
        var protocolOptions = new PgClientProtocolOptions(options);
        configureOptions?.Invoke(protocolOptions);
        var protocol = PgClientProtocol.Create(protocolOptions);
        await protocol.StartAsync(options, transport);
        return protocol;
    }

    // Sync flow exerciser shared across the Pg-layer tests. Driving CommandFlow directly so
    // the assertions attribute to the protocol, not to any ADO surface.
    internal static Task RunSync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: false, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetEnumerator();
        while (e.MoveNext()) { }
        // Sync Dispose, like a real sync consumer. DisposeAsync's await-drain can pend on pipeline
        // retirement and resume on a TP thread, which breaks the caller-thread assertions awaiting this.
        e.Dispose();
        return Task.CompletedTask;
    }

    internal static async Task RunAsync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    // Closes every pooled wire. Called from TestAssemblyHooks so the assembly's single permitted
    // [AssemblyCleanup] sweeps every helper pool.
    internal static async Task DrainAsync() => await _pool.DisposeAsync();

    internal readonly struct Lease : IAsyncDisposable
    {
        readonly PooledProtocol _conn;
        internal Lease(PooledProtocol conn) => _conn = conn;
        public PgClientProtocol Protocol => _conn.Protocol;
        // Nothing to return: the protocol republishes itself to the idle channel when it goes idle
        // (depth -> 0). A still-busy wire just keeps serving (and may be multiplexed onto) until it drains.
        public ValueTask DisposeAsync() => default;
    }

    // Thin IPoolConnection<T> over a bare protocol - the test-pool analogue of PgConnection's pool-unit
    // wiring. Mirrors its Start gate: the protocol's idle signal is suppressed until the pool has committed
    // the lease (Start), so a depth-0 transition during startup can't publish the wire before it is owned.
    internal sealed class PooledProtocol : IPoolConnection<PooledProtocol>
    {
        int _started;
        Action? _idleSignal;
        public PgClientProtocol Protocol { get; }

        PooledProtocol(PgClientProtocol protocol) => Protocol = protocol;

        public static async ValueTask<PooledProtocol> CreateAsync(PgClientOptions options, ConnectionPoolContext<PooledProtocol> poolContext, CancellationToken cancellationToken)
        {
            var transport = await SocketStreamConnection.ConnectAsync(options.EndPoint);
            try
            {
                var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
                var conn = new PooledProtocol(protocol);
                conn._idleSignal = poolContext.CreateConnectionIdleSignal(conn);
                // Pool drives the heartbeat (a non-null onIdle disables the protocol's self-heartbeat).
                poolContext.OnHeartbeat(static (c, interval) => c.Protocol.Heartbeat(interval), conn);
                await protocol.StartAsync(options, transport, conn.SignalIdleIfStarted, cancellationToken).ConfigureAwait(false);
                return conn;
            }
            catch (Exception ex)
            {
                // Release the just-connected socket the same way the protocol would: abortive close + error-
                // complete the endpoints (discard buffers). The protocol never took ownership.
                transport.Abort();
                await transport.Writer.CompleteAsync(ex).ConfigureAwait(false);
                await transport.Reader.CompleteAsync().ConfigureAwait(false);
                throw;
            }
        }

        void SignalIdleIfStarted()
        {
            if (Volatile.Read(ref _started) == 1)
                _idleSignal!();
        }

        public void Start() => Volatile.Write(ref _started, 1);
        public bool IsIdle => Protocol.IsIdle;
        public bool IsCompleted => Protocol.IsCompleted;
        public int CompareTo(PooledProtocol? other) => Protocol.CompareTo(other?.Protocol);
        public ValueTask CompleteAsync(Exception? exception = null) => Protocol.CompleteAsync(exception);
    }

    sealed class Factory : IPoolConnectionFactory<PooledProtocol>
    {
        public PooledProtocol Create(ConnectionPoolContext<PooledProtocol> poolContext, TimeSpan timeout = default)
            => throw new NotSupportedException("PgTestPool leases asynchronously.");

        public ValueTask<PooledProtocol> CreateAsync(ConnectionPoolContext<PooledProtocol> poolContext, CancellationToken cancellationToken = default)
            => PooledProtocol.CreateAsync(NewOptions(), poolContext, cancellationToken);
    }
}
