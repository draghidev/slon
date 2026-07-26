using BenchmarkDotNet.Attributes;
using Slon.Pools;

namespace Slon.Benchmark;

// Pool-only benchmark: fake IPoolConnection so we measure pool selection cost without network.
// Scenarios:
//   - GetAndRelease: hot path. Get a conn, mark it idle, repeat. Exercises the idle-channel
//     fast path and the idle-signal pushback.
//   - MultiplexBusy: stripe-walk path. Pre-setup drains the channel and marks all conns busy
//     (depth > 0, IsIdle = false). Every Get falls through to the stripe walker, which is
//     supposed to multiplex onto a non-idle conn.
// Parameterized over MaxConnections + Concurrency for both.
[MemoryDiagnoser]
[WarmupCount(2)]
[IterationCount(5)]
public class ConnectionPoolBenchmark
{
    [Params(4, 16, 64, 200)]
    public int MaxConnections;

    [Params(1, 8, 16)]
    public int Concurrency;

    ConnectionPool<FakeConnection> _pool = null!;
    FakeFactory _factory = null!;
    static readonly Func<ConnectionCandidate<FakeConnection>, object?, bool> AlwaysTrue = static (_, _) => true;

    [GlobalSetup]
    public async Task Setup()
    {
        _factory = new FakeFactory();
        _pool = new ConnectionPool<FakeConnection>(_factory, new ConnectionPoolOptions { MaxConnections = MaxConnections });
        // Pre-warm: ensure all slots have live connections so we measure steady-state selection.
        await _pool.OpenAllConnectionsAsync(timeout: TimeSpan.FromSeconds(5)).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _pool.DisposeAsync().ConfigureAwait(false);
    }

    // Per-iteration: drain the channel and mark all conns non-idle so each Get in MultiplexBusy
    // body misses the channel and goes through the stripe walker. Captured FakeConnection refs
    // are held in _busyConns so we can keep incrementing their depth between iterations.
    FakeConnection[]? _busyConns;

    [IterationSetup(Target = nameof(MultiplexBusy))]
    public void SetupMultiplexBusy()
    {
        // Drain the channel by claiming every conn the pool currently advertises as idle.
        _busyConns = new FakeConnection[MaxConnections];
        for (var i = 0; i < MaxConnections; i++)
        {
            _busyConns[i] = _pool.Get(AlwaysTrue, (object?)null, Timeout.InfiniteTimeSpan);
            _busyConns[i].IncrementDepth();
        }
    }

    [IterationCleanup(Target = nameof(MultiplexBusy))]
    public void CleanupMultiplexBusy()
    {
        // Restore so the next iteration can re-drain from a fresh idle state.
        if (_busyConns is null) return;
        foreach (var c in _busyConns)
        {
            c.DecrementDepth();
            c.MarkIdleAndSignal();
        }
        _busyConns = null;
    }

    [Benchmark]
    public async Task MultiplexBusy()
    {
        // All conns non-idle, channel empty: every Get goes through the stripe walker.
        var tasks = new Task[Concurrency];
        const int iterations = 1000;
        for (var t = 0; t < Concurrency; t++)
        {
            tasks[t] = Task.Run(async () =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    var conn = await _pool.GetAsync(AlwaysTrue, (object?)null, Timeout.InfiniteTimeSpan).ConfigureAwait(false);
                    // Don't signal idle. Keep conns busy so the next iteration also hits stripe walk.
                }
            });
        }
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task GetAndRelease()
    {
        var tasks = new Task[Concurrency];
        const int iterations = 1000;
        for (var t = 0; t < Concurrency; t++)
        {
            tasks[t] = Task.Run(async () =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    // The bare GetAsync(timeout) doesn't work, DoSchedule with null schedule
                    // returns false and TrySchedule never claims a conn. Pass always-true.
                    var conn = await _pool.GetAsync(AlwaysTrue, (object?)null, Timeout.InfiniteTimeSpan).ConfigureAwait(false);
                    // Simulate completing a unit of work: mark idle, fires the idle-signal callback
                    // which writes back to the pool's idle channel.
                    conn.MarkIdleAndSignal();
                }
            });
        }
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

}

sealed class FakeConnection : IPoolConnection<FakeConnection>
{
    static readonly Task NeverCompletes = new TaskCompletionSource().Task;
    int _depth;
    ConnectionPoolContext<FakeConnection>? _poolContext;

    internal void SetPoolContext(ConnectionPoolContext<FakeConnection> context) => _poolContext = context;
    internal void IncrementDepth() => Interlocked.Increment(ref _depth);
    internal void DecrementDepth() => Interlocked.Decrement(ref _depth);
    internal void MarkIdleAndSignal()
    {
        // Idle = depth 0. Simulate work-done by firing the pool's idle callback.
        Volatile.Write(ref _depth, 0);
        _poolContext?.SignalAvailability(this, isIdle: true);
    }

    public bool IsIdle => Volatile.Read(ref _depth) is 0;
    public bool IsSchedulable => true;
    public Task Completion => NeverCompletes;

    public int CompareTo(FakeConnection? other)
    {
        if (other is null)
            return 1;
        var l = Volatile.Read(ref _depth);
        var r = Volatile.Read(ref other._depth);
        return l < r ? -1 : l == r ? 0 : 1;
    }

    public bool TryBeginPruning() => false;
    public Task CompleteAsync(Exception? exception = null) => Task.CompletedTask;

    // The fake drives idle publication explicitly via MarkIdleAndSignal, so the startup
    // suppression gate Start() exists for has nothing to unblock here.
    public void Start() { }
}

sealed class FakeFactory : IPoolConnectionFactory<FakeConnection>
{
    public FakeConnection Create(ConnectionPoolContext<FakeConnection> poolContext, TimeSpan timeout = default)
    {
        var conn = new FakeConnection();
        conn.SetPoolContext(poolContext);
        // Start idle so it lands in the pool's idle channel.
        conn.MarkIdleAndSignal();
        return conn;
    }

    public ValueTask<FakeConnection> CreateAsync(ConnectionPoolContext<FakeConnection> poolContext, CancellationToken cancellationToken = default)
        => new(Create(poolContext));
}
