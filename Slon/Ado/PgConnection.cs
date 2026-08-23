using Microsoft.Extensions.Logging;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pooling;
using Slon.Text;
using Slon.Threading;
using Slon.Transport;

namespace Slon;

// Per-session prepared-statement presence. FIFO ordering lets followers use a Preparing statement
// behind its Parse without a separate completion promise.
enum TrackedStatus
{
    None,
    Preparing,
    Tracked,
}

// Intrusive maintenance work. A completion on the final FIFO item also covers preceding work.
abstract class MaintenanceWork
{
    public TaskCompletionSource? Completion { get; init; }
    internal MaintenanceWork? Next { get; set; }
}

// LRU eviction whose presence is removed after the server confirms DEALLOCATE.
sealed class EvictDeallocate(TrackedCommand tracked) : MaintenanceWork
{
    public TrackedCommand Tracked { get; } = tracked;
    public EncodedCString Name { get; } = tracked.StoredCommandName;
}

// Name-only DEALLOCATE, typically for leaked ownership whose presence is already absent.
sealed class CloseStatement(EncodedCString name) : MaintenanceWork
{
    public EncodedCString Name { get; } = name;
}

// ADO-owned session state around the protocol. Prepared presence and maintenance survive pool leases.
sealed class PgConnection : IPoolConnection<PgConnection>
{
    internal sealed class FlowBindingContext(PgConnection connection) : PgClientFlowBindingContext
    {
        internal PgConnection Connection { get; } = connection;
    }

    sealed class ProtocolLoadObserver(PgConnection connection) : PgClientProtocol.LoadObserver
    {
        internal override void OnFlowQueued(bool stallsPipeline)
            => connection.OnFlowQueued(stallsPipeline);

        internal override void OnFlowActivated()
            => connection.OnFlowActivated();

        internal override void OnFlowReleased(bool stallsPipeline)
            => connection.OnFlowReleased(stallsPipeline);
    }
    readonly PgClientProtocol _protocol;
    readonly CommandTracker? _tracker;
    readonly ILogger _logger;
    // FIFO ordering synchronizes Preparing followers behind the winning Parse.
    readonly System.Collections.Concurrent.ConcurrentDictionary<TrackedCommand, TrackedStatus> _tracked = new();

    // A flow captures and drains a list prefix while producers may append the next prefix.
    MaintenanceWork? _maintenanceHead;
    MaintenanceWork? _maintenanceTail;
    readonly Lock _maintenanceLock = new();
    int _maintenanceArmed;
    MaintenanceFlow? _cachedMaintenanceFlow;
    // Time-based maintenance cadence independent of heartbeat frequency.
    readonly TimeSpan _maintenanceInterval;
    TimeSpan _maintenanceAccum;
    // Standalone protocols own this heartbeat. Pooled protocols use the pool dispatcher.
    IDisposable? _selfHeartbeat;
    IDisposable? _poolHeartbeatRegistration;
    int _sessionLifetimeReleased;
    ConnectionPool<PgConnection>.Registration _poolRegistration;
    int _pipelineStalls;
    int _heartbeatTick;
    int _completionCount;
    int _lastTickCompletions;
    double _throughputPerTick;
    int _currentFlowStartTick;
    // Session-wide explicit-prepare names remain unique across successive leases.
    int _connectionPrepareCounter;

    public string MintConnectionPrepareName() => $"_cp{Interlocked.Increment(ref _connectionPrepareCounter)}";

    // Startup suppresses idle publication until the create path has committed the initial lease.
    ConnectionPoolContext<PgConnection>? _poolContext;
    int _isStarted;

    PgConnection(PgClientProtocol protocol, CommandTracker? tracker, TimeSpan maintenanceInterval,
        ILogger logger)
    {
        _protocol = protocol;
        _tracker = tracker;
        _maintenanceInterval = maintenanceInterval;
        _logger = logger;
        _protocol.SetFlowBindingContext(new FlowBindingContext(this));
    }

    // Armed only after wiring succeeds, so release never races resource installation. On a
    // protocol that already completed this fires inline and tears down what was just wired.
    void ArmSessionLifetimeRelease()
        => _protocol.Completion.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(ReleaseSessionLifetime);

    public PgClientProtocol Protocol => _protocol;

    // Creates a fully open but unpublished connection. Start enables idle publication only after
    // the pool commits the initial lease. Heartbeats route through this wrapper for maintenance.
    public static PgConnection Create(PgClientProtocolOptions protocolOptions,
        PgClientOptions clientOptions, TransportConnection transport,
        CommandTracker? tracker = null, ConnectionPoolContext<PgConnection>? poolContext = null,
        TimeSpan timeout = default)
    {
        var protocol = PgClientProtocol.Create(protocolOptions);
        var conn = CreateUnstarted(protocol, clientOptions, tracker, poolContext);
        protocol.Start(clientOptions, transport, conn.CreateProtocolHosting(), timeout);
        conn.CompleteStart(clientOptions);
        return conn;
    }

    static PgConnection CreateUnstarted(PgClientProtocol protocol,
        PgClientOptions clientOptions, CommandTracker? tracker = null,
        ConnectionPoolContext<PgConnection>? poolContext = null)
    {
        var conn = new PgConnection(protocol, tracker, clientOptions.MaintenanceInterval,
            clientOptions.LoggerFactory.CreateLogger("Slon.Pg.Connection"))
        {
            _poolContext = poolContext
        };
        if (poolContext is not null)
        {
            protocol.SetFlowMigration(conn.TryMigrateFlow);
        }
        return conn;
    }

    internal static PgConnection Create(PgClientProtocol.Startup startup,
        CommandTracker? tracker = null,
        ConnectionPoolContext<PgConnection>? poolContext = null)
    {
        var conn = CreateUnstarted(startup.Protocol, startup.Options, tracker, poolContext);
        startup.Start(conn.CreateProtocolHosting());
        conn.CompleteStart(startup.Options);
        return conn;
    }

    internal static async ValueTask<PgConnection> CreateAsync(PgClientProtocol.Startup startup,
        CommandTracker? tracker = null,
        ConnectionPoolContext<PgConnection>? poolContext = null,
        CancellationToken cancellationToken = default)
    {
        var conn = CreateUnstarted(startup.Protocol, startup.Options, tracker, poolContext);
        await startup.StartAsync(conn.CreateProtocolHosting(), cancellationToken).ConfigureAwait(false);
        await conn.CompleteStartAsync(startup.Options).ConfigureAwait(false);
        return conn;
    }

    void CompleteStart(PgClientOptions clientOptions)
    {
        try
        {
            _tracker?.Register(this);
            WireHeartbeat(clientOptions, _poolContext);
        }
        catch (Exception ex)
        {
            _protocol.CompleteAsync(ex).GetAwaiter().GetResult();
            // Nothing armed the release yet. Tear down whatever wiring installed before surfacing.
            ReleaseSessionLifetime();
            throw;
        }
        ArmSessionLifetimeRelease();
    }

    public static ValueTask<PgConnection> CreateAsync(PgClientProtocolOptions protocolOptions,
        PgClientOptions clientOptions, TransportConnection transport,
        CommandTracker? tracker = null, ConnectionPoolContext<PgConnection>? poolContext = null,
        CancellationToken cancellationToken = default)
    {
        var protocol = PgClientProtocol.Create(protocolOptions);
        var conn = CreateUnstarted(protocol, clientOptions, tracker, poolContext);
        return CompleteAsync(protocol, conn, clientOptions, transport, cancellationToken);

        static async ValueTask<PgConnection> CompleteAsync(PgClientProtocol protocol,
            PgConnection conn, PgClientOptions clientOptions, TransportConnection transport,
            CancellationToken cancellationToken)
        {
            await protocol.StartAsync(clientOptions, transport,
                conn.CreateProtocolHosting(), cancellationToken).ConfigureAwait(false);
            await conn.CompleteStartAsync(clientOptions).ConfigureAwait(false);
            return conn;
        }
    }

    PgClientProtocol.Hosting CreateProtocolHosting()
        => _poolContext is null
            ? PgClientProtocol.Hosting.Connection
            : PgClientProtocol.Hosting.Pooled(
                SignalAvailabilityIfStarted, new ProtocolLoadObserver(this));

    async ValueTask CompleteStartAsync(PgClientOptions clientOptions)
    {
        try
        {
            _tracker?.Register(this);
            WireHeartbeat(clientOptions, _poolContext);
        }
        catch (Exception ex)
        {
            await _protocol.CompleteAsync(ex).ConfigureAwait(false);
            // Nothing armed the release yet. Tear down whatever wiring installed before surfacing.
            ReleaseSessionLifetime();
            throw;
        }
        ArmSessionLifetimeRelease();
    }

    void SignalAvailabilityIfStarted()
    {
        if (_poolContext is not null && Volatile.Read(ref _isStarted) != 0 && _protocol.IsSchedulable)
            _poolRegistration.SignalAvailability(_protocol.Outstanding is 0);
    }

    // Enables depth-to-zero publication after installation. The unopened slot future still owns
    // the connection here, so establish the initial idle level without minting or signaling a token;
    // the placement immediately following Start owns that initial capacity.
    void IPoolConnection<PgConnection>.Start(ConnectionPool<PgConnection>.Registration registration)
    {
        _poolRegistration = registration;
        if (_protocol.Outstanding is not 0)
            throw new InvalidOperationException("A newly admitted connection must be idle.");
        Volatile.Write(ref _isStarted, 1);
    }

    void WireHeartbeat(PgClientOptions options, ConnectionPoolContext<PgConnection>? poolContext)
    {
        if (poolContext is not null)
        {
            _poolHeartbeatRegistration = poolContext.OnHeartbeat(static (conn, interval) => conn.OnHeartbeat(interval), this);
        }
        else
        {
            var heartbeat = new Heartbeat(options.HeartbeatInterval, options.TimeProvider, _logger);
            heartbeat.Register(OnHeartbeat);
            _selfHeartbeat = heartbeat;
        }
    }

    // Starts due maintenance before advancing protocol heartbeat duties.
    ValueTask OnHeartbeat(TimeSpan interval)
    {
        if (_poolContext is not null)
        {
            _heartbeatTick++;
            var completedThisTick = _completionCount - _lastTickCompletions;
            _lastTickCompletions = _completionCount;
            const double Alpha = 0.3;
            _throughputPerTick = Alpha * completedThisTick + (1 - Alpha) * _throughputPerTick;
        }

        _maintenanceAccum += interval;
        if (_maintenanceAccum >= _maintenanceInterval)
        {
            _maintenanceAccum = TimeSpan.Zero;
            if (HasMaintenance())
                TryArmAndSchedule();
        }
        return _protocol.Heartbeat(interval);
    }

    // IPoolConnection<PgConnection>
    bool IPoolConnection<PgConnection>.IsIdle => _protocol.Outstanding is 0;
    bool IPoolConnection<PgConnection>.IsSchedulable => _protocol.IsSchedulable;
    public Task Completion => _protocol.Completion;
    public Exception? CompletionException => _protocol.CompletionException;
    int IPoolConnection<PgConnection>.CompareTo(PgConnection? other)
    {
        if (other is null)
            return 1;
        return LoadScore().CompareTo(other.LoadScore());
    }
    public Task CompleteAsync(Exception? exception = null) => _protocol.CompleteAsync(exception);

    // Estimated wait in ticks: outstanding / throughput, plus serialization and head-age penalties.
    // Power-of-two selection needs directional accuracy rather than a precise latency model.
    double LoadScore()
    {
        var outstanding = _protocol.Outstanding;
        if (outstanding == 0)
            return 0;

        const double RateFloor = 0.5, StallWeight = 2, AgePenalty = 5;
        const int AgeThresholdTicks = 3;

        var rate = Math.Max(_throughputPerTick, RateFloor);
        var score = outstanding / rate + _pipelineStalls * StallWeight;
        if (_heartbeatTick - _currentFlowStartTick > AgeThresholdTicks)
            score += AgePenalty;
        return score;
    }

    void OnFlowQueued(bool stallsPipeline)
    {
        if (stallsPipeline)
            Interlocked.Increment(ref _pipelineStalls);
    }

    void OnFlowActivated()
        => _currentFlowStartTick = _heartbeatTick;

    void OnFlowReleased(bool stallsPipeline)
    {
        Interlocked.Increment(ref _completionCount);
        if (stallsPipeline)
            Interlocked.Decrement(ref _pipelineStalls);
    }
    internal void ReportUnobservedCallback(Exception exception, string callback)
        => SlonLogMessages.UnobservedCallbackException(_logger, exception, callback);
    internal void ReportMaintenanceError(string sqlState, string messageText)
        => SlonLogMessages.MaintenanceCommandFailed(_logger, sqlState, messageText);

    void ReleaseSessionLifetime()
    {
        if (Interlocked.Exchange(ref _sessionLifetimeReleased, 1) != 0)
            return;

        Interlocked.Exchange(ref _poolHeartbeatRegistration, null)?.Dispose();
        Interlocked.Exchange(ref _selfHeartbeat, null)?.Dispose();
        // PgConnection spans many ADO leases. Tracker membership follows the protocol session,
        // ending at terminal completion/eviction rather than per-lease proxy disposal.
        _tracker?.Deregister(this);
    }

    public TrackedStatus GetTrackedStatus(TrackedCommand tracked)
        => _tracked.TryGetValue(tracked, out var status) ? status : TrackedStatus.None;

    // Atomic None -> Preparing. Winner enqueues the Parse flow (its own Sync window so a
    // sibling error can't kill it). Losers see Preparing and enqueue only their use flow.
    public bool TryBeginPreparing(TrackedCommand tracked)
        => _tracked.TryAdd(tracked, TrackedStatus.Preparing);

    // Called from the Parse flow's completion handler on success.
    public void SetTracked(TrackedCommand tracked)
        => _tracked[tracked] = TrackedStatus.Tracked;

    public void CompletePreparing(TrackedCommand tracked, in CommandDescriptor descriptor)
    {
        tracked.Complete(descriptor);

        // Global invalidation may race this wire's Parse. Eviction skips Preparing entries, so
        // the winner must close a statement that became invalid while its Parse was in flight.
        if (tracked.IsInvalid)
        {
            CloseInvalidated();
            return;
        }

        SetTracked(tracked);
        if (tracked.IsInvalid)
            CloseInvalidated();

        void CloseInvalidated()
        {
            RemoveTracked(tracked);
            PushMaintenance(new CloseStatement(tracked.StoredCommandName));
        }
    }

    // Called on eviction (DEALLOCATE fanout) or to roll back an optimistic Preparing marker
    // when the Parse flow failed.
    public void RemoveTracked(TrackedCommand tracked)
        => _tracked.TryRemove(tracked, out _);

    // Session-wide reset (DISCARD ALL, role change, reconnect) destroyed every prepared
    // statement on this session. Drop all tracked entries so subsequent uses re-Parse.
    public void ClearTracked()
        => _tracked.Clear();

    internal void ReconcilePreparedError(in CommandDescriptor descriptor, string sqlState)
    {
        if (!descriptor.IsPrepared)
            return;

        if (sqlState == PgErrorCodes.InvalidSqlStatementName)
        {
            ClearTracked();
            return;
        }

        if (sqlState != PgErrorCodes.FeatureNotSupported)
            return;

        foreach (var tracked in _tracked.Keys)
        {
            if (!tracked.CommandName.ValueEquals(descriptor.CommandName))
                continue;

            if (tracked.Kind is TrackedCommandKind.Auto)
            {
                RemoveTracked(tracked);
                _tracker?.InvalidateAuto(tracked);
            }
            return;
        }
    }

    // Test/diagnostic accessors. Exposed via InternalsVisibleTo so tests can verify
    // auto-prepare behavior without round-tripping through pg_prepared_statements.
    internal int TrackedCount => _tracked.Count;
    internal IEnumerable<(TrackedCommand Command, TrackedStatus Status)> TrackedEntries
        => _tracked.Select(kv => (kv.Key, kv.Value));

    // Snapshot the pending maintenance items for inspection. Used by tests to verify that
    // eviction fan-out actually pushed work to this connection.
    internal MaintenanceWork[] PeekMaintenance()
    {
        lock (_maintenanceLock)
        {
            var list = new List<MaintenanceWork>();
            var node = _maintenanceHead;
            while (node is not null)
            {
                list.Add(node);
                node = node.Next;
            }
            return list.ToArray();
        }
    }

    public bool TryQueue(PgClientFlow flow, CancellationToken cancellationToken = default)
        => _protocol.TryQueue(flow, cancellationToken: cancellationToken);

    bool TryMigrateFlow(FlowMigration migration)
    {
        if (_poolContext is not { } poolContext)
            return false;
        poolContext.TrackBackgroundOperation(() => MigrateFlowAsync(poolContext, migration));
        return true;
    }

    static async Task MigrateFlowAsync(ConnectionPoolContext<PgConnection> poolContext,
        FlowMigration migration)
    {
        try
        {
            var timeout = migration.GetRemainingTimeout();
            await poolContext.GetAsync(
                static (candidate, item) => TryScheduleMigrated(candidate, item), migration,
                timeout, migration.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            migration.Fail(ex);
        }
    }

    static bool TryScheduleMigrated(ConnectionCandidate<PgConnection> candidate, FlowMigration migration)
    {
        var flow = migration.PreparePlacement();
        var options = FlowEnqueueOptions.AllowMigration |
            (candidate.IsIdleCandidate
                ? FlowEnqueueOptions.None
                : FlowEnqueueOptions.RequireExistingPipeline);
        return migration.CompletePlacement(
            candidate.Connection.Protocol.TryQueue(flow, options, candidate.CancellationToken));
    }

    public void PushMaintenance(MaintenanceWork work)
    {
        lock (_maintenanceLock)
        {
            if (_maintenanceTail is null)
                _maintenanceHead = work;
            else
                _maintenanceTail.Next = work;
            _maintenanceTail = work;
        }
        // Default: no immediate schedule. The next heartbeat tick checks HasMaintenance and
        // arms the flow. Natural debounce/batching at the heartbeat cadence with no extra
        // primitive. Exception: if this item carries a completion TCS, the caller is awaiting
        // the drain and shouldn't pay heartbeat latency. Force-arm so the flow runs ASAP.
        if (work.Completion is not null)
            TryArmAndSchedule();
    }

    // Capture the (head, tail) range to process. Items remain linked in the list, committed
    // only after the flow confirms success. Producers append beyond `tail` during the flow's
    // execution. Those stay for the next flow.
    internal (MaintenanceWork? Head, MaintenanceWork? Tail) SnapshotMaintenanceRange()
    {
        lock (_maintenanceLock)
            return (_maintenanceHead, _maintenanceTail);
    }

    // Remove the prefix from head up to and including `upToInclusive`. Clears Next pointers as
    // it walks so the GC can reclaim the processed nodes. Producers that appended beyond the
    // snapshotted tail (now head after this commit) stay linked.
    internal void CommitMaintenanceRange(MaintenanceWork upToInclusive)
    {
        lock (_maintenanceLock)
        {
            var node = _maintenanceHead;
            while (node is not null)
            {
                var next = node.Next;
                node.Next = null;
                if (ReferenceEquals(node, upToInclusive))
                {
                    _maintenanceHead = next;
                    if (next is null)
                        _maintenanceTail = null;
                    return;
                }
                node = next;
            }
            // upToInclusive wasn't in the list (shouldn't happen if the caller passed back what
            // SnapshotMaintenanceRange returned). Defensive: leave head/tail as-is.
        }
    }

    internal bool HasMaintenance()
    {
        lock (_maintenanceLock)
            return _maintenanceHead is not null;
    }

    // Called by MaintenanceFlow's completed observer after the protocol has marked the flow
    // fully completed, safe to Reset+reuse the instance for the successor. Returns the flow to
    // the cache, disarms, and if there's outstanding work (either retries from a failed flow or
    // items appended during this flow), schedules a successor.
    internal void OnMaintenanceFlowCompleted(MaintenanceFlow flow)
    {
        ReturnMaintenanceFlow(flow);
        // Both queue observations pass through _maintenanceLock. If we see no work, a later
        // producer's lock acquire observes this disarm before its arm CAS.
        _maintenanceArmed = 0;
        if (HasMaintenance())
            TryArmAndSchedule();
    }

    void TryArmAndSchedule()
    {
        if (Interlocked.CompareExchange(ref _maintenanceArmed, 1, 0) is not 0)
            return;
        // We won the arm race. Rent (or allocate) the flow and queue it. A producer that pushed
        // after our enqueue and lost their own arm CAS will rely on either this in-flight flow's
        // drain to pick up their item, or on the flow's completed observer's re-check to schedule
        // a successor (which can reuse this same instance).
        var flow = Interlocked.Exchange(ref _cachedMaintenanceFlow, null) ?? new MaintenanceFlow();
        flow.Bind(this);
        if (!_protocol.TryQueue(flow))
        {
            // Couldn't queue. Protocol may be draining. Disarm so a future push can re-arm.
            Volatile.Write(ref _maintenanceArmed, 0);
            ReturnMaintenanceFlow(flow);
        }
    }

    void ReturnMaintenanceFlow(MaintenanceFlow flow)
    {
        flow.Reset();
        _ = Interlocked.CompareExchange(ref _cachedMaintenanceFlow, flow, null);
    }

}
