using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pools;
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
    public EncodedString Name { get; } = tracked.StoredCommandName;
}

// Name-only DEALLOCATE, typically for leaked ownership whose presence is already absent.
sealed class CloseStatement(EncodedString name) : MaintenanceWork
{
    public EncodedString Name { get; } = name;
}

// Multiple DEALLOCATEs sharing one maintenance node and Sync window.
sealed class CloseStatements(EncodedString[] names) : MaintenanceWork
{
    public EncodedString[] Names { get; } = names;
}

// ADO-owned session state around the protocol. Prepared presence and maintenance survive pool leases.
sealed class PgConnection : IPoolConnection<PgConnection>
{
    readonly PgClientProtocol _protocol;
    readonly CommandTracker? _tracker;
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
    // Standalone protocols own this heartbeat; pooled protocols use the pool dispatcher.
    Heartbeat? _selfHeartbeat;
    IDisposable? _poolHeartbeatRegistration;
    int _sessionLifetimeReleased;
    // Session-wide explicit-prepare names remain unique across successive leases.
    int _explicitPrepareCounter;

    public string MintExplicitPrepareName() => $"_ep{Interlocked.Increment(ref _explicitPrepareCounter)}";

    // Startup suppresses idle publication until the create path has committed the initial lease.
    ConnectionPoolContext<PgConnection>? _poolContext;
    int _isStarted;

    PgConnection(PgClientProtocol protocol, CommandTracker? tracker, TimeSpan maintenanceInterval)
    {
        _protocol = protocol;
        _tracker = tracker;
        _maintenanceInterval = maintenanceInterval;
        _tracker?.Register(this);
        protocol.Completion.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(ReleaseSessionLifetime);
    }

    public PgClientProtocol Protocol => _protocol;

    // Creates a fully open but unpublished connection. Start enables idle publication only after
    // the pool commits the initial lease. Heartbeats route through this wrapper for maintenance.
    public static PgConnection Create(PgClientProtocolOptions protocolOptions, PgClientOptions clientOptions, TransportConnection transport, CommandTracker? tracker = null, ConnectionPoolContext<PgConnection>? poolContext = null, TimeSpan timeout = default)
    {
        var protocol = PgClientProtocol.Create(protocolOptions);
        var conn = new PgConnection(protocol, tracker, clientOptions.MaintenanceInterval);
        conn._poolContext = poolContext;
        protocol.Start(clientOptions, transport,
            poolContext is null ? NoopAvailability : conn.SignalAvailabilityIfStarted,
            timeout);
        conn.WireHeartbeat(clientOptions, poolContext);
        return conn;
    }

    public static async ValueTask<PgConnection> CreateAsync(PgClientProtocolOptions protocolOptions, PgClientOptions clientOptions, TransportConnection transport, CommandTracker? tracker = null, ConnectionPoolContext<PgConnection>? poolContext = null, CancellationToken cancellationToken = default)
    {
        var protocol = PgClientProtocol.Create(protocolOptions);
        var conn = new PgConnection(protocol, tracker, clientOptions.MaintenanceInterval);
        conn._poolContext = poolContext;
        await protocol.StartAsync(clientOptions, transport,
            poolContext is null ? NoopAvailability : conn.SignalAvailabilityIfStarted,
            cancellationToken).ConfigureAwait(false);
        conn.WireHeartbeat(clientOptions, poolContext);
        return conn;
    }

    void SignalAvailabilityIfStarted(bool isIdle)
    {
        if (Volatile.Read(ref _isStarted) != 0)
            _poolContext!.SignalAvailability(this, isIdle);
    }

    // Enables depth-to-zero publication after installation and initial lease assignment.
    public void Start() => Volatile.Write(ref _isStarted, 1);

    // A non-null callback tells the protocol that this wrapper supplies heartbeat orchestration.
    static readonly Action<bool> NoopAvailability = static _ => { };

    void WireHeartbeat(PgClientOptions options, ConnectionPoolContext<PgConnection>? poolContext)
    {
        if (poolContext is not null)
        {
            SetPoolHeartbeatRegistration(poolContext.OnHeartbeat(static (conn, interval) => conn.OnHeartbeat(interval), this));
        }
        else
        {
            var heartbeat = new Heartbeat(options.HeartbeatInterval);
            heartbeat.Register(OnHeartbeat);
            SetSelfHeartbeat(heartbeat);
        }
    }

    void SetSelfHeartbeat(Heartbeat heartbeat)
    {
        if (Volatile.Read(ref _sessionLifetimeReleased) != 0)
        {
            heartbeat.Dispose();
            return;
        }

        _selfHeartbeat = heartbeat;
        if (Volatile.Read(ref _sessionLifetimeReleased) != 0)
            Interlocked.Exchange(ref _selfHeartbeat, null)?.Dispose();
    }

    // Starts due maintenance before advancing protocol heartbeat duties.
    ValueTask OnHeartbeat(TimeSpan interval)
    {
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
    public bool IsIdle => _protocol.IsIdle;
    public bool IsSchedulable => _protocol.IsSchedulable;
    public Task Completion => _protocol.Completion;
    public Exception? CompletionException => _protocol.CompletionException;
    public int CompareTo(PgConnection? other) => _protocol.CompareTo(other?._protocol);
    public bool TryBeginPruning() => _protocol.TryBeginPruning();

    public Task CompleteAsync(Exception? exception = null) => _protocol.CompleteAsync(exception);

    void SetPoolHeartbeatRegistration(IDisposable registration)
    {
        if (Volatile.Read(ref _sessionLifetimeReleased) != 0)
        {
            registration.Dispose();
            return;
        }

        _poolHeartbeatRegistration = registration;
        if (Volatile.Read(ref _sessionLifetimeReleased) != 0)
            Interlocked.Exchange(ref _poolHeartbeatRegistration, null)?.Dispose();
    }

    void ReleaseSessionLifetime()
    {
        if (Interlocked.Exchange(ref _sessionLifetimeReleased, 1) != 0)
            return;

        Interlocked.Exchange(ref _poolHeartbeatRegistration, null)?.Dispose();
        Interlocked.Exchange(ref _selfHeartbeat, null)?.Dispose();
        // PgConnection spans many ADO leases; tracker membership follows the protocol session,
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
            if (tracked.CommandName != descriptor.CommandName)
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

    public bool TryQueue<TState, TFlow>(Func<PgConnection, TState, TFlow> materialize, TState state,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TFlow? flow,
        CancellationToken cancellationToken = default, bool mustPipeline = false)
        where TFlow : PgClientFlow
        => _protocol.TryQueue(
            static args => args.Materialize(args.Connection, args.State),
            (Materialize: materialize, Connection: this, State: state),
            out flow,
            cancellationToken,
            mustPipeline);

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

    // Called by MaintenanceFlow's completion action after the protocol has marked the flow
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
        // drain to pick up their item, or on the flow's completion action's re-check to schedule
        // a successor (which can reuse this same instance, see MaintenanceFlow.OnCompletedAction).
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
