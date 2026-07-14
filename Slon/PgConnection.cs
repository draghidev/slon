using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pools;
using Slon.Transport;

namespace Slon;

// Per-session tracked status of a workload-known TrackedCommand. None (= missing from the map)
// means no Parse has been issued for this name on this session. Preparing means a Parse flow has
// been enqueued and hasn't completed. Pipeline FIFO ordering lets follower flows enqueue their
// Bind/Execute behind the Parse without re-issuing it. Tracked means the Parse succeeded and the
// name is ready to use.
enum TrackedStatus
{
    None,
    Preparing,
    Tracked,
}

// Maintenance work pushed by upstream producers (eviction, salvage, future keepalive). Discrete
// kinds let the MaintenanceFlow handle each shape during drain. Optional Completion TCS lets
// callers await batch completion, typically only set on the LAST item of a producer's batch
// since FIFO ordering means its completion guarantees all earlier items also landed.
//
// Intrusive linked list: each MaintenanceWork holds its own Next pointer. PgConnection's queue
// is just head/tail refs. Nodes themselves are the storage. Avoids the "List<T> retains peak
// capacity for the connection's lifetime" issue, drained nodes are GC'd, memory returns.
abstract class MaintenanceWork
{
    public TaskCompletionSource? Completion { get; init; }
    internal MaintenanceWork? Next { get; set; }
}

// LRU-evicted TrackedCommand whose server-side prepared statement needs to be DEALLOCATEd. Filled
// out by CommandTracker.OnEvict once it has fanned out across the registered connections. The
// flow does RemoveTracked after CloseComplete so presence stays consistent with wire state.
sealed class EvictDeallocate(TrackedCommand tracked) : MaintenanceWork
{
    public TrackedCommand Tracked { get; } = tracked;
}

// Server-side prepared statement that needs to be DEALLOCATEd by name only, no TrackedCommand
// available (e.g. leak salvage where the owning command's tracker has been finalized). Pure wire
// cleanup. Presence already absent (or was never the producer's concern).
sealed class CloseStatement(EncodedString name) : MaintenanceWork
{
    public EncodedString Name { get; } = name;
}

// Batched variant of CloseStatement for producers (e.g. UnprepareAll) that want to enqueue many
// names as a single node, one allocation, one optional completion TCS, all closes go in the
// same Sync window.
sealed class CloseStatements(EncodedString[] names) : MaintenanceWork
{
    public EncodedString[] Names { get; } = names;
}

// ADO-layer wrapper around a PgClientProtocol that owns prepared-statement presence and
// maintenance state. The protocol package stays Slon-agnostic. This is where the ADO layer hangs
// its bookkeeping. Pool unit (replaces PgClientProtocol in IPoolConnection<T>) so presence +
// maintenance survive lease boundaries naturally, one instance per protocol-session lifetime.
sealed class PgConnection : IPoolConnection<PgConnection>
{
    readonly PgClientProtocol _protocol;
    readonly CommandTracker? _tracker;
    // Per-session map of which TrackedCommands have been (or are being) Parsed on this connection.
    // Synchronization between Preparing and follower use is provided by pipeline FIFO order,
    // not by an explicit promise, followers see Preparing and enqueue their use behind the
    // winner's Parse flow.
    readonly System.Collections.Concurrent.ConcurrentDictionary<TrackedCommand, TrackedStatus> _tracked = new();

    // Maintenance: intrusive linked list (head/tail refs, nodes are MaintenanceWork). The in-flight
    // flow captures (head, tail) and processes that range. Producers append beyond tail during
    // execution and stay for the next flow. Commits clear Next pointers on the processed prefix
    // so the GC can reclaim them, capacity tracks live size, not historical peak.
    MaintenanceWork? _maintenanceHead;
    MaintenanceWork? _maintenanceTail;
    readonly Lock _maintenanceLock = new();
    int _maintenanceArmed;
    MaintenanceFlow? _cachedMaintenanceFlow;
    // Heartbeat-driven maintenance interval. Ticks accumulate and only fire maintenance once the
    // accumulator reaches this interval, time-based so it survives heartbeat-interval changes.
    readonly TimeSpan _maintenanceInterval;
    TimeSpan _maintenanceAccum;
    // Used in non-pool mode where we own the heartbeat tick ourselves. In pool mode the pool's
    // heartbeat dispatcher drives OnHeartbeat and this stays null.
    Heartbeat? _selfHeartbeat;
    // Per-session monotonic counter for explicit-prepare names. Lives here (not on CommandTracker)
    // so successive SlonConnections that share this PgConnection through the pool can't collide
    // on `_ep{N}`. The counter persists for the protocol-session lifetime.
    int _explicitPrepareCounter;

    public string MintExplicitPrepareName() => $"_ep{Interlocked.Increment(ref _explicitPrepareCounter)}";

    // Pool's idle-signal closure, captured during Start. Invoked via SignalIdleIfStarted, which
    // gates on _isStarted so the connection's startup-time idle transition doesn't publish to
    // the idle channel before the create-path has committed the lease (otherwise a concurrent
    // OpenConnectionAsync would grab the channel-published conn and end up sharing the wire).
    Action? _underlyingPoolIdleSignal;
    int _isStarted;

    PgConnection(PgClientProtocol protocol, CommandTracker? tracker, TimeSpan maintenanceInterval)
    {
        _protocol = protocol;
        _tracker = tracker;
        _maintenanceInterval = maintenanceInterval;
        _tracker?.Register(this);
    }

    public PgClientProtocol Protocol => _protocol;

    // Static factories: combine constructor + protocol-startup + idle-signal wiring into one
    // atomic creation step. Callers can't get a half-initialized PgConnection.
    //
    // Wires the pool's idle signal + heartbeat onto the protocol, then drives protocol startup.
    // PgClientProtocol stays Slon.Pools-oblivious. The typed callbacks land here. The
    // heartbeat goes through OnHeartbeat (not protocol.Heartbeat directly) so we get a periodic
    // tick to check pending maintenance. In non-pool mode we own the tick ourselves.
    //
    // The returned connection is fully open but NOT yet started. That's <see cref="Start"/>'s
    // job, called by the pool after the create-path commits the lease (or by the caller
    // directly in the no-pool case). Until Start fires, idle-channel publication is suppressed
    // so the connection's startup-time depth-to-zero transition can't race itself into the
    // channel before its first lease is assigned.
    public static PgConnection Create(PgClientProtocolOptions protocolOptions, PgClientOptions clientOptions, TransportConnection transport, CommandTracker? tracker = null, ConnectionPoolContext<PgConnection>? poolContext = null, TimeSpan timeout = default)
    {
        var protocol = PgClientProtocol.Create(protocolOptions);
        var conn = new PgConnection(protocol, tracker, clientOptions.MaintenanceInterval);
        conn._underlyingPoolIdleSignal = poolContext?.CreateConnectionIdleSignal(conn);
        protocol.Start(clientOptions, transport, conn._underlyingPoolIdleSignal is null ? NoopOnIdle : conn.SignalIdleIfStarted, timeout);
        conn.WireHeartbeat(clientOptions, poolContext);
        return conn;
    }

    public static async ValueTask<PgConnection> CreateAsync(PgClientProtocolOptions protocolOptions, PgClientOptions clientOptions, TransportConnection transport, CommandTracker? tracker = null, ConnectionPoolContext<PgConnection>? poolContext = null, CancellationToken cancellationToken = default)
    {
        var protocol = PgClientProtocol.Create(protocolOptions);
        var conn = new PgConnection(protocol, tracker, clientOptions.MaintenanceInterval);
        conn._underlyingPoolIdleSignal = poolContext?.CreateConnectionIdleSignal(conn);
        await protocol.StartAsync(clientOptions, transport, conn._underlyingPoolIdleSignal is null ? NoopOnIdle : conn.SignalIdleIfStarted, cancellationToken).ConfigureAwait(false);
        conn.WireHeartbeat(clientOptions, poolContext);
        return conn;
    }

    void SignalIdleIfStarted()
    {
        if (Volatile.Read(ref _isStarted) != 0)
            _underlyingPoolIdleSignal!();
    }

    // Lifecycle gate: called once, after Open + lease-commit, to put the connection in service.
    // From this point on, depth-to-zero transitions publish to the pool's idle channel.
    // Before this, they're suppressed.
    public void Start() => Volatile.Write(ref _isStarted, 1);

    // Passed to protocol.Start when not pooled, the protocol uses "onIdle is non-null" as the
    // "external orchestrator present, stay passive about heartbeat" signal. There's nothing for
    // us to actually do on idle in the non-pool case.
    static readonly Action NoopOnIdle = static () => { };

    void WireHeartbeat(PgClientOptions options, ConnectionPoolContext<PgConnection>? poolContext)
    {
        if (poolContext is not null)
        {
            poolContext.OnHeartbeat(static (conn, interval) => conn.OnHeartbeat(interval), this);
        }
        else
        {
            _selfHeartbeat = new Heartbeat(options.HeartbeatInterval);
            _selfHeartbeat.Register(OnHeartbeat);
        }
    }

    // Heartbeat tick: opportunity to start a maintenance flow if work has accumulated, then drive
    // the protocol's own heartbeat (flow activation timeouts etc.). Time-based subsampling. When
    // _maintenanceInterval is positive, ticks accumulate and only fire maintenance once the
    // accumulator reaches the interval. Default (TimeSpan.Zero) fires every tick.
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
    public bool IsCompleted => _protocol.IsCompleted;
    public Exception? CompletionException => _protocol.CompletionException;
    public int CompareTo(PgConnection? other) => _protocol.CompareTo(other?._protocol);

    public ValueTask CompleteAsync(Exception? exception = null)
    {
        // Tracker deregister happens here (true session end), not on per-lease proxy Dispose.
        // PgConnection's lifetime spans many leases under pooling.
        _tracker?.Deregister(this);
        _selfHeartbeat?.Dispose();
        return new ValueTask(_protocol.CompleteAsync(exception));
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

    // Called on eviction (DEALLOCATE fanout) or to roll back an optimistic Preparing marker
    // when the Parse flow failed.
    public void RemoveTracked(TrackedCommand tracked)
        => _tracked.TryRemove(tracked, out _);

    // Session-wide reset (DISCARD ALL, role change, reconnect) destroyed every prepared
    // statement on this session. Drop all tracked entries so subsequent uses re-Parse.
    public void ClearTracked()
        => _tracked.Clear();

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

    public bool TryQueue(PgClientFlow flow) => _protocol.TryQueue(flow);

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
