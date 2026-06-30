namespace Slon.Pg.Protocol.Flows;

// A wire-takeover flow: the user-driven sibling of StartupFlow. `supportsPipelining: false` pins the
// outer pipeline at this flow for its whole lifetime (the same exclusivity lever StartupFlow uses),
// and the body waits on activation before owning the wire. Instead of running a scripted handshake,
// it hands the connection to the user as an exclusive scope: the user submits their commands as
// subflows on a NESTED pipeline - the same PgClientFlowSource + Policy + Control machine one level
// down, so the sync handoff and pipelining compose recursively - and the body returns only when the
// user ends the scope, releasing the connection.
//
// The flow never touches the wire itself: the inner pipeline's subflows do, rebinding the shared
// decoder through the inner Control. Nesting is fractal - an exclusive scope inside an exclusive
// scope is just another ExclusiveAccessFlow on the inner pipeline. StartupFlow is the protocol's own
// first exclusive scope, scripted rather than handed off.
//
// REUSABLE: the inner pipeline is a flyweight (reused via Initialize), and this flow is pooled with
// it - on the common ADO path an open connection IS an exclusive scope, so a fresh flow per scope
// would add an alloc to every execute (ExclusiveAccessFlow + the command's CommandFlow). The protocol
// re-Initializes the pipeline with a fresh source and calls PrepareScope each scope; OnReset refreshes
// the gates.
sealed class ExclusiveAccessFlow : PgClientFlow
{
    // Stable across scopes (close over the cached flyweight). Reason carries the protocol's close into the
    // inner CompleteAsync so inner items complete WITH the reason on abort. The state is the shared scope
    // flyweight this flow acquires at its turn (AcquireForTurn builds/re-inits the inner pipeline and
    // returns the fresh per-scope source).
    readonly PgClientProtocol _protocol;
    readonly PgClientProtocol.Control _innerControl;
    readonly PgClientProtocol.ExclusiveScopeState _state;
    readonly Func<Exception?, ValueTask> _completeInner;
    // Per-scope. _innerSource is acquired at the turn (null pre-turn); _activationTimeout set by PrepareScope.
    // _acquired flips true once AcquireForTurn has built/started the inner pipeline; pre-turn it is false,
    // so the cascade hooks know there is no inner executor to drain (nothing was ever started).
    PgClientFlowSource? _innerSource;
    bool _acquired;
    TimeSpan _activationTimeout;
    TaskCompletionSource _handoffReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
    TaskCompletionSource _scopeEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Consumer-gone signal: set when the caller cancels its BeginScopeAsync wait (consumer-DETACH, NOT
    // flow removal - the flow is issued, it still takes its turn). Pre-turn it makes the flow skip the
    // acquire and retire fast (done-before-executed); mid-hold it ends the scope so the body returns
    // even though the user will never call CompleteScopeAsync. NOT an activation-source fault, so the
    // in-order wire handoff is preserved.
    TaskCompletionSource _consumerGone = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Idempotence guards for the cascade hooks (fire once per tenure). Reset in OnReset.
    bool _innerStopping;
    bool _innerAborting;
    // Sync-flow handoff rendezvous. A sync scope flow (async: false) is enqueued via the source's
    // EnqueueSyncWaiter + WaitForExecutor, which parks the caller's thread on THIS mres until the executor
    // hands the scope flow over - so the caller drives activation + its subflows end-to-end on its own
    // thread (the single-thread locality the sync path exists for). Allocated lazily on sync selection
    // (PrepareScope) - async scopes never park here, so they never pay for it. Once allocated it is reused
    // across tenures of the cached flyweight (Reset in OnReset).
    ManualResetEventSlim? _handoffMres;

    internal ExclusiveAccessFlow(PgClientProtocol protocol, PgClientProtocol.Control innerControl, PgClientProtocol.ExclusiveScopeState state, Func<Exception?, ValueTask> completeInner)
        : base(supportsPipelining: false)
    {
        _protocol = protocol;
        _innerControl = innerControl;
        _state = state;
        _completeInner = completeInner;
    }

    // TODO: should be `true` to emulate ConnectionTimeout - the caller's patience for ACQUIRING
    // exclusive access, enforced by the CONSOLIDATED heartbeat timer (a per-open CancellationTokenSource
    // would reintroduce the per-op BCL-timer churn the heartbeat consolidation exists to remove). Left
    // off for now because this flow is POOLED, and EnableActivationTimeout=true trips the Reset guard /
    // the heartbeat's generation-agnostic wrong-tenure completer. Flip it once the activation-timeout x
    // pooling fix lands (global monotonic placement stamp; see slon-heartbeat-enum-tenure-race). Until
    // then an acquire never times out - it waits for the wire indefinitely.
    // protected override bool EnableActivationTimeout => true;

    // Re-point at a fresh scope (begin-time, cheap). The inner source + inner pipeline are NOT set here -
    // the flow acquires them at its TURN (AcquireForTurn), so a never-consumed scope starts nothing.
    internal void PrepareScope(bool async, TimeSpan activationTimeout)
    {
        _innerSource = null;
        _acquired = false;
        _activationTimeout = activationTimeout;
        IsAsync = async;
        // Release the cached-flow claim at terminal (success OR fault) via the completion action, which runs
        // AFTER Complete sets IsCompleted - the reclaiming begin keys its Reset on IsCompleted, so releasing
        // any earlier would let it grab a not-yet-completed flow. Set per-tenure (Reset clears the action),
        // never once: a pooled flow must not carry a stale action. Exception ignored - release is lifecycle.
        // A no-op for overflow flows (ReleaseFlow ref-checks the cached instance).
        SetCompletionAction(static (f, _, s) => ((PgClientProtocol.ExclusiveScopeState)s!).ReleaseFlow((ExclusiveAccessFlow)f), _state);
        // Sync scope flow uses the handoff rendezvous (WaitForExecutor parks on GetHandoffMres). Allocate
        // it on first sync selection; an async scope leaves it null and never parks.
        if (!async)
            _handoffMres ??= new(false);
    }

    // Non-null only once a sync scope has selected it - lets a sync scope flow be handed off to its
    // caller's thread via the source's WaitForExecutor. Async scopes return null (they never park).
    protected override ManualResetEventSlim? GetHandoffMres() => _handoffMres;

    protected override void OnReset()
    {
        _handoffReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _scopeEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _consumerGone = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _innerStopping = false;
        _innerAborting = false;
        _handoffMres?.Reset();
    }

    /// Resolves once this flow is activated and owns the wire - the caller awaits it to gain exclusive
    /// access before submitting any subflow. Faults if the flow is torn down before activation.
    public Task HandoffReady => _handoffReady.Task;

    /// Acquire exclusive access, cancelably. Awaits the turn (HandoffReady); if the caller cancels before
    /// the turn lands, DETACHES the consumer (the flow still takes its turn and retires fast - it is
    /// already issued, can't be pulled from the outer pipeline) and throws OperationCanceledException.
    /// Cancellation after the handoff is too late and resolves normally.
    public async Task BeginScopeAsync(CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
        {
            await _handoffReady.Task.ConfigureAwait(false);
            return;
        }
        var cancelTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var reg = cancellationToken.UnsafeRegister(static (s, ct) => ((TaskCompletionSource)s!).TrySetCanceled(ct), cancelTcs);
        var winner = await Task.WhenAny(_handoffReady.Task, cancelTcs.Task).ConfigureAwait(false);
        if (winner == _handoffReady.Task)
        {
            await _handoffReady.Task.ConfigureAwait(false); // observe a pre-activation fault, if any
            return;
        }
        // Caller gave up before the turn: detach the consumer and surface the cancel. The flow proceeds
        // to its turn and retires fast (done-before-executed).
        _consumerGone.TrySetResult();
        cancellationToken.ThrowIfCancellationRequested();
    }

    /// Queue a user subflow into the inner pipeline (FIFO - the inner pipeline is the same source +
    /// single-pump executor as the protocol's outer Queue, so submission order is execution order).
    /// A sync subflow takes the recursive handoff so the caller's thread drives the inner executor for
    /// it - without this the nested executor would block a thread-pool thread = sync-over-async one
    /// level down.
    public T Queue<T>(T subflow, CancellationToken cancellationToken = default) where T : PgClientFlow
    {
        // The scope source is acquired at the turn; Queue is only valid after HandoffReady has resolved
        // (the caller awaits it before submitting), so the source is non-null here.
        var innerSource = _innerSource ?? throw new InvalidOperationException("Cannot submit a subflow before the scope is acquired (await HandoffReady first).");
        if (cancellationToken.CanBeCanceled)
            subflow.BindCallerToken(cancellationToken);
        subflow.GetExecutionControl(_innerControl).Bind(_activationTimeout);
        // Same routing gate as the protocol's outer Queue: hand off only when a caller is parked
        // (NeedsSyncHandoff). An async subflow, or an autonomous sync subflow (null handoff MRES), is
        // dispatched so the inner executor drives it instead of holding it for an absent caller.
        if (!subflow.NeedsSyncHandoff)
            innerSource.Enqueue(subflow).Execute(runContinuationsAsynchronously: true);
        else
            innerSource.EnqueueSyncWithHandoff(subflow);
        return subflow;
    }

    /// End the exclusive scope: drain the inner pipeline, let the body return, and await this flow's
    /// retirement so the wire is provably released (and the outer pipeline advanced) on return.
    public async ValueTask CompleteScopeAsync()
    {
        // Caller-initiated end: the caller has already drained its subflows, so a graceful inner drain
        // (null reason) reaches rest immediately and leaves the scope signal untripped - the flyweight
        // is reusable for the next scope.
        await _completeInner(null).ConfigureAwait(false);
        _scopeEnded.TrySetResult();
        // WaitForComplete resolves only after the framework has run OnComplete (Complete orders teardown
        // before the done-signal), so on return the prior tenure is provably fully torn down and the
        // flyweight is safe for the next BeginExclusiveScope to re-Initialize.
        await WaitForComplete().ConfigureAwait(false);
    }

    protected override async ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        // Wait for activation - we now own the wire exclusively. We never read/write the wire
        // ourselves; the inner pipeline's subflows do, rebinding the shared decoder via the inner
        // Control, so the decoder we receive here is discarded. A successful return is the won-turn
        // proof (the activation source is the single arbiter), so the acquire below is race-free.
        _ = await context.GetDecoderAsync().ConfigureAwait(false);
        // Done-before-executed: the consumer already detached (canceled BeginScopeAsync before the turn).
        // We took our turn - can't be pulled from the outer pipeline - but there is no consumer to hand to,
        // so skip the acquire entirely (no source, no inner executor) and retire immediately, handing the
        // wire to the next waiter.
        if (_consumerGone.Task.IsCompleted)
            return ValueTask.CompletedTask;
        // Acquire the scope at the turn: create the fresh source + start the inner executor. Deferred to
        // here so the inner pipeline only ever runs for a flow that actually won its turn.
        _innerSource = _state.AcquireForTurn(_protocol);
        Volatile.Write(ref _acquired, true);
        // Hand the connection to the user; they submit subflows and end the scope.
        _handoffReady.SetResult();
        // Hold the wire until the scope ends (CompleteScopeAsync) OR the consumer detaches mid-hold (the
        // user canceled after handoff and will never call CompleteScopeAsync). Either way the held scope
        // must end so the body returns; a consumer-gone mid-hold drains the inner pipeline like a graceful
        // end so the flyweight stays reusable.
        var ended = await Task.WhenAny(_scopeEnded.Task, _consumerGone.Task).ConfigureAwait(false);
        if (ended == _consumerGone.Task && Volatile.Read(ref _acquired))
            await _completeInner(null).ConfigureAwait(false);
        return ValueTask.CompletedTask;
    }

    // Cascade hooks: the wire-death verdict reaching this flow. For a DISPATCHED scope flow they run from
    // the OUTER heartbeat (this flow lives on the outer pipeline, so it IS enumerated); for a BACKLOG flow
    // the shutdown drain delivers them via Control.DeliverClose (the heartbeat never reaches the backlog).
    // The scope signal is already tripped via its link to the protocol's _close, so inner flows already see
    // the close on their tokens. What a token alone does NOT do, done here: (1) stop the inner EXECUTOR
    // (drive the inner pipeline's CompleteAsync), (2) release the body's _scopeEnded gate so ExecuteAuto
    // returns (the protocol is tearing the scope down, the user will not call CompleteScopeAsync), and (3)
    // for a never-activated flow, fault the parked BeginScopeAsync caller (the secondary gate completion
    // does not touch). Must be cheap + non-blocking (heartbeat contract): fire-and-forget, faults observed.
    // Idempotent across ticks.

    // Graceful: drain the inner pipeline to a clean RFQ, THEN release the body. Releasing only after the
    // drain settles lets in-flight inner subflows finish cleanly first. Never-activated (not acquired):
    // there is no inner executor to drain - fault the parked caller and release the body's gate. The scope
    // signal is already tripped via its link, so an acquire racing this sees the close on its tokens.
    protected override void OnStopping(PgClientClosedException exception)
    {
        if (_innerStopping)
            return;
        _innerStopping = true;
        if (Volatile.Read(ref _acquired))
        {
            DrainThenEndScope(_completeInner(null));
        }
        else
        {
            _handoffReady.TrySetException(exception);
            _scopeEnded.TrySetResult();
        }
    }

    // Forceful: complete the inner items WITH the reason, and release the body's gate immediately (the
    // wire is dead - no clean drain to wait for). Never-activated: fault the parked caller instead.
    protected override void OnAbort(PgClientClosedException exception)
    {
        if (_innerAborting)
            return;
        _innerAborting = true;
        if (Volatile.Read(ref _acquired))
            FireAndForget(_completeInner(exception));
        else
            _handoffReady.TrySetException(exception);
        _scopeEnded.TrySetResult();
    }

    // Drain the inner pipeline, then end the scope so the body returns. async void so the fire-and-forget
    // faults are observed here rather than lost; never throws into the heartbeat thread.
    async void DrainThenEndScope(ValueTask drain)
    {
        try { await drain.ConfigureAwait(false); }
        catch { /* TODO route to an unobserved-exception hook once one exists */ }
        finally { _scopeEnded.TrySetResult(); }
    }

    // async void so the fire-and-forget's faults are observed here rather than lost; never throws into
    // the heartbeat thread. The inner CompleteAsync is single-winner + idempotent, so racing a user
    // CompleteScopeAsync is inert.
    static async void FireAndForget(ValueTask task)
    {
        try { await task.ConfigureAwait(false); }
        catch { /* TODO route to an unobserved-exception hook once one exists */ }
    }
}
