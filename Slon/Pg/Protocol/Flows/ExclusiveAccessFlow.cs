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
    // Stable across scopes (close over the cached flyweight pipeline). Reason carries the protocol's
    // close into the inner CompleteAsync so inner items complete WITH the reason on abort.
    readonly PgClientProtocol.Control _innerControl;
    readonly Func<Exception?, ValueTask> _completeInner;
    // Per-scope, (re)set by PrepareScope.
    PgClientFlowSource _innerSource;
    TimeSpan _activationTimeout;
    TaskCompletionSource _handoffReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
    TaskCompletionSource _scopeEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Idempotence guards for the cascade hooks (fire once per tenure). Reset in OnReset.
    bool _innerStopping;
    bool _innerAborting;

    internal ExclusiveAccessFlow(PgClientProtocol.Control innerControl, Func<Exception?, ValueTask> completeInner)
        : base(supportsPipelining: false)
    {
        _innerControl = innerControl;
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

    // Re-point at a fresh scope. Called by the protocol after re-Initializing the inner pipeline with
    // innerSource, and after Reset (on reuse) has refreshed the framework + gate state.
    internal void PrepareScope(bool async, PgClientFlowSource innerSource, TimeSpan activationTimeout)
    {
        _innerSource = innerSource;
        _activationTimeout = activationTimeout;
        IsAsync = async;
    }

    protected override void OnReset()
    {
        _handoffReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _scopeEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _innerStopping = false;
        _innerAborting = false;
    }

    /// Resolves once this flow is activated and owns the wire - the caller awaits it to gain exclusive
    /// access before submitting any subflow. Faults if the flow is torn down before activation.
    public Task HandoffReady => _handoffReady.Task;

    /// Queue a user subflow into the inner pipeline (FIFO - the inner pipeline is the same source +
    /// single-pump executor as the protocol's outer Queue, so submission order is execution order).
    /// A sync subflow takes the recursive handoff so the caller's thread drives the inner executor for
    /// it - without this the nested executor would block a thread-pool thread = sync-over-async one
    /// level down.
    public T Queue<T>(T subflow, CancellationToken cancellationToken = default) where T : PgClientFlow
    {
        if (cancellationToken.CanBeCanceled)
            subflow.BindCallerToken(cancellationToken);
        subflow.GetExecutionControl(_innerControl).Bind(_activationTimeout);
        if (subflow.IsAsyncForEnqueue)
            _innerSource.Enqueue(subflow).Execute(runContinuationsAsynchronously: true);
        else
            _innerSource.EnqueueSyncWithHandoff(subflow);
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
        // Control, so the decoder we receive here is discarded.
        _ = await context.GetDecoderAsync().ConfigureAwait(false);
        // Hand the connection to the user; they submit subflows and end the scope.
        _handoffReady.SetResult();
        // Hold the wire until the scope ends (inner pipeline drained by CompleteScopeAsync).
        await _scopeEnded.Task.ConfigureAwait(false);
        return ValueTask.CompletedTask;
    }

    // A pre-activation fault must reach the caller's HandoffReady await so it never gets a stranded
    // scope, and we never take the wire. A post-activation completion just releases the body's gate
    // (HandoffReady was already resolved). Backstop for the cascade hooks below.
    protected override void OnComplete(Exception? exception)
    {
        if (exception is not null)
            _handoffReady.TrySetException(exception);
        _scopeEnded.TrySetResult();
    }

    // Cascade hooks (run from the OUTER heartbeat - this flow lives on the outer pipeline, so it IS
    // enumerated and DOES get OnStopping/OnAbort; this is the lever into the scope). The scope signal is
    // already tripped via its link to the protocol's _close, so inner flows already see the close on
    // their tokens. Two things a token alone does NOT do, both done here: (1) stop the inner EXECUTOR
    // (drive the inner pipeline's CompleteAsync), and (2) release the body's _scopeEnded gate so
    // ExecuteAuto returns - the protocol is tearing the scope down, the user will not call
    // CompleteScopeAsync. Must be cheap + non-blocking (heartbeat contract): fire-and-forget, faults
    // observed (never thrown into the heartbeat). Idempotent across ticks.

    // Graceful: drain the inner pipeline to a clean RFQ, THEN release the body. Releasing only after the
    // drain settles lets in-flight inner subflows finish cleanly first.
    protected override void OnStopping(PgClientClosedException exception)
    {
        if (_innerStopping)
            return;
        _innerStopping = true;
        DrainThenEndScope(_completeInner(null));
    }

    // Forceful: complete the inner items WITH the reason, and release the body's gate immediately (the
    // wire is dead - no clean drain to wait for).
    protected override void OnAbort(PgClientClosedException exception)
    {
        if (_innerAborting)
            return;
        _innerAborting = true;
        FireAndForget(_completeInner(exception));
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
