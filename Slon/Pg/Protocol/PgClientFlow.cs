using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Slon.Pg.Protocol;

abstract class PgClientFlowObserver
{
    internal virtual void OnCompleting(PgClientFlow flow, Exception? exception, object? state) { }
    internal virtual void OnCompleted(PgClientFlow flow, Exception? exception, object? state) { }
}

// Per-wire composition supplied by the owner above the raw protocol. The protocol treats this as
// an opaque bind token: portable flows interpret the concrete context only when dispatch removes
// them from the migratable source backlog, while standalone low-level flows remain initialized.
abstract class PgClientFlowBindingContext;

abstract class PgClientFlow : IValueTaskSource<PgDecoder>, IValueTaskSource<PgClientFlow>, IThreadPoolWorkItem
{
    protected internal enum BackendCancellationTiming : byte
    {
        AfterGrace,
        AtReadFrontier,
        Immediate
    }

    PgClientProtocol.Control? _pendingActivationControl;
    PgClientFlowSource.State? _placementSource;
    int _placementWasDetached;
    FlowEnqueueOptions _enqueueOptions;
    bool _ownsWireCapacity;

    internal bool OwnsAdmissionBarrier => (_enqueueOptions & FlowEnqueueOptions.BlockAdmission) != 0;
    internal bool AllowsMigration => (_enqueueOptions & FlowEnqueueOptions.AllowMigration) != 0;
    internal void SetEnqueueOptions(FlowEnqueueOptions options)
    {
        Debug.Assert(_enqueueOptions is FlowEnqueueOptions.None);
        _enqueueOptions = options;
    }

    internal void MarkWireCapacityOwned() => _ownsWireCapacity = true;

    void AttachPlacementSource(PgClientFlowSource.State source)
    {
        if (!AllowsMigration)
        {
            Debug.Assert(_placementSource is null);
            _placementSource = source;
            return;
        }

        if (Interlocked.CompareExchange(ref _placementSource, source, null) is not null)
            ThrowHelper.ThrowInvalidOperation("The flow is already assigned to a protocol source.");
        // Initial placement leaves the edge clean. Replacement placement wakes a synchronous
        // consumer which may be waiting between the retired source and this one.
        if (Interlocked.Exchange(ref _placementWasDetached, 0) != 0)
            HandoffEvent?.Set();
    }

    void DetachPlacementSource(PgClientFlowSource.State source)
    {
        Volatile.Write(ref _placementWasDetached, 1);
        if (!ReferenceEquals(Interlocked.CompareExchange(ref _placementSource, null, source), source))
            ThrowHelper.ThrowInvalidOperation("The flow is not assigned to the expected protocol source.");
    }

    void CompletePlacement()
    {
        // Placement terminality is published by Release before this edge. A sync consumer may have
        // consumed an earlier source-stopping wake, so terminal placement owns the final notification.
        if (Interlocked.Exchange(ref _placementSource, null) is not null)
            HandoffEvent?.Set();
    }

    internal void UpdatePendingTimeout(TimeSpan remaining)
        => _remainingActivationTimeout = remaining;

    internal virtual void Bind(PgClientFlowBindingContext? context) { }

    /// Pairs this flow with its protocol control for a queued activation dispatch. The flow
    /// itself is the IThreadPoolWorkItem: an immutable (flow, control) pairing per queued
    /// activation, zero-alloc, immune to the shared-work-item lost-update where a second
    /// Initialize overwrote the first's item before its Execute ran (one pending activation
    /// per flow tenure makes the field safe).
    internal void PrepareActivationDispatch(PgClientProtocol.Control control)
        => _pendingActivationControl = control;

    void IThreadPoolWorkItem.Execute()
    {
        var control = _pendingActivationControl;
        Debug.Assert(control is not null);
        _pendingActivationControl = null;
        // The decoder bind already ran synchronously at activation; this dispatch is only the body
        // wake. Skip it for a flow the abort retired before the dispatch ran: its activation source is
        // already faulted so the wake would no-op, and skipping keeps us off a dead tenure.
        if (Volatile.Read(ref _completed))
            return;
        control!.Activate(this);
    }

    readonly bool _supportsDeferredFlush;
    internal bool SupportsDeferredFlush => _supportsDeferredFlush;
    Action<TimeSpan>? _decoderOnHeartbeatAction; // TODO should we have this here?
    int _rfqCount;
    int _cancellationWindow;
    internal int CancellationWindow => Volatile.Read(ref _cancellationWindow);
    bool _lastMessageInducesRfq;
    // We store the IsAsync value at bind time so the protocol can keep track of pipeline stalls correctly.
    bool _isAsyncAtDispatch;
    // Tri-state int (0 = unset, 1 = true, 2 = false) instead of bool? so reads / writes can be
    // ordered via Volatile.Read / Volatile.Write. The flow body and the consumer (via MoveNext's
    // sync<->async flip) can race on this; without ordering the post-wake-protocol body can read
    // a stale value and take the wrong I/O path.
    const int ModeUnset = 0;
    const int ModeAsync = 1;
    const int ModeSync = 2;
    int _isAsyncState;

    // Flow lifecycle state. Reads happen on the consumer thread after the executor has settled,
    // so plain flags are sufficient.
    bool _started;
    bool _completed;
    // Completion signal over the forked core: completers are cross-strand (retirement, teardown),
    // waiters resume via the continuation dispatcher on the ambient scheduler. The flow itself is
    // the IValueTaskSource, typed <PgClientFlow> because every other identity slot is claimed by
    // the flow types; the flow-as-result is the free disambiguator (the old Slon.Protocols
    // pattern). At most one pending waiter per tenure; post-completion awaits resolve
    // synchronously.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<PgClientFlow> _completionCore;
    ManualResetEventSlim? _completionEvent;
    // 1 while a WaitForComplete token is live (set at capture, cleared after GetResult consumed the
    // core). Guards reuse: Reset bumps the core's version, so it must not run while this is set.
    int _completionWaiterPending;

    // Activation state.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<PgDecoder> _activationTaskSource;
    CancellationTokenRegistration _activationCancellationTokenRegistration;
    TimeSpan _remainingActivationTimeout;
    bool _pendingTimeoutStarted;

    PgClientFlowObserver? _observer;
    object? _observerState;

    /// The flow's body. "Auto" is the protocol-package convention for "adapts to the bound mode
    /// (sync or async)": the body dispatches between sync and async I/O per read based on IsAsync,
    /// calling explicit sync/async helper pairs (ReadUntilExecute / ReadUntilExecuteAsync) at each
    /// site. Prefer async/await in both modes - sync mode affects scheduling, not syntax. Don't mix
    /// sync and async I/O calls within one body.
    protected abstract ValueTask<FlowTasks> ExecuteAuto(Context context);

    protected bool IsAsync
    {
        // Volatile.Read: the consumer thread can flip _isAsyncState (sync->async) concurrently with
        // the body reading it. Without the fence a post-wake check could see a stale value and take
        // the sync I/O path on a now-async flow, blocking on I/O that never completes sync.
        get => Volatile.Read(ref _isAsyncState) == ModeAsync;
        set => Volatile.Write(ref _isAsyncState, value ? ModeAsync : ModeSync);
    }


    // The dispatch-mode snapshot, stable across the flow's tenure (unlike IsAsync, which a body may
    // mutate). Autonomous sync flows use sync I/O while still dispatching on the executor. The policy
    // uses it to decide inline vs TP activation, and the executor's
    // HeadIsSyncHandoff peek to fake-miss sync flows for caller takeover.
    internal bool IsAsyncAtDispatch => _isAsyncAtDispatch;

    // Pre-start read of IsAsync for the enqueue path: the protocol routes sync flows through an
    // inline wake-signal dispatch so the producer's thread takes over the executor for that flow.
    // Asserts the flow set its mode before queueing (the same precondition Start enforces).
    internal bool IsAsyncForEnqueue
    {
        get
        {
            var state = Volatile.Read(ref _isAsyncState);
            if (state == ModeUnset)
            {
                ThrowHelper.ThrowInvalidOperation("IsAsync was not set by flow before it was queued.");
                return default;
            }
            return state == ModeAsync;
        }
    }

    // A sync flow with a handoff event is held for its eventual consumer. Autonomous sync flows have no
    // event and run through normal executor dispatch instead.
    internal bool NeedsSyncHandoff => !IsAsyncForEnqueue && HandoffEvent is not null;

    // Consumer-driven flows establish their FIFO position at admission and take the handoff on their
    // first synchronous consumer operation. Other sync flows take it as part of admission.
    internal virtual bool DefersSyncHandoff => false;

    // Sync admission establishes FIFO position only. The first synchronous consumer operation
    // announces that its caller is ready to take the source pump and waits for this flow's turn.
    internal void WaitForSyncHandoff()
    {
        var waitEvent = HandoffEvent
            ?? throw new InvalidOperationException("A synchronous caller handoff requires a wait event.");
        while (!IsCompleted)
        {
            var source = Volatile.Read(ref _placementSource);
            if (source is null)
            {
                waitEvent.Reset();
                if (Volatile.Read(ref _placementSource) is null && !IsCompleted)
                    waitEvent.Wait();
                // Attachment is a level change, not the source handoff edge. Clear its wake before
                // asking the newly observed source to publish/claim the actual handoff.
                waitEvent.Reset();
                continue;
            }

            if (source.WaitForExecutor(this))
                return;

            // Source completion is not a handoff. Its inert drain will either fault this placement
            // or detach it for migration; wait for that level change before consulting a source again.
            waitEvent.Reset();
            if (ReferenceEquals(source, Volatile.Read(ref _placementSource)) && !IsCompleted)
                waitEvent.Wait();
        }
    }

    /// <param name="supportsDeferredFlush">
    /// Permits predecessor writes to remain buffered while this flow executes its first phase. Set
    /// only when that phase will not wait for decoder input. Decoder work must live in the returned
    /// pipeline task. The conservative default flushes before execution.
    /// </param>
    protected PgClientFlow(bool supportsDeferredFlush = false)
    {
        _supportsDeferredFlush = supportsDeferredFlush;
        _activationTaskSource.CanCompleteConcurrently = true;
        _completionCore.CanCompleteConcurrently = true;
    }

    protected void SetObserver(PgClientFlowObserver observer, object? state)
    {
        _observer = observer;
        _observerState = state;
    }

    protected PgClientFlowObserver? GetObserver(out object? state)
    {
        state = _observerState;
        return _observer;
    }

    // Bind the caller's cancellation token at submit so the (eager) write, and the reads by default,
    // honor it. No-op for flows without a caller; the queue binds only a cancelable token, so the
    // common no-token submit pays no field write.
    internal virtual void BindCallerToken(CancellationToken cancellationToken) { }
    internal virtual CancellationToken MigrationCancellationToken => CancellationToken.None;

    public bool IsCompleted => _completed;
    internal bool IsStarted => _started && !_completed;
    internal bool IsPending => !_started;

    internal void DiscardUnqueued()
    {
        Debug.Assert(!_started && !_completed);
        _completed = true;
        OnDiscarded();
    }

    // Internal completion sync for the dispose drain, Postgres startup wait, and benchmarks.
    // Scheduler-aware: the signal completes through the forked value-task-source core, so the
    // waiter resumes on the ambient scheduler instead of the TCS's unconditional thread-pool
    // dispatch. At most one PENDING waiter per tenure (post-completion awaits resolve
    // synchronously). The token is checked on entry only: the park itself is not cancelable, the
    // signal fires on every exit path (terminal, fault delivery, teardown), including the
    // cancel-delivered terminal.
    internal ValueTask<PgClientFlow> WaitForComplete(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Publish waiter-pending before the token capture: a reuse path that observes the flag
        // clear (acquire) is guaranteed the consumption below already happened, so a Reset cannot
        // invalidate a live token. Ordering contract on reuse-tracked flows: register BEFORE the
        // enqueue (always safe - the tenure doesn't exist yet, covers autonomous flows), or before
        // unleashing retirement for a consumer that controls it (the exclusive close path). Any
        // later registration races Reset and can capture the wrong tenure's version - undefined,
        // and asserted against in Reset. (A late-await API would need a generation-checked capture:
        // a single packed gen|pending word, CAS on both sides - deferred until pooling needs it.)
        Volatile.Write(ref _completionWaiterPending, 1);
        return new(this, _completionCore.Version);
    }

    // Synchronous consumers must not block on an async continuation whose dispatch requires another
    // scheduler turn. The event is allocated only for that uncommon path and is signaled after the
    // completion core has published its result.
    internal PgClientFlow WaitForCompleteSynchronously(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Volatile.Write(ref _completionWaiterPending, 1);
        var token = _completionCore.Version;
        var completionEvent = _completionEvent;
        if (completionEvent is null)
        {
            var created = new ManualResetEventSlim();
            completionEvent = Interlocked.CompareExchange(ref _completionEvent, created, null) ?? created;
            if (!ReferenceEquals(completionEvent, created))
                created.Dispose();
        }

        if (_completionCore.GetStatus(token) is ValueTaskSourceStatus.Pending)
            completionEvent.Wait();
        return ((IValueTaskSource<PgClientFlow>)this).GetResult(token);
    }

    /// True while a completion waiter holds an unconsumed token on this tenure's signal. Reuse
    /// paths (the exclusive flyweight's rent) must not Reset the flow while set: the waiter's
    /// continuation may still be in dispatch and its GetResult would land on a bumped version.
    internal bool CompletionWaiterPending => Volatile.Read(ref _completionWaiterPending) != 0;

    // Public for pooling. Reset is called by consumers between uses.
    public void Reset()
    {
        Debug.Assert(IsPending || IsCompleted, "Cannot reset a flow that is mid-execution.");
        Debug.Assert(!CompletionWaiterPending,
            "Reset while a completion waiter's token is live - the waiter's GetResult would land on a bumped version. " +
            "Reuse paths must gate on CompletionWaiterPending (consumption), not on completion.");
        // Enforcement (TODO until landed). Pooling (reset + reuse) a flow that arms the activation
        // timeout is unsafe: the heartbeat's activation-timeout TrySetException is generation-agnostic,
        // so a recycled instance can be wrong-tenure-completed by a stale timeout from the prior tenure.
        // The fix is a global monotonic placement stamp carried with the item and on the flow, validated
        // at the completer (tear-tolerant by uniqueness, no seqlock; failure reduces to a full int
        // rollover, a fail-loud TimeoutException at worst). Until that lands, refuse to recycle a
        // timeout-armed flow rather than let the race silently reappear.
        if (EnableActivationTimeout)
            ThrowHelper.ThrowInvalidOperation("Cannot pool a flow with EnableActivationTimeout: a recycled instance can be wrong-tenure-completed by a stale activation timeout. Implement generation-checked completion first.");
        _started = false;
        _completed = false;
        // Version bump per tenure. Cross-tenure completer staleness rests on the done -> torn-down
        // -> retired layering (Complete precedes recycle), the same basis as the rest of this reset.
        _completionCore.Reset();
        _completionEvent?.Reset();
        _activationTaskSource.Reset();
        _rfqCount = 0;
        _cancellationWindow = 0;
        _lastMessageInducesRfq = false;
        _placementSource = null;
        _pendingTimeoutStarted = false;
        _placementWasDetached = 0;
        _enqueueOptions = FlowEnqueueOptions.None;
        _ownsWireCapacity = false;
        _observer = null;
        _observerState = null;
        OnReset();
    }

    // Interactive flows (CommandFlow) override this to opt in to the activation timeout, which models
    // caller patience. Background flows have no caller, so by default they
    // wait indefinitely for activation rather than busy-looping queue/timeout/re-arm, and stay off
    // the heartbeat's generation-agnostic timeout completer.
    protected virtual bool EnableActivationTimeout => false;
    protected virtual TimeSpan? PendingTimeout => null;
    internal virtual TimeSpan? BackendCancellationGracePeriod => null;

    protected virtual void OnHeartbeat(TimeSpan interval) {}
    protected virtual void OnAbort(Exception exception) {}
    /// Graceful-shutdown observation point. Fires while StoppingToken is set but before the
    /// AbortToken escalation. Flow types whose body can park on a non-IO rendezvous (CommandFlow's
    /// GateTask) override this to wake it so the body short-circuits instead of waiting for
    /// AbortToken. Idempotent across heartbeat ticks (subclasses use TrySet).
    protected virtual void OnStopping(Exception exception) {}
    /// Releases tenure-owned resources before the terminal signal makes this instance reusable.
    /// No overridable hook may run after that signal: a completion waiter can immediately Reset and
    /// enqueue the same object for its next tenure.
    protected virtual void OnReleasing(Exception? exception) {}
    protected virtual void OnCancellationWindowCompleted(int completedWindow, int remainingWindowCount) {}
    protected virtual void OnDiscarded() {}
    protected virtual void OnReset() {}

    // The per-flow handoff rendezvous primitive for the (wait-list-free) sync source handoff: non-null only
    // for a flow that needs a caller takeover (a sync CommandFlow with a parked caller). The source signals
    // it when it dequeues-and-holds the flow for that caller (OnExecutorSuspended), and the caller parks on
    // it in WaitForExecutor. null = no handoff (async flows, or a flow with no waiting caller) - the source
    // runs it autonomously on the executor, nothing to rendezvous. null/non-null IS the waiter-presence gate.
    // The sync handoff MRES a caller parks on (null = autonomous, no waiter). protected, NOT internal:
    // it is reachable only by the flow's own subclasses (which override it) and by ExecutionControl (the
    // nested write-side handle) - the source pulls it via ExecutionControl.HandoffEvent, never off a
    // bare flow ref. Keeps the handoff primitive off PgClientFlow's internal API, like _rfqCount.
    protected virtual ManualResetEventSlim? HandoffEvent => null;

    PgClientFlow IValueTaskSource<PgClientFlow>.GetResult(short token)
    {
        // Consume-then-clear: the release store orders the core consumption before the flag clear,
        // so a reuse path's acquire read of "not pending" proves the token's lifetime ended. Fault
        // delivery (GetResult throwing the completion exception) is consumption too.
        try
        {
            return _completionCore.GetResult(token);
        }
        finally
        {
            Volatile.Write(ref _completionWaiterPending, 0);
        }
    }
    ValueTaskSourceStatus IValueTaskSource<PgClientFlow>.GetStatus(short token) => _completionCore.GetStatus(token);
    void IValueTaskSource<PgClientFlow>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _completionCore.OnCompleted(continuation, state, token, flags);

    PgDecoder IValueTaskSource<PgDecoder>.GetResult(short token) => _activationTaskSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<PgDecoder>.GetStatus(short token) => _activationTaskSource.GetStatus(token);
    void IValueTaskSource<PgDecoder>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _activationTaskSource.OnCompleted(continuation, state, token, flags);


    protected readonly struct Context
    {
        readonly ExecutionControl _executionControl;
        internal Context(ExecutionControl executionControl)
            => _executionControl = executionControl;

        /// Graceful drain signal. Poll at handoff/coordination boundaries (per-CommandResult for
        /// CommandFlow) to switch to drain mode. I/O keeps running so the wire reaches a clean
        /// state. Do NOT thread this into I/O methods - the analyzer will suggest it but that
        /// converts graceful semantics into forceful cancellation on the next I/O op.
        public CancellationToken StoppingToken => _executionControl.StoppingToken;

        /// True when this protocol has entered <c>Shutdown</c>. Use as the <c>when</c> filter on
        /// a <c>PgClientClosedException</c> catch so a closed exception bubbling up from a
        /// nested protocol isn't mistaken for ours; the check naturally scopes to the current
        /// nesting layer.
        public bool IsProtocolClosed => _executionControl.IsProtocolClosed;

        /// The canonical PgClientClosedException for this protocol once Shutdown has entered.
        /// Materialized before the StoppingToken / AbortToken cancellations, so observers waking on
        /// those tokens always see a non-null value.
        public PgClientClosedException? ClosedException => _executionControl.ClosedException;

        /// The per-flow terminal verdict. Internal protocol condemnation retains the canonical
        /// close cause separately and presents affected siblings with a collateral exception.
        public Exception FlowTerminationException => _executionControl.FlowTerminationException;

        internal int OutstandingRfqCount => _executionControl.RfqCount;

        public ref readonly TState GetProtocolStatic<TState>()
            => ref _executionControl.GetProtocolStatic<TState>();

        public PgEncoder GetEncoder()
            => _executionControl.GetEncoder();

        internal ValueTask WaitForCancellationAttempt()
            => _executionControl.WaitForCancellationAttempt();

        internal void RequestBackendCancellation(PgClientFlow instigator, int window,
            BackendCancellationTiming timing, TaskCompletionSource? delivery)
            => _executionControl.RequestServerCancellation(instigator, window, timing, delivery);

        /// Returns an awaitable for the decoder. Activation is a cross-flow rendezvous completed by
        /// another flow's thread, so GetResult throws if not yet completed - async bodies await,
        /// sync bodies use GetDecoderAuto, and direct dispatchers use IsCompleted + (Unsafe)OnCompleted.
        /// The optional token lets the flow unwind rather than hold a continuation that may never
        /// complete. Bytes it already emitted are drained by the protocol on its behalf.
        public DecoderAwaitable GetDecoderAsync(CancellationToken cancellationToken = default)
            => new(_executionControl, cancellationToken, auto: false);

        /// Mode-adaptive wrapper. For a sync body whose activation hasn't fired, blocks via the AsTask
        /// bridge and runs the continuation inline. Async bodies get standard async continuation. Call
        /// sites await uniformly without branching on IsAsync.
        public DecoderAwaitable GetDecoderAuto(CancellationToken cancellationToken = default)
            => new(_executionControl, cancellationToken, auto: true);
    }

    // Self-awaitable for `await context.GetDecoderAsync()` and direct-dispatch. Under await the
    // compiler checks IsCompleted and only schedules via (Unsafe)OnCompleted(Action) when not ready.
    // Direct dispatchers (CommandFlow's shared-promise pattern) instead use IsCompleted +
    // (Unsafe)OnCompleted(Action<object?>, object?) to register without a closure allocation.
    protected readonly struct DecoderAwaitable(ExecutionControl control, CancellationToken cancellationToken, bool auto) : ICriticalNotifyCompletion
    {
        public DecoderAwaitable GetAwaiter() => this;

        // Sync-flow auto path claims completed up front so the await machinery takes the sync shortcut
        // (no box, no continuation) straight to GetResult, which blocks via AsTask if activation hasn't
        // fired. Async flows reflect SETTLED not just succeeded, so a faulted activation completes the
        // await and GetResult rethrows.
        public bool IsCompleted => control.IsDecoderSettled || (auto && !control.IsAsync);
        // Settled with a real decoder (Activate ran), vs woken by a teardown completion. A deferred
        // dispatch only has a claim on the shared promise when this is true.
        public bool IsCompletedSuccessfully => control.IsDecoderReady;

        // Only valid after IsCompleted. The sync-flow auto path reports IsCompleted unconditionally,
        // so this may run before the decoder is ready and blocks via the AsTask bridge.
        public PgDecoder GetResult()
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (control.IsDecoderSettled)
                return control.GetDecoderResult();
            return control.GetDecoderTask(cancellationToken).GetAwaiter().GetResult();
        }

        // Returns a configured variant that controls whether the continuation resumes on the
        // captured SynchronizationContext. Mirrors Task/ValueTask's ConfigureAwait shape.
        public ConfiguredDecoderAwaitable ConfigureAwait(bool continueOnCapturedContext)
            => new(control, cancellationToken, continueOnCapturedContext);

        // Bridge to Task for sync-wait or Task-combinator composition. Sync flow bodies that
        // want to block call <c>AsTask().GetAwaiter().GetResult()</c>.
        public Task<PgDecoder> AsTask() => control.GetDecoderTask(cancellationToken);

        // Action-only overloads: the C# compiler calls these for `await` syntax. Mirrors
        // ValueTaskAwaiter's defaults, capture both SynchronizationContext and ExecutionContext
        // so an awaiting body resumes on the context it suspended on. Unsafe* skips EC capture
        // (the state machine builder handles it) but still honors scheduling context.
        public void OnCompleted(Action continuation)
        {
            control.RegisterActivationCancellation(cancellationToken);
            control.OnDecoder(static state => ((Action)state!)(), continuation,
                ValueTaskSourceOnCompletedFlags.UseSchedulingContext | ValueTaskSourceOnCompletedFlags.FlowExecutionContext);
        }
        public void UnsafeOnCompleted(Action continuation)
        {
            control.RegisterActivationCancellation(cancellationToken);
            control.OnDecoder(static state => ((Action)state!)(), continuation,
                ValueTaskSourceOnCompletedFlags.UseSchedulingContext);
        }

        // State-taking overloads support direct dispatch (e.g. CommandFlow's shared-promise
        // pattern). Capture defaults mirror the Action overloads, callers that want to skip
        // scheduling-context capture go through ConfigureAwait(false) first.
        public void OnCompleted(Action<object?> continuation, object? state)
        {
            control.RegisterActivationCancellation(cancellationToken);
            control.OnDecoder(continuation, state,
                ValueTaskSourceOnCompletedFlags.UseSchedulingContext | ValueTaskSourceOnCompletedFlags.FlowExecutionContext);
        }
        public void UnsafeOnCompleted(Action<object?> continuation, object? state)
        {
            control.RegisterActivationCancellation(cancellationToken);
            control.OnDecoder(continuation, state, ValueTaskSourceOnCompletedFlags.UseSchedulingContext);
        }
    }

    // The ConfigureAwait(false) variant: skips scheduling-context capture. Action overloads are
    // for the C# `await` syntax (compiler calls UnsafeOnCompleted on ICriticalNotifyCompletion).
    protected readonly struct ConfiguredDecoderAwaitable(ExecutionControl control, CancellationToken cancellationToken, bool continueOnCapturedContext) : ICriticalNotifyCompletion
    {
        public ConfiguredDecoderAwaitable GetAwaiter() => this;
        // See IsDecoderSettled: a faulted activation must complete the await so GetResult
        // rethrows into the body's catch paths.
        public bool IsCompleted => control.IsDecoderSettled;
        // Mirrors DecoderAwaitable.IsCompletedSuccessfully: settled with a REAL decoder (Activate ran),
        // not woken by a teardown fault. A direct dispatcher only has a claim on the shared promise here.
        public bool IsCompletedSuccessfully => control.IsDecoderReady;

        public PgDecoder GetResult()
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (control.IsDecoderSettled)
                return control.GetDecoderResult();
            if (control.IsAsync)
                ThrowHelper.ThrowInvalidOperation("Decoder is not ready and the flow is async. GetResult violates the awaiter contract.");
            return control.GetDecoderTask(cancellationToken).GetAwaiter().GetResult();
        }

        public void OnCompleted(Action continuation)
        {
            control.RegisterActivationCancellation(cancellationToken);
            var flags = ValueTaskSourceOnCompletedFlags.FlowExecutionContext;
            if (continueOnCapturedContext)
                flags |= ValueTaskSourceOnCompletedFlags.UseSchedulingContext;
            control.OnDecoder(static state => ((Action)state!)(), continuation, flags);
        }
        public void UnsafeOnCompleted(Action continuation)
        {
            control.RegisterActivationCancellation(cancellationToken);
            var flags = ValueTaskSourceOnCompletedFlags.None;
            if (continueOnCapturedContext)
                flags |= ValueTaskSourceOnCompletedFlags.UseSchedulingContext;
            control.OnDecoder(static state => ((Action)state!)(), continuation, flags);
        }

        // State-taking overloads. Mirror DecoderAwaitable's pair so direct-dispatch callers can
        // also opt out of scheduling-context capture via ConfigureAwait(false).
        public void OnCompleted(Action<object?> continuation, object? state)
        {
            control.RegisterActivationCancellation(cancellationToken);
            var flags = ValueTaskSourceOnCompletedFlags.FlowExecutionContext;
            if (continueOnCapturedContext)
                flags |= ValueTaskSourceOnCompletedFlags.UseSchedulingContext;
            control.OnDecoder(continuation, state, flags);
        }
        public void UnsafeOnCompleted(Action<object?> continuation, object? state)
        {
            control.RegisterActivationCancellation(cancellationToken);
            var flags = ValueTaskSourceOnCompletedFlags.None;
            if (continueOnCapturedContext)
                flags |= ValueTaskSourceOnCompletedFlags.UseSchedulingContext;
            control.OnDecoder(continuation, state, flags);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ExecutionControl GetExecutionControl(PgClientProtocol.Control control) => new(this, control);
    internal readonly struct ExecutionControl(PgClientFlow flow, PgClientProtocol.Control control)
    {
        internal PgClientFlow Flow => flow;

        public bool SupportsDeferredFlush => flow is { _supportsDeferredFlush: true, _isAsyncAtDispatch: true };
        public bool StallsPipeline => !SupportsDeferredFlush;
        public bool IsAsync => flow.IsAsync;
        public bool HasQueuedFlow => control.HasQueuedFlow;
        public bool IsInlineDrive => control.IsInlineDrive;

        // Small optimization to allow us to skip the final sync message if we can piggyback on the flow's final rfq.
        public bool LastMessageInducesRfq => flow._lastMessageInducesRfq;

        // Outstanding server-obligation count: RFQs the server still owes the wire for what's
        // been written. Read by TryRecoverItemFailure to decide drain length.
        public int RfqCount => flow._rfqCount;

        // The flow's sync handoff MRES (null = autonomous). The ONLY way to reach it: the source pulls it
        // through this control-mediated handle rather than off a bare flow ref, so the primitive stays
        // encapsulated on the flow (HandoffEvent is protected). Used by the source's WaitForExecutor /
        // OnExecutorSuspended.
        public ManualResetEventSlim? HandoffEvent => flow.HandoffEvent;

        // Initializes a recovery flow's RFQ obligation to what the failed flow's wire activity
        // left outstanding. Routed through the write-side handle (alongside OnMessageWrite) so
        // _rfqCount mutation stays concentrated on this surface rather than leaking onto
        // PgClientFlow's public-ish API. Only called from PgClientProtocol.Control.TryRecoverItemFailure.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void TransferInheritedRfqCount(int count)
        {
            Debug.Assert(flow._rfqCount == 0, "Inherited RFQ count can only be set on a freshly-reset flow.");
            flow._rfqCount = count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnMessageWrite(PgTypes.FrontendType type)
        {
            switch (type)
            {
                case PgTypes.FrontendType.Query:
                case PgTypes.FrontendType.Sync:
                    flow._rfqCount = checked(flow._rfqCount + 1);
                    flow._lastMessageInducesRfq = true;
                    break;
                default:
                    flow._lastMessageInducesRfq = false;
                    break;
            }
        }

        /// Try-shape sync attempt: returns true if the message was processed without I/O. handled is
        /// true if the protocol layer consumed it (caller skips and pulls the next), false if it
        /// should be surfaced to the flow. Returns false only when a handler genuinely needs async
        /// work - no branch does today, so it never bails. A false return must not commit peeked state
        /// and must propagate up to a caller that can await (via HandleMessageAuto).
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryHandleMessage(in BackendMessage backendMessage, out bool handled)
        {
            if (backendMessage.Header.Type
                is PgTypes.BackendType.ReadyForQuery
                or PgTypes.BackendType.NoticeResponse
                or PgTypes.BackendType.NotificationResponse
                or PgTypes.BackendType.ParameterStatus)
            {
                return TryHandleMessageCore(backendMessage, out handled);
            }
            handled = false;
            return true;
        }

        /// True if the message was fully handled (else it's surfaced to the flow). Async-capable
        /// counterpart of TryHandleMessage, for callers that can await; sync hot-path callers use
        /// TryHandleMessage and bail recursively on false.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> HandleMessageAuto(in BackendMessage backendMessage)
        {
            return backendMessage.Header.Type
                is PgTypes.BackendType.ReadyForQuery
                or PgTypes.BackendType.NoticeResponse
                or PgTypes.BackendType.NotificationResponse
                or PgTypes.BackendType.ParameterStatus
                ? HandleMessageAutoCore(backendMessage) : new(false);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        bool TryHandleMessageCore(BackendMessage backendMessage, out bool handled)
        {
            switch (backendMessage.Header.Type)
            {
                case PgTypes.BackendType.ReadyForQuery:
                    flow._rfqCount -= 1;
                    var completedWindow = flow._cancellationWindow++;
                    control.OnFlowRfq(flow, backendMessage, completedWindow);
                    flow.OnCancellationWindowCompleted(completedWindow, flow._rfqCount);
                    handled = false;
                    return true;
                case PgTypes.BackendType.NoticeResponse:
                    handled = true;
                    return true;
                case PgTypes.BackendType.NotificationResponse:
                    handled = true;
                    return true;
                case PgTypes.BackendType.ParameterStatus:
                    control.OnParameterStatus(backendMessage);
                    handled = true;
                    return true;
                default:
                    handled = false;
                    return true;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        ValueTask<bool> HandleMessageAutoCore(BackendMessage backendMessage)
        {
            switch (backendMessage.Header.Type)
            {
                case PgTypes.BackendType.ReadyForQuery:
                    flow._rfqCount -= 1;
                    var completedWindow = flow._cancellationWindow++;
                    control.OnFlowRfq(flow, backendMessage, completedWindow);
                    flow.OnCancellationWindowCompleted(completedWindow, flow._rfqCount);
                    goto default;
                case PgTypes.BackendType.NoticeResponse:
                    // We sink all notices (this includes RAISE notices) and expect those to end up on the flow for user retrieval/logging.
                    // There aren't many interesting notices emitted (and of those, not all of them are even sent to the frontend)
                    // See: https://github.com/search?q=repo%3Apostgres%2Fpostgres+ereport%28NOTICE&type=code
                    // TODO send to the flow out of band (some virtual method).
                    return new(true);
                case PgTypes.BackendType.NotificationResponse:
                    return new(true);
                case PgTypes.BackendType.ParameterStatus:
                    control.OnParameterStatus(backendMessage);
                    return new(true);
                default:
                    return new(false);
            }
        }

        /// Starts this logical operation's pending tenure immediately before source publication.
        /// Migration preserves the remaining budget and therefore must not restart it on a replacement source.
        public void Start(PgClientFlowSource.State source, TimeSpan activationTimeout, bool dispatchAsync)
        {
            var state = Volatile.Read(ref flow._isAsyncState);
            if (state == ModeUnset)
            {
                ThrowHelper.ThrowInvalidOperation("IsAsync was not set by flow before it was queued.");
                return;
            }

            flow._isAsyncAtDispatch = dispatchAsync;
            if (!dispatchAsync)
                flow.AttachPlacementSource(source);
            // Only interactive flows arm the activation timeout. Infinite means the heartbeat's
            // timeout branch never fires for this flow (see OnHeartbeat).
            if (!flow._pendingTimeoutStarted)
            {
                flow._remainingActivationTimeout = flow.EnableActivationTimeout
                    ? flow.PendingTimeout ?? activationTimeout
                    : Timeout.InfiniteTimeSpan;
                flow._pendingTimeoutStarted = true;
            }
        }

        internal TimeSpan RemainingActivationTimeout => flow._remainingActivationTimeout;

        // Tokens are routed from Control (protocol-owned). No per-flow storage.
        public CancellationToken AbortToken => control.AbortToken;
        public CancellationToken StoppingToken => control.StoppingToken;
        internal ValueTask WaitForCancellationAttempt() => control.WaitForCancellationAttempt();
        internal void RequestServerCancellation(PgClientFlow instigator, int window,
            BackendCancellationTiming timing, TaskCompletionSource? delivery)
            => control.RequestServerCancellation(instigator, window, timing, delivery);
        public bool IsProtocolClosed => control.ClosedException is not null;
        public PgClientClosedException? ClosedException => control.ClosedException;
        public Exception FlowTerminationException => control.FlowTerminationException;

        public ValueTask<FlowTasks> ExecuteAuto()
            => IsAsync ? flow.ExecuteAuto(new(this)) : ExecuteSynchronously();

        // For ease of debugging we add a stackframe that tells us whether we're a sync flow.
        [MethodImpl(MethodImplOptions.NoInlining)]
        ValueTask<FlowTasks> ExecuteSynchronously() => flow.ExecuteAuto(new(this));

        public void Activate(PgDecoder decoder)
        {
            flow._activationCancellationTokenRegistration.Dispose();
            // If none of the cancellations triggered, we have a problem, throw.
            if (!flow._activationTaskSource.TrySetResult(decoder, runContinuationsAsynchronously: false)
                && !(flow._remainingActivationTimeout <= TimeSpan.Zero)
                && !control.AbortToken.IsCancellationRequested
                && !flow._activationCancellationTokenRegistration.Token.IsCancellationRequested)
                ThrowHelper.ThrowInvalidOperation("Flow was already activated unexpectedly.");
        }

        public void RegisterDecoderOnHeartbeat(Action<TimeSpan> action)
        {
            flow._decoderOnHeartbeatAction = action;
        }

        public void OnHeartbeat(TimeSpan interval)
        {
            if (PropagateTermination())
                return;

            OnActivationHeartbeat(interval);

            flow._decoderOnHeartbeatAction?.Invoke(interval);
            flow.OnHeartbeat(interval);
        }

        // Returns true after abort propagation because no timed callback may run once the flow has been
        // forcefully stopped. Graceful stopping still permits ordinary heartbeat work while it drains.
        public bool PropagateTermination()
        {
            // Abort propagation gates on AbortToken. Graceful Shutdown materializes
            // ClosedException up front but defers AbortToken until CompletionTimeout
            // escalation, so in-flight flows drain naturally until then. ClosedException is
            // guaranteed non-null when AbortToken fires because Shutdown materializes it
            // before cancelling _abortCts (and _abortCts is only fired from Shutdown).
            // TrySetException on an already-completed activation source is a no-op, so
            // iterating the head flow is harmless.
            if (control.AbortToken.IsCancellationRequested && !flow._completed)
            {
                var ex = control.FlowTerminationException;
                flow._activationTaskSource.TrySetException(ex, runContinuationsAsynchronously: true);
                flow.OnAbort(ex);
                return true;
            }

            // Graceful-stopping propagation. AbortToken faults the activation source, but StoppingToken
            // must reach the body-side gates too, else a flow dispatched but never cranked by the
            // consumer stays parked until CompletionTimeout escalates to AbortToken. ClosedException is
            // materialized before _stoppingCts fires, so it's non-null here.
            if (control.StoppingToken.IsCancellationRequested && !flow._completed)
                flow.OnStopping(control.FlowTerminationException);
            return false;
        }

        public void OnActivationHeartbeat(TimeSpan interval)
        {
            // InfiniteTimeSpan and Zero mean no activation timeout. Timeout-armed flows cannot be
            // pooled, so a concurrent observer cannot fault a later tenant through a stale reference.
            if (flow._remainingActivationTimeout != Timeout.InfiniteTimeSpan && flow._remainingActivationTimeout != TimeSpan.Zero
                && flow._activationTaskSource.GetStatus(flow._activationTaskSource.Version) is ValueTaskSourceStatus.Pending
                && (flow._remainingActivationTimeout -= interval) <= TimeSpan.Zero)
                flow._activationTaskSource.TrySetException(new TimeoutException("Operation timed out waiting for activation."), runContinuationsAsynchronously: true);
        }

        /// Fail a never-started flow drained from the backlog at shutdown with the wire-death reason. The
        /// heartbeat fires OnStopping/OnAbort for the flows the pipeline enumerates (dispatched); it never
        /// reaches the backlog, so the drain delivers here. Because the body never ran, OnStopping and OnAbort
        /// take the IDENTICAL branch - no inner executor to drain, no graceful/forceful distinction to make -
        /// so one hook faults the caller gate (the flow is a bystander to the wire's death). Release then
        /// signals done (the TCS) and fires the action, whose exception drives e.g. the ADO connection Break.
        public void FailUnstarted(Exception exception)
        {
            flow.OnStopping(exception);
            Release(exception);
            flow.CompletePlacement();
        }

        /// Commits an inert flow to this wire. Connection-local binding remains fallible and runs
        /// before execution ownership is published, so a binding failure never touches the wire.
        public void Bind(PgClientFlowBindingContext? context)
        {
            flow.Bind(context);
            Volatile.Write(ref flow._started, true);
        }

        /// Delivers a dispatch-time failure that happened before the flow touched the wire. The
        /// framework still owns retirement; this only completes the caller-facing side.
        public void FailBeforeStart(Exception exception) => flow.OnStopping(exception);

        /// Releases this source's ownership without completing the caller-visible flow. The executor
        /// has stopped before the inert drain, so an unstarted flow cannot concurrently activate.
        public void DetachForMigration(PgClientFlowSource source)
        {
            Debug.Assert(!flow._started && !flow._completed);
            // No body has awaited activation yet, so there is no activation-cycle registration to
            // dismantle and reconstruct on the replacement source.
            Debug.Assert(flow._activationCancellationTokenRegistration == default);
            // Forceful shutdown may have propagated the retired wire's abort into this inert flow
            // before the source drain transferred it. No body can have observed the pre-start gate;
            // reset that wire-local verdict so replacement dispatch can activate the same operation.
            if (IsDecoderSettled)
            {
                Debug.Assert(control.AbortToken.IsCancellationRequested);
                flow._activationTaskSource.Reset();
            }
            if (flow._placementSource is not null)
                flow.DetachPlacementSource(source.SourceState);
            if (flow.OwnsAdmissionBarrier)
                control.ReleaseAdmissionBarrier();
            flow._enqueueOptions = FlowEnqueueOptions.None;
            if (flow._ownsWireCapacity)
            {
                flow._ownsWireCapacity = false;
                control.ReleaseWireCapacity();
            }
        }

        /// Framework lifecycle: releases the flow after execution and trailing work have settled.
        /// Tenure-owned resources are released before the per-flow terminal signal publishes the
        /// reuse gate. Called from the pipeline policy's CompleteItem.
        public void Release(Exception? exception = null)
        {
            if (flow._completed)
                return;
            flow._completed = true;
            if (flow.OwnsAdmissionBarrier)
                control.ReleaseAdmissionBarrier();
            flow._activationCancellationTokenRegistration.Dispose();
            var observer = flow._observer;
            var observerState = flow._observerState;
            try { flow.OnReleasing(exception); }
            catch (Exception ex)
            {
                control.FailProtocolFromCallback(ex, "a flow release hook");
            }
            if (flow._ownsWireCapacity)
                control.ReleaseWireCapacity();
            // State transitions which must be visible with the terminal result belong here.
            try { observer?.OnCompleting(flow, exception, observerState); }
            catch (Exception ex)
            {
                control.FailProtocolFromCallback(ex, "a flow completing callback");
            }
            // Wire-death fault delivery is NOT done here - it rides the OnStopping/OnAbort hooks (dispatched
            // flows from the heartbeat, backlog flows from the shutdown drain's close delivery), so a flow's
            // caller gate is faulted by the close verdict, not by completion. Completion just signals done
            // and notifies the post-done action. Deliberately NO activation-source faulting here:
            // a parked deferred dispatch holds no resources, and Reset clears the registration on reuse.
            // Async continuation dispatch: completers run in retirement/teardown contexts where
            // inline caller continuations are a re-entrancy hazard, the contract the old TCS's
            // RunContinuationsAsynchronously carried, minus its unconditional thread-pool destination.
            if (exception is not null)
                flow._completionCore.TrySetException(exception, runContinuationsAsynchronously: true);
            else
                flow._completionCore.TrySetResult(flow, runContinuationsAsynchronously: true);
            flow._completionEvent?.Set();
            // The completed observer runs from CompleteItem in the advancer/retirement work-item
            // context: a raw throw would crash that thread unobserved. Don't swallow either - a
            // throwing completed observer means the consumer-side integration is broken, so the
            // pipeline won't drain naturally. Tear down via FailProtocol (fire-and-forget self-evict).
            // The flow itself is already completed (signal fired above); this callback is a notification.
            try { observer?.OnCompleted(flow, exception, observerState); }
            catch (Exception ex)
            {
                control.FailProtocolFromCallback(ex, "a flow completed observer");
            }
        }

        public ref readonly TState GetProtocolStatic<TState>()
            => ref ((IProtocolStatic<TState>)(object)control).Value;

        public PgEncoder GetEncoder()
        {
            ThrowIfCannotWrite();
            return new PgEncoder(this, control.Writer);
        }

        public void ThrowIfCannotWrite()
        {
            var executing = control.ExecutingFlow;
            if (ReferenceEquals(flow, executing))
                return;
            // Substitution-substrate gate: the failed flow's trailing task continues writing
            // legitimately during the recovery's tenure (recovery took the executor slot in
            // its place; the failed flow's write phase is extended through the substitute).
            // Cold path - never hit on the hot common case.
            if (executing is Flows.ResyncRecoveryFlow recovery && ReferenceEquals(flow, recovery.FailedFlow))
                return;
            ThrowHelper.ThrowInvalidOperation(
                "Flow cannot write anymore. All writes must happen during the first execution phase " +
                "which ends after the Execute method returns the inner task.");
        }

        // Activation-task-source primitives surfaced as "decoder ready / on decoder" because that's
        // what consumers care about. Keeps the version token internal. Exposed publicly through
        // Context as the DecoderAwaitable.
        public bool IsDecoderReady
            => flow._activationTaskSource.GetStatus(flow._activationTaskSource.Version) is ValueTaskSourceStatus.Succeeded;

        /// Awaiter-completion check: completed means SETTLED, not succeeded. A faulted activation
        /// (timeout, abort) must complete the await so GetResult rethrows into the body's catch paths.
        /// Treating Faulted as pending parks the body on a source that never transitions again, and
        /// its late registration lands on the slot the dispatch bridge still occupies.
        public bool IsDecoderSettled
            => flow._activationTaskSource.GetStatus(flow._activationTaskSource.Version) is not ValueTaskSourceStatus.Pending;
        public PgDecoder GetDecoderResult()
            => flow._activationTaskSource.GetResult(flow._activationTaskSource.Version);
        public void OnDecoder(Action<object?> continuation, object? state, ValueTaskSourceOnCompletedFlags flags)
            => flow._activationTaskSource.OnCompleted(continuation, state, flow._activationTaskSource.Version, flags);

        // Bridge to Task for callers that need to block (sync flow body using
        // .GetAwaiter().GetResult()) or to compose with Task-based combinators. MVTSC has no
        // blocking GetResult of its own, so this is the only safe sync-wait path.
        public Task<PgDecoder> GetDecoderTask(CancellationToken cancellationToken)
        {
            RegisterActivationCancellation(cancellationToken);
            return new ValueTask<PgDecoder>(flow, flow._activationTaskSource.Version).AsTask();
        }

        // Registers caller cancellation against the activation source so a flow can unwind
        // itself rather than hold a continuation that may never complete. No-op for default
        // tokens. Only one registration is supported per activation cycle.
        public void RegisterActivationCancellation(CancellationToken cancellationToken)
        {
            if (!cancellationToken.CanBeCanceled)
                return;
            if (flow._activationCancellationTokenRegistration != default)
                ThrowHelper.ThrowInvalidOperation("Concurrent activation result awaits are not supported.");
            flow._activationCancellationTokenRegistration = cancellationToken.UnsafeRegister(
                static (state, token) =>
                    ((PgClientFlow)state!)._activationTaskSource.TrySetException(new OperationCanceledException(token), runContinuationsAsynchronously: true),
                flow);
        }
    }
}
