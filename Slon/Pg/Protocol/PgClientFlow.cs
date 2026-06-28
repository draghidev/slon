using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Slon.Pg.Protocol;

abstract class PgClientFlow : IValueTaskSource<PgDecoder>, IThreadPoolWorkItem
{
    PgClientProtocol.Control? _pendingActivationControl;

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

    readonly bool _supportsPipelining;
    Action<TimeSpan>? _decoderOnHeartbeatAction; // TODO should we have this here?
    int _rfqCount;
    bool _lastMessageInducesRfq;
    // We store the IsAsync value at bind time so the protocol can keep track of pipeline stalls correctly.
    bool _isAsyncAtBind;
    // Tri-state int (0 = unset, 1 = true, 2 = false) instead of bool? so reads / writes can be
    // ordered via Volatile.Read / Volatile.Write. The flow body and the consumer (via MoveNext's
    // sync<->async flip) can race on this; without ordering the post-wake-protocol body can read
    // a stale value and take the wrong I/O path.
    int _isAsyncState;

    // Flow lifecycle state. Reads happen on the consumer thread after the executor has settled,
    // so plain flags are sufficient.
    bool _started;
    bool _completed;
    TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Activation state.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<PgDecoder> _activationTaskSource;
    CancellationTokenRegistration _activationCancellationTokenRegistration;
    TimeSpan _remainingActivationTimeout;

    // Completion state.
    Action<PgClientFlow, Exception?, object?>? _completionAction;
    object? _completionState;

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
        get => Volatile.Read(ref _isAsyncState) == 1;
        set => Volatile.Write(ref _isAsyncState, value ? 1 : 2);
    }


    // The bind-time async snapshot, stable across the flow's tenure (unlike IsAsync, which a body
    // may mutate). The policy uses it to decide inline vs TP activation, and the executor's
    // HeadIsSyncHandoff peek to fake-miss sync flows for caller takeover.
    internal bool IsAsyncAtBind => _isAsyncAtBind;

    // Capture the routing async-mode as the stable snapshot at ENQUEUE, before the flow is published
    // to the executor's pull. Bind sets the same field, but for a sync flow Bind runs only AFTER its
    // blocking takeover - too late for the executor's pre-dispatch HeadIsSyncHandoff peek, which would
    // then read the mutable IsAsync and could mistake a sync flow for async. Set with the value the
    // enqueue already read so the peek and the routing agree.
    internal void CaptureAsyncRoutingSnapshot(bool isAsync) => _isAsyncAtBind = isAsync;

    // Pre-bind read of IsAsync for the enqueue path: the protocol routes sync flows through an
    // inline wake-signal dispatch so the producer's thread takes over the executor for that flow.
    // Asserts the flow set its mode before queueing (same precondition Bind enforces).
    internal bool IsAsyncForEnqueue
    {
        get
        {
            var state = Volatile.Read(ref _isAsyncState);
            if (state == 0)
            {
                ThrowHelper.ThrowInvalidOperation("IsAsync was not set by flow before it was queued.");
                return default;
            }
            return state == 1;
        }
    }

    // Routing gate for the sync caller-handoff path (fec0355's waiter-presence gate, evaluated directly).
    // A flow takes the handoff (park a caller, run its body on that caller's thread) only if it routes
    // SYNC and posts a handoff MRES (non-null = a caller is there to hand to). A sync flow with NO waiter
    // (null MRES) runs AUTONOMOUSLY: it is routed like an async flow so the executor DRIVES it (the body
    // still does sync I/O via IsAsync), never held for a caller that will never come. Without this an
    // autonomous sync flow would NRE on the null MRES in WaitForExecutor or hang held in
    // OnExecutorSuspended. Short-circuits on the async path, so GetHandoffMres is only consulted for sync
    // flows - where it is the genuine question being asked.
    internal bool NeedsSyncHandoff => !IsAsyncForEnqueue && GetHandoffMres() is not null;

    // Probably should use a cts here.
    public void Cancel()
    {
        throw new NotImplementedException();
    }

    protected PgClientFlow(bool supportsPipelining)
    {
        _supportsPipelining = supportsPipelining;
        _activationTaskSource.CanCompleteConcurrently = true;
    }

    public void SetCompletionAction(Action<PgClientFlow, Exception?, object?> action, object? state)
    {
        _completionAction = action;
        _completionState = state;
    }

    // Bind the caller's cancellation token at submit so the (eager) write, and the reads by default,
    // honor it. No-op for flows without a caller; the queue binds only a cancelable token, so the
    // common no-token submit pays no field write.
    internal virtual void BindCallerToken(CancellationToken cancellationToken) { }

    public bool IsCompleted => _completed;
    internal bool IsStarted => _started && !_completed;
    internal bool IsPending => !_started;

    // Internal completion sync for benchmarks + Postgres startup wait.
    // Refreshed per Reset cycle so pooled flows get a fresh signal each tenure.
    internal ValueTask WaitForComplete(CancellationToken cancellationToken = default)
        => new(_completionTcs.Task.WaitAsync(cancellationToken));

    // Public for pooling. Reset is called by consumers between uses.
    public void Reset()
    {
        Debug.Assert(IsPending || IsCompleted, "Cannot reset a flow that is mid-execution.");
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
        _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _activationTaskSource.Reset();
        _rfqCount = 0;
        _lastMessageInducesRfq = false;
        OnReset();
    }

    // Interactive flows (CommandFlow) override this to opt in to the activation timeout, which models
    // a caller's patience (ConnectionTimeout). Background flows have no caller, so by default they
    // wait indefinitely for activation rather than busy-looping queue/timeout/re-arm, and stay off
    // the heartbeat's generation-agnostic timeout completer.
    protected virtual bool EnableActivationTimeout => false;

    protected virtual void OnHeartbeat(TimeSpan interval) {}
    protected virtual void OnAbort(PgClientClosedException exception) {}
    /// Graceful-shutdown observation point. Fires while StoppingToken is set but before the
    /// AbortToken escalation. Flow types whose body can park on a non-IO rendezvous (CommandFlow's
    /// GateTask) override this to wake it so the body short-circuits instead of waiting for
    /// AbortToken. Idempotent across heartbeat ticks (subclasses use TrySet).
    protected virtual void OnStopping(PgClientClosedException exception) {}
    protected virtual void OnReset() {}
    /// Completion observation point for the flow. On exceptional completion a flow must fault
    /// its caller-facing sources here: the body's own fault paths only run when the body ran,
    /// and a flow can fail before that (dispatch-time throw, pre-body protocol fault) or be
    /// supplanted by recovery. Implementations must tolerate racing the body's own fault path
    /// (use TrySet semantics).
    protected virtual void OnComplete(Exception? exception) {}

    // The per-flow handoff rendezvous primitive for the (wait-list-free) sync source handoff: non-null only
    // for a flow that needs a caller takeover (a sync CommandFlow with a parked caller). The source signals
    // it when it dequeues-and-holds the flow for that caller (OnExecutorSuspended), and the caller parks on
    // it in WaitForExecutor. null = no handoff (async flows, or a flow with no waiting caller) - the source
    // runs it autonomously on the executor, nothing to rendezvous. null/non-null IS the waiter-presence gate.
    // The sync handoff MRES a caller parks on (null = autonomous, no waiter). protected, NOT internal:
    // it is reachable only by the flow's own subclasses (which override it) and by ExecutionControl (the
    // nested write-side handle) - the source pulls it via ExecutionControl.GetHandoffMres, never off a
    // bare flow ref. Keeps the handoff primitive off PgClientFlow's internal API, like _rfqCount.
    protected virtual ManualResetEventSlim? GetHandoffMres() => null;

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

        public ref readonly TState GetProtocolStatic<TState>()
            => ref _executionControl.GetProtocolStatic<TState>();

        public PgEncoder GetEncoder()
            => _executionControl.GetEncoder();

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

        // State-taking overloads: direct-dispatch escape hatch (e.g. CommandFlow's shared-promise
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
        // Any sync flow causes pipeline stalls (blocks execution loop until decoder is available) so effectively can't pipeline.
        public bool IsPipelined => flow is { _supportsPipelining: true, _isAsyncAtBind: true };
        public bool IsAsync => flow.IsAsync;

        // Small optimization to allow us to skip the final sync message if we can piggyback on the flow's final rfq.
        public bool LastMessageInducesRfq => flow._lastMessageInducesRfq;

        // Outstanding server-obligation count: RFQs the server still owes the wire for what's
        // been written. Read by TryRecoverItemFailure to decide drain length.
        public int RfqCount => flow._rfqCount;

        // The flow's sync handoff MRES (null = autonomous). The ONLY way to reach it: the source pulls it
        // through this control-mediated handle rather than off a bare flow ref, so the primitive stays
        // encapsulated on the flow (GetHandoffMres is protected). Used by the source's WaitForExecutor /
        // OnExecutorSuspended.
        public ManualResetEventSlim? GetHandoffMres() => flow.GetHandoffMres();

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
                    if (flow._rfqCount is 0)
                        control.OnFlowRfq(backendMessage);
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
                    if (flow._rfqCount is 0)
                        control.OnFlowRfq(backendMessage);
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

        public void Bind(TimeSpan activationTimeout)
        {
            var state = Volatile.Read(ref flow._isAsyncState);
            if (state == 0)
            {
                ThrowHelper.ThrowInvalidOperation("IsAsync was not set by flow before it was queued.");
                return;
            }

            flow._isAsyncAtBind = state == 1;
            // Only interactive flows arm the activation timeout. Infinite means the heartbeat's
            // timeout branch never fires for this flow (see OnHeartbeat).
            flow._remainingActivationTimeout = flow.EnableActivationTimeout ? activationTimeout : Timeout.InfiniteTimeSpan;
        }

        // Tokens are routed from Control (protocol-owned). No per-flow storage.
        public CancellationToken AbortToken => control.AbortToken;
        public CancellationToken StoppingToken => control.StoppingToken;
        public bool IsProtocolClosed => control.ClosedException is not null;
        public PgClientClosedException? ClosedException => control.ClosedException;

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
            // Abort propagation gates on AbortToken. Graceful Shutdown materializes
            // ClosedException up front but defers AbortToken until CompletionTimeout
            // escalation, so in-flight flows drain naturally until then. ClosedException is
            // guaranteed non-null when AbortToken fires because Shutdown materializes it
            // before cancelling _abortCts (and _abortCts is only fired from Shutdown).
            // TrySetException on an already-completed activation source is a no-op, so
            // iterating the head flow is harmless.
            if (control.AbortToken.IsCancellationRequested && !flow._completed)
            {
                var ex = control.ClosedException!;
                flow._activationTaskSource.TrySetException(ex, runContinuationsAsynchronously: true);
                flow.OnAbort(ex);
                return;
            }

            // Graceful-stopping propagation. AbortToken faults the activation source, but StoppingToken
            // must reach the body-side gates too, else a flow dispatched but never cranked by the
            // consumer stays parked until CompletionTimeout escalates to AbortToken. ClosedException is
            // materialized before _stoppingCts fires, so it's non-null here.
            if (control.StoppingToken.IsCancellationRequested && !flow._completed)
                flow.OnStopping(control.ClosedException!);

            // Same sentinel guard as PgDecoder.OnHeartbeat: InfiniteTimeSpan and Zero both mean "no
            // activation timeout". Without it an infinite budget reads as instantly expired and the
            // first heartbeat tick times out any flow still pending activation.
            // Wrong-tenure hazard if a timeout-armed flow is ever pooled (this TrySetException is
            // generation-agnostic). Enforced against in PgClientFlow.Reset; the gen-checked completer
            // lands here.
            if (flow._remainingActivationTimeout != Timeout.InfiniteTimeSpan && flow._remainingActivationTimeout != TimeSpan.Zero
                && flow._activationTaskSource.GetStatus(flow._activationTaskSource.Version) is ValueTaskSourceStatus.Pending
                && (flow._remainingActivationTimeout -= interval) <= TimeSpan.Zero)
                flow._activationTaskSource.TrySetException(new TimeoutException("Operation timed out waiting for activation."), runContinuationsAsynchronously: true);

            flow._decoderOnHeartbeatAction?.Invoke(interval);
            flow.OnHeartbeat(interval);
        }

        /// Framework lifecycle: marks the flow started. Called from the pipeline policy's
        /// ExecuteItemAsync before the flow body runs.
        public void Start() => flow._started = true;

        /// Framework lifecycle: marks the flow completed, signals the per-flow completion TCS,
        /// disposes any per-await activation-cancellation registration, fires the registered
        /// completion action. Called from the pipeline policy's CompleteItem.
        public void Complete(Exception? exception = null)
        {
            if (flow._completed)
                return;
            flow._completed = true;
            flow._activationCancellationTokenRegistration.Dispose();
            // OnComplete BEFORE the completion TCS, so WaitForComplete (the done-signal) resolves only
            // after teardown has fully run - "done" means "fully torn down" for every flow. Otherwise a
            // waiter keyed on WaitForComplete observes done and, for a pooled flyweight, re-Initializes
            // the instance while this OnComplete is still in flight: a stale OnComplete then lands on the
            // next tenure's freshly-Reset state and its teardown overlaps the next tenure's shared-wire
            // use. Wrapped so a throwing teardown can't strand the TCS (every WaitForComplete would hang).
            // Deliberately NO activation-source faulting here: a parked deferred dispatch holds no
            // resources, the caller is faulted via OnComplete, and Reset clears the registration on
            // reuse. Invoking the bridge on a completed flow would create-and-start the body for a
            // dead tenure, taking the shared read promise for nothing and racing instance reuse. The
            // heartbeat abort path keeps its own faulting - that's protocol teardown, not completion.
            try { flow.OnComplete(exception); }
            catch (Exception ex) { /* TODO log */ control.FailProtocol(ex); }
            if (exception is not null)
                flow._completionTcs.TrySetException(exception);
            else
                flow._completionTcs.TrySetResult();
            // The completion callback runs from CompleteItem in the advancer/retirement work-item
            // context: a raw throw would crash that thread unobserved. Don't swallow either - a
            // throwing completion callback means the consumer-side integration is broken, so the
            // pipeline won't drain naturally. Tear down via FailProtocol (fire-and-forget self-evict).
            // The flow itself is already completed (TCS set above); this callback is a notification.
            try { flow._completionAction?.Invoke(flow, exception, flow._completionState); }
            catch (Exception ex) { /* TODO log */ control.FailProtocol(ex); }
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
            var executing = control.ExecutorFlow;
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
