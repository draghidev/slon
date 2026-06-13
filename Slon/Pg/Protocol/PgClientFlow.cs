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
        control!.Activate(this);
    }

    readonly bool _supportsPipelining;
    Action<TimeSpan>? _decoderOnHeartbeatAction; // TODO should we have this here?
    int _rfqCount;
    bool _lastMessageInducesRfq;
    // We store the IsAsync value at bind time so the protocol can keep track of pipeline stalls correctly.
    bool _isAsyncAtBind;
    bool? _isAsync;

    // Flow lifecycle state. Replaces the spin-state-laden FlowStatus machine that
    // used to live in ProtocolFlow. Reads happen on the consumer thread after the
    // executor has settled, so plain flags are sufficient.
    bool _started;
    bool _completed;
    TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Actvation state.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<PgDecoder> _activationTaskSource;
    CancellationTokenRegistration _activationCancellationTokenRegistration;
    TimeSpan _remainingActivationTimeout;

    // Completion state.
    Action<PgClientFlow, Exception?, object?>? _completionAction;
    object? _completionState;

    /// The flow's body. The "Auto" suffix is the protocol package's convention for "this method's
    /// contract adapts to the bound flow mode (sync or async)". The body internally
    /// dispatches between sync and async I/O based on <see cref="IsAsync"/> for each per-call read.
    /// Helper APIs the body calls come as explicit sync/async pairs (e.g.
    /// <c>ReadUntilExecute</c> / <c>ReadUntilExecuteAsync</c>, <c>Write</c> / <c>WriteAsync</c>).
    /// The body picks one at each call site rather than threading a mode flag through a wrapper.
    ///
    /// Prefer async/await in both modes. Sync mode affects scheduling, not body syntax. A fully
    /// sync body that needs the decoder calls <c>GetDecoderAuto</c> for a blocking get. Don't
    /// mix sync I/O calls inside an <c>async</c> body or vice versa.
    protected abstract ValueTask<FlowTasks> ExecuteAuto(Context context);

    protected bool IsAsync
    {
        get => _isAsync ?? false;
        set => _isAsync = value;
    }


    // The bind-time async snapshot. Stable across the flow's tenure (unlike IsAsync, which a
    // flow body may mutate for sync/async mixing). Used by the policy to decide whether
    // activation can run inline: sync flows park their caller via Task wait-handle (bounded
    // signal cost), so inline-Activate is safe. Async flows can attach arbitrary continuations,
    // so they must go through TP to avoid pinning the advancer latch.
    internal bool IsAsyncAtBind => _isAsyncAtBind;

    // Pre-bind read of IsAsync for the enqueue path: the protocol routes sync flows through an
    // inline wake-signal dispatch so the producer's thread takes over the executor for that flow.
    // Asserts the flow set its mode before queueing (same precondition Bind enforces).
    internal bool IsAsyncForEnqueue
    {
        get
        {
            if (_isAsync is not { } async)
            {
                ThrowHelper.ThrowInvalidOperation("IsAsync was not set by flow before it was queued.");
                return default;
            }
            return async;
        }
    }

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
        _started = false;
        _completed = false;
        _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _activationTaskSource.Reset();
        _rfqCount = 0;
        _lastMessageInducesRfq = false;
        OnReset();
    }



    protected virtual void OnHeartbeat(TimeSpan interval) {}
    protected virtual void OnAbort(PgClientClosedException exception) {}
    protected virtual void OnReset() {}
    /// Completion observation point for the flow. On exceptional completion a flow must fault
    /// its caller-facing sources here: the body's own fault paths only run when the body ran,
    /// and a flow can fail before that (dispatch-time throw, pre-body protocol fault) or be
    /// supplanted by recovery. Implementations must tolerate racing the body's own fault path
    /// (use TrySet semantics).
    protected virtual void OnComplete(Exception? exception) {}

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

        public ref readonly TState GetProtocolStatic<TState>()
            => ref _executionControl.GetProtocolStatic<TState>();

        public PgEncoder GetEncoder()
            => _executionControl.GetEncoder();

        /// Returns an awaitable for the decoder. The <c>Async</c> suffix marks this as
        /// intrinsically asynchronous. Activation is a cross-flow rendezvous, completed by
        /// another flow's thread. The awaiter's <c>GetResult</c> throws if not yet completed.
        /// async bodies use <c>await</c>. Sync bodies that want a blocking call should use
        /// <see cref="GetDecoderAuto"/>. Direct-dispatch callers (e.g. CommandFlow's
        /// shared-promise pattern) use IsCompleted + (Unsafe)OnCompleted to register
        /// continuations without going through await machinery.
        ///
        /// The optional cancellation token lets the flow unwind itself rather than holding a
        /// continuation that may never complete. Wire bytes the flow already emitted are owned
        /// by the protocol and drained on the flow's behalf when it unwinds.
        public DecoderAwaitable GetDecoderAsync(CancellationToken cancellationToken = default)
            => new(_executionControl, cancellationToken, auto: false);

        /// Mode-adaptive wrapper. Returns a <see cref="DecoderAwaitable"/> that, when awaited
        /// by a sync flow body and activation hasn't fired yet, blocks via the AsTask bridge
        /// inside <c>OnCompleted</c> and runs the continuation inline. Async flow bodies get the
        /// standard async-continuation behavior. Call sites <c>await</c> uniformly without
        /// branching on <c>IsAsync</c> themselves.
        public DecoderAwaitable GetDecoderAuto(CancellationToken cancellationToken = default)
            => new(_executionControl, cancellationToken, auto: true);
    }

    // Self-awaitable for `await context.GetDecoderAsync()` plus direct-dispatch interaction:
    //
    // - `await` syntax: the C# compiler checks IsCompleted and calls GetResult only when true.
    //   If not yet ready it schedules the continuation via OnCompleted(Action) /
    //   UnsafeOnCompleted(Action).
    //
    // - Direct dispatchers (CommandFlow's shared-promise pattern): use IsCompleted +
    //   OnCompleted(Action<object?>, object?) / UnsafeOnCompleted(Action<object?>, object?) to
    //   register continuations without closure allocation. GetResult is only valid after
    //   IsCompleted is true (standard awaiter contract).
    protected readonly struct DecoderAwaitable(ExecutionControl control, CancellationToken cancellationToken, bool auto) : ICriticalNotifyCompletion
    {
        public DecoderAwaitable GetAwaiter() => this;

        // Sync-flow auto path claims completed up front so the await machinery takes the sync
        // shortcut (no state machine box allocated, no continuation registered) and falls
        // straight to GetResult, which blocks via AsTask if activation hasn't fired yet.
        // Async flows reflect SETTLED (not just succeeded): a faulted activation completes
        // the await so GetResult rethrows (see IsDecoderSettled).
        public bool IsCompleted => control.IsDecoderSettled || (auto && !control.IsAsync);

        // Only valid after IsCompleted is true. For the sync-flow auto path, IsCompleted is
        // true unconditionally, so this may run while the decoder isn't actually ready. In
        // that case we block via the AsTask bridge. Async flows follow the standard awaiter
        // contract and the compiler guarantees IsCompleted has fired before this.
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="backendMessage"></param>
        /// <returns>True if the message was fully handled (if not it will be surfaced to the flow).</returns>
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
            if (flow._isAsync is not { } async)
            {
                ThrowHelper.ThrowInvalidOperation("IsAsync was not set by flow before it was queued.");
                return;
            }

            flow._isAsyncAtBind = async;
            flow._remainingActivationTimeout = activationTimeout;
        }

        // Tokens are routed from Control (protocol-owned). No per-flow storage.
        public CancellationToken AbortToken => control.AbortToken;
        public CancellationToken StoppingToken => control.StoppingToken;
        public bool IsProtocolClosed => control.ClosedException is not null;

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

            // Same sentinel guard as PgDecoder.OnHeartbeat: InfiniteTimeSpan (-1ms) and Zero
            // both mean "no activation timeout". Without the guard an infinite budget reads
            // as instantly expired and the first heartbeat tick spuriously times out any
            // flow still pending activation (observed as ~1s "Operation timed out waiting
            // for activation" failures under contention with the protocol-level default
            // ConnectionTimeout = Infinite).
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
            if (exception is not null)
                flow._completionTcs.TrySetException(exception);
            else
                flow._completionTcs.TrySetResult();
            // Deliberately NO activation-source faulting here: a parked deferred dispatch
            // holds no resources (TryStart happens only when the bridge runs), the caller is
            // faulted via OnComplete, and Reset clears the registration on reuse. Invoking
            // the bridge on a completed flow would create-and-start the body for a dead
            // tenure - it takes the shared pipelined-read promise tenure for nothing and
            // races instance reuse on this very source (observed as double continuation
            // registration aborts). The heartbeat abort path keeps its own faulting; that is
            // protocol teardown, not per-flow completion.
            flow.OnComplete(exception);
            flow._completionAction?.Invoke(flow, exception, flow._completionState);
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
            if (executing is Flows.RecoveryDrainFlow recovery && ReferenceEquals(flow, recovery.FailedFlow))
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

        /// Awaiter-completion check: completed means SETTLED, not succeeded. A faulted
        /// activation (timeout, abort) must complete the await so GetResult rethrows into
        /// the body's catch paths. Treating Faulted as pending parks the body on a source
        /// that will never transition again, and its late registration lands on the slot the
        /// dispatch bridge still occupies (invocation does not clear the continuation; only
        /// Reset does) - observed as an unhandled InvalidOperationException on the TP under
        /// activation-timeout conditions.
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
