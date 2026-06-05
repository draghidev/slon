using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Slon.Pg.Protocol;

abstract class PgClientFlow : IProtocolFlow, IValueTaskSource<PgDecoder>
{
    readonly bool _supportsPipelining;
    CancellationToken? _abortToken;
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
    /// contract adapts to the bound flow mode (sync or async)" — the same convention applies to
    /// the helper APIs the body calls (FlushAuto, WriteAuto, ReadUntilExecuteAuto, ...).
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

    protected CancellationToken AbortToken
    {
        get
        {
            if (_abortToken is not { } token)
            {
                ThrowHelper.ThrowInvalidOperation("Flow is not yet enqueued on a protocol.");
                return default;
            }
            return token;
        }
    }

    // The bind-time async snapshot. Stable across the flow's tenure (unlike IsAsync, which a
    // flow body may mutate for sync/async mixing). Used by the policy to decide whether
    // activation can run inline: sync flows park their caller via Task wait-handle (bounded
    // signal cost), so inline-Activate is safe. Async flows can attach arbitrary continuations,
    // so they must go through TP to avoid pinning the advancer latch.
    internal bool IsAsyncAtBind => _isAsyncAtBind;

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

    // IProtocolFlow lifecycle. Explicit impls so direct callers can't bypass the framework
    // without making the bypass visible in code (via a cast).
    void IProtocolFlow.Start() => _started = true;

    void IProtocolFlow.Complete(Exception? exception)
    {
        if (_completed)
            return;
        _completed = true;
        _activationCancellationTokenRegistration.Dispose();
        if (exception is not null)
            _completionTcs.TrySetException(exception);
        else
            _completionTcs.TrySetResult();
        _completionAction?.Invoke(this, exception, _completionState);
    }

    void IProtocolFlow.Abort()
    {
        var exception = new OperationCanceledException(_abortToken.GetValueOrDefault());
        _activationTaskSource.TrySetException(exception, runContinuationsAsynchronously: true);
        OnAbort(exception);
    }

    protected virtual void OnHeartbeat(TimeSpan interval) {}
    protected virtual void OnAbort(OperationCanceledException exception) {}
    protected virtual void OnReset() {}

    PgDecoder IValueTaskSource<PgDecoder>.GetResult(short token) => _activationTaskSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<PgDecoder>.GetStatus(short token) => _activationTaskSource.GetStatus(token);
    void IValueTaskSource<PgDecoder>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _activationTaskSource.OnCompleted(continuation, state, token, flags);


    protected readonly struct Context
    {
        readonly ExecutionControl _executionControl;
        internal Context(ExecutionControl executionControl)
            => _executionControl = executionControl;

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
            => new(_executionControl, cancellationToken);

        /// Blocks the caller's thread until the decoder is available, then returns it. The
        /// <c>Auto</c> suffix marks this as the per-flow-mode adaptive helper: sync flow bodies
        /// call this directly for a blocking get; async flow bodies should prefer
        /// <see cref="GetDecoderAsync"/> with <c>await</c>. If activation has already happened
        /// when this is called the fast path returns the decoder without ever blocking; otherwise
        /// the call bridges via <c>AsTask().GetAwaiter().GetResult()</c>.
        public PgDecoder GetDecoderAuto(CancellationToken cancellationToken = default)
        {
            var awaitable = GetDecoderAsync(cancellationToken);
            return awaitable.IsCompleted
                ? awaitable.GetResult()
                : awaitable.AsTask().GetAwaiter().GetResult();
        }
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
    protected readonly struct DecoderAwaitable(ExecutionControl control, CancellationToken cancellationToken) : ICriticalNotifyCompletion
    {
        public DecoderAwaitable GetAwaiter() => this;
        public bool IsCompleted => control.IsDecoderReady;

        // Only valid after IsCompleted is true — MVTSC has no blocking GetResult, so a Pending
        // status throws InvalidOperationException. The C# compiler guarantees the IsCompleted
        // check for `await` syntax; direct callers must check it themselves.
        // Also throws OCE if the token raced ahead of activation completing.
        public PgDecoder GetResult()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return control.GetDecoderResult();
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
        public bool IsCompleted => control.IsDecoderReady;

        public PgDecoder GetResult()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return control.GetDecoderResult();
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

        public void Bind(CancellationToken abortToken, TimeSpan activationTimeout)
        {
            if (flow._isAsync is not { } async)
            {
                ThrowHelper.ThrowInvalidOperation("IsAsync was not set by flow before it was queued.");
                return;
            }

            flow._abortToken = abortToken;
            flow._isAsyncAtBind = async;
            flow._remainingActivationTimeout = activationTimeout;
        }

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
                && flow._abortToken?.IsCancellationRequested == false
                && !flow._activationCancellationTokenRegistration.Token.IsCancellationRequested)
                ThrowHelper.ThrowInvalidOperation("Flow was already activated unexpectedly.");
        }

        public void RegisterDecoderOnHeartbeat(Action<TimeSpan> action)
        {
            flow._decoderOnHeartbeatAction = action;
        }

        public void OnHeartbeat(TimeSpan interval)
        {
            if (flow._activationTaskSource.GetStatus(flow._activationTaskSource.Version) is ValueTaskSourceStatus.Pending
                && (flow._remainingActivationTimeout -= interval) <= TimeSpan.Zero)
                flow._activationTaskSource.TrySetException(new TimeoutException("Operation timed out waiting for activation."), runContinuationsAsynchronously: true);

            flow._decoderOnHeartbeatAction?.Invoke(interval);
            flow.OnHeartbeat(interval);
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
            if (!ReferenceEquals(flow, control.ExecutorFlow))
                ThrowHelper.ThrowInvalidOperation(
                    "Flow cannot write anymore. All writes must happen during the first execution phase " +
                    "which ends after the Execute method returns the inner task.");
        }

        // Activation-task-source primitives surfaced as "decoder ready / on decoder" because that's
        // what consumers care about. Keeps the version token internal. Exposed publicly through
        // Context as the DecoderAwaitable.
        public bool IsDecoderReady
            => flow._activationTaskSource.GetStatus(flow._activationTaskSource.Version) is ValueTaskSourceStatus.Succeeded;
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
