using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg.Protocol;

abstract class PgClientFlow : IProtocolFlow, IValueTaskSource<PgDecoder>, IValueTaskSource
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

    // Pipeline state.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _executePipelinedCore;
    ValueTaskSourcePromise<bool>? _pipelinePromise;
    Context _context;
    ValueTask _task;

    // Completion state.
    Action<PgClientFlow, Exception?, object?>? _completionAction;
    object? _completionState;

    protected abstract ValueTask<FlowTasks> Execute(Context context);

    protected virtual ValueTask ExecutePipelined(Context context)
        => ValueTask.FromException(new NotSupportedException("Flow has no implementation of ExecutePipelined."));

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

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _executePipelinedCore.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _executePipelinedCore.OnCompleted(continuation, state, token, flags);

    void IValueTaskSource.GetResult(short token)
        => _executePipelinedCore.GetResult(token);

    protected readonly struct Context
    {
        readonly ExecutionControl _executionControl;
        internal Context(ExecutionControl executionControl)
            => _executionControl = executionControl;

        public ref readonly TState GetProtocolStatic<TState>()
            => ref _executionControl.GetProtocolStatic<TState>();

        public PgEncoder GetEncoder()
            => _executionControl.GetEncoder();

        public PgDecoder GetDecoder()
        {
            var task = _executionControl.GetActivationResultAsync();
            if (task.IsCompletedSuccessfully)
                return task.Result;

            // This will stall the pipeline and informs the runtime we have a thread blocking on a task completion.
            // https://learn.microsoft.com/en-us/dotnet/core/runtime-config/threading#thread-injection-in-response-to-blocking-work-items
            return task.AsTask().GetAwaiter().GetResult();
        }

        public ValueTask<PgDecoder> GetDecoderAsync(CancellationToken cancellationToken = default)
            => _executionControl.GetActivationResultAsync(cancellationToken);

        public ValueTask<PgDecoder> GetDecoderAuto(CancellationToken cancellationToken = default)
            => _executionControl.IsAsync ? GetDecoderAsync(cancellationToken) : new(GetDecoder());

        public ValueTask ExecutePipelinedWithPromise(ValueTaskSourcePromise<bool> promise)
            => _executionControl.ExecutePipelinedWithPromise(promise);
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

        public ValueTask<FlowTasks> Execute()
            => IsAsync ? flow.Execute(new(this)) : ExecuteSynchronously();

        // For ease of debugging we add a stackframe that tells us whether we're a sync flow.
        [MethodImpl(MethodImplOptions.NoInlining)]
        ValueTask<FlowTasks> ExecuteSynchronously() => flow.Execute(new(this));

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

        public ValueTask<PgDecoder> GetActivationResultAsync(CancellationToken cancellationToken = default)
        {
            if (flow._activationTaskSource.GetStatus(flow._activationTaskSource.Version) is not ValueTaskSourceStatus.Pending)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return new(flow._activationTaskSource.GetResult(flow._activationTaskSource.Version));
            }

            if (cancellationToken.CanBeCanceled)
            {
                if (flow._activationCancellationTokenRegistration != default)
                    ThrowHelper.ThrowInvalidOperation("Concurrent activation result awaits are not supported.");

                flow._activationCancellationTokenRegistration = cancellationToken.UnsafeRegister(
                    static (state, token) =>
                        ((PgClientFlow)state!)._activationTaskSource.TrySetException(new OperationCanceledException(token), runContinuationsAsynchronously: true),
                    flow);
            }

            return new ValueTask<PgDecoder>(flow, flow._activationTaskSource.Version);
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

        public ValueTask ExecutePipelinedWithPromise(ValueTaskSourcePromise<bool> promise)
        {
            var version = flow._activationTaskSource.Version;
            if (flow._activationTaskSource.GetStatus(version) is ValueTaskSourceStatus.Succeeded)
            {
                PromiseAsyncValueTaskMethodBuilder.Promise = promise;
                try
                {
                    return flow.ExecutePipelined(new(this));
                }
                finally
                {
                    PromiseAsyncValueTaskMethodBuilder.Promise = null;
                }
            }

            flow._context = new(this);
            flow._pipelinePromise = promise;
            ((IValueTaskSource<PgDecoder>)flow).OnCompleted(static state =>
            {
                var flow = (PgClientFlow)state!;
                var promise = flow._pipelinePromise;
                var context = flow._context;
                PromiseAsyncValueTaskMethodBuilder.Promise = promise;
                try
                {
                    var task = flow.ExecutePipelined(context);
                    if (!task.IsCompleted)
                    {
                        flow._task = task;
                        ((IValueTaskSource)promise!).OnCompleted(static state =>
                        {
                            var flow = (PgClientFlow)state!;
                            try
                            {
                                flow._task.GetAwaiter().GetResult();
                                flow._executePipelinedCore.SetResult(true);
                            }
                            catch (Exception ex)
                            {
                                flow._executePipelinedCore.SetException(ex);
                            }
                        }, flow, promise.Token, ValueTaskSourceOnCompletedFlags.FlowExecutionContext);
                    }
                    else
                    {
                        try
                        {
                            task.GetAwaiter().GetResult();
                            flow._executePipelinedCore.SetResult(true);
                        }
                        catch (Exception ex)
                        {
                            flow._executePipelinedCore.SetException(ex);
                        }
                    }
                }
                finally
                {
                    PromiseAsyncValueTaskMethodBuilder.Promise = null;
                }
            }, flow, version, ValueTaskSourceOnCompletedFlags.FlowExecutionContext);

            return new(flow, flow._executePipelinedCore.Version);
        }
    }
}
