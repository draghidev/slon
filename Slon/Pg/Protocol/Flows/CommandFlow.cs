using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;
using Slon.Pg.Serialization;
// A unique result type distinguishes the caller gate from this flow's other IValueTaskSource faces.
using FlowCallerInteractionCoreResult = System.ValueTuple;

namespace Slon.Pg.Protocol.Flows;

abstract class CommandFlowObserver : PgClientFlowObserver
{
    internal virtual void OnStarted(CommandFlow flow, object? state) { }
    internal virtual void OnCommandResult(CommandFlow flow, CommandResult result, object? state) { }
    internal virtual void OnSemanticDrainStarted(CommandFlow flow, object? state) { }
}

readonly struct CommandFlowOptions
{
    public CommandFlowObserver? Observer { get; init; }
    public object? ObserverState { get; init; }
    public CommandList Commands { get; init; }
    // Captured while the ADO command is bound. Catalog reload publishes a new options instance,
    // while this flow continues against the immutable revision it resolved with.
    public PgSerializerOptions? SerializerOptions { get; init; }
    public ParameterWriterStrategy? ParameterWriterStrategy { get; init; }
    // Optional per-flow override for time spent waiting in the protocol backlog.
    public TimeSpan? PendingTimeout { get; init; }
}

abstract class CommandFlowBindingStrategy
{
    internal abstract CommandFlowOptions Bind(
        PgClientFlowBindingContext context, in CommandFlowBinding binding, TimeSpan? pendingTimeout);
}

readonly struct CommandFlowBinding
{
    internal CommandFlowBindingStrategy? Strategy { get; init; }
    internal object? Owner { get; init; }
    internal object? Parameters { get; init; }
    internal object? Dependencies { get; init; }
    internal nint Getter { get; init; }
    internal int Behavior { get; init; }
    internal int CommandCount { get; init; }
    internal bool IsPreparing { get; init; }
}

sealed partial class CommandFlow : PgClientFlow, IValueTaskSource<bool>, IValueTaskSource<FlowCallerInteractionCoreResult>, IValueTaskSource
{
    static readonly TimeSpan ConsumerDrainCancellationGracePeriod = TimeSpan.FromSeconds(1);

    internal override bool DefersSyncHandoff => true;

    internal enum CancellationScope : byte
    {
        None,
        CurrentWindow = 1,
        RemainingFlow = 2
    }

    // Flow state
    CommandFlowOptions _options;
    readonly CommandFlowBinding _binding;
    FlowCallerInteractionCore<FlowCallerInteractionCoreResult> _callerInteractionCore;
    // Per-read caller token: overlaid by MoveNextAsync(ct) for a single read.
    CancellationToken _callerCancellationToken;
    // Flow-scoped token, bound at submission or enumeration and retained across results.
    CancellationToken _flowCancellationToken;
    CancellationTokenRegistration _callerCancellationTokenRegistration;
    CancellationTokenRegistration _flowCancellationTokenRegistration;
    // Callbacks only latch cancellation and wake the body. The body delivers cancellation at its
    // terminal, after releasing pipeline promise tenure.
    bool _cancelRequested;
    int _cancellationScope;
    int _backendCancellationTiming;
    int _subsequentBackendCancellationTiming;
    // The token to carry on the terminal OCE, captured when the body observes the cancel.
    CancellationToken _cancelDeliverToken;
    TaskCompletionSource? _cancelDelivery;
    // Set by the body's cancel drain transition; the terminal SetResult delivers OCE (not a clean end).
    bool _deliverCancelOce;
    // Errors encountered while draining are surfaced by a waiting DisposeAsync. Live consumers observe
    // their errors directly, and the list remains unallocated on the successful path.
    List<Exception>? _drainErrors;
    // Set only by ConsumeNonQueryAsync, after enqueue but before any consumer-side gate release.
    // The body cannot reach first publication until such a release, and every release publishes
    // this write, so plain accesses suffice and the body observes the mode at first wake.
    bool _consumeNonQuery;
    bool IsConsumingNonQuery => _consumeNonQuery;
    bool IsConsumingAutonomously => IsDraining || IsConsumingNonQuery;
    long _nonQueryRecordsAffected;

    // Once draining, the body bypasses result handoffs and reads autonomously to RFQ. This is state, not
    // an I/O cancellation token: canceling the I/O would prevent restoration of a clean wire boundary.
    bool _draining;
    internal bool IsDraining => Volatile.Read(ref _draining);
    // Body-thread-only guard: later commands must not change the drive mode chosen on drain entry.
    bool _drainModeEntered;


    // Distinguishes consumer disposal from a body-initiated drain; disposal suppresses terminal OCE delivery.
    bool _consumerDisposed;
    // When true, DisposeAsync awaits the body's drain to RFQ. Otherwise it returns while the body drains;
    // pipeline retirement still prevents the next flow from observing a dirty wire.
    internal bool WaitForDrainOnDispose { get; set; } = true;

    // Consumer disposal without waiting for the autonomous drain.
    void MarkConsumerGone()
    {
        Volatile.Write(ref _consumerDisposed, true);
        RequestCancel(default, CancellationScope.RemainingFlow, BackendCancellationTiming.AfterGrace,
            BackendCancellationTiming.AtReadFrontier);
    }

    // Consumer disposal while waiting for the autonomous drain.
    void MarkConsumerWaitForDrain()
    {
        Volatile.Write(ref _consumerDisposed, true);
        RequestCancel(default, CancellationScope.RemainingFlow, BackendCancellationTiming.AfterGrace,
            BackendCancellationTiming.AtReadFrontier);
    }

    // A synchronous consumer cannot abandon its drive obligation while the body is parked. Give the
    // cancellation path a delivery source so WakeBody transfers that obligation to the dedicated driver.
    void MarkSyncConsumerGone()
    {
        Volatile.Write(ref _consumerDisposed, true);
        _ = GetOrCreateCancelDelivery();
        RequestCancel(default, CancellationScope.RemainingFlow, BackendCancellationTiming.AfterGrace,
            BackendCancellationTiming.AtReadFrontier);
        _callerInteractionCore.ResumeBody(runContinuationsAsynchronously: true);
        _callerInteractionCore.WakeBody(useDedicatedDriver: true);
    }

    // Enumeration already ended; only transfer the live body tail to autonomous ownership.
    // There is no unfinished consumer work to justify a backend cancellation intent.
    void MarkTerminalConsumerGone()
    {
        Volatile.Write(ref _consumerDisposed, true);
        Volatile.Write(ref _draining, true);
    }

    // A body-initiated drain keeps the consumer attached for terminal cancellation or close delivery.
    void MarkBodyInitiatedDrain() => Volatile.Write(ref _draining, true);

    // Result-production state
    RowDescription? _requestedRowDescription;
    PgError? _pgError;
    int _commandIndex = -1;
    PgDecoder? _decoder;
    bool _readFlowRfq;

    // Pipelined dispatch state. Lives here (not on PgClientFlow base) because the shared-promise
    // optimization that needs these fields is CommandFlow-specific (see DispatchPipelinedRead).
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _executePipelinedCore;
    ValueTaskSourcePromise<bool>? _pipelinePromise;
    Context _context;
    bool _contextPublished;
    object? _cancellationEpisodeKey;
    ValueTask _task;
    // Consumer terminality, body terminality, and framework release are distinct phases. Start and
    // pre-start termination race during shutdown, so one atomic state owns that decision.
    const int BodyNotStarted = 0;
    const int BodyRunning = 1;
    const int BodyTerminated = 2;
    int _bodyState;
    CommandFlow() : base(supportsDeferredFlush: true)
    {
        _callerInteractionCore.Initialize();
    }

    // Interactive commands carry caller patience, so arm the activation timeout.
    protected override bool EnableActivationTimeout => true;
    protected override TimeSpan? PendingTimeout => _options.PendingTimeout;
    internal override TimeSpan? BackendCancellationGracePeriod
        => Volatile.Read(ref _consumerDisposed) ? ConsumerDrainCancellationGracePeriod : null;

    public CommandFlow(bool async, params ReadOnlySpan<Command> commands) : this()
        => Initialize(async, commands);
    public CommandFlow(bool async, in CommandFlowOptions options) : this()
        => Initialize(async, options);

    internal CommandFlow(bool async, in CommandFlowBinding binding, TimeSpan? pendingTimeout = null) : this()
    {
        IsAsync = async;
        _binding = binding;
        _options = new() { PendingTimeout = pendingTimeout };
        _enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
    }

    internal override void Bind(PgClientFlowBindingContext? context)
    {
        if (_binding.Strategy is not { } strategy)
            return;
        if (context is null)
            throw new InvalidOperationException("The command flow requires a wire binding context.");
        Initialize(IsAsync, strategy.Bind(context, _binding, _options.PendingTimeout));
    }

    public CommandFlow Initialize(bool async, params ReadOnlySpan<Command> commands)
        => Initialize(async, options: new() { Commands = new(commands) });

    public CommandFlow Initialize(bool async, in CommandFlowOptions options)
    {
        IsAsync = async;
        if (options.Observer is { } observer)
            SetObserver(observer, options.ObserverState);
        _options = new()
        {
            Commands = options.Commands,
            SerializerOptions = options.SerializerOptions,
            ParameterWriterStrategy = options.ParameterWriterStrategy,
            PendingTimeout = options.PendingTimeout
        };
        options.Observer?.OnStarted(this, options.ObserverState);
        // Arm before publication: teardown may complete the source concurrently even before enumeration.
        _enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
        return this;
    }

    // Declares internal non-query consumption and runs it to completion. The single entry point
    // makes the ownership rule structural: no enumerator is exposed on this path, so mixing
    // enumeration with internal consumption is unrepresentable. The declaration precedes any
    // consumer-side gate release, so the body observes it at first wake and never publishes.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<long> ConsumeNonQueryAsync(CancellationToken cancellationToken = default)
    {
        _consumeNonQuery = true;
        var enumerator = GetAsyncEnumerator(cancellationToken);
        try
        {
            // Release a body parked pre-publication and wake the flow.
            _callerInteractionCore.ResumeBody(runContinuationsAsynchronously: false);
            _callerInteractionCore.WakeBody();
            _ = await EnumeratorMoveNextTask.ConfigureAwait(false);
            // Internal consumption owns error delivery. Errors collected during the internal
            // drain have no other outlet: DisposeAsync deliberately skips a completed flow.
            if (_drainErrors is { Count: > 0 } errors)
                throw errors.Count == 1 ? errors[0] : new AggregateException(errors);
            return _nonQueryRecordsAffected;
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    // The token in force for the current read: the flow token once fired (whole-flow cancel), else the
    // per-read token if cancelable, else the flow token.
    CancellationToken EffectiveCancellationToken
        => _flowCancellationToken.IsCancellationRequested ? _flowCancellationToken
            : _callerCancellationToken.CanBeCanceled ? _callerCancellationToken
            : _flowCancellationToken;

    public int CommandCount => _options.Commands.Count;
    internal int VisibleCommandCount
        => _binding.CommandCount is > 0 ? _binding.CommandCount : _options.Commands.VisibleCount;
    public bool IsResultReady => _isResultReady;

    public Enumerator GetEnumerator()
    {
        return new Enumerator(this);
    }

    // Bind at submission because eager writing precedes the first MoveNextAsync.
    internal override void BindCallerToken(CancellationToken cancellationToken) => _flowCancellationToken = cancellationToken;
    internal override CancellationToken MigrationCancellationToken => _flowCancellationToken;

    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        // Body, teardown, and cancellation may complete the source concurrently. A missing per-call token
        // must not replace the flow token captured at submission.
        _enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
        if (cancellationToken.CanBeCanceled)
            _flowCancellationToken = cancellationToken;
        return new(this);
    }

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        if (!IsAsync && _callerInteractionCore.IsWaiting)
            return ExecuteAfterHandoff(context);

        return new(ExecuteAutoCore(context));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<FlowTasks> ExecuteAfterHandoff(Context context)
    {
        try
        {
            await YieldToCaller();
        }
        catch (Exception ex)
        {
            TerminateBodyBeforeStart();
            CompleteEnumerationWithException(ex);
            throw;
        }

        return ExecuteAutoCore(context);
    }

    FlowTasks ExecuteAutoCore(Context context)
    {
        _context = context;
        Volatile.Write(ref _contextPublished, true);
        if (_flowCancellationToken.IsCancellationRequested)
            RequestCancel(_flowCancellationToken, CancellationScope.RemainingFlow);
        else if (Volatile.Read(ref _cancelRequested))
            RequestBackendCancellation((BackendCancellationTiming)Volatile.Read(ref _backendCancellationTiming),
                Volatile.Read(ref _cancelDelivery));
        ValueTask writeTask;
        try
        {
            // Writes are independent of consumer admission. Inter-result gates provide backpressure.
            // Async writes use transport completion; sync writes use the resumable non-blocking path so
            // the caller thread retains execution ownership across readiness waits.
            var encoder = IsAsync ? default : context.GetEncoder();
            var appendSync = !_options.Commands[CommandCount - 1].WithSync;
            _readFlowRfq = appendSync;
            if (IsAsync)
            {
                // Caller cancellation never cancels wire I/O. A partially cancelled write requires
                // protocol recovery and can strand already-pipelined successors; the body instead
                // observes the latched intent and drains every written command to RFQ.
                writeTask = _options.Commands.WriteCommandsAsync(context.GetEncoder(), appendSync,
                    _options.ParameterWriterStrategy ?? ParameterWriterStrategy.Raw, default);
            }
            else
            {
                using (encoder.BeginResumableScope())
                    writeTask = _options.Commands.WriteCommandsResumable(encoder, appendSync,
                        _options.ParameterWriterStrategy ?? ParameterWriterStrategy.Raw);
            }

            // Observe synchronous faults here; pending writes remain the framework-owned trailing task.
            if (writeTask.IsCompleted)
                writeTask.GetAwaiter().GetResult();
            else if (!IsAsync)
                writeTask = encoder.RunResumableTask(writeTask);
        }
        catch (Exception ex)
        {
            TerminateBodyBeforeStart();
            CompleteEnumerationWithException(ex);
            throw;
        }

        // Read and write run concurrently; the framework observes the trailing write before releasing
        // the flow, preserving single-writer tenure without blocking reads behind socket backpressure.
        return new FlowTasks(
            trailingExecutionTask: writeTask,
            pipelineTask: DispatchPipelinedRead(context, context.GetProtocolStatic<ReadState>().ReadPromise));
    }

    // Defer state-machine creation until activation because all flows share one protocol-static promise.
    ValueTask DispatchPipelinedRead(Context context, ValueTaskSourcePromise<bool> promise)
    {
        // The shared promise may be tenured only after successful decoder activation.
        var waiter = context.GetDecoderAsync().ConfigureAwait(false);
        if (waiter.IsCompleted)
        {
            // Only successful activation owns the shared promise. A settled fault belongs to this flow's
            // private completion source because it never claimed the wire.
            if (!waiter.IsCompletedSuccessfully)
            {
                // Preserve the activation's close, timeout, or cancellation identity.
                try { waiter.GetAwaiter().GetResult(); }
                catch (Exception ex) { _executePipelinedCore.SetException(ex); }
                return new ValueTask(this, _executePipelinedCore.Version);
            }
            // Handing the shared-promise-backed task to the framework is safe: the contract guarantees
            // the waiter is consumed (releasing the promise tenure via GetResult's Reset) before the
            // item's position is republished, so a successor's dispatch always finds the tenure released.
            PromiseAsyncValueTaskMethodBuilder.Promise = promise;
            try
            {
                return ExecutePipelined(context);
            }
            finally
            {
                PromiseAsyncValueTaskMethodBuilder.Promise = null;
            }
        }

        _pipelinePromise = promise;
        // Static continuation: a bridge into framework state, so no captured scheduling context is needed.
        waiter.OnCompleted(static state =>
        {
            var flow = (CommandFlow)state!;
            var ctx = flow._context;
            // A faulted activation never claimed the shared promise; complete only this flow's source.
            var activation = ctx.GetDecoderAsync().GetAwaiter();
            if (!activation.IsCompletedSuccessfully)
            {
                // Preserve the activation's close, timeout, or cancellation identity.
                try { activation.GetResult(); }
                catch (Exception ex) { flow._executePipelinedCore.SetException(ex); }
                return;
            }
            var promise = flow._pipelinePromise!;
            PromiseAsyncValueTaskMethodBuilder.Promise = promise;
            ValueTask task = flow.ExecutePipelined(ctx);
            try
            {
                if (!task.IsCompleted)
                {
                    flow._task = task;
                    ((IValueTaskSource)promise).OnCompleted(static state =>
                    {
                        var flow = (CommandFlow)state!;
                        try
                        {
                            flow._task.GetAwaiter().GetResult();
                            flow._executePipelinedCore.SetResult(true);
                        }
                        catch (Exception ex)
                        {
                            flow._executePipelinedCore.SetException(ex);
                        }
                        // This internal bridge runs no user code and requires no ExecutionContext flow.
                    }, flow, promise.Token, ValueTaskSourceOnCompletedFlags.None);
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
        }, this);

        return new ValueTask(this, _executePipelinedCore.Version);
    }

    [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder))]
    async ValueTask ExecutePipelined(Context context)
    {
        // Pre-start teardown may have already claimed terminality. In that case the consumer has its
        // fault and this late dispatch has no body tenure to establish.
        if (Interlocked.CompareExchange(ref _bodyState, BodyRunning, BodyNotStarted) != BodyNotStarted)
            return;
        try
        {
            // If we have a continuation stored we must already be on the caller thread,
            // otherwise we must make sure to unblock the executor (see comment in the write phase).
            // This first handoff is body execution too: close may fault it, so it belongs inside the
            // same terminal envelope that publishes the consumer fault and body termination.
            if (!IsAsync && !_callerInteractionCore.HasHandoff)
                await YieldToCaller();

            // User cancellation must not cancel activation or wire I/O. The flow observes it after
            // activation, drains itself to RFQ, then delivers OCE without invoking pipeline recovery.
            _decoder = await context.GetDecoderAuto().ConfigureAwait(false);
            var publishedResult = false;
            while (++_commandIndex < CommandCount)
            {
                _isResultReady = false;
                bool hasPreparedDescription;
                bool suppressEnumeration;
                bool describeForPreparation;
                {
                    ref readonly var command = ref _options.Commands.ItemRef(_commandIndex);
                    _decoder.UseReadTimeout(command.Timeout);
                    suppressEnumeration = command.SuppressEnumeration;
                    describeForPreparation = command.DescribeForPreparation;
                    hasPreparedDescription = command.Descriptor is { IsPrepared: true, PreparedRowDescription: not null }
                        && !command.DescribeOnly;
                }

                // Registrations only latch and wake; terminal delivery remains body-owned. Dispose them
                // before promise tenure ends so callbacks cannot reach the next flow. Do not rearm after
                // consumer disposal, where a persistent cancellation could escape its intended wait.
                if (_callerCancellationToken.CanBeCanceled && !IsDraining)
                {
                    Debug.Assert(IsAsync);
                    _callerCancellationTokenRegistration = _callerCancellationToken.UnsafeRegister(static (state, token)
                        => ((CommandFlow)state!).RequestCancelAndWake(token, CancellationScope.CurrentWindow), this);
                }
                if (_flowCancellationToken.CanBeCanceled && !IsDraining)
                {
                    _flowCancellationTokenRegistration = _flowCancellationToken.UnsafeRegister(static (state, token)
                        => ((CommandFlow)state!).RequestCancelAndWake(token, CancellationScope.RemainingFlow), this);
                }
                // After close, a fresh command must not consume bytes left by its predecessor. A draining
                // flow may continue reading its own response to restore RFQ.
                if (!IsDraining && context.IsProtocolClosed)
                    throw context.FlowTerminationException;

                ParameterTypeList describedParameterTypes = default;
                if (describeForPreparation)
                {
                    var rowDescription = context.GetProtocolStatic<ReadState>().RowDescription;
                    if (IsAsync)
                    {
                        (_pgError, describedParameterTypes, _requestedRowDescription) =
                            await _options.Commands.ItemRef(_commandIndex)
                                .ReadPreparationDescriptionAsync(_decoder, rowDescription).ConfigureAwait(false);
                    }
                    else
                    {
                        (_pgError, describedParameterTypes, _requestedRowDescription) =
                            _options.Commands.ItemRef(_commandIndex)
                                .ReadPreparationDescription(_decoder, rowDescription);
                    }
                }
                else if (IsAsync && hasPreparedDescription)
                {
                    // Prepared commands with a known description have the compact BindComplete ->
                    // DataRow/CommandComplete prelude. Await the decoder directly so a read wake resumes
                    // this outer body rather than a nested parser coroutine; the second message normally
                    // comes from the same batch and is consumed synchronously.
                    if (!_decoder.TryGetNext(out var message))
                    {
                        if (!await _decoder.MoveNextAsync().ConfigureAwait(false))
                            _decoder.ThrowUnexpectedEof();
                        message = _decoder.Current;
                    }

                    if (message.EnsureExpectedOrError(PgTypes.BackendType.BindComplete) is { } bindError)
                    {
                        _pgError = bindError;
                        _requestedRowDescription = null;
                    }
                    else
                    {
                        if (!_decoder.TryGetNext(out message))
                        {
                            if (!await _decoder.MoveNextAsync().ConfigureAwait(false))
                                _decoder.ThrowUnexpectedEof();
                            message = _decoder.Current;
                        }
                        message.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
                        _pgError = null;
                        _requestedRowDescription = null;
                    }
                }
                else if (IsAsync)
                {
                    var rowDescription = context.GetProtocolStatic<ReadState>().RowDescription;
                    var read = _options.Commands.ItemRef(_commandIndex).ReadUntilExecuteAsync(_decoder, rowDescription);
                    (_pgError, _requestedRowDescription) = await read.ConfigureAwait(false);
                }
                else
                {
                    var rowDescription = context.GetProtocolStatic<ReadState>().RowDescription;
                    (_pgError, _requestedRowDescription) = _options.Commands.ItemRef(_commandIndex)
                        .ReadUntilExecute(_decoder, rowDescription);
                }

                // A draining consumer cannot observe CommandResult, so retain read errors for disposal.
                var capturedThisCommand = false;
                var readErrorIsOwnCancellation = _pgError is { } readError
                    && IsOwnCancellation(readError);
                if (IsConsumingAutonomously && _pgError is { } readErrorToCapture
                    && !readErrorIsOwnCancellation)
                {
                    (_drainErrors ??= new()).Add(PgErrorException.Create(readErrorToCapture));
                    capturedThisCommand = true;
                }

                // Await in-flight callbacks before releasing shared promise tenure.
                if (_callerCancellationToken.CanBeCanceled)
                    await _callerCancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
                if (_flowCancellationToken.CanBeCanceled)
                    await _flowCancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
                // Cancellation switches to autonomous drain; terminal delivery follows RFQ and tenure release.
                if ((Volatile.Read(ref _cancelRequested) || EffectiveCancellationToken.IsCancellationRequested)
                    && !IsEnumerationCompleted)
                {
                    _cancelDeliverToken = EffectiveCancellationToken.IsCancellationRequested ? EffectiveCancellationToken : _cancelDeliverToken;
                    _deliverCancelOce = true;
                    if (!IsDraining)
                        MarkBodyInitiatedDrain();
                }

                CommandResult<ResultMessageEnumerator> result;
                {
                    ref readonly var readState = ref context.GetProtocolStatic<ReadState>();
                    readState.ResultMessageEnumerator.Initialize(this, _decoder);
                    result = _enumeratorCurrent ?? readState.CommandResult;

                    ref readonly var resultCommand = ref _options.Commands.ItemRef(_commandIndex);
                    var descriptor = resultCommand.Descriptor;
                    // We were preparing and we have no error from parse, make a prepared descriptor.
                    if (!descriptor.IsPrepared && !descriptor.CommandName.IsDefault
                        && (_pgError is not { } err || !err.Expected.Contains(PgTypes.BackendType.ParseComplete)))
                    {
                        descriptor = CommandDescriptor.CreatePrepared(
                            descriptor.CommandName,
                            describeForPreparation ? describedParameterTypes : descriptor.ParameterTypes,
                            _requestedRowDescription?.Preserve());
                    }
                    result.Initialize(_commandIndex, descriptor, _requestedRowDescription,
                        !resultCommand.DescribeOnly, resultCommand.IsSimple(), _options.SerializerOptions,
                        _decoder.ClientEncoding, _pgError);
                }
                ((CommandFlowObserver?)GetObserver(out var observerState))
                    ?.OnCommandResult(this, result, observerState);

                // Disposal drains without another result handoff. Graceful close instead faults the
                // attached consumer, then uses the same autonomous drain. Command errors remain results.
                if (context.StoppingToken.IsCancellationRequested && !IsDraining
                    && !IsEnumerationCompleted)
                {
                    // Latch the close (a consumer that Resets past this point self-delivers it), wake a
                    // parked consumer, then drain.
                    var close = context.FlowTerminationException;
                    _callerInteractionCore.SetCloseLatch(close);
                    CompleteEnumerationWithClose(close);
                    MarkBodyInitiatedDrain();
                }
                var consumeInternally = IsConsumingNonQuery || suppressEnumeration;
                if (!IsDraining && !consumeInternally)
                {
                    // Eager async execution must wait for the consumer to arm generation zero before
                    // publishing its first result. Synchronous execution already runs on that caller.
                    if (!publishedResult && IsAsync)
                    {
                        await _callerInteractionCore.WaitForCaller(this).ConfigureAwait(false);
                        EnterStoppingDrainIfNeeded(context);
                    }

                    if (!IsDraining && !IsConsumingNonQuery)
                    {
                        _isResultReady = true;
                        publishedResult = true;
                        // Result continuations run asynchronously so the body can reach the next gate
                        // before user code asks for the next result. Buffered batches then advance inline
                        // from MoveNextAsync instead of suspending one Task state machine per result.
                        SetResult(result);

                        if (!IsDraining && !IsConsumingNonQuery)
                        {
                            if (IsAsync)
                            {
                                await _callerInteractionCore.WaitForCaller(this).ConfigureAwait(false);
                                EnterStoppingDrainIfNeeded(context);
                            }
                            else
                                await YieldToCaller();

                            /* The next MoveNext or MoveNextAsync call resumes here. */
                        }
                    }
                }
                else if (!_drainModeEntered && IsAsyncAtDispatch && !IsAsync)
                {
                    // An async I/O wake raced a synchronous disposer before the body reached its handoff.
                    if (WaitForDrainOnDispose)
                    {
                        // Hand the continuation to the disposer, which waits on the rendezvous rather than
                        // this task and can therefore drive the remaining drain without sync-over-async.
                        await YieldToCaller();
                    }
                    else
                    {
                        // No disposer is waiting to drive; retain asynchronous background draining.
                        IsAsync = IsAsyncAtDispatch;
                    }
                }
                // Preserve the drive mode chosen on first drain entry.
                _drainModeEntered = _drainModeEntered || IsDraining;

                // Disposing the message enumerator completes the command and, in drain mode, consumes its
                // remaining rows. Re-read the current execution mode after every resumption.
                (PgError Error, TransactionStatus TransactionStatus)? completeError;
                // Consumption mode may change while the body is suspended; use the current value.
                if (consumeInternally || IsConsumingNonQuery)
                {
                    while (_decoder.Current.Header.Type is PgTypes.BackendType.DataRow)
                    {
                        if (!_decoder.TryGetNext(out _))
                            await _decoder.GetNextAsync().ConfigureAwait(false);
                    }
                    result.CompleteNonQuery(_decoder.Current);
                    var completion = _options.Commands.ItemRef(_commandIndex).CompleteAsync(_decoder);
                    completeError = await completion.ConfigureAwait(false);
                    if (_pgError is null && completeError is null)
                        _nonQueryRecordsAffected = checked(_nonQueryRecordsAffected + result.RecordsAffected);
                }
                else if (IsAsync)
                {
                    var resultEnumerator = context.GetProtocolStatic<ReadState>().ResultMessageEnumerator;
                    await resultEnumerator.DisposeAsync().ConfigureAwait(false);
                    completeError = resultEnumerator.CompleteError;
                }
                else
                {
                    var resultEnumerator = context.GetProtocolStatic<ReadState>().ResultMessageEnumerator;
                    resultEnumerator.Dispose();
                    completeError = resultEnumerator.CompleteError;
                }

                if (_options.Commands.ItemRef(_commandIndex).DescribeOnly && _pgError is null)
                    result.CompleteDescribe(completeError?.Error);

                var resultErrorIsOwnCancellation = result.Error is { } resultError
                    && IsOwnCancellation(resultError);
                if (suppressEnumeration && result.Error is { } suppressedError
                    && !resultErrorIsOwnCancellation)
                {
                    (_drainErrors ??= new()).Add(PgErrorException.Create(suppressedError));
                    capturedThisCommand = true;
                    if (!IsDraining)
                        MarkBodyInitiatedDrain();
                }

                {
                    // Accumulate each command's fresh error while draining, but do not duplicate an error
                    // already captured during its read phase or delivered to a live consumer.
                    var completeErrorIsOwnCancellation = completeError is { } completedWithError
                        && IsOwnCancellation(completedWithError.Error);
                    if ((consumeInternally || IsConsumingNonQuery || IsDraining && !_isResultReady)
                        && !capturedThisCommand && completeError is { } err
                        && !completeErrorIsOwnCancellation)
                        (_drainErrors ??= new()).Add(PgErrorException.Create(err.Error));
                }

                // Extended-query errors discard every following command through the next Sync. Skip
                // those commands locally and consume the RFQ which is their only wire response.
                if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                {
                    while (++_commandIndex < CommandCount && !_options.Commands[_commandIndex].WithSync) { }

                    if (IsAsync)
                        await ReadRfqAsync(_decoder).ConfigureAwait(false);
                    else
                        ReadRfq(_decoder);

                    // Reaching the end means the discarded segment terminated at our appended Sync.
                    if (_commandIndex == CommandCount)
                        _readFlowRfq = false;
                }
            }

            // The framework observes trailing write failure before releasing this flow.
            if (_readFlowRfq)
            {
                if (_decoder.TryGetNext(out var message))
                {
                    if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                        PgErrorException.Throw(rfqError);
                }
                else if (IsAsync)
                {
                    await ReadRfqAsync(_decoder).ConfigureAwait(false);
                }
                else
                {
                    ReadRfq(_decoder);
                }
            }

            SetResult(null);
        }
        catch (PgClientClosedException) when (context.IsProtocolClosed)
        {
            // Scope to our own closure so a nested protocol's close doesn't get treated as ours.
            // Latch the close so a consumer that Resets after this point self-delivers it.
            _callerInteractionCore.SetCloseLatch(context.FlowTerminationException);
            // A detached consumer treats close as drain completion; a live consumer observes the close.
            if (IsDraining)
            {
                if (!IsEnumerationCompleted)
                    SetResult(null);
                return;
            }
            CompleteEnumerationWithException(context.FlowTerminationException);
            throw;
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _callerCancellationToken || ex.CancellationToken == _flowCancellationToken)
        {
            CompleteEnumerationWithException(ex);
            throw;
        }
        catch (TimeoutException ex)
        {
            CompleteEnumerationWithException(ex);
            RequestCancel(default, CancellationScope.RemainingFlow, BackendCancellationTiming.Immediate,
                BackendCancellationTiming.AtReadFrontier, allowCompletedEnumeration: true);
            if (context.IsProtocolClosed)
                throw;

            // The timeout is terminal for the consumer, not for the body. Keep ownership of the
            // command sequence and drain every remaining RFQ window; reaching each RFQ requests
            // cancellation for the next window through OnCancellationWindowCompleted. Recovery is
            // reserved for a failure of this semantic drain, where only wire obligations remain.
            if (_callerCancellationToken.CanBeCanceled)
                await _callerCancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
            if (_flowCancellationToken.CanBeCanceled)
                await _flowCancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
            ((CommandFlowObserver?)GetObserver(out var observerState))
                ?.OnSemanticDrainStarted(this, observerState);
            try
            {
                while (context.OutstandingRfqCount != 0)
                    _ = await _decoder!.GetNextAuto().ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // The semantic drain owns the same cancellation episode. A timeout here would
                // otherwise bypass the outer catch and leave the episode unaware that its first
                // read-timeout escalation produced no protocol progress.
                RequestCancel(default, CancellationScope.RemainingFlow,
                    BackendCancellationTiming.Immediate, BackendCancellationTiming.AtReadFrontier,
                    allowCompletedEnumeration: true);
                throw;
            }
            return;
        }
        catch (Exception ex)
        {
            CompleteEnumerationWithException(ex);
            throw;
        }
        finally
        {
            // The body is the sole owner of protocol-static row metadata. Recovery consumes only
            // decoder/wire state, so a faulted body can release oversized storage while recovery
            // retains the failed flow's framework tenure.
            context.GetProtocolStatic<ReadState>().RowDescription.PrepareForReuse();
            PublishBodyTerminated();
        }
        void SetResult(CommandResult<ResultMessageEnumerator>? next)
        {
            var completed = next is null;
            if (completed)
            {
                _enumeratorCurrent = null;
            }
            else
            {
                if (_callerCancellationToken.CanBeCanceled)
                    _callerCancellationToken = CancellationToken.None;

                if (!ReferenceEquals(_enumeratorCurrent, next))
                    _enumeratorCurrent = next;

            }

            // Close is durable across generations; complete the current one without publishing a result.
            if (_callerInteractionCore.CloseException is not null)
            {
                CompleteEnumerationWithClose(_callerInteractionCore.CloseException);
                return;
            }
            if (completed)
            {
                // Publish durable terminal state and complete the current generation atomically with
                // respect to consumer rearming. Completion dispatches asynchronously, so it cannot reenter
                // this lock or the pipeline frame that still owns the shared promise.
                using (_rearmLock.EnterScope())
                {
                    PublishEnumerationCompleted();
                    CompleteEnumeration();
                }
                return;
            }
            _enumeratorMoveNextTaskSource.SetResult(true, runContinuationsAsynchronously: true);
        }

        async ValueTask ReadRfqAsync(PgDecoder decoder)
        {
            var message = await decoder.GetNextAsync().ConfigureAwait(false);
            if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                PgErrorException.Throw(rfqError);
        }

        static void ReadRfq(PgDecoder decoder)
        {
            var message = decoder.GetNext();
            if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                PgErrorException.Throw(rfqError);
        }
    }

    // Cancellation callbacks only latch intent and wake the body. The body delivers cancellation after
    // it has restored the wire boundary and is ready to release execution tenure.
    internal Task CancelAsync()
    {
        var delivery = GetOrCreateCancelDelivery();
        if (IsCompleted)
        {
            delivery.TrySetResult();
            return delivery.Task;
        }
        RequestCancelAndWake(default, CancellationScope.RemainingFlow);
        if (IsCompleted)
            delivery.TrySetResult();
        return delivery.Task;
    }

    TaskCompletionSource GetOrCreateCancelDelivery()
    {
        var delivery = Volatile.Read(ref _cancelDelivery);
        if (delivery is not null)
            return delivery;
        var created = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        return Interlocked.CompareExchange(ref _cancelDelivery, created, null) ?? created;
    }

    bool RequestCancel(CancellationToken token, CancellationScope scope,
        BackendCancellationTiming timing = BackendCancellationTiming.AfterGrace,
        BackendCancellationTiming subsequentTiming = BackendCancellationTiming.AfterGrace,
        bool allowCompletedEnumeration = false)
    {
        if (IsEnumerationCompleted && !allowCompletedEnumeration)
            return false;
        _cancelDeliverToken = token;
        var observedScope = Volatile.Read(ref _cancellationScope);
        while ((int)scope > observedScope)
        {
            var priorScope = Interlocked.CompareExchange(ref _cancellationScope, (int)scope, observedScope);
            if (priorScope == observedScope)
                break;
            observedScope = priorScope;
        }
        Volatile.Write(ref _cancelRequested, true);
        Volatile.Write(ref _draining, true);
        var observedTiming = Volatile.Read(ref _backendCancellationTiming);
        while ((int)timing > observedTiming)
        {
            var priorTiming = Interlocked.CompareExchange(ref _backendCancellationTiming, (int)timing, observedTiming);
            if (priorTiming == observedTiming)
                break;
            observedTiming = priorTiming;
        }
        var observedSubsequentTiming = Volatile.Read(ref _subsequentBackendCancellationTiming);
        while ((int)subsequentTiming > observedSubsequentTiming)
        {
            var priorTiming = Interlocked.CompareExchange(ref _subsequentBackendCancellationTiming,
                (int)subsequentTiming, observedSubsequentTiming);
            if (priorTiming == observedSubsequentTiming)
                break;
            observedSubsequentTiming = priorTiming;
        }
        var delivery = Volatile.Read(ref _cancelDelivery);
        RequestBackendCancellation(timing, delivery);
        return true;
    }

    void RequestCancelAndWake(CancellationToken token, CancellationScope scope)
    {
        if (!RequestCancel(token, scope))
            return;
        var delivery = Volatile.Read(ref _cancelDelivery);
        _callerInteractionCore.ResumeBody(runContinuationsAsynchronously: true);
        _callerInteractionCore.WakeBody(useDedicatedDriver: !IsAsync && delivery is not null);
    }

    void RequestBackendCancellation(BackendCancellationTiming timing = BackendCancellationTiming.AfterGrace,
        TaskCompletionSource? delivery = null)
    {
        // Cancellation which wins before body entry remains latched in _cancelRequested and is
        // replayed immediately after the execution context is published.
        if (Volatile.Read(ref _contextPublished))
        {
            var key = Volatile.Read(ref _cancellationEpisodeKey);
            if (key is null)
            {
                var created = new object();
                key = Interlocked.CompareExchange(ref _cancellationEpisodeKey, created, null) ?? created;
            }
            _context.RequestBackendCancellation(this, CancellationWindow, timing, delivery,
                key, Volatile.Read(ref _cancellationScope),
                (BackendCancellationTiming)Volatile.Read(ref _subsequentBackendCancellationTiming));
        }
    }

    protected override void OnCancellationWindowCompleted(int completedWindow, int remainingWindowCount)
    { }

    bool IsOwnCancellation(PgError error)
    {
        if (!Volatile.Read(ref _cancelRequested) || error.SqlState != PgErrorCodes.QueryCanceled)
            return false;
        // PgDecoder records each ErrorResponse arrival exactly once. Classification may inspect the
        // preserved error through several command-result paths, so it must not report the same strike.
        return true;
    }

    void EnterStoppingDrainIfNeeded(Context context)
    {
        if (_callerInteractionCore.CloseException is { } close && context.StoppingToken.IsCancellationRequested
            && !IsDraining && !IsEnumerationCompleted)
        {
            CompleteEnumerationWithException(close);
            MarkBodyInitiatedDrain();
        }
    }

    // Publish progress unconditionally. A terminal delivery can lose its task-source CAS to an already-
    // completed generation; a synchronous disposer must still observe this sticky level rather than park.
    void SignalPumpProgress()
        => _callerInteractionCore.SignalProgress();

    void CompleteEnumerationWithException(Exception ex)
    {
        // Close state must survive task-source rearming, including flows whose body never started.
        if (ex is PgClientClosedException or PgCollateralException)
            _callerInteractionCore.SetCloseLatch(ex);
        if (IsEnumerationCompleted)
            return;
        // Teardown may race the consumer. The task source is the completion authority;
        // _enumeratorCompleted follows only when this call wins the current generation.
        if (_enumeratorMoveNextTaskSource.TrySetException(ex, runContinuationsAsynchronously: true))
            PublishEnumerationCompleted();
        // A faulted body will not publish another continuation.
        SignalPumpProgress();
        // Wire recovery remains with the body or the framework recovery flow; this method only completes
        // the consumer-facing generation.
    }

    void PublishBodyTerminated()
    {
        Volatile.Write(ref _bodyState, BodyTerminated);
        SignalPumpProgress();
    }

    bool TerminateBodyBeforeStart()
        => Interlocked.CompareExchange(ref _bodyState, BodyTerminated, BodyNotStarted) == BodyNotStarted;

    bool IsBodyRunning => Volatile.Read(ref _bodyState) == BodyRunning;
    bool IsBodyTerminated => Volatile.Read(ref _bodyState) == BodyTerminated;

    // Source handoff finishes before body/consumer rendezvous begins, so both reuse the same wait event.
    protected override ManualResetEventSlim? HandoffEvent => _callerInteractionCore.GetWaitEvent();

    // Return the rendezvous directly. An async wrapper could signal the disposer before registering the
    // body's continuation, causing a late ThreadPool dispatch instead of caller-thread drain execution.
    FlowCallerInteractionCore<FlowCallerInteractionCoreResult>.CallerHandoffAwaitable YieldToCaller()
    {
        FieldRef<FlowCallerInteractionCore<FlowCallerInteractionCoreResult>> fieldRef;
        unsafe
        {
            fieldRef = FieldRef<FlowCallerInteractionCore<FlowCallerInteractionCoreResult>>.Create(&GetCallerInteractionCore, this);
        }
        return _callerInteractionCore.YieldToCaller(fieldRef);
    }

    static ref FlowCallerInteractionCore<FlowCallerInteractionCoreResult> GetCallerInteractionCore(CommandFlow instance)
        => ref instance._callerInteractionCore;

    protected override void OnAbort(Exception exception) => FaultCaller(exception);

    // Graceful stopping is the early wire-close wake and is idempotent across heartbeat ticks.
    protected override void OnStopping(Exception exception)
    {
        if (!IsBodyRunning || !IsAsync)
        {
            FaultCaller(exception);
            return;
        }

        // Resume normally so the body observes the close latch and drains; abort faults the gate.
        _callerInteractionCore.SetCloseLatch(exception);
        _callerInteractionCore.ResumeBody(runContinuationsAsynchronously: true);
    }

    // Wake a running body so it owns fault delivery; directly fault a flow whose body never started.
    void FaultCaller(Exception exception)
    {
        if (TerminateBodyBeforeStart())
        {
            CompleteEnumerationWithException(exception);
            // A synchronous flow may already have entered ExecuteAfterHandoff while its inner read
            // body is still NotStarted. Terminating that body does not complete the outer execution:
            // fault and wake its initial handoff so the framework task can settle and release tenure.
            _callerInteractionCore.FaultBodyWait(exception);
            _callerInteractionCore.WakeBody();
            return;
        }

        // A concurrent body start may have beaten the pre-start terminal claim.
        if (IsBodyRunning)
            _callerInteractionCore.FaultBodyWait(exception);
        else
            CompleteEnumerationWithException(exception);
    }

    protected override void OnReleasing(Exception? exception)
    {
        Volatile.Read(ref _cancelDelivery)?.TrySetResult();
        _options.Commands.Return();
    }

    protected override void OnDiscarded()
    {
        // Discarded flows never enter the base release path.
        GetObserver(out var observerState)?.OnCompleting(this, null, observerState);
        _options.Commands.Return();
    }

    protected override void OnReset()
    {
        Debug.Assert(IsPending || IsCompleted);
        _commandIndex = -1;
        _executePipelinedCore.Reset();
        _enumeratorMoveNextTaskSource.Reset();
        // Disarm while idle in the pool (no teardown can target a non-live flow). Initialize re-arms it
        // before the flow is queued, so a live flow is always in concurrent-completion mode.
        _enumeratorMoveNextTaskSource.CanCompleteConcurrently = false;
        _enumeratorCurrent = default;
        _isResultReady = false;
        _callerInteractionCore.Reset();
        _callerCancellationToken = default;
        _flowCancellationToken = default;
        _callerCancellationTokenRegistration.Dispose();
        _callerCancellationTokenRegistration = default;
        _flowCancellationTokenRegistration.Dispose();
        _flowCancellationTokenRegistration = default;
        _cancelRequested = false;
        _cancellationScope = (int)CancellationScope.None;
        _backendCancellationTiming = (int)BackendCancellationTiming.AfterGrace;
        _subsequentBackendCancellationTiming = (int)BackendCancellationTiming.AfterGrace;
        _cancelDeliverToken = default;
        _cancelDelivery = null;
        _cancellationEpisodeKey = null;
        _deliverCancelOce = false;
        _drainErrors = null;
        _consumeNonQuery = false;
        _nonQueryRecordsAffected = 0;
        _consumerDisposed = false;
        _draining = false;
        _drainModeEntered = false;
        WaitForDrainOnDispose = true;
        // Dispatch state is per-tenure.
        _pipelinePromise = null;
        _contextPublished = false;
        _context = default;
        _task = default;
        _bodyState = BodyNotStarted;
        _consumerAdvanced = false;
    }

    FlowCallerInteractionCoreResult IValueTaskSource<FlowCallerInteractionCoreResult>.GetResult(short token)
        => _callerInteractionCore.ConsumeGateResult(token);

    ValueTaskSourceStatus IValueTaskSource<FlowCallerInteractionCoreResult>.GetStatus(short token)
        => _callerInteractionCore.GateStatus(token);

    void IValueTaskSource<FlowCallerInteractionCoreResult>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        _callerInteractionCore.OnGateCompleted(continuation, state, token, flags);
        // Drain is a sticky level. Recheck it after registering so an earlier gate edge cannot be lost
        // across gate reset. This is the body's suspending stack, so the wake must dispatch asynchronously.
        if (IsDraining)
        {
            // A synchronous takeover would already have resumed the body inline. Reaching this callback
            // means autonomous execution still owns the body.
            IsAsync = true;
            _callerInteractionCore.ResumeBody(runContinuationsAsynchronously: true);
        }
    }

    // Backing for the pipelined-dispatch ValueTask. Returned to the framework when activation
    // hasn't fired yet. Nested callback completes it when ExecutePipelined finishes.
    void IValueTaskSource.GetResult(short token) => _executePipelinedCore.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _executePipelinedCore.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _executePipelinedCore.OnCompleted(continuation, state, token, flags);

}
