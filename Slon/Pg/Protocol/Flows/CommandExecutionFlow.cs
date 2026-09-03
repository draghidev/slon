using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg.Protocol.Flows;

internal abstract class CommandExecutionFlowObserver : PgClientFlowObserver
{
    protected internal virtual void OnStarted(CommandExecutionFlow flow, object? state) { }
    protected internal virtual void OnCommandResult(
        CommandExecutionFlow flow, CommandResult result, object? state) { }
    protected internal virtual void OnDrainStarted(CommandExecutionFlow flow, object? state) { }
}

internal readonly struct CommandExecutionFlowOptions
{
    public CommandExecutionFlowObserver? Observer { get; init; }
    public object? ObserverState { get; init; }
    public CommandList Commands { get; init; }
    public TimeSpan? PendingTimeout { get; init; }
}

// Replacement-flow prototype: one consumer-owned decoder lifecycle for synchronous and asynchronous
// execution, with the general multi-command result shape. Kept internal until it covers the complete
// CommandFlow contract; the eventual single-command specialization will remain a separate sealed type.
internal sealed class CommandExecutionFlow : PgClientFlow, IValueTaskSource<bool>, IValueTaskSource
{
    // Decoder ownership. Reading and Draining name a frame that owns the decoder. Initial and
    // ResultReady are idle states which exactly one party leaves by compare-exchange.
    const int PhaseInitial = 0;
    const int PhaseReading = 1;
    const int PhaseResultReady = 2;
    const int PhaseDraining = 3;
    const int PhaseCompleted = 4;
    int _phase;

    CommandList _commands;
    TimeSpan? _pendingTimeout;
    CommandExecutionFlowObserver? _commandObserver;
    object? _commandObserverState;
    int _commandIndex = -1;
    Context _context;
    CommandResult? _current;
    bool _currentPublished;
    bool _readFlowRfq;
    // Set by the consumer once it has started reading, so a drain knows whether to publish nothing.
    bool _consumerDetached;
    bool _consumerObservedCompletion;

    // Completed once the request is written and activation settled, faulted by teardown before then.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _readySource;
    int _readyCompletion;
    // The framework's pipeline task, completed by whichever frame consumes RFQ.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _pipelineTaskSource;

    // The submission token occupies this slot until a consumer attaches, after which the consumer
    // token replaces it, including with default. Cancellation delivery, close, and failures stay cold.
    CancellationToken _flowToken;
    CancellationTokenRegistration _flowRegistration;
    ColdState? _coldState;
    FlowHandoffEvent? _handoffEvent;
    bool _syncHandoffClaimed;
    int _drainStarted;
    bool _enableActivationTimeout = true;

    sealed class ColdState
    {
        internal bool CancelRequested;
        internal CancellationToken DeliverToken;
        internal Exception? CloseException;
        // Replayed by later consumer calls once the flow reached its terminal.
        internal Exception? TerminalException;
        // A command error observed while draining without a consumer.
        internal Exception? DrainError;
    }

    CommandExecutionFlow(bool async, TimeSpan? pendingTimeout = null)
        : base(supportsDeferredFlush: true)
    {
        _pendingTimeout = pendingTimeout;
        IsAsync = async;
        if (!async)
            _handoffEvent = new(false);
    }

    internal CommandExecutionFlow(bool async, params ReadOnlySpan<Command> commands)
        : this(async)
        => Initialize(async, commands);

    internal CommandExecutionFlow(
        bool async, bool enableActivationTimeout, params ReadOnlySpan<Command> commands)
        : this(async, commands)
        => _enableActivationTimeout = enableActivationTimeout;

    internal CommandExecutionFlow(bool async, CommandList commands, TimeSpan? pendingTimeout = null)
        : this(async, pendingTimeout)
        => Initialize(async, new CommandExecutionFlowOptions
        {
            Commands = commands,
            PendingTimeout = pendingTimeout
        });

    internal CommandExecutionFlow(bool async, in CommandExecutionFlowOptions options)
        : this(async, options.PendingTimeout)
        => Initialize(async, options);

    internal CommandExecutionFlow Initialize(bool async, params ReadOnlySpan<Command> commands)
        => Initialize(async, new CommandExecutionFlowOptions { Commands = new(commands) });

    internal CommandExecutionFlow Initialize(bool async, in CommandExecutionFlowOptions options)
    {
        IsAsync = async;
        if (!async)
            _handoffEvent ??= new(false);
        var commands = options.Commands;
        if (commands.Count is 0)
            return this;
        _commands = commands;
        _pendingTimeout = options.PendingTimeout;
        _commandObserver = options.Observer;
        _commandObserverState = options.ObserverState;
        if (options.Observer is { } observer)
        {
            SetObserver(observer, options.ObserverState);
            observer.OnStarted(this, options.ObserverState);
        }
        return this;
    }

    internal override bool DefersSyncHandoff => true;
    private protected override FlowHandoffEvent? HandoffEvent => _handoffEvent;
    protected override bool EnableActivationTimeout => _enableActivationTimeout;
    protected override TimeSpan? PendingTimeout => _pendingTimeout;

    internal override void BindCallerToken(CancellationToken cancellationToken)
        => _flowToken = cancellationToken;
    internal override CancellationToken MigrationCancellationToken
        => _flowToken;

    public Enumerator GetEnumerator()
        => new(this, default);

    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        _flowToken = cancellationToken;
        return new(this, cancellationToken);
    }

    internal CommandResult? CurrentResult => _current;
    internal bool IsResultReady => Volatile.Read(ref _phase) is PhaseResultReady;
    internal int VisibleCommandCount => _commands.Count;
    internal ValueTask<bool> MoveNextResultAsync(CancellationToken cancellationToken)
        => MoveNextAsync(cancellationToken);
    internal void DisposeResults() => Dispose();
    internal ValueTask DisposeResultsAsync() => DisposeAsync();

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<long> ConsumeNonQueryAsync(
        CancellationToken cancellationToken = default)
    {
        var recordsAffected = -1L;
        var results = GetAsyncEnumerator(cancellationToken);
        try
        {
            while (await results.MoveNextAsync().ConfigureAwait(false))
            {
                var result = results.Current;
                await result.CompleteAsync().ConfigureAwait(false);
                result.GetCommandComplete();
                if (result.RecordsAffected is { } affected and >= 0)
                    recordsAffected = recordsAffected < 0
                        ? affected
                        : checked(recordsAffected + affected);
            }
            return recordsAffected;
        }
        finally
        {
            await results.DisposeAsync().ConfigureAwait(false);
        }
    }

    ColdState GetOrCreateColdState()
        => Volatile.Read(ref _coldState) ??
            Interlocked.CompareExchange(ref _coldState, new(), null) ?? _coldState;

    bool IsClosed => Volatile.Read(ref _coldState)?.CloseException is not null;
    bool IsCancelRequested => Volatile.Read(ref _coldState) is { CancelRequested: true };
    bool HasDecoder => HasSuccessfulActivation;

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        _context = context;
        ValueTask writeTask;
        try
        {
            ref readonly var template = ref _commands.ItemRef(_commands.Count - 1);
            var appendSync = !template.WithSync;
            _readFlowRfq = appendSync;
            // Caller cancellation never cancels wire I/O. The consumer observes the latched intent and
            // drains its command to RFQ instead.
            writeTask = IsAsync
                ? _commands.WriteCommandsAsync(context.GetEncoder(), appendSync, default)
                : WriteCommandsResumable(context, appendSync);
            // Observe synchronous faults here; pending writes remain the framework-owned trailing task.
            if (writeTask.IsCompleted)
                writeTask.GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            // The framework recovers the wire from this throw. Only the consumer needs the fault.
            FaultReady(ex);
            throw;
        }

        // Activation may precede or follow execution. Bridging it here guarantees a consumer resumes
        // against a published context, and delivers an activation fault to the pipeline task when no
        // consumer ever arrives.
        var activation = context.GetDecoderAsync().ConfigureAwait(false);
        if (activation.IsCompleted)
            OnActivationSettled(onExecutorStrand: true);
        else
            activation.UnsafeOnCompleted(static state => ((CommandExecutionFlow)state!).OnActivationSettled(onExecutorStrand: false), this);
        return new(new FlowTasks(writeTask, new ValueTask(this, _pipelineTaskSource.Version)));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    ValueTask WriteCommandsResumable(Context context, bool appendSync)
    {
        var encoder = context.GetEncoder();
        ValueTask writeTask;
        using (encoder.BeginResumableWriteScope())
            writeTask = _commands.WriteCommandsResumable(encoder, appendSync);
        return writeTask.IsCompleted ? writeTask : encoder.RunResumableTask(writeTask);
    }

    // Runs on the executor strand when activation already settled, else on the activation dispatch.
    // The executor strand never runs consumer code. An activation dispatch is a detached work item
    // whose only remaining work is this wake, so the consumer may continue on it directly.
    void OnActivationSettled(bool onExecutorStrand)
    {
        try
        {
            _ = _context.GetDecoderAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception fault)
        {
            // Preserve the activation's close, timeout, or cancellation identity. The pipeline task
            // faults first so a consumer woken by the ready source never races its retirement.
            CompletePipelineTask(fault, runContinuationsAsynchronously: true);
            FaultReady(fault);
            return;
        }

        if (IsCancelRequested)
            RequestBackendCancellation();
        if (!CompleteReady(null, runContinuationsAsynchronously: onExecutorStrand))
        {
            // Teardown released the consumer while this flow waited for its turn. Nothing reads the
            // response, the closing wire owns it.
            CompletePipelineTask(null, runContinuationsAsynchronously: true);
            return;
        }
        // A cancel latched before activation may have released its caller already. The response
        // still has to reach RFQ, so drain it unless a consumer already owns the decoder.
        if (IsCancelRequested)
            TryTakeOverDrain();
    }

    void FaultReady(Exception exception)
    {
        Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, exception, null);
        CompleteReady(exception, runContinuationsAsynchronously: true);
    }

    bool CompleteReady(Exception? exception, bool runContinuationsAsynchronously)
    {
        if (Interlocked.CompareExchange(ref _readyCompletion, 1, 0) != 0)
            return false;
        if (exception is null)
            _readySource.SetResult(true, runContinuationsAsynchronously);
        else
            _readySource.SetException(exception, runContinuationsAsynchronously);
        return true;
    }

    void CompletePipelineTask(Exception? exception, bool runContinuationsAsynchronously = false)
    {
        if (Interlocked.Exchange(ref _phase, PhaseCompleted) is PhaseCompleted)
            return;
        if (exception is null)
            _pipelineTaskSource.SetResult(true, runContinuationsAsynchronously);
        else
            _pipelineTaskSource.SetException(exception, runContinuationsAsynchronously);
    }

    void EnsureSyncHandoff()
    {
        if (IsAsyncAtDispatch)
            ThrowHelper.ThrowInvalidOperation(
                "Synchronous result consumption requires a flow initialized for synchronous execution.");
        if (_syncHandoffClaimed)
            return;
        WaitForSyncHandoff();
        _syncHandoffClaimed = true;
    }

    bool MoveNext()
    {
        EnsureSyncHandoff();
        while (true)
        {
            var phase = Volatile.Read(ref _phase);
            switch (phase)
            {
                case PhaseInitial:
                    if (Interlocked.CompareExchange(ref _phase, PhaseReading, PhaseInitial) != PhaseInitial)
                        continue;
                    _commandIndex = 0;
                    return First();
                case PhaseResultReady:
                    if (Interlocked.CompareExchange(ref _phase, PhaseReading, PhaseResultReady) != PhaseResultReady)
                        continue;
                    return NextBatch();
                case PhaseReading:
                    ThrowHelper.ThrowInvalidOperation("A read is already in progress on this flow.");
                    return false;
                case PhaseDraining:
                    WaitForCompleteSynchronously();
                    throw Volatile.Read(ref _coldState)?.TerminalException
                        ?? ThrowHelper.ThrowInvalidOperation("The flow was disposed.");
                default:
                    if (Volatile.Read(ref _coldState)?.TerminalException is { } terminal)
                        ExceptionDispatchInfo.Throw(terminal);
                    return false;
            }
        }
    }

    bool First()
    {
        try
        {
            WaitForReadySynchronously();
            Debug.Assert(!_consumerDetached);
            RegisterCancellation(default);
            var result = ReadNextPublishedResult();
            return result is not null && PublishSynchronousResult(result);
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
            throw;
        }
    }

    bool NextBatch()
    {
        try
        {
            RegisterCancellation(default);
            var result = _current!;
            var completeError = CompleteCurrentResult();
            _currentPublished = false;
            if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                SkipDiscardedCommands();

            _commandIndex++;
            var next = ReadNextPublishedResult();
            if (next is not null)
                return PublishSynchronousResult(next);

            return false;
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
            throw;
        }
    }

    bool PublishSynchronousResult(CommandResult result)
    {
        _current = result;
        _currentPublished = true;
        Interlocked.Exchange(ref _phase, PhaseResultReady);
        var context = _context;
        if (!IsClosed && context.StoppingToken.IsCancellationRequested)
            Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException,
                context.FlowTerminationException, null);
        if (!IsCancelRequested && !IsClosed)
            return true;

        if (Interlocked.CompareExchange(ref _phase, PhaseReading, PhaseResultReady) == PhaseResultReady)
            Drain();
        else
            WaitForCompleteSynchronously();
        throw Volatile.Read(ref _coldState)?.TerminalException
            ?? ThrowHelper.ThrowUnexpected("A latched flow completed without a terminal outcome.");
    }

    void WaitForReadySynchronously()
    {
        var ready = new ValueTask<bool>(this, _readySource.Version);
        if (ready.IsCompleted)
            _ = ready.GetAwaiter().GetResult();
        else
            _ = ready.AsTask().GetAwaiter().GetResult();
    }

    ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
    {
        if (!IsAsyncAtDispatch)
            return ValueTask.FromException<bool>(ThrowHelper.ThrowInvalidOperation(
                "Asynchronous result consumption requires a flow initialized for asynchronous execution."));
        while (true)
        {
            var phase = Volatile.Read(ref _phase);
            switch (phase)
            {
                case PhaseInitial:
                    if (cancellationToken.IsCancellationRequested)
                        return CancelBeforeRead(cancellationToken);
                    if (Interlocked.CompareExchange(ref _phase, PhaseReading, PhaseInitial) != PhaseInitial)
                        continue;
                    _commandIndex = 0;
                    return FirstAsync(cancellationToken);
                case PhaseResultReady:
                    if (cancellationToken.IsCancellationRequested)
                        return CancelBeforeRead(cancellationToken);
                    if (Interlocked.CompareExchange(ref _phase, PhaseReading, PhaseResultReady) != PhaseResultReady)
                        continue;
                    return NextBatchAsync(cancellationToken);
                case PhaseReading:
                    return ValueTask.FromException<bool>(
                        ThrowHelper.ThrowInvalidOperation("A read is already in progress on this flow."));
                case PhaseDraining:
                    return AwaitTakeoverAsync();
                default:
                    return Volatile.Read(ref _coldState)?.TerminalException is { } terminal
                        ? ValueTask.FromException<bool>(terminal)
                        : new(false);
            }
        }
    }

    // A pre-cancelled token releases the caller immediately. The wire still drains to RFQ.
    ValueTask<bool> CancelBeforeRead(CancellationToken cancellationToken)
    {
        RequestCancel(cancellationToken);
        return ValueTask.FromException<bool>(new OperationCanceledException(cancellationToken));
    }

    // The consumer parks behind a takeover drain and receives the outcome that caused it.
    async ValueTask<bool> AwaitTakeoverAsync()
    {
        await WaitForCompletionAsync().ConfigureAwait(false);
        throw Volatile.Read(ref _coldState)?.TerminalException ?? ThrowHelper.ThrowInvalidOperation("The flow was disposed.");
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<bool> FirstAsync(CancellationToken cancellationToken)
    {
        Exception? deliver;
        try
        {
            await new ValueTask<bool>(this, _readySource.Version).ConfigureAwait(false);
            Debug.Assert(!_consumerDetached);
            RegisterCancellation(cancellationToken);
            var result = await ReadNextPublishedResultAsync().ConfigureAwait(false);
            if (result is null)
                return false;
            _current = result;
            _currentPublished = true;
            // Publish the idle state, then recheck the latches. A latch that landed between the read
            // and this publication found no idle owner to take over, so this frame must act on it.
            Interlocked.Exchange(ref _phase, PhaseResultReady);
            // Graceful stopping faults a result that arrives after the close began, as the ordinary
            // flow does at each result boundary. Latch it so the drain delivers that close.
            var context = _context;
            if (!IsClosed && context.StoppingToken.IsCancellationRequested)
                Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException,
                    context.FlowTerminationException, null);
            if (!IsCancelRequested && !IsClosed)
                return true;
            if (Interlocked.CompareExchange(ref _phase, PhaseReading, PhaseResultReady) == PhaseResultReady)
            {
                await DrainAsync().ConfigureAwait(false);
            }
            else
            {
                // The latching side took the decoder first. Park behind its drain.
                await WaitForCompletionAsync().ConfigureAwait(false);
            }
            deliver = Volatile.Read(ref _coldState)?.TerminalException;
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
            throw;
        }
        throw deliver ?? ThrowHelper.ThrowUnexpected("A latched flow completed without a terminal outcome.");
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<bool> NextBatchAsync(CancellationToken cancellationToken)
    {
        try
        {
            RegisterCancellation(cancellationToken);
            var result = _current!;
            var resultEnumerator = _context.GetProtocolStatic<CommandFlow.ReadState>()
                .ResultMessageEnumerator;
            await resultEnumerator.DisposeAsync().ConfigureAwait(false);
            var completeError = resultEnumerator.CompleteError;
            _currentPublished = false;
            if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                await SkipDiscardedCommandsAsync().ConfigureAwait(false);

            _commandIndex++;
            var next = await ReadNextPublishedResultAsync().ConfigureAwait(false);
            if (next is not null)
            {
                result = next;
                _current = result;
                _currentPublished = true;
                Interlocked.Exchange(ref _phase, PhaseResultReady);
                var context = _context;
                if (!IsClosed && context.StoppingToken.IsCancellationRequested)
                    Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException,
                        context.FlowTerminationException, null);
                if (!IsCancelRequested && !IsClosed)
                    return true;

                if (Interlocked.CompareExchange(ref _phase, PhaseReading, PhaseResultReady) == PhaseResultReady)
                    await DrainAsync().ConfigureAwait(false);
                else
                    await WaitForCompletionAsync().ConfigureAwait(false);
                throw Volatile.Read(ref _coldState)?.TerminalException
                    ?? ThrowHelper.ThrowUnexpected("A latched flow completed without a terminal outcome.");
            }

            return false;
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
            throw;
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<CommandResult?> ReadNextPublishedResultAsync()
    {
        CommandResult? result = _current;
        while (_commandIndex < _commands.Count)
        {
            result = await ReadResultAsync(_commandIndex).ConfigureAwait(false);
            _current = result;
            _currentPublished = false;
            if (!_commands.ItemRef(_commandIndex).SuppressEnumeration)
                return result;

            var completeError = await CompleteCurrentResultAsync().ConfigureAwait(false);
            var suppressedError = result.Error;
            if (suppressedError is null && completeError is { } completionError)
                suppressedError = completionError.Error;
            if (suppressedError is not null)
            {
                var exception = PgErrorException.Create(suppressedError);
                var cold = GetOrCreateColdState();
                Interlocked.CompareExchange(ref cold.TerminalException, exception, null);
                cold.DrainError ??= exception;
                Interlocked.Exchange(ref _phase, PhaseDraining);
                NotifyDrainStarted();
                _consumerDetached = true;
                await DrainAsync().ConfigureAwait(false);
                throw exception;
            }

            _commandIndex++;
        }

        if (result is null)
            throw ThrowHelper.ThrowInvalidOperation("The flow contains no commands.");
        await CompleteBatchAsync(result).ConfigureAwait(false);
        _consumerObservedCompletion = true;
        return null;
    }

    CommandResult? ReadNextPublishedResult()
    {
        CommandResult? result = _current;
        while (_commandIndex < _commands.Count)
        {
            result = ReadResult(_commandIndex);
            _current = result;
            _currentPublished = false;
            if (!_commands.ItemRef(_commandIndex).SuppressEnumeration)
                return result;

            var completeError = CompleteCurrentResult();
            var suppressedError = result.Error;
            if (suppressedError is null && completeError is { } completionError)
                suppressedError = completionError.Error;
            if (suppressedError is not null)
            {
                var exception = PgErrorException.Create(suppressedError);
                var cold = GetOrCreateColdState();
                Interlocked.CompareExchange(ref cold.TerminalException, exception, null);
                cold.DrainError ??= exception;
                Interlocked.Exchange(ref _phase, PhaseDraining);
                NotifyDrainStarted();
                _consumerDetached = true;
                Drain();
                throw exception;
            }

            _commandIndex++;
        }

        if (result is null)
            throw ThrowHelper.ThrowInvalidOperation("The flow contains no commands.");
        CompleteBatch(result);
        _consumerObservedCompletion = true;
        return null;
    }

    // Reads through the command's execute prelude and initializes the protocol-static result.
    async ValueTask<CommandResult> ReadResultAsync(int commandIndex)
    {
        var context = _context;
        var decoder = context.Decoder;
        // After close, a fresh command must not consume bytes left by its predecessor.
        if (context.IsProtocolClosed)
            throw context.FlowTerminationException;
        PgError? error;
        RowDescription? requestedRowDescription;
        ref readonly var command = ref _commands.ItemRef(commandIndex);
        var describeOnly = command.DescribeOnly;
        var hasPreparedDescription = command.Descriptor
            is { IsPrepared: true, PreparedRowDescription: not null };
        decoder.UseReadTimeout(command.Timeout);
        ParameterTypeList? preparationParameterTypes = null;
        if (command.DescribeForPreparation)
        {
            var preparation = await command.ReadPreparationDescriptionAsync(
                decoder, context.GetProtocolStatic<CommandFlow.ReadState>().RowDescription)
                .ConfigureAwait(false);
            error = preparation.Item1;
            preparationParameterTypes = preparation.Item2;
            requestedRowDescription = preparation.Item3;
        }
        else if (hasPreparedDescription && !describeOnly)
        {
            // Prepared commands with a known description have the compact BindComplete ->
            // DataRow/CommandComplete prelude. Await the decoder directly so a read wake resumes this
            // frame rather than a nested parser coroutine.
            if (!decoder.TryMoveNext())
            {
                if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                    decoder.ThrowUnexpectedEof();
            }
            var message = decoder.Current;
            if (message.EnsureExpectedOrError(PgTypes.BackendType.BindComplete) is { } bindError)
            {
                error = bindError;
            }
            else
            {
                if (!decoder.TryMoveNext())
                {
                    if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                        decoder.ThrowUnexpectedEof();
                }
                decoder.Current.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
                error = null;
            }
            requestedRowDescription = null;
        }
        else
        {
            (error, requestedRowDescription) = await command
                .ReadUntilExecuteAsync(decoder, context.GetProtocolStatic<CommandFlow.ReadState>().RowDescription)
                .ConfigureAwait(false);
        }
        return InitializeResult(
            commandIndex, error, requestedRowDescription, preparationParameterTypes);
    }

    CommandResult ReadResult(int commandIndex)
    {
        var context = _context;
        var decoder = context.Decoder;
        if (context.IsProtocolClosed)
            throw context.FlowTerminationException;
        ref readonly var command = ref _commands.ItemRef(commandIndex);
        decoder.UseReadTimeout(command.Timeout);
        PgError? error;
        RowDescription? requestedRowDescription;
        ParameterTypeList? preparationParameterTypes = null;
        if (command.DescribeForPreparation)
        {
            var preparation = command.ReadPreparationDescription(
                decoder, context.GetProtocolStatic<CommandFlow.ReadState>().RowDescription);
            error = preparation.Item1;
            preparationParameterTypes = preparation.Item2;
            requestedRowDescription = preparation.Item3;
        }
        else
        {
            (error, requestedRowDescription) = command.ReadUntilExecute(
                decoder, context.GetProtocolStatic<CommandFlow.ReadState>().RowDescription);
        }
        return InitializeResult(
            commandIndex, error, requestedRowDescription, preparationParameterTypes);
    }

    CommandResult InitializeResult(
        int commandIndex, PgError? error, RowDescription? requestedRowDescription,
        ParameterTypeList? preparationParameterTypes = null)
    {
        var context = _context;
        ref readonly var readState = ref context.GetProtocolStatic<CommandFlow.ReadState>();
        ref readonly var command = ref _commands.ItemRef(commandIndex);
        readState.ResultMessageEnumerator.Initialize(command, context.Decoder);
        var result = readState.CommandResult;
        var descriptor = command.Descriptor;
        // A named unprepared statement that parsed becomes a prepared descriptor.
        if (!descriptor.IsPrepared && !descriptor.CommandName.IsDefault
            && (error is not { } err || !err.Expected.Contains(PgTypes.BackendType.ParseComplete)))
        {
            descriptor = CommandDescriptor.CreatePrepared(descriptor.CommandName,
                preparationParameterTypes ?? descriptor.ParameterTypes,
                requestedRowDescription?.Preserve());
        }
        result.Initialize(this, commandIndex, descriptor, requestedRowDescription,
            !command.DescribeOnly, command.IsSimple(), error);
        _commandObserver?.OnCommandResult(this, result, _commandObserverState);
        return result;
    }

    async ValueTask<(PgError Error, TransactionStatus TransactionStatus)?> CompleteCurrentResultAsync()
    {
        var enumerator = _context.GetProtocolStatic<CommandFlow.ReadState>().ResultMessageEnumerator;
        await enumerator.DisposeAsync().ConfigureAwait(false);
        return enumerator.CompleteError;
    }

    (PgError Error, TransactionStatus TransactionStatus)? CompleteCurrentResult()
    {
        var enumerator = _context.GetProtocolStatic<CommandFlow.ReadState>().ResultMessageEnumerator;
        enumerator.Dispose();
        return enumerator.CompleteError;
    }

    async ValueTask SkipDiscardedCommandsAsync()
    {
        while (++_commandIndex < _commands.Count && !_commands[_commandIndex].WithSync) { }
        await ReadRfqAsync().ConfigureAwait(false);
        if (_commandIndex == _commands.Count)
            _readFlowRfq = false;
    }

    void SkipDiscardedCommands()
    {
        while (++_commandIndex < _commands.Count && !_commands[_commandIndex].WithSync) { }
        ReadRfq();
        if (_commandIndex == _commands.Count)
            _readFlowRfq = false;
    }

    async ValueTask ReadRfqAsync()
    {
        var message = await _context.Decoder.GetNextAsync().ConfigureAwait(false);
        if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
            PgErrorException.Throw(rfqError);
    }

    void ReadRfq()
    {
        var message = _context.Decoder.GetNext();
        if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
            PgErrorException.Throw(rfqError);
    }

    async ValueTask CompleteBatchAsync(CommandResult result)
    {
        if (_readFlowRfq)
            await ReadRfqAsync().ConfigureAwait(false);
        await DisposeRegistrationsAsync().ConfigureAwait(false);
        Finish(result);
    }

    void CompleteBatch(CommandResult result)
    {
        if (_readFlowRfq)
            ReadRfq();
        DisposeRegistrations();
        Finish(result);
    }

    // The wire is at this command's RFQ. Release the shared read objects, record the outcome the
    // consumer must observe, then complete the pipeline task.
    void Finish(CommandResult result)
    {
        if (result.Error is { } error && _consumerDetached && !_currentPublished
            && !IsOwnCancellation(error))
            GetOrCreateColdState().DrainError = PgErrorException.Create(error);
        _context.GetProtocolStatic<CommandFlow.ReadState>().Reset();
        _current = null;
        _currentPublished = false;
        if (IsCancelRequested)
            Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException,
                new OperationCanceledException(_coldState!.DeliverToken), null);
        else if (Volatile.Read(ref _coldState)?.CloseException is { } close)
            Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, close, null);
        CompletePipelineTask(null);
    }

    bool IsOwnCancellation(PgError error)
        => IsCancelRequested && error.SqlState == PgErrorCodes.QueryCanceled;

    // The owning frame failed. The read error is the pipeline task's failure, which the framework
    // recovers or drains. Later consumer calls replay it.
    void FaultFromOwner(Exception exception)
    {
        Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, exception, null);
        if (Volatile.Read(ref _phase) == PhaseCompleted)
            return;
        DisposeRegistrations();
        _current = null;
        _currentPublished = false;
        if (HasDecoder)
            _context.GetProtocolStatic<CommandFlow.ReadState>().Reset();
        CompletePipelineTask(exception);
    }

    // Takes an idle decoder for a drain. Returns false when a frame already owns it, which will observe
    // the latch itself, or when the flow reached its terminal.
    bool TryTakeOverDrain()
    {
        while (true)
        {
            var phase = Volatile.Read(ref _phase);
            if (phase is PhaseInitial && !HasDecoder)
                return false;
            if (phase is not (PhaseInitial or PhaseResultReady))
                return false;
            if (Interlocked.CompareExchange(ref _phase, PhaseDraining, phase) != phase)
                continue;
            NotifyDrainStarted();
            _consumerDetached = true;
            ThreadPool.UnsafeQueueUserWorkItem(static state => _ = ((CommandExecutionFlow)state!).DrainAsync(), this);
            return true;
        }
    }

    void NotifyDrainStarted()
    {
        if (Interlocked.Exchange(ref _drainStarted, 1) is 0)
            _commandObserver?.OnDrainStarted(this, _commandObserverState);
    }

    // Autonomous drain. Owns the decoder until the pipeline task completes. Never throws.
    async ValueTask DrainAsync()
    {
        try
        {
            var result = _current;
            if (result is null)
            {
                await new ValueTask<bool>(this, _readySource.Version).ConfigureAwait(false);
                _commandIndex = 0;
                result = await ReadResultAsync(_commandIndex).ConfigureAwait(false);
            }

            while (true)
            {
                var completeError = await CompleteCurrentResultAsync().ConfigureAwait(false);
                if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                    await SkipDiscardedCommandsAsync().ConfigureAwait(false);
                if (++_commandIndex >= _commands.Count)
                    break;
                result = await ReadResultAsync(_commandIndex).ConfigureAwait(false);
            }
            await CompleteBatchAsync(result).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
        }
    }

    void Drain()
    {
        try
        {
            var result = _current;
            if (result is null)
            {
                // Synchronous disposal before any read. The activation bridge normally completed long
                // ago, so this bridge is rarely more than a status check.
                new ValueTask<bool>(this, _readySource.Version).AsTask().GetAwaiter().GetResult();
                _commandIndex = 0;
                result = ReadResult(_commandIndex);
            }

            while (true)
            {
                var completeError = CompleteCurrentResult();
                if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                    SkipDiscardedCommands();
                if (++_commandIndex >= _commands.Count)
                    break;
                result = ReadResult(_commandIndex);
            }
            CompleteBatch(result);
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
        }
    }

    ValueTask DisposeAsync()
    {
        while (true)
        {
            var phase = Volatile.Read(ref _phase);
            switch (phase)
            {
                case PhaseInitial:
                case PhaseResultReady:
                    if (Interlocked.CompareExchange(ref _phase, PhaseDraining, phase) != phase)
                        continue;
                    NotifyDrainStarted();
                    _consumerDetached = true;
                    if (_current is { IsComplete: false }
                        || _commandIndex + 1 < _commands.Count)
                        RequestCancel(default);
                    return WaitForDrainOnDispose ? DisposeDrainAsync() : FireAndForgetDrain();
                case PhaseReading:
                    return ValueTask.FromException(
                        ThrowHelper.ThrowInvalidOperation("Cannot dispose the flow while a read is in progress."));
                default:
                    return !WaitForDrainOnDispose || _consumerObservedCompletion
                        ? default
                        : DisposeCompletedAsync();
            }
        }
    }

    // Drains on the disposer's frame, then waits for framework release so a drain error can surface.
    async ValueTask DisposeDrainAsync()
    {
        await DrainAsync().ConfigureAwait(false);
        await DisposeCompletedAsync().ConfigureAwait(false);
    }

    ValueTask FireAndForgetDrain()
    {
        _ = DrainAsync();
        return default;
    }

    async ValueTask DisposeCompletedAsync()
    {
        await WaitForCompletionAsync().ConfigureAwait(false);
        if (Volatile.Read(ref _coldState)?.DrainError is { } drainError)
            throw drainError;
    }

    // Flow completion is independent of errors accumulated while draining. A close is a clean
    // terminal for a disposing consumer.
    async ValueTask WaitForCompletionAsync()
    {
        try
        {
            await WaitForComplete().ConfigureAwait(false);
        }
        catch (PgClientClosedException)
        {
        }
    }

    void Dispose()
    {
        if (!IsAsyncAtDispatch)
            EnsureSyncHandoff();
        while (true)
        {
            var phase = Volatile.Read(ref _phase);
            switch (phase)
            {
                case PhaseInitial:
                case PhaseResultReady:
                    if (Interlocked.CompareExchange(ref _phase, PhaseDraining, phase) != phase)
                        continue;
                    NotifyDrainStarted();
                    _consumerDetached = true;
                    if (_current is { IsComplete: false }
                        || _commandIndex + 1 < _commands.Count)
                        RequestCancel(default);
                    Drain();
                    if (WaitForDrainOnDispose)
                        DisposeCompleted();
                    return;
                case PhaseReading:
                    ThrowHelper.ThrowInvalidOperation("Cannot dispose the flow while a read is in progress.");
                    return;
                default:
                    if (WaitForDrainOnDispose && !_consumerObservedCompletion)
                        DisposeCompleted();
                    return;
            }
        }
    }

    void DisposeCompleted()
    {
        try
        {
            WaitForCompleteSynchronously();
        }
        catch (PgClientClosedException)
        {
        }
        if (Volatile.Read(ref _coldState)?.DrainError is { } drainError)
            throw drainError;
    }

    // When true, disposal waits for the drain to reach RFQ and for framework release. Otherwise it
    // returns while the drain continues autonomously.
    internal bool WaitForDrainOnDispose { get; set; } = true;

    void RegisterCancellation(CancellationToken callerToken)
    {
        if (callerToken == _flowToken &&
            (_flowRegistration != default || !callerToken.CanBeCanceled))
            return;
        var registration = _flowRegistration;
        _flowRegistration = default;
        registration.Dispose();
        _flowToken = callerToken;
        if (callerToken.CanBeCanceled)
            _flowRegistration = callerToken.UnsafeRegister(static (state, token)
                => ((CommandExecutionFlow)state!).RequestCancel(token), this);
    }

    ValueTask DisposeRegistrationsAsync()
    {
        if (_flowRegistration == default)
            return default;
        var flowRegistration = _flowRegistration;
        _flowRegistration = default;
        return flowRegistration.DisposeAsync();
    }

    void DisposeRegistrations()
    {
        var flowRegistration = _flowRegistration;
        _flowRegistration = default;
        flowRegistration.Dispose();
    }

    // Cancellation only latches intent and requests a backend cancel. The frame owning the decoder
    // delivers it after the wire is back at RFQ. An idle flow drains autonomously first.
    void RequestCancel(CancellationToken token)
    {
        if (Volatile.Read(ref _phase) == PhaseCompleted)
            return;
        var cancellation = GetOrCreateColdState();
        cancellation.DeliverToken = token;
        Interlocked.Exchange(ref cancellation.CancelRequested, true);
        if (HasDecoder)
            RequestBackendCancellation();
        TryTakeOverDrain();
    }

    internal Task CancelAsync()
    {
        RequestCancel(default);
        return WaitForComplete().AsTask();
    }

    void RequestBackendCancellation()
        => _context.RequestBackendCancellation(this, CancellationWindow, BackendCancellationTiming.AfterGrace);

    // Finish and FaultFromOwner reset the shared read objects before the pipeline task completes, and
    // Current is null once the consumer observed the terminal, so nothing outlives the flow.
    internal override bool ResetsSharedReadStateBeforeRelease => true;

    // Graceful stop. An unactivated flow releases its consumer, the closing wire owns its response.
    // An idle activated flow drains itself to RFQ so the pipeline can complete.
    protected override void OnStopping(Exception exception)
    {
        Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException, exception, null);
        if (CompleteReady(exception, runContinuationsAsynchronously: true))
        {
            Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, exception, null);
            return;
        }
        TryTakeOverDrain();
    }

    // Forceful abort. No frame can read a dead wire, so an idle owner faults the pipeline task
    // directly. A frame in flight fails on its own read.
    protected override void OnAbort(Exception exception)
    {
        Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException, exception, null);
        if (CompleteReady(exception, runContinuationsAsynchronously: true))
        {
            Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, exception, null);
            return;
        }
        while (true)
        {
            var phase = Volatile.Read(ref _phase);
            if (phase is not (PhaseInitial or PhaseResultReady))
                return;
            if (Interlocked.CompareExchange(ref _phase, PhaseCompleted, phase) != phase)
                continue;
            Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, exception, null);
            _pipelineTaskSource.SetException(exception, runContinuationsAsynchronously: true);
            return;
        }
    }

    internal override void Fail(Exception exception)
    {
        // A result callback failed on the frame that owns the decoder. Its throw propagates there.
        Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, exception, null);
    }

    protected override void OnReleasing(Exception? exception)
    {
        DisposeRegistrations();
        _commands.Return();
    }

    protected override void OnDiscarded()
    {
        GetObserver(out var observerState)?.OnCompleting(this, null, observerState);
        _commands.Return();
    }

    protected override void OnReset()
    {
        _phase = PhaseInitial;
        _commandIndex = -1;
        _context = default;
        _current = null;
        _currentPublished = false;
        _readFlowRfq = false;
        _consumerDetached = false;
        _consumerObservedCompletion = false;
        _readySource.Reset();
        _pipelineTaskSource.Reset();
        _readyCompletion = 0;
        _drainStarted = 0;
        _coldState = null;
        _syncHandoffClaimed = false;
        _handoffEvent?.ResetInteraction();
        WaitForDrainOnDispose = true;
    }

    bool IValueTaskSource<bool>.GetResult(short token) => _readySource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _readySource.GetStatus(token);
    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _readySource.OnCompleted(continuation, state, token, flags);

    void IValueTaskSource.GetResult(short token) => _pipelineTaskSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _pipelineTaskSource.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _pipelineTaskSource.OnCompleted(continuation, state, token, flags);

    public readonly struct Enumerator : IAsyncEnumerator<CommandResult>, IDisposable
    {
        readonly CommandExecutionFlow? _flow;
        readonly CancellationToken _cancellationToken;

        public Enumerator(CommandExecutionFlow flow)
            : this(flow, default)
        { }

        internal Enumerator(CommandExecutionFlow flow, CancellationToken cancellationToken)
        {
            _flow = flow;
            _cancellationToken = cancellationToken;
        }

        public Enumerator GetAsyncEnumerator() => this;

        public Enumerator GetEnumerator() => this;

        public bool MoveNext() => _flow?.MoveNext() ?? false;

        public ValueTask<bool> MoveNextAsync() => MoveNextAsync(_cancellationToken);

        public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
            => _flow is null ? new(false) : _flow.MoveNextAsync(cancellationToken);

        public CommandResult Current => _flow?._current ?? default!;

        public ValueTask DisposeAsync() => _flow is null ? default : _flow.DisposeAsync();

        public void Dispose() => _flow?.Dispose();
    }
}
