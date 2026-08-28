using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg.Protocol.Flows;

/// Executes one command for an asynchronous consumer that reads the response directly. There is no
/// execution body. The consumer's MoveNextAsync owns the decoder from activation until it consumes
/// ReadyForQuery, and abandonment hands that ownership to a drain exactly once. The framework's
/// pipeline task completes when the wire reaches this command's RFQ, or faults with the read error.
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed class ReaderDrivenCommandFlow : PgClientFlow, IValueTaskSource<bool>, IValueTaskSource
{
    // Decoder ownership. Reading and Draining name a frame that owns the decoder. Initial and
    // ResultReady are idle states which exactly one party leaves by compare-exchange.
    const int PhaseInitial = 0;
    const int PhaseReading = 1;
    const int PhaseResultReady = 2;
    const int PhaseDraining = 3;
    const int PhaseCompleted = 4;
    int _phase;

    readonly CommandList _commands;
    readonly TimeSpan? _pendingTimeout;
    Context _context;
    PgDecoder? _decoder;
    CommandResult? _current;
    bool _readFlowRfq;
    // Set by the consumer once it has started reading, so a drain knows whether to publish nothing.
    bool _consumerDetached;
    bool _consumerObservedCompletion;

    // Completed once the request is written and activation settled, faulted by teardown before then.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _readySource;
    // The framework's pipeline task, completed by whichever frame consumes RFQ.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _pipelineTaskSource;

    // Cancellation, close, and failure state is cold. A successful uncancelled operation carries one
    // null reference instead of two registrations, three tokens, their latches, and two exceptions.
    ColdState? _coldState;

    sealed class ColdState
    {
        internal CancellationToken FlowToken;
        internal CancellationTokenRegistration FlowRegistration;
        internal CancellationTokenRegistration CallerRegistration;
        internal bool CancelRequested;
        internal CancellationToken DeliverToken;
        internal Exception? CloseException;
        // Replayed by later consumer calls once the flow reached its terminal.
        internal Exception? TerminalException;
        // A command error observed while draining without a consumer.
        internal Exception? DrainError;
    }

    public ReaderDrivenCommandFlow(in Command command, TimeSpan? pendingTimeout = null)
        : base(supportsDeferredFlush: true)
    {
        if (command.DescribeForPreparation || command.SuppressEnumeration)
            ThrowHelper.ThrowArgumentException(nameof(command),
                "Preparation and suppressed commands require the general command flow.");
        _commands = new(command);
        _pendingTimeout = pendingTimeout;
        IsAsync = true;
        _readySource.CanCompleteConcurrently = true;
        _pipelineTaskSource.CanCompleteConcurrently = true;
    }

    protected override bool EnableActivationTimeout => true;
    protected override TimeSpan? PendingTimeout => _pendingTimeout;

    internal override void BindCallerToken(CancellationToken cancellationToken)
        => GetOrCreateColdState().FlowToken = cancellationToken;
    internal override CancellationToken MigrationCancellationToken
        => Volatile.Read(ref _coldState)?.FlowToken ?? default;

    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.CanBeCanceled)
            GetOrCreateColdState().FlowToken = cancellationToken;
        return new(this);
    }

    ColdState GetOrCreateColdState()
        => Volatile.Read(ref _coldState) ??
            Interlocked.CompareExchange(ref _coldState, new(), null) ?? _coldState;

    bool IsClosed => Volatile.Read(ref _coldState)?.CloseException is not null;
    bool IsCancelRequested => Volatile.Read(ref _coldState) is { CancelRequested: true };

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        Debug.Assert(IsAsync);
        _context = context;
        ValueTask writeTask;
        try
        {
            var appendSync = !_commands.ItemRef(0).WithSync;
            _readFlowRfq = appendSync;
            // Caller cancellation never cancels wire I/O. The consumer observes the latched intent and
            // drains its command to RFQ instead.
            writeTask = _commands.WriteCommandsAsync(context.GetEncoder(), appendSync, default);
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
            activation.UnsafeOnCompleted(static state => ((ReaderDrivenCommandFlow)state!).OnActivationSettled(onExecutorStrand: false), this);
        return new(new FlowTasks(writeTask, new ValueTask(this, _pipelineTaskSource.Version)));
    }

    // Runs on the executor strand when activation already settled, else on the activation dispatch.
    // The executor strand never runs consumer code. An activation dispatch is a detached work item
    // whose only remaining work is this wake, so the consumer may continue on it directly.
    void OnActivationSettled(bool onExecutorStrand)
    {
        var activation = _context.GetDecoderAsync().ConfigureAwait(false).GetAwaiter();
        if (!activation.IsCompletedSuccessfully)
        {
            Exception fault;
            try
            {
                activation.GetResult();
                fault = ThrowHelper.ThrowUnexpected("A settled activation neither faulted nor produced a decoder.");
            }
            catch (Exception ex)
            {
                fault = ex;
            }
            // Preserve the activation's close, timeout, or cancellation identity. The pipeline task
            // faults first so a consumer woken by the ready source never races its retirement.
            CompletePipelineTask(fault, runContinuationsAsynchronously: true);
            FaultReady(fault);
            return;
        }

        Volatile.Write(ref _decoder, activation.GetResult());
        if (IsCancelRequested)
            RequestBackendCancellation();
        if (!_readySource.TrySetResult(true, runContinuationsAsynchronously: onExecutorStrand))
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
        _readySource.TrySetException(exception, runContinuationsAsynchronously: true);
    }

    void CompletePipelineTask(Exception? exception, bool runContinuationsAsynchronously = false)
    {
        Interlocked.Exchange(ref _phase, PhaseCompleted);
        if (exception is null)
            _pipelineTaskSource.TrySetResult(true, runContinuationsAsynchronously);
        else
            _pipelineTaskSource.TrySetException(exception, runContinuationsAsynchronously);
    }

    ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
    {
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
                    return FirstAsync(cancellationToken);
                case PhaseResultReady:
                    if (cancellationToken.IsCancellationRequested)
                        return CancelBeforeRead(cancellationToken);
                    if (Interlocked.CompareExchange(ref _phase, PhaseReading, PhaseResultReady) != PhaseResultReady)
                        continue;
                    return LastAsync(cancellationToken);
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

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<bool> FirstAsync(CancellationToken cancellationToken)
    {
        Exception? deliver;
        try
        {
            await new ValueTask<bool>(this, _readySource.Version).ConfigureAwait(false);
            _consumerDetached = false;
            RegisterCancellation(cancellationToken);
            CommandResult result;
            if (!_commands.ItemRef(0).DescribeOnly
                && _commands.ItemRef(0).Descriptor is { IsPrepared: true, PreparedRowDescription: not null })
            {
                var decoder = _decoder!;
                if (_context.IsProtocolClosed)
                    throw _context.FlowTerminationException;
                decoder.UseReadTimeout(_commands.ItemRef(0).Timeout);
                PgError? error;
                if (!decoder.TryMoveNext())
                {
                    if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                        decoder.ThrowUnexpectedEof();
                }
                if (decoder.Current.EnsureExpectedOrError(PgTypes.BackendType.BindComplete) is { } bindError)
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
                    decoder.Current.DebugEnsureExpected(
                        PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
                    error = null;
                }
                result = InitializeResult(error, null);
            }
            else
            {
                result = await ReadResultAsync().ConfigureAwait(false);
            }
            _current = result;
            // Publish the idle state, then recheck the latches. A latch that landed between the read
            // and this publication found no idle owner to take over, so this frame must act on it.
            Interlocked.Exchange(ref _phase, PhaseResultReady);
            // Graceful stopping faults a result that arrives after the close began, as the ordinary
            // flow does at each result boundary. Latch it so the drain delivers that close.
            if (!IsClosed && _context.StoppingToken.IsCancellationRequested)
                Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException,
                    _context.FlowTerminationException, null);
            if (!IsCancelRequested && !IsClosed)
                return true;
            if (Interlocked.CompareExchange(ref _phase, PhaseReading, PhaseResultReady) == PhaseResultReady)
            {
                await DrainCoreAsync(result).ConfigureAwait(false);
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

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<bool> LastAsync(CancellationToken cancellationToken)
    {
        Exception? deliver;
        try
        {
            RegisterCallerToken(cancellationToken);
            await DrainCoreAsync(_current!).ConfigureAwait(false);
            deliver = Volatile.Read(ref _coldState)?.TerminalException;
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
            throw;
        }
        if (deliver is not null)
            throw deliver;
        Debug.Assert(IsCompleted);
        _consumerObservedCompletion = true;
        return false;
    }

    // Reads through the command's execute prelude and initializes the protocol-static result.
    async ValueTask<CommandResult> ReadResultAsync()
    {
        var decoder = _decoder!;
        var context = _context;
        // After close, a fresh command must not consume bytes left by its predecessor.
        if (context.IsProtocolClosed)
            throw context.FlowTerminationException;
        PgError? error;
        RowDescription? requestedRowDescription;
        var describeOnly = _commands.ItemRef(0).DescribeOnly;
        var hasPreparedDescription = _commands.ItemRef(0).Descriptor is { IsPrepared: true, PreparedRowDescription: not null };
        decoder.UseReadTimeout(_commands.ItemRef(0).Timeout);
        if (hasPreparedDescription && !describeOnly)
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
            (error, requestedRowDescription) = await _commands.ItemRef(0)
                .ReadUntilExecuteAsync(decoder, context.GetProtocolStatic<CommandFlow.ReadState>().RowDescription)
                .ConfigureAwait(false);
        }
        return InitializeResult(error, requestedRowDescription);
    }

    CommandResult ReadResult()
    {
        var decoder = _decoder!;
        var context = _context;
        if (context.IsProtocolClosed)
            throw context.FlowTerminationException;
        decoder.UseReadTimeout(_commands.ItemRef(0).Timeout);
        var (error, requestedRowDescription) = _commands.ItemRef(0)
            .ReadUntilExecute(decoder, context.GetProtocolStatic<CommandFlow.ReadState>().RowDescription);
        return InitializeResult(error, requestedRowDescription);
    }

    CommandResult InitializeResult(PgError? error, RowDescription? requestedRowDescription)
    {
        ref readonly var readState = ref _context.GetProtocolStatic<CommandFlow.ReadState>();
        ref readonly var command = ref _commands.ItemRef(0);
        readState.ResultMessageEnumerator.Initialize(command, _decoder!);
        var result = readState.CommandResult;
        var descriptor = command.Descriptor;
        // A named unprepared statement that parsed becomes a prepared descriptor.
        if (!descriptor.IsPrepared && !descriptor.CommandName.IsDefault
            && (error is not { } err || !err.Expected.Contains(PgTypes.BackendType.ParseComplete)))
        {
            descriptor = CommandDescriptor.CreatePrepared(descriptor.CommandName, descriptor.ParameterTypes,
                requestedRowDescription?.Preserve());
        }
        result.Initialize(this, 0, descriptor, requestedRowDescription, !command.DescribeOnly, command.IsSimple(), error);
        return result;
    }

    // Drains the current result to RFQ and completes the pipeline task. Runs on the frame that owns
    // the decoder. Throws to that frame on I/O failure.
    ValueTask DrainCoreAsync(CommandResult result)
    {
        var enumerator = _context.GetProtocolStatic<CommandFlow.ReadState>().ResultMessageEnumerator;
        // Completes the command and, if rows remain, consumes them first. A WithSync command reads
        // its own RFQ here.
        var dispose = enumerator.DisposeAsync();
        if (!dispose.IsCompletedSuccessfully)
            return AwaitDispose(this, result, dispose);
        dispose.GetAwaiter().GetResult();
        return CompleteDrain(this, result);

        static ValueTask CompleteDrain(ReaderDrivenCommandFlow flow, CommandResult result)
        {
            var decoder = flow._decoder!;
            if (flow._readFlowRfq)
            {
                if (!decoder.TryMoveNext())
                {
                    var moveNext = decoder.MoveNextAsync();
                    if (!moveNext.IsCompletedSuccessfully)
                        return AwaitMoveNext(flow, result, moveNext);
                    if (!moveNext.GetAwaiter().GetResult())
                        decoder.ThrowUnexpectedEof();
                }
                if (decoder.Current.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                    PgErrorException.Throw(rfqError);
            }

            var registrations = flow.DisposeRegistrationsAsync();
            if (!registrations.IsCompletedSuccessfully)
                return AwaitRegistrations(flow, result, registrations);
            registrations.GetAwaiter().GetResult();
            flow.Finish(result);
            return default;
        }

        static async ValueTask AwaitDispose(
            ReaderDrivenCommandFlow flow, CommandResult result, ValueTask dispose)
        {
            await dispose.ConfigureAwait(false);
            await CompleteDrain(flow, result).ConfigureAwait(false);
        }

        static async ValueTask AwaitMoveNext(
            ReaderDrivenCommandFlow flow, CommandResult result, ValueTask<bool> moveNext)
        {
            if (!await moveNext.ConfigureAwait(false))
                flow._decoder!.ThrowUnexpectedEof();
            await CompleteDrain(flow, result).ConfigureAwait(false);
        }

        static async ValueTask AwaitRegistrations(
            ReaderDrivenCommandFlow flow, CommandResult result, ValueTask registrations)
        {
            await registrations.ConfigureAwait(false);
            flow.Finish(result);
        }
    }

    void DrainCore(CommandResult result)
    {
        var decoder = _decoder!;
        var enumerator = _context.GetProtocolStatic<CommandFlow.ReadState>().ResultMessageEnumerator;
        enumerator.Dispose();
        if (_readFlowRfq)
        {
            var message = decoder.GetNext();
            if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                PgErrorException.Throw(rfqError);
        }
        DisposeRegistrations();
        Finish(result);
    }

    // The wire is at this command's RFQ. Release the shared read objects, record the outcome the
    // consumer must observe, then complete the pipeline task.
    void Finish(CommandResult result)
    {
        if (result.Error is { } error && _consumerDetached && !IsOwnCancellation(error))
            GetOrCreateColdState().DrainError = PgErrorException.Create(error);
        _context.GetProtocolStatic<CommandFlow.ReadState>().Reset();
        _current = null;
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
        if (Volatile.Read(ref _decoder) is not null)
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
            if (phase is PhaseInitial && Volatile.Read(ref _decoder) is null)
                return false;
            if (phase is not (PhaseInitial or PhaseResultReady))
                return false;
            if (Interlocked.CompareExchange(ref _phase, PhaseDraining, phase) != phase)
                continue;
            _consumerDetached = true;
            ThreadPool.UnsafeQueueUserWorkItem(static state => _ = ((ReaderDrivenCommandFlow)state!).DrainAsync(), this);
            return true;
        }
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
                result = await ReadResultAsync().ConfigureAwait(false);
            }
            await DrainCoreAsync(result).ConfigureAwait(false);
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
                result = ReadResult();
            }
            DrainCore(result);
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
                    _consumerDetached = true;
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
        while (true)
        {
            var phase = Volatile.Read(ref _phase);
            switch (phase)
            {
                case PhaseInitial:
                case PhaseResultReady:
                    if (Interlocked.CompareExchange(ref _phase, PhaseDraining, phase) != phase)
                        continue;
                    _consumerDetached = true;
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
        var cancellation = Volatile.Read(ref _coldState);
        if (cancellation is null && !callerToken.CanBeCanceled)
            return;
        cancellation ??= GetOrCreateColdState();
        if (cancellation.FlowToken.CanBeCanceled)
            cancellation.FlowRegistration = cancellation.FlowToken.UnsafeRegister(static (state, token)
                => ((ReaderDrivenCommandFlow)state!).RequestCancel(token), this);
        RegisterCallerToken(cancellation, callerToken);
    }

    void RegisterCallerToken(ColdState cancellation, CancellationToken callerToken)
    {
        if (cancellation.CallerRegistration != default)
        {
            cancellation.CallerRegistration.Dispose();
            cancellation.CallerRegistration = default;
        }
        if (callerToken.CanBeCanceled)
            cancellation.CallerRegistration = callerToken.UnsafeRegister(static (state, token)
                => ((ReaderDrivenCommandFlow)state!).RequestCancel(token), this);
    }

    void RegisterCallerToken(CancellationToken callerToken)
    {
        var cancellation = Volatile.Read(ref _coldState);
        if (cancellation is null)
        {
            if (!callerToken.CanBeCanceled)
                return;
            cancellation = GetOrCreateColdState();
        }
        RegisterCallerToken(cancellation, callerToken);
    }

    ValueTask DisposeRegistrationsAsync()
    {
        var cancellation = Volatile.Read(ref _coldState);
        if (cancellation is null ||
            (cancellation.FlowRegistration == default && cancellation.CallerRegistration == default))
            return default;
        return Core(cancellation);

        static async ValueTask Core(ColdState cancellation)
        {
            var flowRegistration = cancellation.FlowRegistration;
            var callerRegistration = cancellation.CallerRegistration;
            cancellation.FlowRegistration = default;
            cancellation.CallerRegistration = default;
            await flowRegistration.DisposeAsync().ConfigureAwait(false);
            await callerRegistration.DisposeAsync().ConfigureAwait(false);
        }
    }

    void DisposeRegistrations()
    {
        var cancellation = Volatile.Read(ref _coldState);
        if (cancellation is null)
            return;
        var flowRegistration = cancellation.FlowRegistration;
        var callerRegistration = cancellation.CallerRegistration;
        cancellation.FlowRegistration = default;
        cancellation.CallerRegistration = default;
        flowRegistration.Dispose();
        callerRegistration.Dispose();
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
        if (Volatile.Read(ref _decoder) is not null)
            RequestBackendCancellation();
        TryTakeOverDrain();
    }

    void RequestBackendCancellation()
        => _context.RequestBackendCancellation(this, CancellationWindow, BackendCancellationTiming.AfterGrace);

    protected override void OnCancellationWindowCompleted(int completedWindow, int remainingWindowCount)
    { }

    // Graceful stop. An unactivated flow releases its consumer, the closing wire owns its response.
    // An idle activated flow drains itself to RFQ so the pipeline can complete.
    protected override void OnStopping(Exception exception)
    {
        Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException, exception, null);
        if (_readySource.TrySetException(exception, runContinuationsAsynchronously: true))
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
        if (_readySource.TrySetException(exception, runContinuationsAsynchronously: true))
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
            _pipelineTaskSource.TrySetException(exception, runContinuationsAsynchronously: true);
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
        _context = default;
        _decoder = null;
        _current = null;
        _readFlowRfq = false;
        _consumerDetached = false;
        _consumerObservedCompletion = false;
        _readySource.Reset();
        _pipelineTaskSource.Reset();
        _coldState = null;
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

    public readonly struct Enumerator(ReaderDrivenCommandFlow flow) : IAsyncEnumerator<CommandResult>, IDisposable
    {
        public Enumerator GetAsyncEnumerator() => this;

        public ValueTask<bool> MoveNextAsync() => MoveNextAsync(default);

        public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
            => flow is null ? new(false) : flow.MoveNextAsync(cancellationToken);

        public CommandResult Current => flow?._current ?? default!;

        public ValueTask DisposeAsync() => flow is null ? default : flow.DisposeAsync();

        public void Dispose() => flow?.Dispose();
    }
}
