using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg.Protocol.Flows;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public abstract class CommandFlowObserver : PgClientFlowObserver
{
    protected internal virtual void OnStarted(CommandFlow flow, object? state) { }
    protected internal virtual void OnCommandResult(
        CommandFlow flow, CommandResult result, object? state) { }
    protected internal virtual void OnDrainStarted(CommandFlow flow, object? state) { }
}

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct CommandFlowOptions
{
    public CommandFlowObserver? Observer { get; init; }
    public object? ObserverState { get; init; }
    public CommandList Commands { get; init; }
    // Optional per-flow override for time spent waiting in the protocol backlog.
    public TimeSpan? PendingTimeout { get; init; }
}

internal sealed class CommandExecutionColdState
{
    internal bool CancelRequested;
    internal CancellationToken CallerToken;
    internal CancellationTokenRegistration CallerRegistration;
    internal CancellationToken DeliverToken;
    internal int Scope;
    internal int Timing;
    internal int SubsequentTiming;
    internal TaskCompletionSource? Delivery;
    internal object? EpisodeKey;
    internal Exception? CloseException;
    // Replayed by later consumer calls once the flow reached its terminal.
    internal Exception? TerminalException;
    // Command errors observed while draining without a consumer. Multiple Sync windows may each
    // produce an independent ErrorResponse, all of which belong to the waiting disposer.
    internal List<Exception>? DrainErrors;
}

internal enum CommandExecutionCancellationScope : byte
{
    CurrentWindow = 1,
    RemainingFlow = 2
}

// Mutable execution state is stored inline by each concrete host. Core algorithms re-enter this
// field through their host-specific ops value after every await; they never mutate a copied struct.
[StructLayout(LayoutKind.Auto)]
internal struct CommandExecutionState
{
    internal int Phase;
    internal CommandList Commands;
    internal TimeSpan? PendingTimeout;
    internal int CommandIndex;
    internal PgClientFlow.Context Context;
    internal bool ContextPublished;
    internal CommandResult? Current;
    internal bool CurrentPublished;
    internal bool ReadFlowRfq;
    internal bool ConsumerDetached;
    internal bool ConsumerObservedCompletion;
    internal Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> ReadySource;
    internal int ReadyCompletion;
    internal Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> PipelineTaskSource;
    internal CancellationToken FlowToken;
    internal CancellationTokenRegistration FlowRegistration;
    internal CommandExecutionColdState? ColdState;
    internal FlowHandoffEvent? HandoffEvent;
    internal bool SyncHandoffClaimed;
    internal int DrainStarted;
    internal bool EnableActivationTimeout;
    internal bool WaitForDrainOnDispose;
}

/// Executes an ordered command list with consumer-owned synchronous or asynchronous result decoding.
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed partial class CommandFlow : PgClientFlow, IValueTaskSource<bool>, IValueTaskSource
{
    static readonly TimeSpan ConsumerDrainCancellationGracePeriod = TimeSpan.FromSeconds(1);
    CommandExecutionState _state;
    CommandFlowObserver? _commandObserver;
    object? _commandObserverState;

    CommandFlow(bool async, TimeSpan? pendingTimeout = null)
        : base(supportsDeferredFlush: true)
    {
        _state.CommandIndex = -1;
        _state.EnableActivationTimeout = true;
        _state.WaitForDrainOnDispose = true;
        _state.PendingTimeout = pendingTimeout;
        IsAsync = async;
        if (!async)
            _state.HandoffEvent = new(false);
    }

    public CommandFlow(bool async, params ReadOnlySpan<Command> commands)
        : this(async)
        => Initialize(async, commands);

    internal CommandFlow(
        bool async, bool enableActivationTimeout, params ReadOnlySpan<Command> commands)
        : this(async, commands)
        => _state.EnableActivationTimeout = enableActivationTimeout;

    internal CommandFlow(bool async, CommandList commands, TimeSpan? pendingTimeout = null)
        : this(async, pendingTimeout)
        => Initialize(async, new CommandFlowOptions
        {
            Commands = commands,
            PendingTimeout = pendingTimeout
        });

    public CommandFlow(bool async, in CommandFlowOptions options)
        : this(async, options.PendingTimeout)
        => Initialize(async, options);

    public CommandFlow Initialize(bool async, params ReadOnlySpan<Command> commands)
        => Initialize(async, new CommandFlowOptions { Commands = new(commands) });

    public CommandFlow Initialize(bool async, in CommandFlowOptions options)
    {
        IsAsync = async;
        if (!async)
            _state.HandoffEvent ??= new(false);
        var commands = options.Commands;
        if (commands.Count is 0)
            return this;
        _state.Commands = commands;
        _state.PendingTimeout = options.PendingTimeout;
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
    private protected override FlowHandoffEvent? HandoffEvent => _state.HandoffEvent;
    protected override bool EnableActivationTimeout => _state.EnableActivationTimeout;
    protected override TimeSpan? PendingTimeout => _state.PendingTimeout;
    internal override TimeSpan? BackendCancellationGracePeriod
        => Volatile.Read(ref _state.ConsumerDetached)
            ? ConsumerDrainCancellationGracePeriod
            : null;

    internal override void BindCallerToken(CancellationToken cancellationToken)
        => _state.FlowToken = cancellationToken;
    internal override CancellationToken MigrationCancellationToken
        => _state.FlowToken;

    public Enumerator GetEnumerator()
        => new(this, default);

    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        // A missing enumeration token must not erase the token captured when the flow was queued.
        if (cancellationToken.CanBeCanceled)
            _state.FlowToken = cancellationToken;
        return new(this, cancellationToken);
    }

    internal CommandResult? CurrentResult => _state.Current;
    public int CommandCount => _state.Commands.Count;
    public bool IsResultReady => Core.IsResultReady;
    internal int VisibleCommandCount => _state.Commands.VisibleCount;
    internal ValueTask<bool> MoveNextResultAsync(CancellationToken cancellationToken)
        => Core.MoveNextAsync(cancellationToken);
    internal void DisposeResults() => Core.Dispose();
    internal ValueTask DisposeResultsAsync() => Core.DisposeAsync();
    internal bool WaitForDrainOnDispose
    {
        get => _state.WaitForDrainOnDispose;
        set => _state.WaitForDrainOnDispose = value;
    }

    CommandExecutionCore<Ops> Core => new(new(this));

    internal ValueTask<long> ConsumeNonQueryAsync(CancellationToken cancellationToken = default)
        => Core.ConsumeNonQueryAsync(cancellationToken);

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
        => Core.ExecuteAuto(context);

    internal Task CancelAsync() => Core.CancelAsync();

    internal override bool ResetsSharedReadStateBeforeRelease => true;
    protected override void OnStopping(Exception exception) => Core.OnStopping(exception);
    protected override void OnAbort(Exception exception) => Core.OnAbort(exception);
    internal override void Fail(Exception exception) => Core.Fail(exception);
    protected override void OnReleasing(Exception? exception) => Core.OnReleasing(exception);
    protected override void OnDiscarded() => Core.OnDiscarded();
    protected override void OnReset() => Core.OnReset();

    readonly struct Ops(CommandFlow owner) : ICommandExecutionFlowOps<Ops>
    {
        readonly CommandFlow _owner = owner;

        public static Ops Create(PgClientFlow flow) => new((CommandFlow)flow);
        public PgClientFlow Flow => _owner;
        public ref CommandExecutionState State => ref _owner._state;
        public bool IsAsync
        {
            get => _owner.IsAsync;
            set => _owner.IsAsync = value;
        }
        public bool IsAsyncAtDispatch => _owner.IsAsyncAtDispatch;
        public bool HasSuccessfulActivation => _owner.HasSuccessfulActivation;
        public void WaitForSyncHandoff() => _owner.WaitForSyncHandoff();
        public void OnCommandResult(CommandResult result)
            => _owner._commandObserver?.OnCommandResult(
                _owner, result, _owner._commandObserverState);
        public void OnDrainStarted()
            => _owner._commandObserver?.OnDrainStarted(
                _owner, _owner._commandObserverState);
        public void OnDiscarded()
            => _owner.GetObserver(out var observerState)?.OnCompleting(
                _owner, null, observerState);
    }
}

internal interface ICommandExecutionFlowOps<TSelf>
    where TSelf : struct, ICommandExecutionFlowOps<TSelf>
{
    static abstract TSelf Create(PgClientFlow flow);
    PgClientFlow Flow { get; }
    ref CommandExecutionState State { get; }
    bool IsAsync { get; set; }
    bool IsAsyncAtDispatch { get; }
    bool HasSuccessfulActivation { get; }
    void WaitForSyncHandoff();
    void OnCommandResult(CommandResult result);
    void OnDrainStarted();
    void OnDiscarded();
}

readonly struct CommandExecutionCore<TOps>(TOps ops)
    where TOps : struct, ICommandExecutionFlowOps<TOps>
{
    const int PhaseInitial = 0;
    const int PhaseReading = 1;
    const int PhaseResultReady = 2;
    const int PhaseDraining = 3;
    const int PhaseCompleted = 4;

    readonly TOps _ops = ops;
    ref CommandExecutionState _state => ref _ops.State;
    internal bool IsResultReady => Volatile.Read(ref _state.Phase) is PhaseResultReady;
    bool IsSinglePublishedCommand
        => _state.Commands.Count is 1
            && !_state.Commands.ItemRef(0).SuppressEnumeration;

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<long> ConsumeNonQueryAsync(
        CancellationToken cancellationToken = default)
    {
        var recordsAffected = -1L;
        if (cancellationToken.CanBeCanceled)
            _state.FlowToken = cancellationToken;
        try
        {
            while (await MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                var result = _state.Current!;
                await result.CompleteAsync().ConfigureAwait(false);
                var affected = result.GetCommandComplete().BatchRecordsAffected;
                if (affected >= 0)
                    recordsAffected = recordsAffected < 0
                        ? affected
                        : checked(recordsAffected + affected);
            }
            return recordsAffected;
        }
        finally
        {
            await DisposeAsync().ConfigureAwait(false);
        }
    }

    CommandExecutionColdState GetOrCreateColdState()
        => Volatile.Read(ref _state.ColdState) ??
            Interlocked.CompareExchange(ref _state.ColdState, new(), null) ?? _state.ColdState;

    bool IsClosed => Volatile.Read(ref _state.ColdState)?.CloseException is not null;
    bool IsCancelRequested => Volatile.Read(ref _state.ColdState) is { CancelRequested: true };
    bool HasDecoder => _state.ContextPublished && _ops.HasSuccessfulActivation;

    internal ValueTask<FlowTasks> ExecuteAuto(PgClientFlow.Context context)
    {
        _state.Context = context;
        _state.ContextPublished = true;
        ValueTask writeTask;
        try
        {
            ref readonly var template = ref _state.Commands.ItemRef(_state.Commands.Count - 1);
            var appendSync = !template.WithSync;
            _state.ReadFlowRfq = appendSync;
            // Caller cancellation never cancels wire I/O. The consumer observes the latched intent and
            // drains its command to RFQ instead.
            writeTask = _ops.IsAsync
                ? _state.Commands.WriteCommandsAsync(context.GetEncoder(), appendSync, default)
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
            activation.UnsafeOnCompleted(static state =>
                new CommandExecutionCore<TOps>(TOps.Create((PgClientFlow)state!))
                    .OnActivationSettled(onExecutorStrand: false), _ops.Flow);
        return new(new FlowTasks(writeTask, new ValueTask((IValueTaskSource)_ops.Flow, _state.PipelineTaskSource.Version)));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    ValueTask WriteCommandsResumable(PgClientFlow.Context context, bool appendSync)
    {
        var encoder = context.GetEncoder();
        ValueTask writeTask;
        using (encoder.BeginResumableWriteScope())
            writeTask = _state.Commands.WriteCommandsResumable(encoder, appendSync);
        return writeTask.IsCompleted ? writeTask : encoder.RunResumableTask(writeTask);
    }

    // Runs on the executor strand when activation already settled, else on the activation dispatch.
    // The executor strand never runs consumer code. An activation dispatch is a detached work item
    // whose only remaining work is this wake, so the consumer may continue on it directly.
    void OnActivationSettled(bool onExecutorStrand)
    {
        try
        {
            _ = _state.Context.GetDecoderAsync().ConfigureAwait(false).GetAwaiter().GetResult();
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
        // Termination propagation enumerates pipeline positions best-effort. A flow can cross from
        // the in-flight store into the activated slot while that pass is being taken and miss it.
        // Recheck after publishing readiness: OnStopping arbitrates PhaseInitial against a consumer's
        // PhaseReading claim, so exactly one side owns the decoder and eventual pipeline completion.
        if (_state.Context.StoppingToken.IsCancellationRequested)
        {
            OnStopping(_state.Context.FlowTerminationException);
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
        if (Interlocked.CompareExchange(ref _state.ReadyCompletion, 1, 0) != 0)
            return false;
        if (exception is null)
            _state.ReadySource.SetResult(true, runContinuationsAsynchronously);
        else
            _state.ReadySource.SetException(exception, runContinuationsAsynchronously);
        return true;
    }

    void CompletePipelineTask(Exception? exception, bool runContinuationsAsynchronously = false)
    {
        if (Interlocked.Exchange(ref _state.Phase, PhaseCompleted) is PhaseCompleted)
            return;
        if (exception is null)
            _state.PipelineTaskSource.SetResult(true, runContinuationsAsynchronously);
        else
            _state.PipelineTaskSource.SetException(exception, runContinuationsAsynchronously);
    }

    void EnsureSyncHandoff()
    {
        if (_ops.IsAsyncAtDispatch)
            ThrowHelper.ThrowInvalidOperation(
                "Synchronous result consumption requires a flow initialized for synchronous execution.");
        if (_state.SyncHandoffClaimed)
            return;
        _ops.WaitForSyncHandoff();
        _state.SyncHandoffClaimed = true;
    }

    internal bool MoveNext()
    {
        EnsureSyncHandoff();
        while (true)
        {
            var phase = Volatile.Read(ref _state.Phase);
            switch (phase)
            {
                case PhaseInitial:
                    if (Interlocked.CompareExchange(ref _state.Phase, PhaseReading, PhaseInitial) != PhaseInitial)
                        continue;
                    _state.CommandIndex = 0;
                    return First();
                case PhaseResultReady:
                    if (Interlocked.CompareExchange(ref _state.Phase, PhaseReading, PhaseResultReady) != PhaseResultReady)
                        continue;
                    return NextBatch();
                case PhaseReading:
                    ThrowHelper.ThrowInvalidOperation("A read is already in progress on this flow.");
                    return false;
                case PhaseDraining:
                    _ops.Flow.WaitForCompleteSynchronously();
                    throw Volatile.Read(ref _state.ColdState)?.TerminalException
                        ?? ThrowHelper.ThrowInvalidOperation("The flow was disposed.");
                default:
                    if (Volatile.Read(ref _state.ColdState)?.TerminalException is { } terminal)
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
            Debug.Assert(!_state.ConsumerDetached);
            RegisterCancellation(default);
            var result = IsSinglePublishedCommand
                ? ReadResult(0)
                : ReadNextPublishedResult();
            return result is not null && PublishSynchronousResult(result);
        }
        catch (TimeoutException ex)
        {
            HandleReadTimeout(ex);
            throw;
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
            var result = _state.Current!;
            var completeError = CompleteCurrentResult();
            _state.CurrentPublished = false;
            if (Volatile.Read(ref _state.ColdState)?.TerminalException is { } consumerFault)
            {
                Interlocked.Exchange(ref _state.Phase, PhaseDraining);
                NotifyDrainStarted();
                _state.ConsumerDetached = true;
                Drain();
                ExceptionDispatchInfo.Throw(consumerFault);
            }
            if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                SkipDiscardedCommands();

            _state.CommandIndex++;
            if (IsSinglePublishedCommand)
            {
                CompleteBatch();
                _state.ConsumerObservedCompletion = true;
                return false;
            }
            var next = ReadNextPublishedResult();
            if (next is not null)
                return PublishSynchronousResult(next);

            return false;
        }
        catch (TimeoutException ex)
        {
            HandleReadTimeout(ex);
            throw;
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
            throw;
        }
    }

    bool PublishSynchronousResult(CommandResult result)
    {
        _state.Current = result;
        _state.CurrentPublished = true;
        Interlocked.Exchange(ref _state.Phase, PhaseResultReady);
        var context = _state.Context;
        if (!IsClosed && context.StoppingToken.IsCancellationRequested)
            Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException,
                context.FlowTerminationException, null);
        if (!IsCancelRequested && !IsClosed)
            return true;

        if (Interlocked.CompareExchange(ref _state.Phase, PhaseReading, PhaseResultReady) == PhaseResultReady)
            Drain();
        else
            _ops.Flow.WaitForCompleteSynchronously();
        throw Volatile.Read(ref _state.ColdState)?.TerminalException
            ?? ThrowHelper.ThrowUnexpected("A latched flow completed without a terminal outcome.");
    }

    void WaitForReadySynchronously()
    {
        var ready = new ValueTask<bool>((IValueTaskSource<bool>)_ops.Flow, _state.ReadySource.Version);
        if (ready.IsCompleted)
            _ = ready.GetAwaiter().GetResult();
        else
            _ = ready.AsTask().GetAwaiter().GetResult();
    }

    internal ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
    {
        if (!_ops.IsAsyncAtDispatch)
            return ValueTask.FromException<bool>(ThrowHelper.ThrowInvalidOperation(
                "Asynchronous result consumption requires a flow initialized for asynchronous execution."));
        while (true)
        {
            var phase = Volatile.Read(ref _state.Phase);
            switch (phase)
            {
                case PhaseInitial:
                    if (cancellationToken.IsCancellationRequested)
                        return CancelBeforeRead(cancellationToken);
                    if (Interlocked.CompareExchange(ref _state.Phase, PhaseReading, PhaseInitial) != PhaseInitial)
                        continue;
                    _state.CommandIndex = 0;
                    return FirstAsync(cancellationToken);
                case PhaseResultReady:
                    if (cancellationToken.IsCancellationRequested)
                        return CancelBeforeRead(cancellationToken);
                    if (Interlocked.CompareExchange(ref _state.Phase, PhaseReading, PhaseResultReady) != PhaseResultReady)
                        continue;
                    return NextBatchAsync(cancellationToken);
                case PhaseReading:
                    return ValueTask.FromException<bool>(
                        ThrowHelper.ThrowInvalidOperation("A read is already in progress on this flow."));
                case PhaseDraining:
                    return AwaitTakeoverAsync();
                default:
                    return Volatile.Read(ref _state.ColdState)?.TerminalException is { } terminal
                        ? ValueTask.FromException<bool>(terminal)
                        : new(false);
            }
        }
    }

    // A pre-cancelled token releases the caller immediately. The wire still drains to RFQ.
    ValueTask<bool> CancelBeforeRead(CancellationToken cancellationToken)
    {
        RequestCancel(cancellationToken, CommandExecutionCancellationScope.CurrentWindow);
        return ValueTask.FromException<bool>(new OperationCanceledException(cancellationToken));
    }

    // The consumer parks behind a takeover drain and receives the outcome that caused it.
    async ValueTask<bool> AwaitTakeoverAsync()
    {
        await WaitForCompletionAsync().ConfigureAwait(false);
        throw Volatile.Read(ref _state.ColdState)?.TerminalException ?? ThrowHelper.ThrowInvalidOperation("The flow was disposed.");
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<bool> FirstAsync(CancellationToken cancellationToken)
    {
        Exception? deliver;
        try
        {
            await new ValueTask<bool>((IValueTaskSource<bool>)_ops.Flow, _state.ReadySource.Version).ConfigureAwait(false);
            Debug.Assert(!_state.ConsumerDetached);
            RegisterCancellation(cancellationToken);
            var result = IsSinglePublishedCommand
                ? await ReadResultAsync(0).ConfigureAwait(false)
                : await ReadNextPublishedResultAsync().ConfigureAwait(false);
            if (result is null)
                return false;
            _state.Current = result;
            _state.CurrentPublished = true;
            // Publish the idle state, then recheck the latches. A latch that landed between the read
            // and this publication found no idle owner to take over, so this frame must act on it.
            Interlocked.Exchange(ref _state.Phase, PhaseResultReady);
            // Graceful stopping faults a result that arrives after the close began, as the ordinary
            // flow does at each result boundary. Latch it so the drain delivers that close.
            var context = _state.Context;
            if (!IsClosed && context.StoppingToken.IsCancellationRequested)
                Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException,
                    context.FlowTerminationException, null);
            if (!IsCancelRequested && !IsClosed)
                return true;
            if (Interlocked.CompareExchange(ref _state.Phase, PhaseReading, PhaseResultReady) == PhaseResultReady)
            {
                await DrainAsync().ConfigureAwait(false);
            }
            else
            {
                // The latching side took the decoder first. Park behind its drain.
                await WaitForCompletionAsync().ConfigureAwait(false);
            }
            deliver = Volatile.Read(ref _state.ColdState)?.TerminalException;
        }
        catch (TimeoutException ex)
        {
            HandleReadTimeout(ex);
            throw;
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
            var result = _state.Current!;
            var resultEnumerator = _state.Context.GetProtocolStatic<CommandFlow.ReadState>()
                .ResultMessageEnumerator;
            await resultEnumerator.DisposeAsync().ConfigureAwait(false);
            var completeError = resultEnumerator.CompleteError;
            _state.CurrentPublished = false;
            if (Volatile.Read(ref _state.ColdState)?.TerminalException is { } consumerFault)
            {
                Interlocked.Exchange(ref _state.Phase, PhaseDraining);
                NotifyDrainStarted();
                _state.ConsumerDetached = true;
                await DrainAsync().ConfigureAwait(false);
                ExceptionDispatchInfo.Throw(consumerFault);
            }
            if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                await SkipDiscardedCommandsAsync().ConfigureAwait(false);

            _state.CommandIndex++;
            if (IsSinglePublishedCommand)
            {
                await CompleteBatchAsync().ConfigureAwait(false);
                _state.ConsumerObservedCompletion = true;
                return false;
            }
            var next = await ReadNextPublishedResultAsync().ConfigureAwait(false);
            if (next is not null)
            {
                result = next;
                _state.Current = result;
                _state.CurrentPublished = true;
                Interlocked.Exchange(ref _state.Phase, PhaseResultReady);
                var context = _state.Context;
                if (!IsClosed && context.StoppingToken.IsCancellationRequested)
                    Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException,
                        context.FlowTerminationException, null);
                if (!IsCancelRequested && !IsClosed)
                    return true;

                if (Interlocked.CompareExchange(ref _state.Phase, PhaseReading, PhaseResultReady) == PhaseResultReady)
                    await DrainAsync().ConfigureAwait(false);
                else
                    await WaitForCompletionAsync().ConfigureAwait(false);
                throw Volatile.Read(ref _state.ColdState)?.TerminalException
                    ?? ThrowHelper.ThrowUnexpected("A latched flow completed without a terminal outcome.");
            }

            return false;
        }
        catch (TimeoutException ex)
        {
            HandleReadTimeout(ex);
            throw;
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
        CommandResult? result = _state.Current;
        while (_state.CommandIndex < _state.Commands.Count)
        {
            result = await ReadResultAsync(_state.CommandIndex).ConfigureAwait(false);
            _state.Current = result;
            _state.CurrentPublished = false;
            if (!_state.Commands.ItemRef(_state.CommandIndex).SuppressEnumeration)
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
                (cold.DrainErrors ??= new()).Add(exception);
                if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                    await SkipDiscardedCommandsAsync().ConfigureAwait(false);
                _state.CommandIndex++;
                _state.Current = null;
                Interlocked.Exchange(ref _state.Phase, PhaseDraining);
                NotifyDrainStarted();
                _state.ConsumerDetached = true;
                await DrainAsync().ConfigureAwait(false);
                throw exception;
            }

            _state.CommandIndex++;
        }

        if (result is null)
            throw ThrowHelper.ThrowInvalidOperation("The flow contains no commands.");
        await CompleteBatchAsync().ConfigureAwait(false);
        _state.ConsumerObservedCompletion = true;
        return null;
    }

    CommandResult? ReadNextPublishedResult()
    {
        CommandResult? result = _state.Current;
        while (_state.CommandIndex < _state.Commands.Count)
        {
            result = ReadResult(_state.CommandIndex);
            _state.Current = result;
            _state.CurrentPublished = false;
            if (!_state.Commands.ItemRef(_state.CommandIndex).SuppressEnumeration)
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
                (cold.DrainErrors ??= new()).Add(exception);
                if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                    SkipDiscardedCommands();
                _state.CommandIndex++;
                _state.Current = null;
                Interlocked.Exchange(ref _state.Phase, PhaseDraining);
                NotifyDrainStarted();
                _state.ConsumerDetached = true;
                Drain();
                throw exception;
            }

            _state.CommandIndex++;
        }

        if (result is null)
            throw ThrowHelper.ThrowInvalidOperation("The flow contains no commands.");
        CompleteBatch();
        _state.ConsumerObservedCompletion = true;
        return null;
    }

    // Reads through the command's execute prelude and initializes the protocol-static result.
    async ValueTask<CommandResult> ReadResultAsync(int commandIndex)
    {
        var context = _state.Context;
        var decoder = context.Decoder;
        // After close, a fresh command must not consume bytes left by its predecessor.
        if (context.IsProtocolClosed)
            throw context.FlowTerminationException;
        PgError? error;
        RowDescription? requestedRowDescription;
        ref readonly var command = ref _state.Commands.ItemRef(commandIndex);
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
        var context = _state.Context;
        var decoder = context.Decoder;
        if (context.IsProtocolClosed)
            throw context.FlowTerminationException;
        ref readonly var command = ref _state.Commands.ItemRef(commandIndex);
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
        var context = _state.Context;
        ref readonly var readState = ref context.GetProtocolStatic<CommandFlow.ReadState>();
        ref readonly var command = ref _state.Commands.ItemRef(commandIndex);
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
        result.Initialize(_ops.Flow, commandIndex, descriptor, requestedRowDescription,
            !command.DescribeOnly, command.IsSimple(), error);
        _ops.OnCommandResult(result);
        return result;
    }

    async ValueTask<(PgError Error, TransactionStatus TransactionStatus)?> CompleteCurrentResultAsync()
    {
        var enumerator = _state.Context.GetProtocolStatic<CommandFlow.ReadState>().ResultMessageEnumerator;
        await enumerator.DisposeAsync().ConfigureAwait(false);
        return enumerator.CompleteError;
    }

    (PgError Error, TransactionStatus TransactionStatus)? CompleteCurrentResult()
    {
        var enumerator = _state.Context.GetProtocolStatic<CommandFlow.ReadState>().ResultMessageEnumerator;
        enumerator.Dispose();
        return enumerator.CompleteError;
    }

    async ValueTask SkipDiscardedCommandsAsync()
    {
        while (++_state.CommandIndex < _state.Commands.Count && !_state.Commands[_state.CommandIndex].WithSync) { }
        await ReadRfqAsync().ConfigureAwait(false);
        if (_state.CommandIndex == _state.Commands.Count)
            _state.ReadFlowRfq = false;
    }

    void SkipDiscardedCommands()
    {
        while (++_state.CommandIndex < _state.Commands.Count && !_state.Commands[_state.CommandIndex].WithSync) { }
        ReadRfq();
        if (_state.CommandIndex == _state.Commands.Count)
            _state.ReadFlowRfq = false;
    }

    async ValueTask ReadRfqAsync()
    {
        var message = await _state.Context.Decoder.GetNextAsync().ConfigureAwait(false);
        if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
            PgErrorException.Throw(rfqError);
    }

    void ReadRfq()
    {
        var message = _state.Context.Decoder.GetNext();
        if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
            PgErrorException.Throw(rfqError);
    }

    async ValueTask CompleteBatchAsync()
    {
        if (_state.ReadFlowRfq)
            await ReadRfqAsync().ConfigureAwait(false);
        await DisposeRegistrationsAsync().ConfigureAwait(false);
        Finish();
    }

    void CompleteBatch()
    {
        if (_state.ReadFlowRfq)
            ReadRfq();
        DisposeRegistrations();
        Finish();
    }

    // The wire is at this command's RFQ. Release the shared read objects, record the outcome the
    // consumer must observe, then complete the pipeline task.
    void Finish()
    {
        _state.Context.GetProtocolStatic<CommandFlow.ReadState>().Reset();
        _state.Current = null;
        _state.CurrentPublished = false;
        if (IsCancelRequested)
            Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException,
                new OperationCanceledException(_state.ColdState!.DeliverToken), null);
        else if (Volatile.Read(ref _state.ColdState)?.CloseException is { } close)
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
        if (Volatile.Read(ref _state.Phase) == PhaseCompleted)
            return;
        DisposeRegistrations();
        _state.Current = null;
        _state.CurrentPublished = false;
        if (HasDecoder)
            _state.Context.GetProtocolStatic<CommandFlow.ReadState>().Reset();
        CompletePipelineTask(exception);
    }

    // Takes an idle decoder for a drain. Returns false when a frame already owns it, which will observe
    // the latch itself, or when the flow reached its terminal.
    bool TryTakeOverDrain()
    {
        while (true)
        {
            var phase = Volatile.Read(ref _state.Phase);
            if (phase is PhaseInitial && !HasDecoder)
                return false;
            if (phase is not (PhaseInitial or PhaseResultReady))
                return false;
            if (Interlocked.CompareExchange(ref _state.Phase, PhaseDraining, phase) != phase)
                continue;
            NotifyDrainStarted();
            // Decoder takeover does not imply consumer abandonment. Explicit cancellation and
            // graceful close also drain autonomously while retaining their consumer semantics.
            ThreadPool.UnsafeQueueUserWorkItem(static state =>
                _ = new CommandExecutionCore<TOps>(TOps.Create((PgClientFlow)state!)).DrainAsync(),
                _ops.Flow);
            return true;
        }
    }

    void NotifyDrainStarted()
    {
        if (Interlocked.Exchange(ref _state.DrainStarted, 1) is 0)
            _ops.OnDrainStarted();
    }

    // Autonomous drain. Owns the decoder until the pipeline task completes. Never throws.
    async ValueTask DrainAsync()
    {
        try
        {
            var result = _state.Current;
            if (result is null)
            {
                await new ValueTask<bool>((IValueTaskSource<bool>)_ops.Flow, _state.ReadySource.Version).ConfigureAwait(false);
                if (_state.CommandIndex < 0)
                    _state.CommandIndex = 0;
                if (_state.CommandIndex >= _state.Commands.Count)
                {
                    await CompleteBatchAsync().ConfigureAwait(false);
                    return;
                }
                result = await ReadResultAsync(_state.CommandIndex).ConfigureAwait(false);
            }

            while (true)
            {
                var completeError = await CompleteCurrentResultAsync().ConfigureAwait(false);
                CaptureDrainError(result, completeError);
                _state.CurrentPublished = false;
                if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                    await SkipDiscardedCommandsAsync().ConfigureAwait(false);
                if (++_state.CommandIndex >= _state.Commands.Count)
                    break;
                result = await ReadResultAsync(_state.CommandIndex).ConfigureAwait(false);
            }
            await CompleteBatchAsync().ConfigureAwait(false);
        }
        catch (TimeoutException ex)
        {
            // A timeout during semantic drain escalates the same cancellation episode immediately.
            // The pipeline failure then hands any remaining wire obligation to recovery.
            RequestCancel(default, CommandExecutionCancellationScope.RemainingFlow,
                BackendCancellationTiming.Immediate, BackendCancellationTiming.AtReadFrontier);
            FaultFromOwner(ex);
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
        }
    }

    // A read timeout is terminal for the consumer but not for the wire obligation. This frame has
    // released the decoder read tenure, so transfer ownership to an autonomous semantic drain while
    // the original MoveNext returns its timeout immediately.
    void HandleReadTimeout(TimeoutException exception)
    {
        Interlocked.CompareExchange(
            ref GetOrCreateColdState().TerminalException, exception, null);
        _state.ConsumerDetached = true;
        NotifyDrainStarted();
        Interlocked.Exchange(ref _state.Phase, PhaseDraining);
        RequestCancel(default, CommandExecutionCancellationScope.RemainingFlow,
            BackendCancellationTiming.Immediate, BackendCancellationTiming.AtReadFrontier);
        ThreadPool.UnsafeQueueUserWorkItem(static state =>
            _ = new CommandExecutionCore<TOps>(TOps.Create((PgClientFlow)state!)).DrainAsync(),
            _ops.Flow);
    }

    void Drain()
    {
        try
        {
            var result = _state.Current;
            if (result is null)
            {
                // Synchronous disposal before any read. The activation bridge normally completed long
                // ago, so this bridge is rarely more than a status check.
                new ValueTask<bool>((IValueTaskSource<bool>)_ops.Flow, _state.ReadySource.Version).AsTask().GetAwaiter().GetResult();
                if (_state.CommandIndex < 0)
                    _state.CommandIndex = 0;
                if (_state.CommandIndex >= _state.Commands.Count)
                {
                    CompleteBatch();
                    return;
                }
                result = ReadResult(_state.CommandIndex);
            }

            while (true)
            {
                var completeError = CompleteCurrentResult();
                CaptureDrainError(result, completeError);
                _state.CurrentPublished = false;
                if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                    SkipDiscardedCommands();
                if (++_state.CommandIndex >= _state.Commands.Count)
                    break;
                result = ReadResult(_state.CommandIndex);
            }
            CompleteBatch();
        }
        catch (Exception ex)
        {
            FaultFromOwner(ex);
        }
    }

    internal ValueTask DisposeAsync()
    {
        while (true)
        {
            var phase = Volatile.Read(ref _state.Phase);
            switch (phase)
            {
                case PhaseInitial:
                case PhaseResultReady:
                    if (Interlocked.CompareExchange(ref _state.Phase, PhaseDraining, phase) != phase)
                        continue;
                    NotifyDrainStarted();
                    _state.ConsumerDetached = true;
                    if (_state.Current is { IsComplete: false }
                        || _state.CommandIndex + 1 < _state.Commands.Count)
                        RequestConsumerDrainCancellation();
                    return _state.WaitForDrainOnDispose ? DisposeDrainAsync() : FireAndForgetDrain();
                case PhaseReading:
                    _state.ConsumerDetached = true;
                    NotifyDrainStarted();
                    RequestConsumerDrainCancellation();
                    return _state.WaitForDrainOnDispose ? DisposeCompletedAsync() : default;
                default:
                    return !_state.WaitForDrainOnDispose || _state.ConsumerObservedCompletion
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
        ThrowDrainErrors();
    }

    // Flow completion is independent of errors accumulated while draining. A close is a clean
    // terminal for a disposing consumer.
    async ValueTask WaitForCompletionAsync()
    {
        try
        {
            await _ops.Flow.WaitForComplete().ConfigureAwait(false);
        }
        catch (PgClientClosedException)
        {
        }
    }

    internal void Dispose()
    {
        if (!_ops.IsAsyncAtDispatch)
            EnsureSyncHandoff();
        while (true)
        {
            var phase = Volatile.Read(ref _state.Phase);
            switch (phase)
            {
                case PhaseInitial:
                case PhaseResultReady:
                    if (Interlocked.CompareExchange(ref _state.Phase, PhaseDraining, phase) != phase)
                        continue;
                    NotifyDrainStarted();
                    _state.ConsumerDetached = true;
                    if (_state.Current is { IsComplete: false }
                        || _state.CommandIndex + 1 < _state.Commands.Count)
                        RequestConsumerDrainCancellation();
                    Drain();
                    if (_state.WaitForDrainOnDispose)
                        DisposeCompleted();
                    return;
                case PhaseReading:
                    _state.ConsumerDetached = true;
                    NotifyDrainStarted();
                    RequestConsumerDrainCancellation();
                    if (_state.WaitForDrainOnDispose)
                        DisposeCompleted();
                    return;
                default:
                    if (_state.WaitForDrainOnDispose && !_state.ConsumerObservedCompletion)
                        DisposeCompleted();
                    return;
            }
        }
    }

    void DisposeCompleted()
    {
        try
        {
            _ops.Flow.WaitForCompleteSynchronously();
        }
        catch (PgClientClosedException)
        {
        }
        ThrowDrainErrors();
    }

    void CaptureDrainError(CommandResult result,
        (PgError Error, TransactionStatus TransactionStatus)? completeError)
    {
        if (!_state.ConsumerDetached || _state.CurrentPublished)
            return;
        var error = result.Error ?? completeError?.Error;
        if (error is null || IsOwnCancellation(error))
            return;
        var cold = GetOrCreateColdState();
        (cold.DrainErrors ??= new()).Add(PgErrorException.Create(error));
    }

    void ThrowDrainErrors()
    {
        if (Volatile.Read(ref _state.ColdState)?.DrainErrors is not { Count: > 0 } errors)
            return;
        if (errors.Count is 1)
            ExceptionDispatchInfo.Throw(errors[0]);
        throw new AggregateException(errors);
    }

    // Give the current window a chance to finish naturally. Once disposal advances into an unread
    // successor, dispatch at its read frontier instead of paying the grace period for every window.
    void RequestConsumerDrainCancellation()
        => RequestCancel(default, CommandExecutionCancellationScope.RemainingFlow,
            BackendCancellationTiming.AfterGrace,
            BackendCancellationTiming.AtReadFrontier);

    void RegisterCancellation(CancellationToken callerToken)
    {
        // Keep the second token/registration pair off ordinary flow objects. Default-token traffic
        // does not need cancellation state at all and remains the allocation/footprint hot path.
        var cancellation = Volatile.Read(ref _state.ColdState);
        if (callerToken.CanBeCanceled || cancellation is not null)
        {
            cancellation ??= GetOrCreateColdState();
            if (callerToken != cancellation.CallerToken)
            {
                var callerRegistration = cancellation.CallerRegistration;
                cancellation.CallerRegistration = default;
                callerRegistration.Dispose();
                cancellation.CallerToken = callerToken;
            }
            if (callerToken.CanBeCanceled && cancellation.CallerRegistration == default)
                cancellation.CallerRegistration = callerToken.UnsafeRegister(static (state, token)
                    => new CommandExecutionCore<TOps>(TOps.Create((PgClientFlow)state!)).RequestCancel(
                        token, CommandExecutionCancellationScope.CurrentWindow), _ops.Flow);
        }

        if (_state.FlowToken.CanBeCanceled && _state.FlowRegistration == default)
            _state.FlowRegistration = _state.FlowToken.UnsafeRegister(static (state, token)
                => new CommandExecutionCore<TOps>(TOps.Create((PgClientFlow)state!)).RequestCancel(
                    token, CommandExecutionCancellationScope.RemainingFlow), _ops.Flow);
    }

    ValueTask DisposeRegistrationsAsync()
    {
        var cancellation = Volatile.Read(ref _state.ColdState);
        var callerRegistration = cancellation?.CallerRegistration ?? default;
        if (callerRegistration == default && _state.FlowRegistration == default)
            return default;
        if (cancellation is not null)
            cancellation.CallerRegistration = default;
        var flowRegistration = _state.FlowRegistration;
        _state.FlowRegistration = default;
        return DisposeRegistrationsAsync(callerRegistration, flowRegistration);

        static async ValueTask DisposeRegistrationsAsync(
            CancellationTokenRegistration callerRegistration,
            CancellationTokenRegistration flowRegistration)
        {
            await callerRegistration.DisposeAsync().ConfigureAwait(false);
            await flowRegistration.DisposeAsync().ConfigureAwait(false);
        }
    }

    void DisposeRegistrations()
    {
        var cancellation = Volatile.Read(ref _state.ColdState);
        var callerRegistration = cancellation?.CallerRegistration ?? default;
        if (cancellation is not null)
            cancellation.CallerRegistration = default;
        var flowRegistration = _state.FlowRegistration;
        _state.FlowRegistration = default;
        callerRegistration.Dispose();
        flowRegistration.Dispose();
    }

    // Cancellation only latches intent and requests a backend cancel. The frame owning the decoder
    // delivers it after the wire is back at RFQ. An idle flow drains autonomously first.
    void RequestCancel(CancellationToken token, CommandExecutionCancellationScope scope,
        BackendCancellationTiming timing = BackendCancellationTiming.AfterGrace,
        BackendCancellationTiming subsequentTiming = BackendCancellationTiming.AfterGrace)
    {
        if (Volatile.Read(ref _state.Phase) == PhaseCompleted)
            return;
        var cancellation = GetOrCreateColdState();
        cancellation.DeliverToken = token;
        RaiseCancellationScope(cancellation, scope);
        RaiseCancellationTiming(ref cancellation.Timing, timing);
        RaiseCancellationTiming(ref cancellation.SubsequentTiming, subsequentTiming);
        Interlocked.Exchange(ref cancellation.CancelRequested, true);
        if (HasDecoder)
            RequestBackendCancellation();
        TryTakeOverDrain();
    }

    static void RaiseCancellationScope(CommandExecutionColdState cancellation, CommandExecutionCancellationScope scope)
    {
        var requested = (int)scope;
        var current = Volatile.Read(ref cancellation.Scope);
        while (current < requested)
        {
            var observed = Interlocked.CompareExchange(
                ref cancellation.Scope, requested, current);
            if (observed == current)
                return;
            current = observed;
        }
    }

    static void RaiseCancellationTiming(
        ref int location, BackendCancellationTiming timing)
    {
        var requested = (int)timing;
        var current = Volatile.Read(ref location);
        while (current < requested)
        {
            var observed = Interlocked.CompareExchange(ref location, requested, current);
            if (observed == current)
                return;
            current = observed;
        }
    }

    internal Task CancelAsync()
    {
        var cancellation = GetOrCreateColdState();
        var delivery = Volatile.Read(ref cancellation.Delivery)
            ?? Interlocked.CompareExchange(ref cancellation.Delivery,
                new(TaskCreationOptions.RunContinuationsAsynchronously), null)
            ?? cancellation.Delivery;
        if (Volatile.Read(ref _state.Phase) is PhaseCompleted)
        {
            delivery.TrySetResult();
            return delivery.Task;
        }
        RequestCancel(default, CommandExecutionCancellationScope.RemainingFlow);
        if (Volatile.Read(ref _state.Phase) is PhaseCompleted)
            delivery.TrySetResult();
        return delivery.Task;
    }

    void RequestBackendCancellation()
    {
        var cancellation = GetOrCreateColdState();
        var episodeKey = Volatile.Read(ref cancellation.EpisodeKey);
        if (episodeKey is null)
        {
            var created = new object();
            episodeKey = Interlocked.CompareExchange(
                ref cancellation.EpisodeKey, created, null) ?? created;
        }
        _state.Context.RequestBackendCancellation(
            _ops.Flow, _ops.Flow.CancellationWindow,
            (BackendCancellationTiming)Volatile.Read(ref cancellation.Timing),
            Volatile.Read(ref cancellation.Delivery),
            episodeKey,
            Math.Max(Volatile.Read(ref cancellation.Scope), (int)CommandExecutionCancellationScope.CurrentWindow),
            (BackendCancellationTiming)Volatile.Read(ref cancellation.SubsequentTiming));
    }

    // Graceful stop. An unactivated flow releases its consumer, the closing wire owns its response.
    // An idle activated flow drains itself to RFQ so the pipeline can complete.
    internal void OnStopping(Exception exception)
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
    internal void OnAbort(Exception exception)
    {
        Interlocked.CompareExchange(ref GetOrCreateColdState().CloseException, exception, null);
        if (CompleteReady(exception, runContinuationsAsynchronously: true))
        {
            Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, exception, null);
            return;
        }
        while (true)
        {
            var phase = Volatile.Read(ref _state.Phase);
            if (phase is not (PhaseInitial or PhaseResultReady))
                return;
            if (Interlocked.CompareExchange(ref _state.Phase, PhaseCompleted, phase) != phase)
                continue;
            Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, exception, null);
            _state.PipelineTaskSource.SetException(exception, runContinuationsAsynchronously: true);
            return;
        }
    }

    internal void Fail(Exception exception)
    {
        // A result callback failed on the frame that owns the decoder. Its throw propagates there.
        Interlocked.CompareExchange(ref GetOrCreateColdState().TerminalException, exception, null);
    }

    internal void OnReleasing(Exception? exception)
    {
        Volatile.Read(ref _state.ColdState)?.Delivery?.TrySetResult();
        DisposeRegistrations();
        _state.Commands.Return();
    }

    internal void OnDiscarded()
    {
        _ops.OnDiscarded();
        _state.Commands.Return();
    }

    internal void OnReset()
    {
        _state.Phase = PhaseInitial;
        _state.CommandIndex = -1;
        _state.Context = default;
        _state.ContextPublished = false;
        _state.Current = null;
        _state.CurrentPublished = false;
        _state.ReadFlowRfq = false;
        _state.ConsumerDetached = false;
        _state.ConsumerObservedCompletion = false;
        _state.ReadySource.Reset();
        _state.PipelineTaskSource.Reset();
        _state.ReadyCompletion = 0;
        _state.DrainStarted = 0;
        _state.FlowToken = default;
        _state.FlowRegistration = default;
        _state.ColdState = null;
        _state.SyncHandoffClaimed = false;
        _state.HandoffEvent?.ResetInteraction();
        _state.WaitForDrainOnDispose = true;
    }

}

public sealed partial class CommandFlow
{
    bool IValueTaskSource<bool>.GetResult(short token) => _state.ReadySource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _state.ReadySource.GetStatus(token);
    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _state.ReadySource.OnCompleted(continuation, state, token, flags);

    void IValueTaskSource.GetResult(short token) => _state.PipelineTaskSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _state.PipelineTaskSource.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _state.PipelineTaskSource.OnCompleted(continuation, state, token, flags);

    public readonly struct Enumerator : IEnumerator<CommandResult>, IAsyncEnumerator<CommandResult>
    {
        readonly CommandFlow? _flow;
        readonly CancellationToken _cancellationToken;

        public Enumerator(CommandFlow flow)
            : this(flow, default)
        { }

        internal Enumerator(CommandFlow flow, CancellationToken cancellationToken)
        {
            _flow = flow;
            _cancellationToken = cancellationToken;
        }

        public Enumerator GetAsyncEnumerator() => this;

        public Enumerator GetEnumerator() => this;

        public bool MoveNext() => _flow?.Core.MoveNext() ?? false;

        public ValueTask<bool> MoveNextAsync() => MoveNextAsync(_cancellationToken);

        public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
            => _flow is null ? new(false) : _flow.Core.MoveNextAsync(cancellationToken);

        public CommandResult Current => _flow?._state.Current ?? default!;

        object? IEnumerator.Current => Current;

        void IEnumerator.Reset() => throw new NotSupportedException();

        public ValueTask DisposeAsync() => _flow is null ? default : _flow.Core.DisposeAsync();

        public void Dispose() => _flow?.Core.Dispose();
    }
}
