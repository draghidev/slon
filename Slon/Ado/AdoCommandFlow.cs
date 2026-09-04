using System.Data;
using System.Data.Common;
using System.Threading.Tasks.Sources;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Runtime.CompilerServices;

namespace Slon;

readonly struct AdoCommandFlowOptions
{
    internal CommandList Commands { get; init; }
    internal TimeSpan? PendingTimeout { get; init; }
    internal Action<CommandResult, object?>? ResultObserver { get; init; }
    internal object? ResultObserverState { get; init; }
}

interface IAdoCommandExecutionOwner
{
    AdoCommandFlowOptions CreateExecutionOptions(
        DbParameterCollection? parameters, CommandBehavior behavior,
        SlonDataSource.PgDbDependencies dependencies, SlonConnection? connection,
        PgConnection pgConnection, TimeSpan? pendingTimeout, bool preparing);
    void OnFlowStarted(AdoCommandExecutionFlow flow);
    void OnFlowCompleting(AdoCommandExecutionFlow flow, Exception? exception);
}

sealed class AdoCommandExecutionObserver : PgClientFlowObserver
{
    internal static readonly AdoCommandExecutionObserver Instance = new();

    protected internal override void OnCompleting(
        PgClientFlow flow, Exception? exception, object? state)
        => ((AdoCommandExecutionFlow)flow).CompleteLifetime(exception);
}

static class AdoCommandResultObserver
{
    internal static void AttachTerminal(CommandResult result, PgConnection connection)
        => result.OnCompleted(static (completed, state) => ObserveTerminal(completed, (PgConnection)state!), connection);

    internal static void ObserveTerminal(CommandResult completed, PgConnection connection)
    {
        try
        {
            if (completed.Error is not { } error)
                return;

            var metadata = completed.GetMetadata();
            if (metadata.IsPrepared)
                connection.ReconcilePreparedError(metadata.ToPreparedDescriptor(), error.SqlState);
        }
        catch (Exception ex)
        {
            connection.ReportUnobservedCallback(ex, "a command-result observer");
        }
    }

    internal static void AttachPrepared(CommandResult result, object? state)
        => AttachTerminal(result, (PgConnection)state!);

    internal static void AttachPreparedBatch(CommandResult result, object? state)
        => result.OnCompleted(static (completed, completionState) =>
        {
            var (connection, command) = ((PgConnection, SlonBatchCommand))completionState!;
            ObserveTerminal(completed, connection);
            command.ObserveCompletedResult(completed);
        }, state);

    internal static void ObservePreparing(CommandResult result, object? state)
    {
        var (connection, tracked, batchCommand) =
            ((PgConnection, TrackedCommand, SlonBatchCommand?))state!;
        var metadata = result.GetMetadata();
        if (metadata.IsPrepared)
            connection.CompletePreparing(tracked, metadata.ToPreparedDescriptor());
        else
            connection.RemoveTracked(tracked);

        if (metadata.IsPrepared)
        {
            if (batchCommand is null)
                AttachTerminal(result, connection);
            else
                result.OnCompleted(static (completed, completionState) =>
                {
                    var (preparedConnection, _, command) =
                        ((PgConnection, TrackedCommand, SlonBatchCommand))completionState!;
                    ObserveTerminal(completed, preparedConnection);
                    command.ObserveCompletedResult(completed);
                }, state);
        }
        else if (batchCommand is not null)
            ObserveBatch(result, batchCommand);
    }

    internal static void ObserveBatch(CommandResult result, object? state)
        => result.OnCompleted(static (completed, completionState) =>
            ((SlonBatchCommand)completionState!).ObserveCompletedResult(completed), state);

    internal static void DispatchIndexed(CommandResult result, object? state)
    {
        var actions = ((Action<CommandResult, object?> Action, object? State)?[])state!;
        if (actions[result.GetMetadata().CommandIndex] is { } action)
            action.Action(result, action.State);
    }
}

sealed class AdoCommandExecutionFlow : PgClientFlow, IValueTaskSource<bool>, IValueTaskSource
{
    readonly IAdoCommandExecutionOwner _bindingOwner;
    readonly DbParameterCollection? _parameters;
    readonly CommandBehavior _behavior;
    readonly SlonDataSource.PgDbDependencies _dependencies;
    readonly SlonConnection? _connection;
    readonly bool _preparing;
    readonly int _commandCount;
    int _lifetimePending;
    Action<CommandResult, object?>? _resultObserver;
    object? _resultObserverState;
    CommandExecutionState _state;

    internal AdoCommandExecutionFlow(
        bool async, IAdoCommandExecutionOwner bindingOwner,
        DbParameterCollection? parameters, CommandBehavior behavior,
        SlonDataSource.PgDbDependencies dependencies, SlonConnection? connection,
        TimeSpan? pendingTimeout, bool preparing, int commandCount,
        bool ownsLifetime)
        : base(supportsDeferredFlush: true)
    {
        _bindingOwner = bindingOwner;
        _parameters = parameters;
        _behavior = behavior;
        _dependencies = dependencies;
        _connection = connection;
        _preparing = preparing;
        _commandCount = commandCount;
        _lifetimePending = ownsLifetime ? 1 : 0;
        _state.CommandIndex = -1;
        _state.EnableActivationTimeout = true;
        _state.WaitForDrainOnDispose = true;
        _state.PendingTimeout = pendingTimeout;
        IsAsync = async;
        if (!async)
            _state.HandoffEvent = new(false);
        SetObserver(AdoCommandExecutionObserver.Instance, null);
        if (ownsLifetime)
            bindingOwner.OnFlowStarted(this);
    }

    internal override bool DefersSyncHandoff => true;
    private protected override FlowHandoffEvent? HandoffEvent => _state.HandoffEvent;
    protected override bool EnableActivationTimeout => true;
    protected override TimeSpan? PendingTimeout => _state.PendingTimeout;
    internal override TimeSpan? BackendCancellationGracePeriod
        => Volatile.Read(ref _state.ConsumerDetached) ? TimeSpan.FromSeconds(1) : null;

    internal override void BindCallerToken(CancellationToken cancellationToken)
        => _state.FlowToken = cancellationToken;
    internal override CancellationToken MigrationCancellationToken => _state.FlowToken;

    internal int VisibleCommandCount => _commandCount;
    internal CommandResult? CurrentResult => _state.Current;
    internal bool IsResultReady => Core.IsResultReady;
    internal bool HasCancellationState => _state.ColdState is not null;

    public Enumerator GetEnumerator() => new(this, default);

    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.CanBeCanceled)
            _state.FlowToken = cancellationToken;
        return new(this, cancellationToken);
    }

    CommandExecutionCore<Ops> Core => new(new(this));

    internal ValueTask<long> ConsumeNonQueryAsync(CancellationToken cancellationToken = default)
        => Core.ConsumeNonQueryAsync(cancellationToken);

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context) => Core.ExecuteAuto(context);
    internal Task CancelAsync() => Core.CancelAsync();
    internal override bool ResetsSharedReadStateBeforeRelease => true;
    protected override void OnStopping(Exception exception) => Core.OnStopping(exception);
    protected override void OnAbort(Exception exception) => Core.OnAbort(exception);
    internal override void Fail(Exception exception) => Core.Fail(exception);
    protected override void OnReleasing(Exception? exception) => Core.OnReleasing(exception);
    protected override void OnDiscarded() => Core.OnDiscarded();
    protected override void OnReset() => Core.OnReset();

    void ObserveResult(CommandResult result)
        => _resultObserver?.Invoke(result, _resultObserverState);

    internal void CompleteLifetime(Exception? exception)
    {
        if (Interlocked.Exchange(ref _lifetimePending, 0) is not 0)
            _bindingOwner.OnFlowCompleting(this, exception);
    }

    internal override void Bind(PgClientFlowBindingContext? context)
    {
        var pgConnection = (context as PgConnection.FlowBindingContext)?.Connection
            ?? throw new InvalidOperationException(
                "An ADO command requires a PgConnection flow binding context.");
        var options = _bindingOwner.CreateExecutionOptions(
            _parameters, _behavior, _dependencies, _connection, pgConnection,
            PendingTimeout, _preparing);
        _resultObserver = options.ResultObserver;
        _resultObserverState = options.ResultObserverState;
        _state.Commands = options.Commands;
        _state.PendingTimeout = options.PendingTimeout;
    }

    readonly struct Ops(AdoCommandExecutionFlow owner) : ICommandExecutionFlowOps<Ops>
    {
        readonly AdoCommandExecutionFlow _owner = owner;

        public static Ops Create(PgClientFlow flow) => new((AdoCommandExecutionFlow)flow);
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
        public void OnCommandResult(CommandResult result) => _owner.ObserveResult(result);
        public void OnDrainStarted() { }
        public void OnDiscarded() => _owner.CompleteLifetime(null);
    }

    bool IValueTaskSource<bool>.GetResult(short token) => _state.ReadySource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token)
        => _state.ReadySource.GetStatus(token);
    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _state.ReadySource.OnCompleted(continuation, state, token, flags);

    void IValueTaskSource.GetResult(short token) => _state.PipelineTaskSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token)
        => _state.PipelineTaskSource.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _state.PipelineTaskSource.OnCompleted(continuation, state, token, flags);

    public readonly struct Enumerator : IAsyncEnumerator<CommandResult>, IDisposable
    {
        readonly AdoCommandExecutionFlow? _flow;
        readonly CancellationToken _cancellationToken;

        internal Enumerator(AdoCommandExecutionFlow flow, CancellationToken cancellationToken)
            => (_flow, _cancellationToken) = (flow, cancellationToken);

        internal bool IsDefault => _flow is null;

        public bool MoveNext() => _flow?.Core.MoveNext() ?? false;
        public ValueTask<bool> MoveNextAsync() => MoveNextAsync(_cancellationToken);
        public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
            => _flow is null ? new(false) : _flow.Core.MoveNextAsync(cancellationToken);
        public CommandResult Current => _flow?._state.Current ?? default!;
        public ValueTask DisposeAsync() => _flow is null ? default : _flow.Core.DisposeAsync();
        public void Dispose() => _flow?.Core.Dispose();
    }
}

sealed class PreparationClaims(PgConnection connection) : HashSet<TrackedCommand>
{
    public void Rollback()
    {
        foreach (var tracked in this)
            connection.RemoveTracked(tracked);
    }
}
