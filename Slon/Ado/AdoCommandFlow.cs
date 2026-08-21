using System.Data;
using System.Data.Common;
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

sealed class AdoCommandFlowObserver<TCommand> : CommandFlowObserver
    where TCommand : IAdoCommand
{
    internal static readonly AdoCommandFlowObserver<TCommand> Instance = new();

    internal override void OnStarted(CommandFlow flow, object? state)
    {
        switch (((AdoCommandFlow<TCommand>)flow).LifetimeOwner)
        {
            case SlonCommand command:
                command.OnFlowStarted(flow);
                break;
            case SlonBatch batch:
                batch.OnFlowStarted(flow);
                break;
        }
    }

    internal override void OnCommandResult(CommandFlow flow, CommandResult result, object? state)
        => ((AdoCommandFlow<TCommand>)flow).ObserveResult(result);

    internal override void OnCompleting(PgClientFlow flow, Exception? exception, object? state)
    {
        switch (((AdoCommandFlow<TCommand>)flow).LifetimeOwner)
        {
            case SlonCommand command:
                command.OnFlowCompleting((CommandFlow)flow, exception);
                break;
            case SlonBatch batch:
                batch.OnFlowCompleting((CommandFlow)flow, exception);
                break;
        }
    }

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

sealed class AdoCommandFlow<TCommand> : CommandFlow
    where TCommand : IAdoCommand
{
    readonly FieldRef<AdoBatchCore<TCommand>> _core;
    readonly DbParameterCollection? _parameters;
    readonly CommandBehavior _behavior;
    readonly SlonDataSource.PgDbDependencies _dependencies;
    readonly SlonConnection? _connection;
    readonly bool _preparing;
    readonly int _commandCount;
    object? _lifetimeOwner;
    Action<CommandResult, object?>? _resultObserver;
    object? _resultObserverState;

    internal AdoCommandFlow(
        bool async, FieldRef<AdoBatchCore<TCommand>> core,
        DbParameterCollection? parameters, CommandBehavior behavior,
        SlonDataSource.PgDbDependencies dependencies, SlonConnection? connection,
        TimeSpan? pendingTimeout, bool preparing, int commandCount, object? lifetimeOwner)
        : base(async, pendingTimeout)
    {
        _core = core;
        _parameters = parameters;
        _behavior = behavior;
        _dependencies = dependencies;
        _connection = connection;
        _preparing = preparing;
        _commandCount = commandCount;
        _lifetimeOwner = lifetimeOwner;
        SetObserver(AdoCommandFlowObserver<TCommand>.Instance, null);
        AdoCommandFlowObserver<TCommand>.Instance.OnStarted(this, null);
    }

    internal override int VisibleCommandCount => _commandCount;
    internal object? LifetimeOwner => _lifetimeOwner;

    internal void ObserveResult(CommandResult result)
        => _resultObserver?.Invoke(result, _resultObserverState);

    internal override void Bind(PgClientFlowBindingContext? context)
    {
        var pgConnection = (context as PgConnection.FlowBindingContext)?.Connection
            ?? throw new InvalidOperationException(
                "An ADO command requires a PgConnection flow binding context.");
        ref var core = ref _core.Invoke();
        InitializeAdo(IsAsync, core.CreateAdoCommandFlowOptions(
            [_parameters], _behavior, _dependencies, _connection, pgConnection,
            pendingTimeout: PendingTimeout, preparing: _preparing));
    }

    void InitializeAdo(bool async, in AdoCommandFlowOptions options)
    {
        _resultObserver = options.ResultObserver;
        _resultObserverState = options.ResultObserverState;
        Initialize(async, new CommandFlowOptions
        {
            Commands = options.Commands,
            PendingTimeout = options.PendingTimeout
        });
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
