using System.Buffers;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;

namespace Slon;

// Owns the command span and keeps all reference-bearing build state in one stack-bound operation.
// This avoids an allocated builder and ref/out boundaries that can introduce checked write barriers.
readonly ref struct AdoCommandFlowFactory<TCommand>(
    object owner,
    Span<TCommand> commands,
    SlonDataSource.PgDbDependencies dependencies)
    where TCommand : IAdoCommand
{
    readonly Span<TCommand> _commands = commands;

    public AdoCommandFlowOptions Create(
        ReadOnlySpan<DbParameterCollection?> parametersSpan, CommandBehavior behavior,
        bool explicitlyPrepared, bool allowAutoPreparation, bool enableErrorBarriers,
        TimeSpan timeout,
        SlonConnection? connection = null, PgConnection? pgConnection = null,
        TimeSpan? pendingTimeout = null, bool preparing = false)
    {
        var commands = _commands;
        if (commands.Length is 0)
            ThrowHelper.ThrowInvalidOperation("No commands were added to the batch.");

        // We also allow the 1 case for passing all parameters in one collection, mostly for SlonCommand implicit batching.
        var indexParameters = parametersSpan.Length == commands.Length;
        if (!indexParameters && parametersSpan.Length is not (0 or 1))
            ThrowHelper.ThrowArgumentException(nameof(parametersSpan), "The number of parameter collections must match the number of commands.");

        var pendingPrefix = connection?.TakePendingTransactionStatement();
        if (commands.Length is 1 && explicitlyPrepared && !preparing && pendingPrefix is null)
            return CreatePreparedSingle(
                parametersSpan.IsEmpty ? null : parametersSpan[0],
                behavior, enableErrorBarriers,
                timeout, pgConnection, pendingTimeout);

        return CreateGeneral(
            parametersSpan, behavior, explicitlyPrepared, allowAutoPreparation, enableErrorBarriers,
            timeout, connection, pgConnection, pendingTimeout, preparing,
            pendingPrefix, indexParameters);
    }

    AdoCommandFlowOptions CreateGeneral(
        ReadOnlySpan<DbParameterCollection?> parametersSpan, CommandBehavior behavior,
        bool explicitlyPrepared, bool allowAutoPreparation, bool enableErrorBarriers, TimeSpan timeout,
        SlonConnection? connection, PgConnection? pgConnection,
        TimeSpan? pendingTimeout, bool preparing, string? pendingPrefix,
        bool indexParameters)
    {
        var commands = _commands;
        var commandOffset = pendingPrefix is null ? 0 : 1;
        var commandCount = commands.Length + commandOffset;
        var commandArray = commandCount > 1 ? ArrayPool<Command>.Shared.Rent(commandCount) : null;
        if (pendingPrefix is not null)
            commandArray![0] = Command.Create(pendingPrefix) with { SuppressEnumeration = true };
        (Command Command, TrackerResult TrackerResult) result = default;
        Action<CommandResult, object?>? onResultAction = null;
        object? onResultActionState = null;
        (Action<CommandResult, object?>, object?)?[]? resultActions = null;
        // Tracks TrackedCommands that this batch's earlier commands have already issued Parse for
        // as the Winner. Subsequent same-TC commands in the same batch get the Present shape
        // (rely on the earlier in-batch Parse in the same Sync window).
        PreparationClaims? preparationClaims = null;
        try
        {
            for (var i = 0; i < commands.Length; i++)
            {
                ref var adoCommand = ref commands[i];
                var batchCommand = !preparing && !explicitlyPrepared
                    && adoCommand is SlonBatchCommand concreteBatchCommand
                    ? concreteBatchCommand
                    : null;
                batchCommand?.ResetRecordsAffected();
                TrackerContext trackerContext = default;
                if (connection is not null)
                {
                    trackerContext = preparing
                        ? TrackerContext.Create(connection, owner)
                        : TrackerContext.Create(connection, adoCommand.Tracked);
                }
                else
                {
                    trackerContext = preparing
                        ? TrackerContext.Create(dependencies.CommandsTracker, owner)
                        : TrackerContext.Create(dependencies.CommandsTracker, adoCommand.Tracked);
                }

                var parameters = indexParameters ? parametersSpan[i] : !parametersSpan.IsEmpty ? parametersSpan[0] : null;
                result = AdoCommandFactory.CreateCommand(adoCommand, allowAutoPreparation,
                    enableErrorBarriers, behavior, trackerContext, parameters, timeout, preparing,
                    dependencies.SerializerOptions, dependencies.ParameterWriter);
                // Refresh the cache when the tracker resolved to a different TC than the one we
                // passed in (catches workload-tracker recreation via DbDepsRevision++ and similar).
                if (result.TrackerResult.Tracked is not null && !ReferenceEquals(adoCommand.Tracked, result.TrackerResult.Tracked))
                    adoCommand.Tracked = result.TrackerResult.Tracked;

                // Per-command result observation (presence-aware on the connection-bound path with a protocol
                // in scope, falls through to the legacy TrackerResult-driven completion otherwise).
                Action<CommandResult, object?>? thisResultAction;
                object? thisResultActionState;

                if (pgConnection is not null && result.TrackerResult.Tracked is { } tracked)
                {
                    var preparation = ResolvePreparation(
                        adoCommand, result.Command, tracked, batchCommand, pgConnection,
                        ref preparationClaims);
                    result.Command = preparation.Command;
                    thisResultAction = preparation.ResultAction;
                    thisResultActionState = preparation.ResultActionState;
                }
                else
                {
                    thisResultAction = null;
                    thisResultActionState = null;
                }

                if (batchCommand is not null && thisResultAction is null)
                {
                    thisResultAction = AdoCommandResultObserver.ObserveBatch;
                    thisResultActionState = batchCommand;
                }

                if (commandArray is null)
                {
                    if (thisResultAction is not null)
                    {
                        onResultAction = thisResultAction;
                        onResultActionState = thisResultActionState;
                    }
                }
                else
                {
                    if (thisResultAction is not null)
                    {
                        if (resultActions is null)
                        {
                            resultActions = new (Action<CommandResult, object?>, object?)?[commandCount];
                            onResultAction = AdoCommandResultObserver.DispatchIndexed;
                            onResultActionState = resultActions;
                        }
                        resultActions[i + commandOffset] = (thisResultAction, thisResultActionState);
                    }

                    commandArray[i + commandOffset] = result.Command;
                }
            }

            return new()
            {
                ResultObserver = onResultAction,
                ResultObserverState = onResultActionState,
                Commands = commandArray is null ? new(result.Command) : new(commandArray, commandCount, isPooled: true),
                PendingTimeout = pendingTimeout
            };
        }
        catch
        {
            preparationClaims?.Rollback();
            if (commandArray is not null)
                ArrayPool<Command>.Shared.Return(commandArray, clearArray: true);
            if (pendingPrefix is not null)
                connection!.RestorePendingTransactionStatement(pendingPrefix);
            throw;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    AdoCommandFlowOptions CreatePreparedSingle(
        DbParameterCollection? parameters, CommandBehavior behavior,
        bool enableErrorBarriers, TimeSpan timeout,
        PgConnection? pgConnection, TimeSpan? pendingTimeout)
    {
        ref var adoCommand = ref _commands[0];
        var tracked = adoCommand.Tracked
            ?? throw new InvalidOperationException(
                "The explicitly prepared command has no tracked command.");
        if (!tracked.TryGetPreparedDescriptor(out var descriptor))
            throw new InvalidOperationException(
                "The explicitly prepared command has no prepared descriptor.");
        var commandParameters = adoCommand.Parameters;
        var command = parameters is null && commandParameters is null
            && descriptor.ParameterTypes.Count is 0
            ? new Command
            {
                Descriptor = descriptor,
                DescribeOnly = behavior.HasFlag(CommandBehavior.SchemaOnly),
                WithSync = enableErrorBarriers || adoCommand.AppendErrorBarrier,
                Parameters = default,
                Timeout = timeout
            }
            : AdoCommandFactory.CreatePreparedCommandWithParameters(
                adoCommand, tracked, descriptor, commandParameters, enableErrorBarriers,
                behavior, parameters, timeout,
                dependencies.SerializerOptions, dependencies.ParameterWriter);

        Action<CommandResult, object?>? resultAction = null;
        object? resultActionState = null;
        if (pgConnection is not null)
        {
            var status = pgConnection.GetTrackedStatus(tracked);
            if (status is TrackedStatus.Tracked)
            {
                resultAction = AdoCommandResultObserver.AttachPrepared;
                resultActionState = pgConnection;
            }
            else
            {
                var preparation = ResolvePreparedSingleSlow(
                    adoCommand, command, tracked, pgConnection, status);
                command = preparation.Command;
                resultAction = preparation.ResultAction;
                resultActionState = preparation.ResultActionState;
            }
        }

        return new()
        {
            ResultObserver = resultAction,
            ResultObserverState = resultActionState,
            Commands = new(command),
            PendingTimeout = pendingTimeout
        };
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    static PreparationResolution ResolvePreparedSingleSlow(
        TCommand adoCommand, Command command, TrackedCommand tracked,
        PgConnection connection, TrackedStatus status)
    {
        var parameterTypes = command.Descriptor.ParameterTypes;
        if (status is TrackedStatus.Preparing || !connection.TryBeginPreparing(tracked))
        {
            return new(command with
            {
                Descriptor = CommandDescriptor.Create(
                    adoCommand.CommandText, parameterTypes, default)
            }, null, null);
        }

        try
        {
            command = command with
            {
                Descriptor = CommandDescriptor.Create(
                    adoCommand.CommandText, parameterTypes, tracked.CommandName)
            };
            return new(command, AdoCommandResultObserver.ObservePreparing,
                (connection, tracked, (SlonBatchCommand?)null));
        }
        catch
        {
            connection.RemoveTracked(tracked);
            throw;
        }
    }

    static PreparationResolution ResolvePreparation(
        TCommand adoCommand, Command command, TrackedCommand tracked,
        SlonBatchCommand? batchCommand, PgConnection connection,
        ref PreparationClaims? preparationClaims)
    {
        // A later same-SQL command in this batch can ride the winner's earlier Parse.
        var isIntraBatchWinner = preparationClaims?.Contains(tracked) ?? false;
        var status = isIntraBatchWinner
            ? TrackedStatus.Tracked
            : connection.GetTrackedStatus(tracked);

        if (status is TrackedStatus.Tracked)
        {
            if (isIntraBatchWinner && !command.Descriptor.IsPrepared)
            {
                var descriptor = command.Descriptor;
                command = command with
                {
                    Descriptor = CommandDescriptor.CreatePrepared(
                        tracked.CommandName, descriptor.ParameterTypes, rowDescription: null)
                };
            }

            return batchCommand is null
                ? new(command, AdoCommandResultObserver.AttachPrepared, connection)
                : new(command, AdoCommandResultObserver.AttachPreparedBatch,
                    (connection, batchCommand));
        }

        var parameterTypes = command.Descriptor.ParameterTypes;
        if (status is TrackedStatus.Preparing || !connection.TryBeginPreparing(tracked))
        {
            // Never depend on a concurrent preparation attempt: issue an anonymous Parse.
            command = command with
            {
                Descriptor = CommandDescriptor.Create(
                    adoCommand.CommandText, parameterTypes, default)
            };
            return new(command, null, null);
        }

        // The winner emits a named Parse and settles the connection's preparation state.
        command = command with
        {
            Descriptor = CommandDescriptor.Create(
                adoCommand.CommandText, parameterTypes, tracked.CommandName)
        };
        (preparationClaims ??= new(connection)).Add(tracked);
        return new(command, AdoCommandResultObserver.ObservePreparing,
            (connection, tracked, batchCommand));
    }

    readonly record struct PreparationResolution(
        Command Command,
        Action<CommandResult, object?>? ResultAction,
        object? ResultActionState);
}
