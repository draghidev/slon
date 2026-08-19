using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Slon.Pg;
using Slon.Text;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon;

// Shared between DbBatchCommand and DbCommand
interface IAdoCommand
{
    public void MakeReadOnly();
    public TrackedCommand? Tracked { get; set; }

    public string CommandText { get; }
    public CommandType CommandType { get; }
    public SlonParameters? Parameters { get; }
    public bool AppendErrorBarrier { get; }
    public bool DisableAutoPreparation { get; }
}

static class AdoCommandExtensions
{
    public static (Command, TrackerResult) CreateCommand<TCommand>(this TCommand command, bool enableErrorBarriers,
        CommandBehavior behavior, in TrackerContext trackerContext, DbParameterCollection? dbParameters,
        TimeSpan timeout, bool preparing, PgSerializerOptions? serializerOptions = null)
        where TCommand : IAdoCommand
    {
        var commandParameters = command.Parameters;
        var tracked = command.Tracked;
        var isPreparedTemplate = tracked is { Kind: TrackedCommandKind.Command, IsCompleted: true };
        if (!preparing && !isPreparedTemplate
            && dbParameters is not null && commandParameters is { Count: > 0 })
        {
            throw new InvalidOperationException(
                "Execution parameters cannot be combined with parameters stored on the command.");
        }

        dbParameters ??= commandParameters;
        if (dbParameters is not null and not SlonParameters)
        {
            throw new ArgumentException(
                $"Execution parameters must be a {nameof(SlonParameters)} instance.", nameof(dbParameters));
        }
        var slonParameters = (SlonParameters?)dbParameters;
        var hasPreparedDescriptor = !preparing && tracked?.IsCompleted == true;
        var preparedParameterTypes = hasPreparedDescriptor
            ? tracked!.ParameterTypes
            : default;
        if (hasPreparedDescriptor && (dbParameters?.Count ?? 0) != preparedParameterTypes.Count)
        {
            throw new InvalidOperationException(
                $"Prepared command expects {preparedParameterTypes.Count} parameters, " +
                $"received {dbParameters?.Count ?? 0}.");
        }

        ImmutableArray<Parameter> parameters = [];
        ParameterTypeList parameterTypes = default;
        if (dbParameters?.Count > 0)
        {
            var parameterArray = preparing ? null : new Parameter[dbParameters.Count];
            var typeArray = preparing ? new PgTypeId[dbParameters.Count] : null;
            using var preparedTypes = preparedParameterTypes.GetEnumerator();
            var parameterIndex = 0;
            foreach (var kv in slonParameters!.GetStructEnumerator())
            {
                if (kv.Key != SlonParameters.PositionalName)
                {
                    throw new NotSupportedException(
                        "Named parameters are not yet supported; they require client-side SQL parsing.");
                }

                var currentParameterIndex = parameterIndex++;
                var preparedType = preparedTypes.MoveNext() ? preparedTypes.Current : (PgTypeId?)null;
                if (serializerOptions is null)
                {
                    var parameter = preparedType is { } type
                        ? Parameter.Create(kv.Value, type)
                        : Parameter.Create(kv.Value);
                    if (preparing)
                        typeArray![currentParameterIndex] = parameter.PgTypeId;
                    else
                        parameterArray![currentParameterIndex] = parameter;
                    continue;
                }

                var resolution = slonParameters.GetOrResolveTypeInfo(
                    currentParameterIndex, serializerOptions, preparedType, allowUnspecified: preparing);
                if (preparing)
                    typeArray![currentParameterIndex] = resolution.PgTypeId;
                else
                    parameterArray![currentParameterIndex] = resolution.CreateParameter(
                        kv.Value, currentParameterIndex);
            }

            if (preparing)
                parameterTypes = new(ImmutableCollectionsMarshal.AsImmutableArray(typeArray));
            else
            {
                parameters = ImmutableCollectionsMarshal.AsImmutableArray(parameterArray);
                parameterTypes = ParameterTypeList.Create(parameters);
            }
            Debug.Assert(parameterTypes.Count == dbParameters.Count);
        }

        var trackerResult = command.DisableAutoPreparation && !preparing
            ? default
            : trackerContext.TrackCommand(command.CommandText, parameterTypes);
        return (new Command
        {
            Descriptor = trackerResult.GetDescriptor(command.CommandText, parameterTypes),
            DescribeOnly = preparing || behavior.HasFlag(CommandBehavior.SchemaOnly),
            DescribeForPreparation = preparing,
            WithSync = enableErrorBarriers || command.AppendErrorBarrier,
            Parameters = parameters,
            Timeout = timeout
        }, trackerResult);
    }
}

readonly ref struct TrackerContext
{
    readonly object _trackerOrConnection = null!;
    readonly object? _owningInstance;
    readonly TrackedCommand? _tracked;

    TrackerContext(TrackedCommand? tracked) => _tracked = tracked;

    TrackerContext(CommandTracker tracker, TrackedCommand? tracked)
        : this(tracked) => _trackerOrConnection = tracker;

    TrackerContext(CommandTracker tracker, object owningInstance)
    {
        _trackerOrConnection = tracker;
        _owningInstance = owningInstance;
    }

    TrackerContext(SlonConnection connection, TrackedCommand? tracked)
        : this(tracked)
    {
        _trackerOrConnection = connection;
    }

    TrackerContext(SlonConnection connection, object owningInstance)
    {
        _trackerOrConnection = connection;
        _owningInstance = owningInstance;
    }

    public EncodedString CommandName => _tracked?.CommandName ?? default;

    public static TrackerContext Create(CommandTracker tracker, TrackedCommand? tracked)
        => new(tracker, tracked);
    public static TrackerContext Create(CommandTracker tracker, object owningInstance)
        => new(tracker, owningInstance);
    public static TrackerContext Create(SlonConnection connection, TrackedCommand? tracked)
        => new(connection, tracked);
    public static TrackerContext Create(SlonConnection connection, object owningInstance) => new(connection, owningInstance);

    public TrackerResult TrackCommand(string commandText, ParameterTypeList parameterTypes)
    {
        switch (_trackerOrConnection)
        {
            case SlonConnection connection:
                return connection.TrackCommand(
                    descriptor: CommandDescriptor.Create(commandText, parameterTypes, CommandName),
                    tracked: _tracked,
                    owningInstance: _owningInstance
                );
            case CommandTracker tracker:
                return tracker.Track(
                    descriptor: CommandDescriptor.Create(commandText, parameterTypes, CommandName),
                    tracked: _tracked,
                    owningInstance: _owningInstance
                );
        }
        return default;
    }
}
