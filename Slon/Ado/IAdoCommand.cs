using System.Data;
using System.Data.Common;
using System.Diagnostics;
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
        TimeSpan timeout, bool preparing, PgSerializerOptions? serializerOptions = null,
        ParameterWriter? parameterWriter = null)
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

        ParameterSource parameters = default;
        ParameterTypeList parameterTypes = default;
        if (dbParameters?.Count > 0)
        {
            if (serializerOptions is null)
                ThrowHelper.ThrowInvalidOperation("ADO parameter serialization requires serializer options.");
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
                slonParameters.GetOrResolveTypeInfo(
                    currentParameterIndex, serializerOptions, preparedType, allowUnspecified: preparing);
            }

            parameters = new(slonParameters!,
                parameterWriter ?? throw new InvalidOperationException(
                    "ADO parameter serialization requires a parameter writer."));
            parameterTypes = new(parameters);
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
