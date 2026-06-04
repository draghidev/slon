using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Slon.Pg;

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
}

static class AdoCommandExtensions
{
    public static (Command, TrackerResult) CreateCommand<TCommand>(this TCommand command, bool enableErrorBarriers, CommandBehavior behavior, in TrackerContext trackerContext, DbParameterCollection? dbParameters, TimeSpan timeout)
        where TCommand : IAdoCommand
    {
        dbParameters ??= command.Parameters;
        ImmutableArray<Parameter> parameters = [];
        ParameterTypeList parameterTypes = default;
        if (dbParameters?.Count > 0)
        {
            var builder = ImmutableArray.CreateBuilder<Parameter>(dbParameters.Count);
            if (dbParameters is SlonParameters slonParameters)
            {
                foreach (var kv in slonParameters.GetStructEnumerator())
                {
                    if (kv.Key != SlonParameters.PositionalName)
                        throw new NotSupportedException("Named parameters are not yet supported, these require client-side SQL parsing for PostgreSQL.");

                    builder.Add(Parameter.Create(kv.Value));
                }
            }
            else
            {
                foreach (var kv in (ICollection<KeyValuePair<string, object?>>)dbParameters)
                {
                    if (kv.Key != SlonParameters.PositionalName)
                        throw new NotSupportedException("Named parameters are not yet supported, these require client-side SQL parsing for PostgreSQL.");

                    builder.Add(Parameter.Create(kv.Value));
                }
            }

            // Use DrainToImmutable as we don't necessarily trust dbParameters.Count (MoveToImmutable requires exact capacity).
            parameters = builder.DrainToImmutable();
            parameterTypes = ParameterTypeList.Create(parameters);
            Debug.Assert(parameterTypes.Count == parameters.Length);
        }

        var trackerResult = trackerContext.TrackCommand(command.CommandText, parameterTypes);
        return (new Command
        {
            Descriptor = trackerResult.GetDescriptor(command.CommandText, parameterTypes),
            DescribeOnly = behavior.HasFlag(CommandBehavior.SchemaOnly),
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
