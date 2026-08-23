using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Slon.Pg;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon;

// Shared between DbBatchCommand and DbCommand.
interface IAdoCommand
{
    public void MakeReadOnly();
    public TrackedCommand? Tracked { get; set; }

    public string CommandText { get; }
    public CommandType CommandType { get; }
    public SlonParameters? Parameters { get; }
    public bool AppendErrorBarrier { get; }
    public bool AllowAutoPreparation { get; }
}

static class AdoCommandFactory
{
    public static (Command, TrackerResult) CreateCommand<TCommand>(in TCommand command,
        bool allowAutoPreparation, bool enableErrorBarriers, CommandBehavior behavior,
        in TrackerContext trackerContext, DbParameterCollection? dbParameters, TimeSpan timeout,
        bool preparing,
        PgSerializerOptions? serializerOptions = null, ParameterWriter? parameterWriter = null)
        where TCommand : IAdoCommand
    {
        var commandParameters = command.Parameters;
        var tracked = command.Tracked;
        CommandDescriptor preparedDescriptor = default;
        var hasPreparedDescriptor = !preparing
            && tracked?.TryGetPreparedDescriptor(out preparedDescriptor) == true;
        var isPreparedTemplate = hasPreparedDescriptor
            && tracked!.Kind is TrackedCommandKind.Command;
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
        var preparedParameterTypes = hasPreparedDescriptor
            ? preparedDescriptor.ParameterTypes
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

        TrackerResult trackerResult;
        CommandDescriptor descriptor;
        if (hasPreparedDescriptor)
        {
            trackerResult = new(tracked);
            descriptor = preparedDescriptor;
        }
        else
        {
            trackerResult = preparing || allowAutoPreparation && command.AllowAutoPreparation
                ? trackerContext.TrackCommand(command.CommandText, parameterTypes)
                : default;
            descriptor = trackerResult.GetDescriptor(command.CommandText, parameterTypes);
        }
        return (new Command
        {
            Descriptor = descriptor,
            DescribeOnly = preparing || behavior.HasFlag(CommandBehavior.SchemaOnly),
            DescribeForPreparation = preparing,
            WithSync = enableErrorBarriers || command.AppendErrorBarrier,
            Parameters = parameters,
            Timeout = timeout
        }, trackerResult);
    }
}
