using System.Data;
using System.Data.Common;
using Slon.Pg;

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbBatchCommand" />
public sealed class SlonBatchCommand : DbBatchCommand, IAdoCommand
{
    SlonParameters? _parameters;
    bool _isReadOnly;
    long _recordsAffected;

    /// <inheritdoc cref="System.Data.Common.DbBatchCommand.CommandText" />
    public override string CommandText
    {
        get;
        set
        {
            EnsureMutable();
            field = value ?? "";
        }
    } = "";

    /// <inheritdoc cref="System.Data.Common.DbBatchCommand.CommandType" />
    public override CommandType CommandType
    {
        get;
        set
        {
            if (value is not CommandType.Text)
                throw new NotSupportedException();
            EnsureMutable();
            field = value;
        }
    } = CommandType.Text;

    /// Gets or sets whether to append a PostgreSQL error barrier after this command, allowing later
    /// batch commands to continue after it fails.
    public bool AppendErrorBarrier
    {
        get;
        set
        {
            EnsureMutable();
            field = value;
        }
    }

    /// <summary>Gets or sets whether executions of this command are eligible for automatic preparation.</summary>
    /// <remarks>
    /// Explicit preparation of the containing batch creates an owned prepared command regardless of this value.
    /// </remarks>
    public bool AllowAutoPreparation
    {
        get;
        set
        {
            EnsureMutable();
            field = value;
        }
    } = true;

    /// <inheritdoc cref="System.Data.Common.DbBatchCommand.RecordsAffected" />
    /// <remarks>When the value exceeds <see cref="int.MaxValue" />, <see cref="int.MinValue" /> is returned.</remarks>
    public override int RecordsAffected
        => _recordsAffected > int.MaxValue ? int.MinValue : (int)_recordsAffected;

    /// <summary>Gets the number of rows affected without narrowing the PostgreSQL row count.</summary>
    public long LongRecordsAffected => _recordsAffected;

    /// <summary>Gets the collection of <see cref="T:Slon.SlonParameter" /> objects. For more information on parameters, see Configuring Parameters and Parameter Data Types.</summary>
    /// <returns>The parameters of the SQL statement or stored procedure.</returns>
    public new SlonParameters Parameters => _parameters ??= new();
    /// <inheritdoc cref="System.Data.Common.DbBatchCommand.DbParameterCollection" />
    protected override DbParameterCollection DbParameterCollection => Parameters;

    /// <inheritdoc cref="System.Data.Common.DbBatchCommand.CanCreateParameter" />
    public override bool CanCreateParameter => true;
    /// <inheritdoc cref="System.Data.Common.DbBatchCommand.CreateParameter" />
    public override SlonParameter CreateParameter() => new();

    /// <summary>Creates a new mutable instance of a <see cref="T:Slon.SlonBatchCommand" /> object.</summary>
    public SlonBatchCommand Clone()
    {
        var clone = new SlonBatchCommand
        {
            CommandText = CommandText,
            CommandType = CommandType,
            _parameters = _parameters is null ? null : CloneParameters(_parameters),
            AppendErrorBarrier = AppendErrorBarrier,
            AllowAutoPreparation = AllowAutoPreparation
        };
        return clone;

        static SlonParameters CloneParameters(SlonParameters parameters)
        {
            var clone = new SlonParameters(parameters.Count);
            foreach (var (name, value) in (IEnumerable<KeyValuePair<string, object?>>)parameters)
                clone.Add(name, value is SlonParameter parameter ? parameter.Clone() : value);
            return clone;
        }
    }

    void EnsureMutable()
    {
        if (_isReadOnly)
            ThrowHelper.ThrowInvalidOperation("The batch command is read-only.");

        var adoCommand = (IAdoCommand)this;
        if (adoCommand.Tracked is not null)
            adoCommand.Tracked = null;
    }

    internal void ResetRecordsAffected() => _recordsAffected = 0;

    internal void ObserveCompletedResult(CommandResult result)
    {
        _recordsAffected = result.Error is not null
            ? 0
            : result.GetCommandComplete().BatchRecordsAffected;
    }

    void IAdoCommand.MakeReadOnly()
    {
        _recordsAffected = 0;
        _isReadOnly = true;
    }
    TrackedCommand? IAdoCommand.Tracked { get; set; }
    SlonParameters? IAdoCommand.Parameters => _parameters;
}
