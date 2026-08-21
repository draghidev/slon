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
            EnsureMutable();
            field = value;
        }
    } = CommandType.Text;

    /// Gets or sets whether this command ends its PostgreSQL error barrier, allowing later batch
    /// commands to continue after it fails.
    public bool AppendErrorBarrier
    {
        get;
        set
        {
            EnsureMutable();
            field = value;
        }
    }

    /// <summary>Whether executions of this command are excluded from automatic preparation.</summary>
    /// <remarks>
    /// Explicit preparation of the containing batch creates an owned prepared command regardless of this value.
    /// Afterward this setting has no effect.
    /// </remarks>
    public bool DisableAutoPreparation
    {
        get;
        set
        {
            EnsureMutable();
            field = value;
        }
    }

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
            _parameters = _parameters is null ? null : new(_parameters),
            AppendErrorBarrier = AppendErrorBarrier,
            DisableAutoPreparation = DisableAutoPreparation
        };
        ((IAdoCommand)clone).Tracked = ((IAdoCommand)this).Tracked;
        return clone;
    }

    void EnsureMutable()
    {
        if (_isReadOnly)
            ThrowHelper.ThrowInvalidOperation("The batch command collection is read-only.");

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
