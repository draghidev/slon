using System.Data;
using System.Data.Common;

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbBatchCommand" />
public sealed class SlonBatchCommand : DbBatchCommand, IAdoCommand
{
    SlonParameters? _parameters;
    bool _isReadOnly;

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

    // TODO decide what to do with this, we have a few options to pass data back.
    /// <inheritdoc cref="System.Data.Common.DbBatchCommand.RecordsAffected" />
    public override int RecordsAffected { get; }

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

    void IAdoCommand.MakeReadOnly() => _isReadOnly = true;
    TrackedCommand? IAdoCommand.Tracked { get; set; }
    SlonParameters? IAdoCommand.Parameters => _parameters;
}
