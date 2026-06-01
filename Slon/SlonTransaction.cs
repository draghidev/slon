using System.Data;
using System.Data.Common;

namespace Slon;

/// <inheritdoc/>
public sealed class SlonTransaction : DbTransaction
{
    readonly SlonConnection _connection;
    readonly IsolationLevel _isolationLevel;

    internal SlonTransaction(SlonConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        _isolationLevel = isolationLevel;
    }

    /// <inheritdoc/>
    public override void Commit() => _connection.CommitTransaction(this);

    /// <inheritdoc/>
    public override void Rollback() => _connection.RollbackTransaction(this);

    /// <summary>Specifies the <see cref="Slon.SlonConnection" /> object associated with the transaction.</summary>
    /// <returns>The <see cref="Slon.SlonConnection" /> object associated with the transaction.</returns>
    public new SlonConnection Connection => _connection;

    /// <inheritdoc/>
    protected override DbConnection? DbConnection => Connection;

    /// <inheritdoc/>
    public override IsolationLevel IsolationLevel => _isolationLevel;

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
    }
}
