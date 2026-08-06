using System.Data;
using System.Data.Common;

namespace Slon;

/// <inheritdoc/>
public sealed class SlonTransaction : DbTransaction
{
    readonly SlonConnection _connection;
    readonly IsolationLevel _isolationLevel;
    // Set once the transaction is committed, rolled back, or disposed-without-completing. Guards against a
    // second completion and makes Dispose's safety-net rollback a no-op after an explicit Commit/Rollback.
    bool _completed;

    internal SlonTransaction(SlonConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        _isolationLevel = isolationLevel;
    }

    /// <inheritdoc/>
    public override void Commit()
    {
        _connection.CommitTransaction(this);
        _completed = true;
    }

    /// <inheritdoc/>
    public override void Rollback()
    {
        _connection.RollbackTransaction(this);
        _completed = true;
    }

    /// <inheritdoc/>
    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        await _connection.CommitTransactionAsync(this, cancellationToken).ConfigureAwait(false);
        _completed = true;
    }

    /// <inheritdoc/>
    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        await _connection.RollbackTransactionAsync(this, cancellationToken).ConfigureAwait(false);
        _completed = true;
    }

    /// <summary>Specifies the <see cref="Slon.SlonConnection" /> object associated with the transaction.</summary>
    /// <returns>The <see cref="Slon.SlonConnection" /> object associated with the transaction.</returns>
    public new SlonConnection Connection => _connection;

    /// <inheritdoc/>
    protected override DbConnection? DbConnection => Connection;

    /// <inheritdoc/>
    public override IsolationLevel IsolationLevel => _isolationLevel;

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        // ADO contract: disposing an uncommitted transaction rolls it back. Best-effort - a broken/closed
        // connection can't roll back, and dispose must not throw.
        if (disposing && !_completed)
        {
            _completed = true;
            try { _connection.RollbackTransaction(this); }
            catch (Exception ex) { _connection.ReportTransactionDisposeRollbackFailure(ex); }
        }
        base.Dispose(disposing);
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            _completed = true;
            try { await _connection.RollbackTransactionAsync(this, CancellationToken.None).ConfigureAwait(false); }
            catch (Exception ex) { _connection.ReportTransactionDisposeRollbackFailure(ex); }
        }
        GC.SuppressFinalize(this);
    }
}
