using System.Data;
using System.Data.Common;

namespace Slon;

/// Specifies PostgreSQL-specific transaction options.
[Flags]
public enum SlonTransactionOptions
{
    /// Uses PostgreSQL's default transaction options.
    None = 0,
    /// Starts a read-only transaction.
    ReadOnly = 1,
    /// Defers a serializable, read-only transaction until PostgreSQL can acquire a safe snapshot.
    Deferrable = 2
}

/// <inheritdoc/>
public sealed class SlonTransaction : DbTransaction
{
    readonly SlonConnection _connection;
    readonly IsolationLevel _isolationLevel;
    readonly SlonTransactionOptions _options;
    // Set once the transaction is committed, rolled back, or disposed-without-completing. Guards against a
    // second completion and makes Dispose's safety-net rollback a no-op after an explicit Commit/Rollback.
    bool _completed;
    bool _disposed;

    internal SlonTransaction(SlonConnection connection, IsolationLevel isolationLevel, SlonTransactionOptions options)
    {
        _connection = connection;
        _isolationLevel = isolationLevel;
        _options = options;
    }

    /// <inheritdoc/>
    public override void Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _connection.CommitTransaction(this);
        _completed = true;
    }

    /// <inheritdoc/>
    public override void Rollback()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _connection.RollbackTransaction(this);
        _completed = true;
    }

    /// <inheritdoc/>
    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _connection.CommitTransactionAsync(this, cancellationToken).ConfigureAwait(false);
        _completed = true;
    }

    /// <inheritdoc/>
    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _connection.RollbackTransactionAsync(this, cancellationToken).ConfigureAwait(false);
        _completed = true;
    }

    /// <summary>Gets the connection while this transaction remains active.</summary>
    /// <returns>The associated connection, or <see langword="null" /> after completion or disposal.</returns>
    public new SlonConnection? Connection => _completed ? null : _connection;

    /// <inheritdoc/>
    protected override DbConnection? DbConnection => Connection;

    /// <inheritdoc/>
    public override IsolationLevel IsolationLevel => _isolationLevel;

    /// Whether the transaction is read-only.
    public bool IsReadOnly => _options.HasFlag(SlonTransactionOptions.ReadOnly);

    /// Whether the transaction is deferrable.
    public bool IsDeferrable => _options.HasFlag(SlonTransactionOptions.Deferrable);

    internal void Detach() => _completed = true;

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        // ADO contract: disposing an uncommitted transaction rolls it back. Best-effort - a broken/closed
        // connection can't roll back, and dispose must not throw.
        if (disposing && !_completed)
        {
            _completed = true;
            try
            {
                _connection.RollbackTransaction(this);
            }
            catch (Exception ex)
            {
                _connection.ReportTransactionDisposeRollbackFailure(ex);
            }
        }
        if (disposing)
            _disposed = true;
        base.Dispose(disposing);
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            _completed = true;
            try
            {
                await _connection.RollbackTransactionAsync(this, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _connection.ReportTransactionDisposeRollbackFailure(ex);
            }
        }
        _disposed = true;
    }
}
