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

    /// <inheritdoc/>
    public override bool SupportsSavepoints => true;

    /// <inheritdoc/>
    public override void Save(string savepointName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _connection.ExecuteTransactionStatement(this, SavepointStatement("SAVEPOINT", savepointName));
    }

    /// <inheritdoc/>
    public override async Task SaveAsync(string savepointName, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _connection.ExecuteTransactionStatementAsync(
            this, SavepointStatement("SAVEPOINT", savepointName), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override void Rollback(string savepointName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _connection.ExecuteTransactionStatement(this, SavepointStatement("ROLLBACK TO SAVEPOINT", savepointName));
    }

    /// <inheritdoc/>
    public override async Task RollbackAsync(string savepointName, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _connection.ExecuteTransactionStatementAsync(
            this, SavepointStatement("ROLLBACK TO SAVEPOINT", savepointName), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override void Release(string savepointName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _connection.ExecuteTransactionStatement(this, SavepointStatement("RELEASE SAVEPOINT", savepointName));
    }

    /// <inheritdoc/>
    public override async Task ReleaseAsync(string savepointName, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _connection.ExecuteTransactionStatementAsync(
            this, SavepointStatement("RELEASE SAVEPOINT", savepointName), cancellationToken).ConfigureAwait(false);
    }

    static string SavepointStatement(string operation, string savepointName)
    {
        ArgumentException.ThrowIfNullOrEmpty(savepointName);
        if (savepointName.Contains('\0'))
            throw new ArgumentException("Savepoint names cannot contain a null character.", nameof(savepointName));
        return string.Concat(operation, " \"", savepointName.Replace("\"", "\"\"", StringComparison.Ordinal), "\"");
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

    // Closing the connection makes this transaction terminal even when no COMMIT or ROLLBACK can be sent.
    internal void MarkCompleted() => _completed = true;

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
