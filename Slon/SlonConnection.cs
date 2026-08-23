using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Text;

namespace Slon;

/// <summary>Specifies options for a <see cref="SlonConnection"/>.</summary>
[Flags]
public enum SlonConnectionOptions
{
    /// <summary>Uses the datasource's normal connection options.</summary>
    None = 0,

    /// <summary>Prevents unrelated datasource work from being admitted behind this connection.</summary>
    LongRunning = 1
}

// Implementation
/// <inheritdoc cref="DbConnection" />
public sealed partial class SlonConnection
{
    static StateChangeEventArgs StateChangeOpen { get; } = new(originalState: ConnectionState.Closed, ConnectionState.Open);
    static StateChangeEventArgs StateChangeClosed { get; } = new(originalState: ConnectionState.Open, ConnectionState.Closed);

    readonly SlonDataSource _dataSource;
    ConnectionState _state;
    Exception? _breakException;
    bool _disposed;
    AdoConnectionProxy? _proxy;
    bool _stateChangeEventHandlerAdded;
    CommandTracker? _ownedTracker;

    // Test access to auto-prepare and connection state without a server query.
    internal AdoConnectionProxy? UnderlyingProxy => _proxy;
    internal PgConnection? UnderlyingPgConnection => _proxy?.PgConnection;

    internal SlonConnection(SlonDataSource dataSource)
    {
        GC.SuppressFinalize(this);
        _dataSource = dataSource;
    }

    // Used internally to create pre-opened connections.
    internal void SetProxy(AdoConnectionProxy proxy)
    {
        if (_state is ConnectionState.Open)
            ThrowHelper.ThrowInvalidOperation("A proxy is already set.");

        _proxy = proxy;
        _state = ConnectionState.Open;
        if (_stateChangeEventHandlerAdded)
            OnStateChange(StateChangeOpen);
    }

    [MemberNotNullWhen(true, nameof(_proxy))]
    bool HasLease => _state is ConnectionState.Open or ConnectionState.Broken;

    internal SlonDataSource DbDataSource => _dataSource;

    AdoConnectionProxy EnsureConnected()
    {
        if (_disposed || _state is not ConnectionState.Open)
            Throw();

        Debug.Assert(_proxy is not null);
        return _proxy;

        [DoesNotReturn]
        void Throw()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_state is ConnectionState.Broken)
                throw new InvalidOperationException("Connection is in a broken state.", _breakException);

            throw new InvalidOperationException("Connection is not open or ready.");
        }
    }

    void CloseCore()
    {
        // Only Open and Broken retain an active lease.
        if (!HasLease)
            return;
        var proxy = _proxy;

        Exception? rollbackFailure = null;
        try
        {
            RollbackTransactionOnClose();
        }
        catch (Exception ex)
        {
            rollbackFailure = ex;
        }
        ConnectionState previousState;
        try
        {
            proxy.ReleaseExclusiveScope();
            previousState = TransitionToClosed(proxy);
        }
        catch (Exception ex)
        {
            RecordCloseFailure(proxy, ex);
            AdoException.Throw(ex);
            return;
        }

        FinishClose(previousState, rollbackFailure);
    }

    async ValueTask CloseAsyncCore()
    {
        // Only Open and Broken retain an active lease.
        if (!HasLease)
            return;
        var proxy = _proxy;

        Exception? rollbackFailure = null;
        try
        {
            await RollbackTransactionOnCloseAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            rollbackFailure = ex;
        }
        ConnectionState previousState;
        try
        {
            await proxy.ReleaseExclusiveScopeAsync().ConfigureAwait(false);
            previousState = TransitionToClosed(proxy);
        }
        catch (Exception ex)
        {
            RecordCloseFailure(proxy, ex);
            AdoException.Throw(ex);
            return;
        }

        FinishClose(previousState, rollbackFailure);
    }

    ConnectionState TransitionToClosed(AdoConnectionProxy proxy)
    {
        lock (proxy)
        {
            var previousState = _state;
            _state = ConnectionState.Closed;
            _breakException = null;
            return previousState;
        }
    }

    void RecordCloseFailure(AdoConnectionProxy proxy, Exception exception)
    {
        lock (proxy)
        {
            if (_state is ConnectionState.Broken)
                return;

            _state = ConnectionState.Broken;
            _breakException = exception;
        }
    }

    void FinishClose(ConnectionState previousState, Exception? rollbackFailure)
    {
        if (_stateChangeEventHandlerAdded)
            OnStateChange(previousState is ConnectionState.Open
                ? StateChangeClosed
                : new StateChangeEventArgs(previousState, ConnectionState.Closed));
        if (rollbackFailure is not null)
            AdoException.Throw(rollbackFailure);
    }

    internal TimeSpan DefaultCommandTimeout => DbDataSource.DefaultCommandTimeout;
    internal void ReportTransactionDisposeRollbackFailure(Exception exception)
        => DbDataSource.ReportTransactionDisposeRollbackFailure(exception);
    internal SlonTransaction? CurrentTransaction { get; private set; }
    string? _pendingTransactionStatement;

    void DetachTransaction()
    {
        var transaction = CurrentTransaction;
        CurrentTransaction = null;
        _pendingTransactionStatement = null;
        transaction?.MarkCompleted();
    }

    void RollbackTransactionOnClose()
    {
        var transaction = CurrentTransaction;
        if (transaction is null)
            return;

        if (_state is not ConnectionState.Open || _pendingTransactionStatement is not null)
        {
            DetachTransaction();
            return;
        }

        try
        {
            transaction.Rollback();
        }
        finally
        {
            DetachTransaction();
        }
    }

    async ValueTask RollbackTransactionOnCloseAsync()
    {
        var transaction = CurrentTransaction;
        if (transaction is null)
            return;

        if (_state is not ConnectionState.Open || _pendingTransactionStatement is not null)
        {
            DetachTransaction();
            return;
        }

        try
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            DetachTransaction();
        }
    }

    void DisposeCore()
    {
        if (_disposed)
            return;
        try
        {
            try
            {
                ReleaseOwnedAndLeaked(async: false).GetAwaiter().GetResult();
            }
            finally
            {
                CloseCore();
            }
        }
        finally
        {
            _disposed = true;
            base.Dispose(disposing: true);
        }
    }

    async ValueTask DisposeAsyncCore()
    {
        if (_disposed)
            return;
        try
        {
            try
            {
                await ReleaseOwnedAndLeaked(async: true).ConfigureAwait(false);
            }
            finally
            {
                await CloseAsyncCore().ConfigureAwait(false);
            }
        }
        finally
        {
            _disposed = true;
            base.Dispose(disposing: true);
        }
    }

    // Owned statements are ordered through the lease's exclusive pipeline. Only names whose
    // owners were lost go to physical-session maintenance, where no ADO ordering edge remains.
    ValueTask ReleaseOwnedAndLeaked(bool async)
    {
        if (_proxy is null)
            return default;
        var pgConnection = _proxy.PgConnection;
        var owned = UnprepareAllCore(async);
        if (_proxy.Tracker.DrainLeakedNames() is { Count: > 0 } leakedNames)
        {
            foreach (var name in leakedNames)
                pgConnection.PushMaintenance(new CloseStatement(name));
        }
        return owned;
    }

    // The exclusive scope is held for the lease, so the wire is serial: BEGIN opens a transaction on it
    // that this connection's later commands run inside (PG auto-enrolls them on the wire's open block),
    // until COMMIT/ROLLBACK closes it. Emitted as ordinary commands through the held scope.
    SlonTransaction BeginTransactionCore(IsolationLevel isolationLevel, SlonTransactionOptions options = SlonTransactionOptions.None)
    {
        EnsureConnected();
        if (CurrentTransaction is not null)
            ThrowHelper.ThrowInvalidOperation("A transaction is already in progress; nested transactions are not supported (use a savepoint instead).");
        _pendingTransactionStatement = BeginTransactionSql(isolationLevel, options);
        return CurrentTransaction = new SlonTransaction(this, isolationLevel, options);
    }

    ValueTask<TTransaction> BeginTransactionAsyncCore<TTransaction>(IsolationLevel isolationLevel,
        SlonTransactionOptions options, CancellationToken cancellationToken)
        where TTransaction: DbTransaction
    {
        Debug.Assert(typeof(TTransaction) == typeof(DbTransaction) || typeof(TTransaction) == typeof(SlonTransaction));
        EnsureConnected();
        if (CurrentTransaction is not null)
            ThrowHelper.ThrowInvalidOperation("A transaction is already in progress; nested transactions are not supported (use a savepoint instead).");
        cancellationToken.ThrowIfCancellationRequested();
        _pendingTransactionStatement = BeginTransactionSql(isolationLevel, options);
        var transaction = new SlonTransaction(this, isolationLevel, options);
        CurrentTransaction = transaction;
        return new((TTransaction)(object)transaction);
    }

    internal string? TakePendingTransactionStatement()
        => Interlocked.Exchange(ref _pendingTransactionStatement, null);

    internal void RestorePendingTransactionStatement(string statement)
    {
        if (Interlocked.CompareExchange(ref _pendingTransactionStatement, statement, null) is not null)
            ThrowHelper.ThrowInvalidOperation("The pending transaction statement was replaced unexpectedly.");
    }

    // PG BEGIN with the matching isolation level (default = READ COMMITTED). Snapshot maps to REPEATABLE
    // READ, which IS snapshot isolation in PostgreSQL.
    static string BeginTransactionSql(IsolationLevel isolationLevel, SlonTransactionOptions options)
    {
        if ((options & ~(SlonTransactionOptions.ReadOnly | SlonTransactionOptions.Deferrable)) != 0)
            throw new ArgumentOutOfRangeException(nameof(options), options, "Unsupported PostgreSQL transaction options.");

        var begin = isolationLevel switch
        {
            IsolationLevel.Unspecified or IsolationLevel.ReadCommitted => "BEGIN",
            IsolationLevel.Serializable => "BEGIN ISOLATION LEVEL SERIALIZABLE",
            IsolationLevel.RepeatableRead or IsolationLevel.Snapshot => "BEGIN ISOLATION LEVEL REPEATABLE READ",
            IsolationLevel.ReadUncommitted => "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
            _ => throw new ArgumentOutOfRangeException(nameof(isolationLevel), isolationLevel, "Unsupported isolation level for a PostgreSQL transaction."),
        };
        if (options is SlonTransactionOptions.None)
            return begin;

        return string.Concat(begin,
            options.HasFlag(SlonTransactionOptions.ReadOnly) ? " READ ONLY" : null,
            options.HasFlag(SlonTransactionOptions.Deferrable) ? " DEFERRABLE" : null);
    }

    void OpenCore(SlonConnectionOptions options = SlonConnectionOptions.None)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_state is not (ConnectionState.Closed or ConnectionState.Broken))
            ThrowHelper.ThrowInvalidOperation("Connection is already open or being opened.");
        if (_state is ConnectionState.Broken)
            CloseCore();

        _state = ConnectionState.Connecting;
        AdoConnectionProxy proxy;
        try
        {
            proxy = DbDataSource.GetProxy(DbDataSource.ConnectionTimeout, options);
        }
        catch (Exception ex)
        {
            _state = ConnectionState.Closed;
            _breakException = null;
            AdoException.Throw(ex);
            return;
        }
        SetProxy(proxy);
    }

    async Task OpenAsyncCore(SlonConnectionOptions options = SlonConnectionOptions.None,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_state is not (ConnectionState.Closed or ConnectionState.Broken))
            ThrowHelper.ThrowInvalidOperation("Connection is already open or being opened.");
        if (_state is ConnectionState.Broken)
            await CloseAsyncCore().ConfigureAwait(false);

        _state = ConnectionState.Connecting;
        AdoConnectionProxy proxy;
        try
        {
            proxy = await DbDataSource.GetProxyAsync(
                DbDataSource.ConnectionTimeout, cancellationToken, options).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _state = ConnectionState.Closed;
            _breakException = null;
            AdoException.Throw(ex);
            return;
        }
        SetProxy(proxy);
    }

    internal TFlow Enqueue<TFlow>(TFlow flow)
        where TFlow : PgClientFlow
    {
        EnsureConnected().Enqueue(flow);
        return flow;
    }

    internal ValueTask<TFlow> EnqueueAsync<TFlow>(TFlow flow, CancellationToken cancellationToken)
        where TFlow : PgClientFlow
        => EnsureConnected().EnqueueAsync(flow, cancellationToken);

    // We should only get here when we are enqueuing and have confirmed we are connected.
    // Explicit preparation uses a connection-local tracker that stays unallocated until first use
    // and survives Close-Open. Auto-prepare uses the workload tracker owned by the proxy.
    internal TrackerResult TrackCommand(in CommandDescriptor descriptor, TrackedCommand? tracked = null, object? owningInstance = null)
        => owningInstance is null
            ? _proxy!.Tracker.Track(descriptor, tracked)
            : (_ownedTracker ??= new CommandTracker(maxAuto: 0, autoMinimumUses: 0))
                .Track(descriptor, tracked, owningInstance, nameSource: _proxy?.PgConnection);

    // Invalidate the TrackedCommands and clear presence locally, then close every name in one
    // exclusive-pipeline flow. Awaiting it confirms the server-side closes landed.
    async ValueTask UnprepareAllCore(bool async)
    {
        if (_ownedTracker is null)
            return;
        var tracked = _ownedTracker.CollectOwned();
        if (tracked.Length is 0 || _proxy is null)
        {
            _ownedTracker.Dispose();
            _ownedTracker = null;
            return;
        }

        _ownedTracker.Dispose();
        _ownedTracker = null;
        try
        {
            await CloseOwnedStatements(async, tracked).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    internal ValueTask UnprepareOwned(bool async, object owningInstance)
    {
        if (_ownedTracker is null)
            return default;
        return CloseOwnedStatements(async, _ownedTracker.TakeOwned(owningInstance));
    }

    ValueTask CloseOwnedStatements(bool async, TrackedCommand[] tracked)
    {
        if (tracked.Length is 0 || _proxy is null)
            return default;

        var pgConnection = _proxy.PgConnection;
        var names = new EncodedCString[tracked.Length];
        for (var i = 0; i < tracked.Length; i++)
        {
            var command = tracked[i];
            names[i] = command.StoredCommandName;
            command.Invalidate();
            pgConnection.RemoveTracked(command);
        }

        var flow = new MaintenanceFlow(names, async: async);
        if (!async)
        {
            _proxy.Enqueue(flow);
            flow.WaitForCompleteSynchronously();
            return default;
        }

        var completion = flow.WaitForComplete();
        _proxy.Enqueue(flow);
        return AwaitCompletion(completion);

        static async ValueTask AwaitCompletion(ValueTask<PgClientFlow> completion)
            => _ = await completion.ConfigureAwait(false);
    }

    internal void CommitTransaction(SlonTransaction slonTransaction)
    {
        ExecuteTransactionStatement(slonTransaction, "COMMIT");
        CurrentTransaction = null;
    }

    internal void RollbackTransaction(SlonTransaction slonTransaction)
    {
        ExecuteTransactionStatement(slonTransaction, "ROLLBACK");
        CurrentTransaction = null;
    }

    internal async ValueTask CommitTransactionAsync(SlonTransaction slonTransaction, CancellationToken cancellationToken)
    {
        await ExecuteTransactionStatementAsync(slonTransaction, "COMMIT", cancellationToken).ConfigureAwait(false);
        CurrentTransaction = null;
    }

    internal async ValueTask RollbackTransactionAsync(SlonTransaction slonTransaction, CancellationToken cancellationToken)
    {
        await ExecuteTransactionStatementAsync(slonTransaction, "ROLLBACK", cancellationToken).ConfigureAwait(false);
        CurrentTransaction = null;
    }

    internal void ExecuteTransactionStatement(SlonTransaction transaction, string sql)
    {
        if (!ReferenceEquals(CurrentTransaction, transaction))
            ThrowHelper.ThrowInvalidOperation("This transaction is not the connection's active transaction (it has already completed, or belongs to another connection).");
        using var command = new SlonCommand(this, sql);
        command.ExecuteNonQuery();
    }

    internal async ValueTask ExecuteTransactionStatementAsync(SlonTransaction transaction, string sql,
        CancellationToken cancellationToken)
    {
        if (!ReferenceEquals(CurrentTransaction, transaction))
            ThrowHelper.ThrowInvalidOperation("This transaction is not the connection's active transaction (it has already completed, or belongs to another connection).");
        var command = new SlonCommand(this, sql);
        await using (command.ConfigureAwait(false))
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    ConnectionState GetState()
    {
        if (_state is not ConnectionState.Open)
            return _state;

        Debug.Assert(_proxy is not null);
        return _proxy.State;
    }

    internal void Break(Exception exception)
    {
        var proxy = _proxy;
        if (proxy is null)
            return;

        lock (proxy)
        {
            // A completion callback may arrive after Close has already settled the lease.
            // Closed and disposed connections are terminal, while Broken retains its first cause.
            if (!ReferenceEquals(_proxy, proxy) || _disposed || _state is not ConnectionState.Open)
                return;

            _state = ConnectionState.Broken;
            _breakException = exception;
        }
    }

}

// Public surface & ADO.NET
public sealed partial class SlonConnection : DbConnection
{
    /// <inheritdoc />
    [AllowNull]
    public override string ConnectionString
    {
        get => _dataSource.ConnectionString;
        set
        {
            if (value == _dataSource.ConnectionString)
                return;

            throw new NotSupportedException(
                $"{nameof(SlonConnection)} configuration is owned by its {nameof(SlonDataSource)}. " +
                "Create a data source with the desired options instead.");
        }
    }

    /// <inheritdoc />
    public override string Database => DbDataSource.Database;

    /// <inheritdoc />
    public override string DataSource => DbDataSource.DisplayEndpoint;

    /// <inheritdoc />
    public override int ConnectionTimeout => SlonDataSourceOptions.ToAdoTimeoutSeconds(DbDataSource.ConnectionTimeout);

    /// <inheritdoc />
    public override string ServerVersion => DbDataSource.ServerVersion;

    /// <inheritdoc />
    public override ConnectionState State => GetState();

    /// <inheritdoc />
    public override void Open() => OpenCore();

    /// <summary>Opens the connection using the requested connection policy.</summary>
    public void Open(SlonConnectionOptions options) => OpenCore(options);

    /// <inheritdoc />
    public override Task OpenAsync(CancellationToken cancellationToken)
        => OpenAsync(SlonConnectionOptions.None, cancellationToken);

    /// <summary>Opens the connection asynchronously using the requested connection policy.</summary>
    /// <param name="options">The connection options.</param>
    /// <param name="cancellationToken">A token for cancelling the open operation.</param>
    public Task OpenAsync(SlonConnectionOptions options, CancellationToken cancellationToken = default)
        => OpenAsyncCore(options, cancellationToken);

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName)
        => throw new NotSupportedException(
            $"Database selection is owned by {nameof(SlonDataSource)}. Create or use a data source for '{databaseName}'.");

    /// <inheritdoc />
    public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException(
            $"Database selection is owned by {nameof(SlonDataSource)}. Create or use a data source for '{databaseName}'."));

    /// <inheritdoc />
    public override void EnlistTransaction(System.Transactions.Transaction? transaction)
        => throw new NotSupportedException(
            $"{nameof(SlonConnection)} does not support ambient transactions. " +
            $"Use an explicit {nameof(SlonTransaction)} instead.");

    /// <summary>Creates a new object that is a copy of the current instance.</summary>
    /// <returns>A new object that is a copy of this instance.</returns>
    public SlonConnection Clone() => _dataSource.CreateConnection();

    /// <summary>
    /// Begins a database transaction.
    /// </summary>
    /// <returns>A <see cref="SlonTransaction"/> object representing the new transaction.</returns>
    /// <remarks>
    /// Nested transactions are not supported.
    /// Transactions created by this method will have the <see cref="IsolationLevel.ReadCommitted"/> isolation level.
    /// </remarks>
    public new SlonTransaction BeginTransaction()
        => BeginTransactionCore(IsolationLevel.Unspecified);

    /// <summary>
    /// Begins a database transaction with the specified isolation level.
    /// </summary>
    /// <param name="level">The isolation level under which the transaction should run.</param>
    /// <returns>A <see cref="SlonTransaction"/> object representing the new transaction.</returns>
    /// <remarks>Nested transactions are not supported.</remarks>
    public new SlonTransaction BeginTransaction(IsolationLevel level)
        => BeginTransactionCore(level);

    /// <summary>Begins a database transaction with the specified PostgreSQL transaction options.</summary>
    /// <param name="options">The PostgreSQL transaction options.</param>
    /// <returns>A <see cref="SlonTransaction"/> object representing the new transaction.</returns>
    public SlonTransaction BeginTransaction(SlonTransactionOptions options)
        => BeginTransactionCore(IsolationLevel.Unspecified, options);

    /// <summary>Begins a database transaction with the specified isolation level and PostgreSQL transaction options.</summary>
    /// <param name="level">The isolation level under which the transaction should run.</param>
    /// <param name="options">The PostgreSQL transaction options.</param>
    /// <returns>A <see cref="SlonTransaction"/> object representing the new transaction.</returns>
    public SlonTransaction BeginTransaction(IsolationLevel level, SlonTransactionOptions options)
        => BeginTransactionCore(level, options);

    /// <summary>
    /// Begins a database transaction.
    /// </summary>
    /// <returns>A <see cref="SlonTransaction"/> object representing the new transaction.</returns>
    /// <remarks>
    /// Nested transactions are not supported.
    /// Transactions created by this method will have the <see cref="IsolationLevel.ReadCommitted"/> isolation level.
    /// </remarks>
    public new ValueTask<SlonTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        => BeginTransactionAsyncCore<SlonTransaction>(IsolationLevel.Unspecified, SlonTransactionOptions.None, cancellationToken);

    /// <summary>
    /// Begins a database transaction with the specified isolation level.
    /// </summary>
    /// <param name="level">The isolation level under which the transaction should run.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>A <see cref="SlonTransaction"/> object representing the new transaction.</returns>
    /// <remarks>Nested transactions are not supported.</remarks>
    public new ValueTask<SlonTransaction> BeginTransactionAsync(IsolationLevel level, CancellationToken cancellationToken = default)
        => BeginTransactionAsyncCore<SlonTransaction>(level, SlonTransactionOptions.None, cancellationToken);

    /// <summary>Begins a database transaction with the specified PostgreSQL transaction options.</summary>
    /// <param name="options">The PostgreSQL transaction options.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the new <see cref="SlonTransaction"/>.</returns>
    public ValueTask<SlonTransaction> BeginTransactionAsync(SlonTransactionOptions options,
        CancellationToken cancellationToken = default)
        => BeginTransactionAsyncCore<SlonTransaction>(IsolationLevel.Unspecified, options, cancellationToken);

    /// <summary>Begins a database transaction with the specified isolation level and PostgreSQL transaction options.</summary>
    /// <param name="level">The isolation level under which the transaction should run.</param>
    /// <param name="options">The PostgreSQL transaction options.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the new <see cref="SlonTransaction"/>.</returns>
    public ValueTask<SlonTransaction> BeginTransactionAsync(IsolationLevel level, SlonTransactionOptions options,
        CancellationToken cancellationToken = default)
        => BeginTransactionAsyncCore<SlonTransaction>(level, options, cancellationToken);

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        => BeginTransactionCore(isolationLevel);

    /// <inheritdoc />
    protected override ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        => BeginTransactionAsyncCore<DbTransaction>(isolationLevel, SlonTransactionOptions.None, cancellationToken);

    /// <summary>Creates and returns a <see cref="T:System.Data.Common.DbCommand" /> object associated with the current connection.</summary>
    /// <param name="commandText">The command text to be used.</param>
    /// <returns>A <see cref="Slon.SlonCommand" /> object.</returns>
    public SlonCommand CreateCommand(string commandText) => new(connection: this, commandText);

    /// <summary>Creates and returns a <see cref="T:System.Data.Common.DbCommand" /> object associated with the current connection.</summary>
    /// <returns>A <see cref="Slon.SlonCommand" /> object.</returns>
    public new SlonCommand CreateCommand() => new(this);

    /// <inheritdoc />
    public override bool CanCreateBatch => true;

    /// <summary>Returns a new instance of the provider's class that implements the <see cref="T:System.Data.Common.DbBatch" /> class.</summary>
    /// <returns>A new instance of <see cref="Slon.SlonBatch" />.</returns>
    public new SlonBatch CreateBatch() => new(this);

    /// <summary>Releases all explicitly prepared commands held by this connection.</summary>
    /// <remarks>
    /// The server-side prepared statements are deallocated and the per-command bookkeeping is
    /// cleared. Commands previously prepared through <see cref="DbCommand.Prepare"/> are prepared
    /// again transparently on their next use.
    /// </remarks>
    public void UnprepareAll() => UnprepareAllCore(async: false).GetAwaiter().GetResult();

    /// <summary>Asynchronously releases all explicitly prepared commands held by this connection.</summary>
    /// <remarks>
    /// The server-side prepared statements are deallocated and the per-command bookkeeping is
    /// cleared. Commands previously prepared through <see cref="DbCommand.Prepare"/> are prepared
    /// again transparently on their next use.
    /// </remarks>
    public ValueTask UnprepareAllAsync() => UnprepareAllCore(async: true);

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand() => CreateCommand();

    /// <inheritdoc />
    protected override DbBatch CreateDbBatch() => CreateBatch();

    /// <inheritdoc />
    public override void Close() => CloseCore();

    /// <inheritdoc />
    public override Task CloseAsync() => CloseAsyncCore().AsTask();

    /// <inheritdoc />
    public override ValueTask DisposeAsync() => DisposeAsyncCore();

    /// <inheritdoc />
    protected override void Dispose(bool disposing) => DisposeCore();

    /// <inheritdoc />
    public override DataTable GetSchema() => GetSchema(null);

    /// <inheritdoc />
    public override DataTable GetSchema(string? collectionName) => GetSchema(collectionName, null);

    /// <inheritdoc />
    public override DataTable GetSchema(string? collectionName, string?[]? restrictions)
        => throw new NotImplementedException();

    /// <inheritdoc />
    public override Task<DataTable> GetSchemaAsync(CancellationToken cancellationToken = default)
        => GetSchemaAsync("MetaDataCollections", null, cancellationToken);

    /// <inheritdoc />
    public override Task<DataTable> GetSchemaAsync(string collectionName, CancellationToken cancellationToken = default)
        => GetSchemaAsync(collectionName, null, cancellationToken);

    /// <inheritdoc />
    public override Task<DataTable> GetSchemaAsync(string collectionName, string?[]? restrictions, CancellationToken cancellationToken = default)
        => throw new NotImplementedException();

    /// <inheritdoc />
    protected override DbProviderFactory DbProviderFactory => _dataSource.ProviderFactory;

    /// <inheritdoc />
    public override event StateChangeEventHandler? StateChange
    {
        add
        {
            _stateChangeEventHandlerAdded = true;
            base.StateChange += value;
        }
        remove => base.StateChange -= value;
    }
}
