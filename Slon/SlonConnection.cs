using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
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
public sealed partial class SlonConnection : IAdoConnection
{
    static StateChangeEventArgs StateChangeOpen { get; } = new(originalState: ConnectionState.Closed, ConnectionState.Open);
    static StateChangeEventArgs StateChangeClosed { get; } = new(originalState: ConnectionState.Open, ConnectionState.Closed);

    readonly SlonDataSource _dataSource;
    ConnectionState _state;
    Exception? _breakException;
    bool _disposed;
    AdoConnectionProxy? _proxy;
    bool _stateChangeEventHandlerAdded;

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
    bool HasProxy => _state is ConnectionState.Open or ConnectionState.Broken;

    internal SlonDataSource DbDataSource => _dataSource;

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    AdoConnectionProxy EnsureConnected()
    {
        if (_disposed || _state is not ConnectionState.Open)
            Throw();

        Debug.Assert(_proxy is not null);
        return _proxy;

        [DoesNotReturn]
        void Throw()
        {
            ThrowIfDisposed();

            if (_state is ConnectionState.Broken)
                throw new InvalidOperationException("Connection is in a broken state.", _breakException);

            throw new InvalidOperationException("Connection is not open or ready.");
        }
    }

    void CloseCore()
    {
        // The only time SyncObj is null, before the first successful open.
        if (!HasProxy)
            return;

        RollbackTransactionOnClose();
        ConnectionState state;
        try
        {
            if (_proxy.InExclusiveScope)
                _proxy.ReleaseExclusiveScope();

            state = _state;
            _state = ConnectionState.Closed;
            _breakException = null;
        }
        catch (Exception ex)
        {
            lock (BreakSyncObj)
            {
                if (_state is not ConnectionState.Broken)
                {
                    _state = ConnectionState.Broken;
                    _breakException = ex;
                }
            }
            throw;
        }

        if (_stateChangeEventHandlerAdded)
            OnStateChange(StateChangeClosed);
    }

    async ValueTask CloseAsyncCore()
    {
        // The only time SyncObj is null, before the first successful open.
        if (!HasProxy)
            return;

        await RollbackTransactionOnCloseAsync().ConfigureAwait(false);
        ConnectionState state;
        try
        {
            if (_proxy.InExclusiveScope)
                await _proxy.ReleaseExclusiveScopeAsync().ConfigureAwait(false);

            state = _state;
            _state = ConnectionState.Closed;
            _breakException = null;
        }
        catch (Exception ex)
        {
            lock (BreakSyncObj)
            {
                if (_state is not ConnectionState.Broken)
                {
                    _state = ConnectionState.Broken;
                    _breakException = ex;
                }
            }
            throw;
        }

        if (_stateChangeEventHandlerAdded)
            OnStateChange(new StateChangeEventArgs(state, ConnectionState.Closed));
    }

    object BreakSyncObj => this;

    internal void PerformUserCancellation(TimeSpan? timeout = null)
    {
        EnsureConnected().PerformUserCancellation(timeout);
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
        transaction?.Detach();
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
            ReleaseOwnedAndLeaked(awaitable: false).GetAwaiter().GetResult();
            CloseCore();
        }
        finally
        {
            _disposed = true;
            _proxy?.Dispose();
            base.Dispose(disposing: true);
        }
    }

    async ValueTask DisposeAsyncCore()
    {
        if (_disposed)
            return;
        try
        {
            await ReleaseOwnedAndLeaked(awaitable: true).ConfigureAwait(false);
            await CloseAsyncCore().ConfigureAwait(false);
        }
        finally
        {
            _disposed = true;
            if (_proxy is not null)
                await _proxy.DisposeAsync().ConfigureAwait(false);
            base.Dispose(disposing: true);
        }
    }

    // Owned statements are ordered through the lease's exclusive pipeline. Only names whose
    // owners were lost go to physical-session maintenance, where no ADO ordering edge remains.
    ValueTask ReleaseOwnedAndLeaked(bool awaitable)
    {
        if (_proxy is null)
            return default;
        var pgConnection = _proxy.PgConnection;
        var owned = UnprepareAllImpl(awaitable);
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
        ThrowIfTransactionActive();
        _pendingTransactionStatement = BeginTransactionSql(isolationLevel, options);
        return CurrentTransaction = new SlonTransaction(this, isolationLevel, options);
    }

    async ValueTask<TTransaction> BeginTransactionAsyncCore<TTransaction>(IsolationLevel isolationLevel,
        SlonTransactionOptions options, CancellationToken cancellationToken)
        where TTransaction: DbTransaction
    {
        Debug.Assert(typeof(TTransaction) == typeof(DbTransaction) || typeof(TTransaction) == typeof(SlonTransaction));
        EnsureConnected();
        ThrowIfTransactionActive();
        cancellationToken.ThrowIfCancellationRequested();
        _pendingTransactionStatement = BeginTransactionSql(isolationLevel, options);
        var transaction = new SlonTransaction(this, isolationLevel, options);
        CurrentTransaction = transaction;
        return (TTransaction)(object)transaction;
    }

    internal string? TakePendingTransactionStatement()
        => Interlocked.Exchange(ref _pendingTransactionStatement, null);

    internal void RestorePendingTransactionStatement(string statement)
    {
        if (Interlocked.CompareExchange(ref _pendingTransactionStatement, statement, null) is not null)
            ThrowHelper.ThrowInvalidOperation("The pending transaction statement was replaced unexpectedly.");
    }

    void ThrowIfTransactionActive()
    {
        if (CurrentTransaction is not null)
            ThrowHelper.ThrowInvalidOperation("A transaction is already in progress; nested transactions are not supported (use a savepoint instead).");
    }

    void ValidateTransaction(SlonTransaction transaction)
    {
        if (!ReferenceEquals(CurrentTransaction, transaction))
            ThrowHelper.ThrowInvalidOperation("This transaction is not the connection's active transaction (it has already completed, or belongs to another connection).");
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

    void ExecuteTransactionStatement(string sql)
    {
        using var cmd = new SlonCommand(this, sql);
        cmd.ExecuteNonQuery();
    }

    async ValueTask ExecuteTransactionStatementAsync(string sql, CancellationToken cancellationToken)
    {
        var cmd = new SlonCommand(this, sql);
        await using (cmd.ConfigureAwait(false))
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    void OpenCore(SlonConnectionOptions options = SlonConnectionOptions.None)
    {
        ValidateOptions(options);
        ThrowIfDisposed();
        if (_state is not (ConnectionState.Closed or ConnectionState.Broken))
            ThrowHelper.ThrowInvalidOperation("Connection is already open or being opened.");

        _state = ConnectionState.Connecting;
        try
        {
            SetProxy(DbDataSource.GetProxy(this, DbDataSource.ConnectionTimeout, options));
        }
        catch
        {
            CloseCore();
            throw;
        }
    }

    async Task OpenAsyncCore(SlonConnectionOptions options = SlonConnectionOptions.None,
        CancellationToken cancellationToken = default)
    {
        ValidateOptions(options);
        ThrowIfDisposed();
        if (_state is not (ConnectionState.Closed or ConnectionState.Broken))
            ThrowHelper.ThrowInvalidOperation("Connection is already open or being opened.");

        _state = ConnectionState.Connecting;
        try
        {

            SetProxy(await DbDataSource.GetProxyAsync(
                this, DbDataSource.ConnectionTimeout, cancellationToken, options).ConfigureAwait(false));
        }
        catch
        {
            await CloseAsyncCore().ConfigureAwait(false);
            throw;
        }
    }

    static void ValidateOptions(SlonConnectionOptions options)
    {
        if ((options & ~SlonConnectionOptions.LongRunning) != 0)
            throw new ArgumentOutOfRangeException(nameof(options), options, "Unsupported connection options.");
    }

    SlonConnection CloneCore() => _dataSource.CreateConnection();

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
    // Dispatch: explicit-prepare (owningInstance non-null) → connection-local OwnedTracker.
    // Auto-prepare → workload tracker via the proxy.
    internal TrackerResult TrackCommand(in CommandDescriptor descriptor, TrackedCommand? tracked = null, object? owningInstance = null)
        => owningInstance is null
            ? AutoTracker.Track(descriptor, tracked)
            : OwnedTracker.Track(descriptor, tracked, owningInstance, nameSource: _proxy?.PgConnection);

    // Auto-prepare goes through the workload tracker via the proxy.
    CommandTracker AutoTracker => _proxy!.Tracker;

    // Explicit-prepare bookkeeping. Lazy because most connections never call Prepare(). The
    // tracker stays unallocated until first use. Survives Close-Open with the SlonConnection
    // (Policy A, object-identity continuity for prepared commands).
    CommandTracker? _ownedTracker;
    CommandTracker OwnedTracker => _ownedTracker ??= new CommandTracker(maxAuto: 0, autoMinimumUses: 0);

    void UnprepareAllCore() => UnprepareAllImpl(awaitable: false).GetAwaiter().GetResult();

    ValueTask UnprepareAllAsyncCore() => UnprepareAllImpl(awaitable: true);

    // Invalidate the TrackedCommands and clear presence locally, then close every name in one
    // exclusive-pipeline flow. Awaiting it confirms the server-side closes landed.
    ValueTask UnprepareAllImpl(bool awaitable)
    {
        if (_ownedTracker is null)
            return default;
        var tracked = _ownedTracker.CollectOwned();
        if (tracked.Length is 0 || _proxy is null)
        {
            _ownedTracker.Dispose();
            _ownedTracker = null;
            return default;
        }

        _ownedTracker.Dispose();
        _ownedTracker = null;
        return CloseOwnedStatements(tracked, awaitable);
    }

    ValueTask UnprepareOwned(object owningInstance, bool awaitable)
    {
        if (_ownedTracker is null)
            return default;
        return CloseOwnedStatements(_ownedTracker.TakeOwned(owningInstance), awaitable);
    }

    ValueTask CloseOwnedStatements(TrackedCommand[] tracked, bool awaitable)
    {
        if (tracked.Length is 0 || _proxy is null)
            return default;

        var pgConnection = _proxy.PgConnection;
        var names = new EncodedString[tracked.Length];
        for (var i = 0; i < tracked.Length; i++)
        {
            var command = tracked[i];
            names[i] = command.StoredCommandName;
            command.Invalidate();
            pgConnection.RemoveTracked(command);
        }

        var flow = new MaintenanceFlow(names, async: awaitable);
        if (!awaitable)
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

    internal void CloseOwned(object owningInstance)
    {
        UnprepareOwned(owningInstance, awaitable: false).GetAwaiter().GetResult();
    }

    internal ValueTask CloseOwnedAsync(object owningInstance)
        => UnprepareOwned(owningInstance, awaitable: true);

    internal void CommitTransaction(SlonTransaction slonTransaction)
    {
        ValidateTransaction(slonTransaction);
        ExecuteTransactionStatement("COMMIT");
        CurrentTransaction = null;
    }

    internal void RollbackTransaction(SlonTransaction slonTransaction)
    {
        ValidateTransaction(slonTransaction);
        ExecuteTransactionStatement("ROLLBACK");
        CurrentTransaction = null;
    }

    internal async ValueTask CommitTransactionAsync(SlonTransaction slonTransaction, CancellationToken cancellationToken)
    {
        ValidateTransaction(slonTransaction);
        await ExecuteTransactionStatementAsync("COMMIT", cancellationToken).ConfigureAwait(false);
        CurrentTransaction = null;
    }

    internal async ValueTask RollbackTransactionAsync(SlonTransaction slonTransaction, CancellationToken cancellationToken)
    {
        ValidateTransaction(slonTransaction);
        await ExecuteTransactionStatementAsync("ROLLBACK", cancellationToken).ConfigureAwait(false);
        CurrentTransaction = null;
    }

    ConnectionState GetState()
    {
        if (_state is not ConnectionState.Open)
            return _state;

        Debug.Assert(_proxy is not null);
        if (_proxy.CurrentReadingFlow is not { } flow)
            return ConnectionState.Open;

        if (flow is CommandFlow commandFlow)
            return commandFlow.IsResultReady ? ConnectionState.Fetching : ConnectionState.Executing;

        return ConnectionState.Executing;
    }

    void IAdoConnection.Break(Exception exception)
    {
        EnsureConnected();
        lock (BreakSyncObj)
        {
            // We'll just keep the first exception.
            if (_state is ConnectionState.Broken)
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
        set => throw new NotSupportedException(
            $"{nameof(SlonConnection)} configuration is owned by its {nameof(SlonDataSource)}. " +
            "Create a data source with the desired options instead.");
    }

    /// <inheritdoc />
    public override string Database => DbDataSource.Database;

    /// <inheritdoc />
    public override string DataSource => DbDataSource.DisplayEndpoint;

    /// <inheritdoc />
    public override int ConnectionTimeout => (int)DbDataSource.ConnectionTimeout.TotalSeconds;

    /// <inheritdoc />
    public override string ServerVersion => DbDataSource.ServerVersion;

    /// <inheritdoc />
    public override ConnectionState State => GetState();

    /// <inheritdoc />
    public override void Open()
    {
        try
        {
            OpenCore();
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    /// <summary>Opens the connection using the requested connection policy.</summary>
    public void Open(SlonConnectionOptions options)
    {
        try
        {
            OpenCore(options);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    /// <inheritdoc />
    public override Task OpenAsync(CancellationToken cancellationToken)
        => OpenAsyncProjected(SlonConnectionOptions.None, cancellationToken);

    /// <summary>Opens the connection asynchronously using the requested connection policy.</summary>
    /// <param name="options">The connection options.</param>
    /// <param name="cancellationToken">A token for cancelling the open operation.</param>
    public Task OpenAsync(SlonConnectionOptions options, CancellationToken cancellationToken = default)
        => OpenAsyncProjected(options, cancellationToken);

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName)
        => throw new NotSupportedException(
            $"Database selection is owned by {nameof(SlonDataSource)}. Create or use a data source for '{databaseName}'.");

    /// <inheritdoc />
    public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException(
            $"Database selection is owned by {nameof(SlonDataSource)}. Create or use a data source for '{databaseName}'."));

    /// <summary>Creates a new object that is a copy of the current instance.</summary>
    /// <returns>A new object that is a copy of this instance.</returns>
    public SlonConnection Clone() => CloneCore();

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

    /// <summary>
    /// Releases all explicit-prepared commands held by this connection. The server-side
    /// prepared statements are deallocated and the per-command bookkeeping is cleared.
    /// Commands that were previously prepared via <see cref="DbCommand.Prepare"/> will be
    /// re-prepared transparently on next use.
    /// </summary>
    public void UnprepareAll()
    {
        try
        {
            UnprepareAllCore();
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    /// <summary>
    /// Asynchronously releases all explicit-prepared commands held by this connection.
    /// </summary>
    public ValueTask UnprepareAllAsync() => UnprepareAllAsyncProjected();

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand() => CreateCommand();

    /// <inheritdoc />
    protected override DbBatch CreateDbBatch() => CreateBatch();

    /// <inheritdoc />
    public override void Close()
    {
        try
        {
            CloseCore();
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    /// <inheritdoc />
    public override Task CloseAsync() => CloseAsyncProjected().AsTask();

    /// <inheritdoc />
    public override ValueTask DisposeAsync() => DisposeAsyncProjected();

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        try
        {
            DisposeCore();
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    async Task OpenAsyncProjected(SlonConnectionOptions options, CancellationToken cancellationToken)
    {
        try
        {
            await OpenAsyncCore(options, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    async ValueTask UnprepareAllAsyncProjected()
    {
        try
        {
            await UnprepareAllAsyncCore().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    async ValueTask CloseAsyncProjected()
    {
        try
        {
            await CloseAsyncCore().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    async ValueTask DisposeAsyncProjected()
    {
        try
        {
            await DisposeAsyncCore().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

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
    protected override DbProviderFactory? DbProviderFactory => null;

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
