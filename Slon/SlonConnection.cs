using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Text;
using IsolationLevel = System.Data.IsolationLevel;
#pragma warning disable CS0197 // Using a field of a marshal-by-reference class as a ref or out value or taking its address may cause a runtime exception

namespace Slon;

// Implementation
public sealed partial class SlonConnection : IAdoConnection
{
    static StateChangeEventArgs StateChangeOpen { get; } = new(originalState: ConnectionState.Closed, ConnectionState.Open);
    static StateChangeEventArgs StateChangeClosed { get; } = new(originalState: ConnectionState.Open, ConnectionState.Closed);

    readonly SlonDataSource _dataSource;
    ConnectionState _state;
    Exception? _breakException;
    bool _disposed;
    AdoConnectionProxy? _proxy;
    bool _closingConnection;
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

        ConnectionState state;
        try
        {
            if (_proxy.InExclusiveScope)
                _proxy.EndExclusiveScope();

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

        ConnectionState state;
        try
        {
            if (_proxy.InExclusiveScope)
                await _proxy.EndExclusiveScopeAsync().ConfigureAwait(false);

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

    void DisposeCore()
    {
        if (_disposed)
            return;
        _disposed = true;
        try
        {
            ReleaseOwnedAndLeaked(awaitable: false).GetAwaiter().GetResult();
            CloseCore();
        }
        finally
        {
            _proxy?.Dispose();
            base.Dispose();
        }
    }

    async ValueTask DisposeAsyncCore()
    {
        if (_disposed)
            return;
        _disposed = true;
        try
        {
            await ReleaseOwnedAndLeaked(awaitable: true).ConfigureAwait(false);
            await CloseAsyncCore().ConfigureAwait(false);
        }
        finally
        {
            if (_proxy is not null)
                await _proxy.DisposeAsync().ConfigureAwait(false);
            base.Dispose();
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

    // The data-source open paths (SlonDataSource.OpenConnection*) SetProxy directly rather than going
    // through OpenCore/OpenAsyncCore, so they acquire the lease's exclusive scope through these. A
    // SlonConnection holds an exclusive scope for its whole lease however it was opened.
    internal void AcquireExclusiveScope() => EnsureConnected().AcquireExclusiveScope();
    internal ValueTask AcquireExclusiveScopeAsync(CancellationToken cancellationToken) => EnsureConnected().AcquireExclusiveScopeAsync(cancellationToken);

    // The exclusive scope is held for the lease, so the wire is serial: BEGIN opens a transaction on it
    // that this connection's later commands run inside (PG auto-enrolls them on the wire's open block),
    // until COMMIT/ROLLBACK closes it. Emitted as ordinary commands through the held scope.
    SlonTransaction BeginTransactionCore(IsolationLevel isolationLevel)
    {
        EnsureConnected();
        ThrowIfTransactionActive();
        _pendingTransactionStatement = BeginTransactionSql(isolationLevel);
        return CurrentTransaction = new SlonTransaction(this, isolationLevel);
    }

    async ValueTask<TTransaction> BeginTransactionAsyncCore<TTransaction>(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        where TTransaction: DbTransaction
    {
        Debug.Assert(typeof(TTransaction) == typeof(DbTransaction) || typeof(TTransaction) == typeof(SlonTransaction));
        EnsureConnected();
        ThrowIfTransactionActive();
        cancellationToken.ThrowIfCancellationRequested();
        _pendingTransactionStatement = BeginTransactionSql(isolationLevel);
        var transaction = new SlonTransaction(this, isolationLevel);
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
    static string BeginTransactionSql(IsolationLevel isolationLevel) => isolationLevel switch
    {
        IsolationLevel.Unspecified or IsolationLevel.ReadCommitted => "BEGIN",
        IsolationLevel.Serializable => "BEGIN ISOLATION LEVEL SERIALIZABLE",
        IsolationLevel.RepeatableRead or IsolationLevel.Snapshot => "BEGIN ISOLATION LEVEL REPEATABLE READ",
        IsolationLevel.ReadUncommitted => "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
        _ => throw new ArgumentOutOfRangeException(nameof(isolationLevel), isolationLevel, "Unsupported isolation level for a PostgreSQL transaction."),
    };

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

    void OpenCore()
    {
        ThrowIfDisposed();
        if (_state is not (ConnectionState.Closed or ConnectionState.Broken))
            ThrowHelper.ThrowInvalidOperation("Connection is already open or being opened.");

        _state = ConnectionState.Connecting;
        try
        {
            SetProxy(DbDataSource.GetProxy(this, DbDataSource.ConnectionTimeout));
            // SlonConnection holds an exclusive scope for its whole lease: its commands run serially on one
            // wire (safe default - Slon can't parse SQL to spot session state). The data-source command path
            // never Opens, so it stays multiplexed.
            EnsureConnected().AcquireExclusiveScope();
        }
        catch
        {
            CloseCore();
            throw;
        }
    }

    async Task OpenAsyncCore(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (_state is not (ConnectionState.Closed or ConnectionState.Broken))
            ThrowHelper.ThrowInvalidOperation("Connection is already open or being opened.");

        _state = ConnectionState.Connecting;
        try
        {

            SetProxy(await DbDataSource.GetProxyAsync(this, DbDataSource.ConnectionTimeout, cancellationToken).ConfigureAwait(false));
            await EnsureConnected().AcquireExclusiveScopeAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await CloseAsyncCore().ConfigureAwait(false);
            throw;
        }
    }

    SlonConnection CloneCore() => _dataSource.CreateConnection();

    internal CommandFlow EnqueueCommands(in CommandFlowOptions options, bool closeConnection)
    {
        var proxy = EnsureConnected();
        if (closeConnection && Interlocked.CompareExchange(ref _closingConnection, true, false))
            ThrowHelper.ThrowInvalidOperation($"A command has already been committed with the {nameof(CommandBehavior.CloseConnection)} behavior.");

        var commandFlow = proxy.RentCommandFlow(async: false, options);
        proxy.Enqueue(commandFlow);
        return commandFlow;
    }

    // Sync delegate-based enqueue, mirror of the async variant.
    internal TFlow Enqueue<TArg, TFlow>(
        Func<PgConnection, TArg, TFlow> flowFactory,
        TArg arg,
        bool closeConnection)
        where TFlow : PgClientFlow
        where TArg : allows ref struct
    {
        var proxy = EnsureConnected();
        if (closeConnection && Interlocked.CompareExchange(ref _closingConnection, true, false))
            ThrowHelper.ThrowInvalidOperation($"A command has already been committed with the {nameof(CommandBehavior.CloseConnection)} behavior.");
        return proxy.Enqueue(flowFactory, arg);
    }

    internal ValueTask<CommandFlow> EnqueueCommandsAsync(in CommandFlowOptions options, bool closeConnection, CancellationToken cancellationToken)
    {
        var proxy = EnsureConnected();
        if (closeConnection && Interlocked.CompareExchange(ref _closingConnection, true, false))
            ThrowHelper.ThrowInvalidOperation($"A command has already been committed with the {nameof(CommandBehavior.CloseConnection)} behavior.");
        var commandFlow = proxy.RentCommandFlow(async: true, options);
        return proxy.EnqueueAsync(commandFlow, cancellationToken);
    }

    // Delegate-based enqueue: the flowFactory callback runs inside the proxy's atomic PgConnection scope,
    // so flow construction (presence consultation, descriptor baking) sees the connection the flow
    // will run on. State flows through `arg` to avoid closure allocation.
    internal ValueTask<TFlow> EnqueueAsync<TArg, TFlow>(
        Func<PgConnection, TArg, TFlow> flowFactory,
        TArg arg,
        bool closeConnection,
        CancellationToken cancellationToken)
        where TFlow : PgClientFlow
        where TArg : allows ref struct
    {
        var proxy = EnsureConnected();
        if (closeConnection && Interlocked.CompareExchange(ref _closingConnection, true, false))
            ThrowHelper.ThrowInvalidOperation($"A command has already been committed with the {nameof(CommandBehavior.CloseConnection)} behavior.");
        return proxy.EnqueueAsync(flowFactory, arg, cancellationToken);
    }

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

        var flow = new OwnedStatementCloseFlow(names, async: awaitable);
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
    public override void Open() => OpenCore();

    /// <inheritdoc />
    public override Task OpenAsync(CancellationToken cancellationToken) => OpenAsyncCore(cancellationToken);

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

    /// <summary>
    /// Begins a database transaction.
    /// </summary>
    /// <returns>A <see cref="SlonTransaction"/> object representing the new transaction.</returns>
    /// <remarks>
    /// Nested transactions are not supported.
    /// Transactions created by this method will have the <see cref="IsolationLevel.ReadCommitted"/> isolation level.
    /// </remarks>
    public new ValueTask<SlonTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        => BeginTransactionAsyncCore<SlonTransaction>(IsolationLevel.Unspecified, cancellationToken);

    /// <summary>
    /// Begins a database transaction with the specified isolation level.
    /// </summary>
    /// <param name="level">The isolation level under which the transaction should run.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>A <see cref="SlonTransaction"/> object representing the new transaction.</returns>
    /// <remarks>Nested transactions are not supported.</remarks>
    public new ValueTask<SlonTransaction> BeginTransactionAsync(IsolationLevel level, CancellationToken cancellationToken = default)
        => BeginTransactionAsyncCore<SlonTransaction>(level, cancellationToken);

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        => BeginTransactionCore(isolationLevel);

    /// <inheritdoc />
    protected override ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        => BeginTransactionAsyncCore<DbTransaction>(isolationLevel, cancellationToken);

    /// <summary>Creates and returns a <see cref="T:System.Data.Common.DbCommand" /> object associated with the current connection.</summary>
    /// <param name="commandText">The command text to be used.</param>
    /// <returns>A <see cref="Slon.SlonCommand" /> object.</returns>
    public SlonCommand CreateCommand(string commandText) => new(connection: this, commandText);

    /// <summary>Creates and returns a <see cref="T:System.Data.Common.DbCommand" /> object associated with the current connection.</summary>
    /// <returns>A <see cref="Slon.SlonCommand" /> object.</returns>
    public new SlonCommand CreateCommand() => new(this);

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
    public void UnprepareAll() => UnprepareAllCore();

    /// <summary>
    /// Asynchronously releases all explicit-prepared commands held by this connection.
    /// </summary>
    public ValueTask UnprepareAllAsync() => UnprepareAllAsyncCore();

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
