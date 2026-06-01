using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;
using IsolationLevel = System.Data.IsolationLevel;
#pragma warning disable CS0197 // Using a field of a marshal-by-reference class as a ref or out value or taking its address may cause a runtime exception

namespace Slon;

// Implementation
public sealed partial class SlonConnection : IAdoConnection
{
    static StateChangeEventArgs StateChangeOpen { get; } = new(originalState: ConnectionState.Closed, ConnectionState.Open);
    static StateChangeEventArgs StateChangeClosed { get; } = new(originalState: ConnectionState.Open, ConnectionState.Closed);

    SlonDataSource? _dataSource;
    ConnectionState _state;
    Exception? _breakException;
    bool _disposed;
    string? _connectionString;
    AdoConnectionProxy? _proxy;
    bool _closingConnection;
    bool _stateChangeEventHandlerAdded;

    SlonConnection(string? connectionString, SlonDataSource? dataSource)
    {
        GC.SuppressFinalize(this);
        _connectionString = connectionString;
        _dataSource = dataSource;
    }
    internal SlonConnection(SlonDataSource dataSource) : this(dataSource.ConnectionString, dataSource) { }

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

    string GetConnectionString() => _proxy?.ConnectionString ?? _connectionString ?? string.Empty;

    internal SlonDataSource DbDataSource
    {
        get
        {
            return _dataSource ?? Core();

            [MethodImpl(MethodImplOptions.NoInlining)]
            SlonDataSource Core()
            {
                if (_dataSource is null && _connectionString is "")
                    ThrowHelper.ThrowInvalidOperation($"{nameof(DbDataSource)} cannot be resolved, {nameof(ConnectionString)} is not set.");

                return _dataSource ??= ChangeDataSource(_connectionString);
            }
        }
    }

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
    internal SlonTransaction? CurrentTransaction { get; }

    void DisposeCore()
    {
        if (_disposed)
            return;
        _disposed = true;
        try
        {
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
            await CloseAsyncCore().ConfigureAwait(false);
        }
        finally
        {
            if (_proxy is not null)
                await _proxy.DisposeAsync().ConfigureAwait(false);
            base.Dispose();
        }
    }

    SlonTransaction BeginTransactionCore(IsolationLevel isolationLevel)
    {
        var proxy = EnsureConnected();
        proxy.BeginExclusiveScope();
        // TODO either push in a BEGIN TX inside the next flow, or set up a transaction flow to enqueue.
        return new SlonTransaction(this, isolationLevel);
    }

    async ValueTask<TTransaction> BeginTransactionAsyncCore<TTransaction>(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        where TTransaction: DbTransaction
    {
        Debug.Assert(typeof(TTransaction) == typeof(DbTransaction) || typeof(TTransaction) == typeof(SlonTransaction));
        await EnsureConnected().BeginExclusiveScopeAsync(cancellationToken).ConfigureAwait(false);
        // TODO either push in a BEGIN TX inside the next flow, or set up a transaction flow to enqueue.
        return (TTransaction)(object)new SlonTransaction(this, isolationLevel);
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
        }
        catch
        {
            await CloseAsyncCore().ConfigureAwait(false);
            throw;
        }
    }

    SlonDataSource ChangeDataSource(string? connectionString)
    {
        ThrowIfDisposed();
        if (_state is not (ConnectionState.Closed or ConnectionState.Broken))
            throw new InvalidOperationException("Cannot change connection string while the connection is open.");

        _proxy = null;
        _dataSource = null;
        // TODO change the datasource etc.
        _connectionString = _dataSource!.ConnectionString;

        throw new NotImplementedException();
    }

    SlonConnection CloneCore()
    {
        if (_dataSource is not null)
            return _dataSource.CreateConnection();

        return new SlonConnection(_connectionString, null);
    }

    internal CommandFlow EnqueueCommands(in CommandFlowOptions options, bool closeConnection)
    {
        var proxy = EnsureConnected();
        if (closeConnection && Interlocked.CompareExchange(ref _closingConnection, true, false))
            ThrowHelper.ThrowInvalidOperation($"A command has already been committed with the {nameof(CommandBehavior.CloseConnection)} behavior.");

        var commandFlow = proxy.RentCommandFlow(async: false, options);
        proxy.Enqueue(commandFlow);
        return commandFlow;
    }

    internal ValueTask<CommandFlow> EnqueueCommandsAsync(in CommandFlowOptions options, bool closeConnection, CancellationToken cancellationToken)
    {
        var proxy = EnsureConnected();
        if (closeConnection && Interlocked.CompareExchange(ref _closingConnection, true, false))
            ThrowHelper.ThrowInvalidOperation($"A command has already been committed with the {nameof(CommandBehavior.CloseConnection)} behavior.");
        var commandFlow = proxy.RentCommandFlow(async: true, options);
        return proxy.EnqueueAsync(commandFlow, cancellationToken);
    }

    // We should only get here when we are enqueuing and have confirmed we are connected.
    internal TrackerResult TrackCommand(in CommandDescriptor descriptor, TrackedCommand? tracked = null, object? owningInstance = null)
        => _proxy!.TrackCommand(descriptor, tracked, owningInstance);

    void UnprepareOwned(object owningInstance)
    {
        // TODO unprepare all tracked commands for the given instance.
        throw new NotImplementedException();
    }

    internal void CloseOwned(object owningInstance)
    {
        UnprepareOwned(owningInstance);
    }

    internal ValueTask CloseOwnedAsync(object owningInstance)
    {
        UnprepareOwned(owningInstance);
        return new();
    }

    public void CommitTransaction(SlonTransaction slonTransaction)
    {
        throw new NotImplementedException();
    }

    public void RollbackTransaction(SlonTransaction slonTransaction)
    {
        throw new NotImplementedException();
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
    /// <summary>
    /// Initializes a new instance of <see cref="SlonConnection"/> with the given connection string.
    /// </summary>
    /// <param name="connectionString">The connection used to open the PostgreSQL database.</param>
    public SlonConnection(string connectionString) : this(connectionString, null) { }

    /// <inheritdoc />
    [AllowNull]
    public override string ConnectionString
    {
        get => GetConnectionString();
        set => ChangeDataSource(value);
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
    {
        // TODO actually update the databasename.
        throw new NotImplementedException();
        // var updatedConnectionString = DbDataSource.ConnectionString;
        // Close();
        // ChangeDataSource(updatedConnectionString);
        // Open();
    }

    /// <inheritdoc />
    public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
    {
        // TODO actually update the databasename.
        throw new NotImplementedException();
        // var updatedConnectionString = DbDataSource.ConnectionString;
        // await CloseAsyncCore();
        // ChangeDataSource(updatedConnectionString);
        // await OpenAsyncCore(cancellationToken);
    }

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
        => BeginTransactionAsyncCore<SlonTransaction>(IsolationLevel.Unspecified, cancellationToken);

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        => BeginTransactionCore(isolationLevel);

    /// <inheritdoc />
    protected override ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        => BeginTransactionAsyncCore<DbTransaction>(IsolationLevel.Unspecified, cancellationToken);

    /// <summary>Creates and returns a <see cref="T:System.Data.Common.DbCommand" /> object associated with the current connection.</summary>
    /// <param name="commandText">The command text to be used.</param>
    /// <returns>A <see cref="Slon.SlonCommand" /> object.</returns>
    public SlonCommand CreateCommand(string commandText) => new(connection: this, commandText);

    /// <summary>Creates and returns a <see cref="T:System.Data.Common.DbCommand" /> object associated with the current connection.</summary>
    /// <returns>A <see cref="Slon.SlonCommand" /> object.</returns>
    public new SlonCommand CreateCommand() => new();

    public override bool CanCreateBatch => true;

    /// <summary>Returns a new instance of the provider's class that implements the <see cref="T:System.Data.Common.DbBatch" /> class.</summary>
    /// <returns>A new instance of <see cref="Slon.SlonBatch" />.</returns>
    public new SlonBatch CreateBatch() => new(this);

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
