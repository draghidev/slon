using System.Data.Common;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Types;
using Slon.Pg.Serialization;
using Slon.Pooling;
using Slon.Transport;

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbDataSource" />
public sealed class SlonDataSource : DbDataSource
{
    readonly SlonDataSourceOptions _options;
    readonly PostgreSqlBackendProvider _backendProvider;
    readonly PgTypeCatalogPlugin[] _userTypeCatalogPlugins;
    readonly SemaphoreSlim _lifecycleLock;
    readonly CancellationTokenSource _shutdown = new();
    readonly Lock _reloadLock = new();
    readonly ILogger _adoLogger;
    readonly ILoggerFactory _loggerFactory;
    readonly string _connectionString;

    // Initialized on the first real use.
    ConnectionPool<PgConnection> _connectionPool = null!;
    IPoolConnectionFactory<PgConnection> _clientFactory = null!;
    PgDbDependencies _dbDependencies = null!;
    PgTypeCatalogFactory _typeCatalogFactory = null!;
    PgTypeCatalogPlugin[] _typeCatalogPlugins = null!;
    PgConnectionFactory _typeReloadConnectionFactory = null!;
    Task? _typeReload;
    volatile bool _isInitialized;
    int _disposed;

    /// <summary>Initializes a datasource from a snapshot of the specified options.</summary>
    /// <param name="options">The datasource configuration.</param>
    public SlonDataSource(SlonDataSourceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options.Snapshot();
        _loggerFactory = new SlonLoggerFactory(_options.LoggerFactory);
        _adoLogger = _loggerFactory.CreateLogger("Slon");
        DisplayEndpoint = _options.EndPoint.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6 ? $"tcp://{_options.EndPoint}" : _options.EndPoint.ToString()!;
        Name = _options.Name ?? $"{DisplayEndpoint}/{Database}";
        _connectionString = $"Endpoint={DisplayEndpoint};Username={_options.Username};Database={Database}";
        _backendProvider = _options.BackendProvider ?? PgBackendProviders.Create(_options.CompatibilityProfile);
        _userTypeCatalogPlugins = [.. (_options.TypeCatalogPlugins
            ?? throw new ArgumentNullException(nameof(_options.TypeCatalogPlugins)))];
        _lifecycleLock = new(1);
        ProviderFactory = new ProviderFactoryImpl(this);
    }

    ConnectionPool<PgConnection> ConnectionPool => _connectionPool ?? throw NotInitializedException();

    // Store the result if multiple dependencies are required. The instance may be switched out during reloading.
    // To prevent any inconsistencies without having to obtain a lock on the data we instead use an immutable instance.
    // All relevant depedencies are bundled to provide a consistent view, it's either all new or all old data.
    internal PgDbDependencies GetDbDependencies(bool initializedOnly = false)
    {
        if (Volatile.Read(ref _dbDependencies) is { } dependencies)
            return dependencies;
        if (initializedOnly)
            throw NotInitializedException();
        EnsureInitialized(ConnectionTimeout);
        return Volatile.Read(ref _dbDependencies) ?? throw NotInitializedException();
    }

    internal TimeSpan ConnectionTimeout => _options.ConnectionTimeout;
    internal TimeSpan DefaultCommandTimeout => _options.CommandTimeout;
    internal void ReportTransactionDisposeRollbackFailure(Exception exception)
        => SlonLogMessages.TransactionDisposeRollbackFailed(_adoLogger, exception);
    internal string Database => _options.Database ?? _options.Username;
    internal EndPoint EndPoint => _options.EndPoint;
    /// Gets the name used to identify this datasource in diagnostics and metrics.
    public string Name { get; }
    /// <summary>Gets the ADO provider factory bound to this datasource.</summary>
    public DbProviderFactory ProviderFactory { get; }
    internal string DisplayEndpoint { get; }

    internal string ServerVersion => GetDbDependencies().BackendInfo.ServerVersionString;
    int DbDepsRevision { get; set; }

    AdoConnectionProxy CreateProxy(PgConnection pgConnection, IAdoConnection connection)
    {
        return new(this, pgConnection, connection);
    }

    ValueTask Initialize(bool async, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (_isInitialized)
            return new ValueTask();

        return Core();

        async ValueTask Core()
        {
            if (async)
                await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            else
                _lifecycleLock.Wait(cancellationToken);
            try
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) is not 0, this);
                if (_isInitialized)
                    return;

                // We don't flow cancellationToken past this point: one contender must finish the shared
                // initialization. Wire-free dependencies are created before the bootstrap connection;
                // the resulting backend/catalog snapshot is published only after that wire is settled.
                var connectionInit = _options.ConnectionInitializer;
                var asyncConnectionInit = _options.AsyncConnectionInitializer;
                var oauthTokens = _options.OAuth is { } oauth
                    ? new OAuthTokenCache(oauth,
                        new(_options.EndPoint, _options.Username, _options.Database),
                        _loggerFactory.CreateLogger("Slon.Authentication"))
                    : null;
                var clientOptions = _options.ToPgClientOptions(oauthTokens, _loggerFactory);
                var transportFactory = SocketStreamConnection.CreateFactory(clientOptions.EndPoint);
                var commandTracker = new CommandTracker(
                    _options.MaxActiveAutoPreparations, _options.AutoPreparationMinimumUses);
                var factory = new InitializingConnectionFactory<PgConnection>(
                    factory: new PgConnectionFactory(clientOptions, transportFactory,
                        tracker: commandTracker,
                        configureOptions: options =>
                        {
                            options.BackendProvider = _backendProvider;
                            options.ExpectedBackendInfo = Volatile.Read(ref _dbDependencies)?.BackendInfo;
                        }),
                    initializer: connectionInit is not null ? (pgConnection, timeout) =>
                        {
                            if (Volatile.Read(ref _dbDependencies) is null)
                                return;
                            using var conn = CreateConnection();
                            conn.SetProxy(CreateProxy(pgConnection, conn));
                            connectionInit(conn, timeout);
                        }
                    : null,
                    asyncInitializer:
                        asyncConnectionInit is not null ? async (pgConnection, cancellationToken) =>
                        {
                            if (Volatile.Read(ref _dbDependencies) is null)
                                return;
                            var conn = CreateConnection();
                            await using var _ = conn.ConfigureAwait(false);
                            conn.SetProxy(CreateProxy(pgConnection, conn));
                            await asyncConnectionInit(conn, cancellationToken).ConfigureAwait(false);
                        }
                    : null
                );

                ConnectionPool<PgConnection>? pool = null;
                try
                {
                    if (_options.MaxPoolSize > 0)
                        pool = new ConnectionPool<PgConnection>(factory, new()
                        {
                            MinConnections = _options.MinPoolSize,
                            MaxConnections = _options.MaxPoolSize,
                            ConnectionIdleLifetime = _options.ConnectionIdleLifetime,
                            ConnectionPruningInterval = _options.ConnectionPruningInterval,
                            HeartbeatInterval = _options.HeartbeatInterval,
                            TimeProvider = _options.TimeProvider,
                            LoggerFactory = _loggerFactory,
                            MetricsName = Name,
                        }, static connection => connection.Protocol.TryBeginPruning());

                    var bootstrapResult = await CreateDbDeps(
                        async, timeout, clientOptions, transportFactory, commandTracker, pool,
                        retainBootstrap: connectionInit is null, _shutdown.Token).ConfigureAwait(false);
                    var deps = bootstrapResult.Dependencies;

                    // A configured connection initializer may depend on the completed catalog. The
                    // first connection therefore could not run it in the factory before bootstrap;
                    // retire that exceptional connection and let the ordinary factory replace it.
                    // Without an initializer, the bootstrap connection is already a complete pooled
                    // connection and remains the pool's first idle member.
                    if (bootstrapResult.Connection is { } bootstrap && connectionInit is not null)
                    {
                        await bootstrap.CompleteAsync().ConfigureAwait(false);
                    }

                    _connectionPool = pool!;
                    _clientFactory = factory;
                    _typeCatalogFactory = bootstrapResult.Factory;
                    _typeCatalogPlugins = bootstrapResult.Plugins;
                    _typeReloadConnectionFactory = new PgConnectionFactory(
                        clientOptions, transportFactory,
                        configureOptions: options =>
                        {
                            options.BackendProvider = _backendProvider;
                            options.ExpectedBackendInfo = deps.BackendInfo;
                        });

                    // Commit the complete datasource state as one publication. GetDbDependencies is
                    // itself an initialization gateway, so exposing the bundle before the pool and
                    // reload recipe are installed would let a concurrent command bypass the lifecycle
                    // lock and observe a partially initialized datasource.
                    Volatile.Write(ref _dbDependencies, deps);
                    _isInitialized = true;
                    pool = null;
                }
                catch
                {
                    Volatile.Write(ref _dbDependencies, null!);
                    if (pool is not null)
                    {
                        if (async)
                            await pool.DisposeAsync().ConfigureAwait(false);
                        else
                            ((IDisposable)pool).Dispose();
                    }
                    throw;
                }
            }
            finally
            {
                _lifecycleLock.Release();
            }
        }

        async ValueTask<(PgDbDependencies Dependencies, PgTypeCatalogFactory Factory,
            PgTypeCatalogPlugin[] Plugins, PgConnection? Connection)> CreateDbDeps(
            bool async,
            TimeSpan timeout,
            PgClientOptions clientOptions,
            TransportConnection.Factory transportFactory,
            CommandTracker commandTracker,
            ConnectionPool<PgConnection>? pool,
            bool retainBootstrap,
            CancellationToken shutdownToken)
        {
            var bootstrapFactory = new PgConnectionFactory(clientOptions, transportFactory,
                tracker: commandTracker,
                configureOptions: options => options.BackendProvider = _backendProvider);
            PgConnection? bootstrap = null;
            ConnectionPool<PgConnection>.UnqualifiedLease bootstrapLease = default;
            PgTypeCatalogFactoryContext? context = null;
            Exception? error = null;
            try
            {
                if (pool is null)
                {
                    bootstrap = async
                        ? await bootstrapFactory.CreateAsync(shutdownToken).ConfigureAwait(false)
                        : bootstrapFactory.Create(timeout);
                }
                else
                {
                    bootstrapLease = async
                        ? await pool.GetUnqualifiedAsync(timeout, shutdownToken).ConfigureAwait(false)
                        : pool.GetUnqualified(timeout);
                    bootstrap = bootstrapLease.Connection;
                }

                var backendInfo = bootstrap.Protocol.FlowControl.BackendInfo;
                var catalogFactory = _backendProvider.CreateTypeCatalogFactory(backendInfo);
                var dialectPlugins = _backendProvider.CreateTypeCatalogPlugins(backendInfo)
                    ?? throw new InvalidOperationException(
                        "The backend provider returned a null type-catalog plugin collection.");
                var dialectPluginSnapshot = new PgTypeCatalogPlugin[dialectPlugins.Count];
                for (var i = 0; i < dialectPluginSnapshot.Length; i++)
                    dialectPluginSnapshot[i] = dialectPlugins[i]
                        ?? throw new InvalidOperationException(
                            $"The backend provider returned a null type-catalog plugin at index {i}.");
                var configuredRequirements = _options.TypeLoadingSchemas.Count is not 0
                    || _options.LoadTableComposites;
                var plugins = new PgTypeCatalogPlugin[
                    (configuredRequirements ? 1 : 0) + dialectPluginSnapshot.Length + _userTypeCatalogPlugins.Length];
                var pluginOffset = 0;
                if (configuredRequirements)
                    plugins[pluginOffset++] = new ConfiguredTypeLoadingRequirements(
                        _options.TypeLoadingSchemas, _options.LoadTableComposites);
                dialectPluginSnapshot.CopyTo(plugins, pluginOffset);
                _userTypeCatalogPlugins.CopyTo(plugins, pluginOffset + dialectPluginSnapshot.Length);

                context = new PgTypeCatalogFactoryContext(bootstrap.Protocol, shutdownToken);
                PgTypeCatalog catalog;
                if (async)
                {
                    var load = catalogFactory.CreateAsync(context, plugins, shutdownToken);
                    if (catalogFactory.RequiresProtocol && !context.FlowQueued)
                    {
                        if (!load.IsCompleted)
                            throw new InvalidOperationException(
                                "A protocol-backed type catalog factory yielded before queuing its load flow.");
                        // Preserve a synchronous factory failure instead of masking it as a contract error.
                        load.GetAwaiter().GetResult();
                        throw new InvalidOperationException(
                            "A protocol-backed type catalog factory completed without queuing its load flow.");
                    }
                    catalog = await load.ConfigureAwait(false);
                }
                else
                {
                    catalog = catalogFactory.Create(context, plugins);
                    if (catalogFactory.RequiresProtocol && !context.FlowQueued)
                        throw new InvalidOperationException(
                            "A protocol-backed type catalog factory completed without queuing its load flow.");
                }
                var dependencies = new PgDbDependencies(
                    backendInfo, catalog, commandTracker, DbDepsRevision++);
                if (pool is not null && (context.FlowQueued || !retainBootstrap))
                    _ = bootstrapLease.Transfer();
                return (dependencies, catalogFactory, plugins, pool is null ? null : bootstrap);
            }
            catch (Exception ex)
            {
                error = ex;
                throw;
            }
            finally
            {
                if (bootstrap is not null && (pool is null || error is not null))
                {
                    if (pool is not null)
                        _ = bootstrapLease.Transfer();
                    if (async)
                        await bootstrap.CompleteAsync(error).ConfigureAwait(false);
                    else
                        bootstrap.CompleteAsync(error).GetAwaiter().GetResult();
                }
                else if (bootstrap is not null && retainBootstrap && context is { FlowQueued: false })
                {
                    try
                    {
                        bootstrapLease.Dispose();
                    }
                    catch (Exception ex)
                    {
                        // The successful pool callback transferred the idle token to this bootstrap
                        // operation. If it cannot republish that token, terminate the connection so
                        // the placement is still settled on every exit.
                        if (async)
                            await bootstrap.CompleteAsync(ex).ConfigureAwait(false);
                        else
                            bootstrap.CompleteAsync(ex).GetAwaiter().GetResult();
                        throw;
                    }
                }
            }
        }
    }

    void EnsureInitialized(TimeSpan timeout) => Initialize(false, timeout, CancellationToken.None).GetAwaiter().GetResult();
    ValueTask EnsureInitializedAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var initialization = Initialize(true, timeout, CancellationToken.None);
        return cancellationToken.CanBeCanceled
            ? new(initialization.AsTask().WaitAsync(cancellationToken))
            : initialization;
    }

    async ValueTask ReloadTypesAsyncProjected(CancellationToken cancellationToken)
    {
        try
        {
            await ReloadTypesCore(async: true, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    ValueTask ReloadTypesCore(bool async, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) is not 0, this);
        if (!_isInitialized)
        {
            if (async)
                return EnsureInitializedAsync(ConnectionTimeout, cancellationToken);
            EnsureInitialized(ConnectionTimeout);
            return ValueTask.CompletedTask;
        }

        // Prebuilt catalogs have nothing to reload. Keep this an internal factory distinction so
        // generic ADO integrations can request a reload without first discovering the load strategy.
        if (!_typeCatalogFactory.SupportsReload)
            return ValueTask.CompletedTask;

        Task reload;
        TaskCompletionSource owner;
        lock (_reloadLock)
        {
            if (_typeReload is not null)
                return new(_typeReload.WaitAsync(cancellationToken));

            owner = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _typeReload = reload = owner.Task;
        }
        // The synchronous path may run to completion inline, including clearing _typeReload, so
        // never start it while holding the publication lock.
        _ = RunTypeReloadAsync(async, owner);
        return new(reload.WaitAsync(cancellationToken));

    }

    async Task RunTypeReloadAsync(bool async, TaskCompletionSource completion)
    {
        try
        {
            if (async)
                await _lifecycleLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            else
                _lifecycleLock.Wait(CancellationToken.None);

            try
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) is not 0, this);
                var current = GetDbDependencies();
                var loaded = await LoadTypeCatalogAsync(current, async, _shutdown.Token).ConfigureAwait(false);

                // Publish only the complete replacement. Existing executions retain their old
                // immutable bundle; future executions observe this revision in one reference read.
                // The backend identity and catalog retain the same physical-load provenance; the
                // provider's compatibility policy licenses publishing that pair datasource-wide.
                // The tracker can survive only because preparation identity includes the resolved
                // parameter type/OID shape. Keep that shape in the identity as serializer resolution
                // matures; SQL text alone is not stable across mapping-affecting reloads.
                Volatile.Write(ref _dbDependencies, new PgDbDependencies(
                    loaded.BackendInfo, loaded.Catalog, current.CommandsTracker, DbDepsRevision++));
            }
            finally
            {
                _lifecycleLock.Release();
            }

            ClearTypeReloadOwner(completion);
            completion.TrySetResult();
        }
        catch (Exception ex)
        {
            ClearTypeReloadOwner(completion);
            completion.TrySetException(ex);
        }

        void ClearTypeReloadOwner(TaskCompletionSource owner)
        {
            lock (_reloadLock)
            {
                if (ReferenceEquals(_typeReload, owner.Task))
                    _typeReload = null;
            }
        }
    }

    async ValueTask<LoadedTypeCatalog> LoadTypeCatalogAsync(
        PgDbDependencies current, bool async, CancellationToken shutdownToken)
    {
        if (!_typeCatalogFactory.RequiresProtocol)
        {
            var context = new PgTypeCatalogFactoryContext(current.BackendInfo, shutdownToken);
            var catalog = async
                ? await _typeCatalogFactory.CreateAsync(
                    context, _typeCatalogPlugins, shutdownToken).ConfigureAwait(false)
                : _typeCatalogFactory.Create(context, _typeCatalogPlugins);
            return new(catalog, context.BackendInfo);
        }

        if (_connectionPool is not null)
        {
            var state = new TypeReloadScheduleState(
                _typeCatalogFactory, _typeCatalogPlugins, async, shutdownToken);
            if (async)
                await _connectionPool.GetAsync(
                    static (candidate, state) => state.TrySchedule(candidate), state,
                    ConnectionTimeout, shutdownToken).ConfigureAwait(false);
            else
                _connectionPool.GetAsync(
                        static (candidate, state) => state.TrySchedule(candidate), state,
                        ConnectionTimeout, shutdownToken)
                    .AsTask().GetAwaiter().GetResult();
            var catalog = async
                ? await state.Load.ConfigureAwait(false)
                : state.Load.GetAwaiter().GetResult();
            return new(catalog, state.BackendInfo);
        }

        // An unpooled datasource has no configured capacity to adopt from.
        PgConnection? connection = null;
        Exception? error = null;
        try
        {
            connection = async
                ? await _typeReloadConnectionFactory.CreateAsync(shutdownToken).ConfigureAwait(false)
                : _typeReloadConnectionFactory.Create(ConnectionTimeout);
            var context = new PgTypeCatalogFactoryContext(connection.Protocol, shutdownToken);
            var catalog = async
                ? await _typeCatalogFactory.CreateAsync(
                    context, _typeCatalogPlugins, shutdownToken).ConfigureAwait(false)
                : _typeCatalogFactory.Create(context, _typeCatalogPlugins);
            return new(catalog, context.BackendInfo);
        }
        catch (Exception ex)
        {
            error = ex;
            throw;
        }
        finally
        {
            if (connection is not null)
            {
                if (async)
                    await connection.CompleteAsync(error).ConfigureAwait(false);
                else
                    connection.CompleteAsync(error).GetAwaiter().GetResult();
            }
        }
    }

    sealed class ConfiguredTypeLoadingRequirements(
        IReadOnlyList<string> schemas, bool loadTableComposites) : PgTypeCatalogPlugin
    {
        public override void Configure(PgTypeLoadingOptionsBuilder options)
        {
            for (var i = 0; i < schemas.Count; i++)
                options.AddTypeLoadingSchema(schemas[i]);
            options.EnableTableCompositesLoading(loadTableComposites);
        }
    }

    sealed class TypeReloadScheduleState(PgTypeCatalogFactory factory,
        PgTypeCatalogPlugin[] plugins, bool async,
        CancellationToken shutdownToken)
    {
        public ValueTask<PgTypeCatalog> Load { get; private set; }
        public PgBackendInfo BackendInfo { get; private set; } = null!;

        public bool TrySchedule(ConnectionCandidate<PgConnection> candidate)
        {
            // Pool callbacks run outside its synchronization lock. The synchronous path may therefore
            // perform the cold catalog load inline while owning only this candidate.
            var context = new PgTypeCatalogFactoryContext(
                candidate.Connection.Protocol, shutdownToken);
            BackendInfo = context.BackendInfo;
            try
            {
                Load = async
                    ? factory.CreateAsync(context, plugins, shutdownToken)
                    : new(factory.Create(context, plugins));
            }
            catch (Exception ex) when (context.FlowQueued)
            {
                // Queuing transfers candidate retirement to the flow. Preserve that ownership fact
                // even when extensible synchronous setup throws after the transfer; the caller
                // observes the failure from Load after pool placement has committed successfully.
                Load = ValueTask.FromException<PgTypeCatalog>(ex);
                return true;
            }

            if (!context.FlowQueued)
            {
                if (!Load.IsCompleted)
                    throw new InvalidOperationException(
                        "A protocol-backed type catalog factory yielded before queuing its load flow.");
                // Surface a synchronous setup failure inside pool admission so its idle token is
                // returned. A successful protocol factory must have queued a flow.
                Load.GetAwaiter().GetResult();
                throw new InvalidOperationException(
                    "A protocol-backed type catalog factory completed without queuing a load flow.");
            }

            return true;
        }
    }

    readonly record struct LoadedTypeCatalog(PgTypeCatalog Catalog, PgBackendInfo BackendInfo);

    // The multiplexed path lets the pool select a wire before materializing connection-local command
    // state. A rejected candidate rolls that attempt back and reuses the still-unqueued flow shell.
    internal CommandFlow EnqueueCommands(CommandFlow flow, TimeSpan pendingTimeout)
    {
        try
        {
            _connectionPool.Get(static (ctx, f) => TrySchedule(ctx, f), flow, pendingTimeout);
            return flow;
        }
        catch
        {
            flow.DiscardUnqueued();
            throw;
        }
    }

    internal async ValueTask<CommandFlow> EnqueueCommandsAsync(
        CommandFlow flow, TimeSpan pendingTimeout, CancellationToken cancellationToken)
    {
        try
        {
            await _connectionPool.GetAsync(static (ctx, f) => TrySchedule(ctx, f), flow, pendingTimeout,
                cancellationToken).ConfigureAwait(false);
            return flow;
        }
        catch
        {
            flow.DiscardUnqueued();
            throw;
        }
    }

    static bool TrySchedule(ConnectionCandidate<PgConnection> context, CommandFlow flow)
    {
        var enqueueOptions = context.IsIdleCandidate
            ? FlowEnqueueOptions.AllowMigration
            : FlowEnqueueOptions.AllowMigration | FlowEnqueueOptions.RequireExistingPipeline;
        return context.Connection.Protocol.TryQueue(flow, enqueueOptions, context.CancellationToken);
    }

    /// <inheritdoc />
    public override string ConnectionString => _connectionString;

    /// Reloads PostgreSQL type metadata and publishes a new serializer snapshot.
    public void ReloadTypes()
    {
        try
        {
            ReloadTypesCore(async: false, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    /// <summary>Reloads PostgreSQL type metadata and publishes a new serializer snapshot.</summary>
    /// <param name="cancellationToken">A token for cancelling the reload.</param>
    public ValueTask ReloadTypesAsync(CancellationToken cancellationToken = default)
        => ReloadTypesAsyncProjected(cancellationToken);

    internal CommandTracker GetCommandTracker(bool initializedOnly = false)
    {
        if (initializedOnly && !_isInitialized)
            throw NotInitializedException();

        EnsureInitialized(ConnectionTimeout);
        return GetDbDependencies().CommandsTracker;
    }

    internal ValueTask<CommandTracker> GetCommandTrackerAsync(CancellationToken cancellationToken)
    {
        return _isInitialized ? new(GetDbDependencies().CommandsTracker) : Core(cancellationToken);

        async ValueTask<CommandTracker> Core(CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(ConnectionTimeout, cancellationToken).ConfigureAwait(false);
            return GetDbDependencies().CommandsTracker;
        }
    }

    internal ValueTask ReleaseOwnedPreparedCommand(object owner, bool awaitable)
        => GetCommandTracker(initializedOnly: true).ReleaseOwned(owner, awaitable);

    internal ValueTask<PgDbDependencies> GetDbDependenciesAsync(CancellationToken cancellationToken)
    {
        return _isInitialized ? new(GetDbDependencies()) : Core(cancellationToken);

        async ValueTask<PgDbDependencies> Core(CancellationToken token)
        {
            await EnsureInitializedAsync(ConnectionTimeout, token).ConfigureAwait(false);
            return GetDbDependencies();
        }
    }

    /// <inheritdoc />
    protected override DbConnection CreateDbConnection() => CreateConnection();
    /// Creates a closed connection owned by this datasource.
    public new SlonConnection CreateConnection()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) is not 0, this);
        return new(this);
    }

    /// <inheritdoc />
    protected override DbConnection OpenDbConnection() => OpenConnection();
    /// Creates and opens a connection owned by this datasource.
    public new SlonConnection OpenConnection() => OpenConnectionCore(SlonConnectionOptions.None);

    /// <summary>Opens a connection using the requested connection policy.</summary>
    public SlonConnection OpenConnection(SlonConnectionOptions options) => OpenConnectionCore(options);

    SlonConnection OpenConnectionCore(SlonConnectionOptions options)
    {
        var connection = CreateConnection();
        try
        {
            connection.Open(options);
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    protected override async ValueTask<DbConnection> OpenDbConnectionAsync(CancellationToken cancellationToken = default)
        => await OpenConnectionAsyncCore(SlonConnectionOptions.None, cancellationToken).ConfigureAwait(false);

    /// <summary>Creates and asynchronously opens a connection owned by this datasource.</summary>
    /// <param name="cancellationToken">A token for cancelling the open operation.</param>
    public new ValueTask<SlonConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        => OpenConnectionAsyncCore(SlonConnectionOptions.None, cancellationToken);

    /// <summary>Opens a connection asynchronously using the requested connection policy.</summary>
    /// <param name="options">The connection options.</param>
    /// <param name="cancellationToken">A token for cancelling the open operation.</param>
    public ValueTask<SlonConnection> OpenConnectionAsync(SlonConnectionOptions options,
        CancellationToken cancellationToken = default)
        => OpenConnectionAsyncCore(options, cancellationToken);

    async ValueTask<SlonConnection> OpenConnectionAsyncCore(SlonConnectionOptions options,
        CancellationToken cancellationToken)
    {
        var connection = CreateConnection();
        try
        {
            await connection.OpenAsync(options, cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// Creates a batch which executes directly through this datasource.
    public new SlonBatch CreateBatch()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) is not 0, this);
        return new(this);
    }
    /// <inheritdoc />
    protected override DbBatch CreateDbBatch() => CreateBatch();

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand(string? commandText = null) => CreateCommand(commandText);
    /// <summary>Creates a command which executes directly through this datasource.</summary>
    /// <param name="commandText">The SQL statement to execute.</param>
    public new SlonCommand CreateCommand(string? commandText = null)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) is not 0, this);
        return new(null, this, commandText);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        // The data source owns its connection pool (created in EnsureInitialized); disposing it closes
        // every wire. Without this a disposed source leaked its whole pool (MinPoolSize warms one eagerly,
        // up to MaxPoolSize), so a process that creates many short-lived sources exhausts max_connections.
        // Guard on `disposing` so the DisposeAsync path (which runs DisposeAsyncCore then Dispose(false))
        // doesn't double-dispose; the pool's own _disposed makes a double harmless regardless.
        if (!disposing || Interlocked.Exchange(ref _disposed, 1) is not 0)
            return;

        try
        {
            _shutdown.Cancel();
        }
        finally
        {
            _lifecycleLock.Wait();
            try
            {
                (_connectionPool as IDisposable)?.Dispose();
            }
            finally
            {
                _lifecycleLock.Release();
                _shutdown.Dispose();
            }
        }
    }

    /// <inheritdoc />
    protected override async ValueTask DisposeAsyncCore()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is not 0)
            return;

        try
        {
            _shutdown.Cancel();
        }
        finally
        {
            await _lifecycleLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_connectionPool is not null)
                    await _connectionPool.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _lifecycleLock.Release();
                _shutdown.Dispose();
            }
        }
    }

    // Internal for testing.
    internal class PgDbDependencies
    {
        public PgDbDependencies(PgBackendInfo backendInfo, PgTypeCatalog typeCatalog,
            CommandTracker commandTracker, int revision)
        {
            BackendInfo = backendInfo;
            TypeCatalog = typeCatalog;
            SerializerOptions = new(typeCatalog);
            ParameterWriter = SerializerParameterWriter.Instance;
            CommandsTracker = commandTracker;
            Revision = revision;
        }

        public PgBackendInfo BackendInfo { get; }
        public PgBackendCapabilities BackendCapabilities => BackendInfo.Capabilities;
        public PgTypeCatalog TypeCatalog { get; }
        public PgSerializerOptions SerializerOptions { get; }
        public ParameterWriter ParameterWriter { get; }
        public CommandTracker CommandsTracker { get; }
        public int Revision { get; }

    }

    static Exception NotInitializedException() => new InvalidOperationException("DataSource is not initialized yet, at least one connection needs to be opened first.");

    internal AdoConnectionProxy GetProxy(IAdoConnection connection, TimeSpan timeout)
    {
        EnsureInitialized(timeout);
        return GetScopedProxy(connection, timeout, SlonConnectionOptions.None);
    }

    internal async ValueTask<AdoConnectionProxy> GetProxyAsync(IAdoConnection connection, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(timeout, cancellationToken).ConfigureAwait(false);
        return await GetScopedProxyAsync(
            connection, timeout, cancellationToken, SlonConnectionOptions.None).ConfigureAwait(false);
    }

    internal AdoConnectionProxy GetProxy(IAdoConnection connection, TimeSpan timeout,
        SlonConnectionOptions options)
    {
        EnsureInitialized(timeout);
        return GetScopedProxy(connection, timeout, options);
    }

    AdoConnectionProxy GetScopedProxy(IAdoConnection connection, TimeSpan timeout,
        SlonConnectionOptions options)
    {
        var proxy = new AdoConnectionProxy(this, connection);
        _connectionPool.Get(static (candidate, state) => TryStartExclusiveScope(candidate, state),
            (Proxy: proxy, Async: false, Options: options), timeout);
        proxy.AcquireExclusiveScope();
        return proxy;
    }

    internal async ValueTask<AdoConnectionProxy> GetProxyAsync(IAdoConnection connection,
        TimeSpan timeout, CancellationToken cancellationToken, SlonConnectionOptions options)
    {
        await EnsureInitializedAsync(timeout, cancellationToken).ConfigureAwait(false);
        return await GetScopedProxyAsync(connection, timeout, cancellationToken, options).ConfigureAwait(false);
    }

    async ValueTask<AdoConnectionProxy> GetScopedProxyAsync(IAdoConnection connection, TimeSpan timeout,
        CancellationToken cancellationToken, SlonConnectionOptions options)
    {
        var proxy = new AdoConnectionProxy(this, connection);
        await _connectionPool.GetAsync(static (candidate, state) => TryStartExclusiveScope(candidate, state),
            (Proxy: proxy, Async: true, Options: options), timeout, cancellationToken).ConfigureAwait(false);
        await proxy.AcquireExclusiveScopeAsync(cancellationToken).ConfigureAwait(false);
        return proxy;
    }

    static bool TryStartExclusiveScope(ConnectionCandidate<PgConnection> candidate,
        (AdoConnectionProxy Proxy, bool Async, SlonConnectionOptions Options) state)
    {
        var enqueueOptions = ((state.Options & SlonConnectionOptions.LongRunning) != 0
                ? FlowEnqueueOptions.BlockAdmission
                : FlowEnqueueOptions.None) |
            (candidate.IsIdleCandidate ? FlowEnqueueOptions.None : FlowEnqueueOptions.RequireExistingPipeline);
        return state.Proxy.TryStartExclusiveScope(candidate.Connection, state.Async, enqueueOptions);
    }

    sealed class ProviderFactoryImpl(SlonDataSource dataSource) : DbProviderFactory
    {
        public override bool CanCreateBatch => true;
        public override SlonConnection CreateConnection() => dataSource.CreateConnection();
        public override SlonBatch CreateBatch() => new();
        public override SlonBatchCommand CreateBatchCommand() => new();
        public override SlonCommand CreateCommand() => new();
        public override SlonParameter CreateParameter() => new();
    }
}
