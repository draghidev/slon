using Slon.Pg;
using Slon.Pg.Types;
using Slon.Pooling;
using Slon.Transport;

namespace Slon;

public sealed partial class SlonDataSource
{
    ValueTask Initialize(bool async, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _dbDependencies) is not null)
            return default;

        return InitializeSlow();

        async ValueTask InitializeSlow()
        {
            if (async)
                await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            else
                _lifecycleLock.Wait(cancellationToken);
            try
            {
                ThrowIfDisposed();
                if (Volatile.Read(ref _dbDependencies) is not null)
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
                            conn.SetProxy(CreateProxy(pgConnection));
                            connectionInit(new(conn), timeout);
                        }
                    : null,
                    asyncInitializer:
                        asyncConnectionInit is not null ? async (pgConnection, cancellationToken) =>
                        {
                            if (Volatile.Read(ref _dbDependencies) is null)
                                return;
                            var conn = CreateConnection();
                            await using var _ = conn.ConfigureAwait(false);
                            conn.SetProxy(CreateProxy(pgConnection));
                            await asyncConnectionInit(new(conn), cancellationToken).ConfigureAwait(false);
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
                    backendInfo, catalog, commandTracker, _dbDepsRevision++);
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

    void EnsureInitialized(TimeSpan timeout)
        => Initialize(false, timeout, CancellationToken.None).GetAwaiter().GetResult();
    ValueTask EnsureInitializedAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var initialization = Initialize(true, timeout, CancellationToken.None);
        return cancellationToken.CanBeCanceled
            ? new(initialization.AsTask().WaitAsync(cancellationToken))
            : initialization;
    }
}
