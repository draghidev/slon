using System.Data.Common;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Types;
using Slon.Pg.Serialization;
using Slon.Pools;
using Slon.Transport;

namespace Slon;

public sealed record SlonDataSourceOptions
{
    internal static TimeSpan DefaultCommandTimeout = TimeSpan.FromSeconds(30);

    public required EndPoint EndPoint { get; init; }
    public required string Username { get; init; }
    /// <summary>Identifies this datasource in metrics. Defaults to endpoint/database.</summary>
    public string? Name { get; init; }
    public string? Password { get; init; }
    public string? Database { get; init; }
    /// <summary>Configures PostgreSQL TLS negotiation and server authentication.</summary>
    public PostgreSqlSslOptions Ssl { get; init; } = new();
    /// <summary>Configures authentication policy. The data source snapshots these values when built.</summary>
    public PostgreSqlAuthenticationOptions Authentication { get; init; } = new();
    public PostgreSqlOAuthOptions? OAuth { get; init; }
    public PostgreSqlIntegratedSecurityOptions? IntegratedSecurity { get; init; }
    public TimeSpan ConnectionTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan CancellationTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public int MinPoolSize { get; init; } = 1;
    public int MaxPoolSize { get; init; } = 10;
    /// <summary>
    /// Limits datasource operations assigned to one physical PostgreSQL wire. Zero leaves assignment
    /// uncapped. A finite value bounds the collateral failure exposure of one wire; later operations
    /// remain in the pool backlog until another wire can accept them.
    /// </summary>
    public int MaxInFlightOperationsPerWire { get; init; }
    /// <summary>Receives sparse driver diagnostics. Logging is disabled by default.</summary>
    public ILoggerFactory LoggerFactory { get; init; } = NullLoggerFactory.Instance;
    /// Duration over which the pool observes unused capacity before pruning it.
    /// Set to <see cref="Timeout.InfiniteTimeSpan"/> to let the pool grow without shrinking.
    /// Pruning is also disabled when <see cref="MinPoolSize"/> equals <see cref="MaxPoolSize"/>.
    public TimeSpan ConnectionIdleLifetime { get; init; } = TimeSpan.FromMinutes(5);
    /// Interval between idle-capacity observations.
    public TimeSpan ConnectionPruningInterval { get; init; } = TimeSpan.FromSeconds(10);

    /// Configures a fixed-size pool. Fixed-size pools are not pruned.
    public int PoolSize
    {
        init
        {
            MinPoolSize = value;
            MaxPoolSize = value;
        }
    }

    public Action<SlonConnection, TimeSpan>? ConnectionInitializer { get; init; }
    public Func<SlonConnection, CancellationToken, ValueTask>? AsyncConnectionInitializer { get; init; }

    /// <summary>
    /// A command's PendingTimeout initially follows CommandTimeout and bounds datasource admission and
    /// response-order waiting. Once executing, CommandTimeout affects the first IO read after writing
    /// each command.
    /// </summary>
    public TimeSpan CommandTimeout { get; init; } = DefaultCommandTimeout;
    public int AutoPreparationMinimumUses { get; init; } = 5;
    public int MaxActiveAutoPreparations { get; init; }
    /// <summary>
    /// DataRows larger than this may cross the decoder boundary before their complete body has arrived.
    /// </summary>
    public int DataRowStreamingThreshold { get; init; } = BackendMessageBatch.Segmenter.DefaultDataRowStreamingThreshold;
    /// <summary>Configures which connection state is reset when an exclusive scope is released.</summary>
    public ScopeResetOptions ScopeReset { get; init; } = new();
    /// <summary>
    /// Optionally restricts ordinary type loading to <c>pg_catalog</c> and the listed schemas.
    /// Category-U extension types and their canonical array counterparts remain discoverable
    /// across schemas. An empty list leaves catalog loading unrestricted.
    /// </summary>
    public IReadOnlyList<string> TypeLoadingSchemas { get; init; } = [];
    /// <summary>Whether table row types should be loaded as composites.</summary>
    public bool LoadTableComposites { get; init; }

    // Public builder surface follows once the backend/type-loading contracts settle. Keep the
    // configured backend singular; automatic backend detection is not reliable for PostgreSQL-
    // compatible servers that deliberately advertise PostgreSQL identity.
    internal PgBackendProvider BackendProvider { get; init; } = PostgreSqlBackendProvider.Instance;
    internal IReadOnlyList<PgTypeCatalogPlugin> TypeCatalogPlugins { get; init; } = [];

    // Internal, tests need to override these to drive maintenance flows on a tight loop. Public
    // surface would require thinking through "what's a sensible knob for end users."
    internal TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(1);
    internal TimeSpan MaintenanceInterval { get; init; } = TimeSpan.FromSeconds(1);

    internal PgClientOptions ToPgClientOptions(OAuthTokenCache? oauthTokens = null,
        ILoggerFactory? loggerFactory = null) => new()
    {
        EndPoint = EndPoint,
        Username = Username,
        Database = Database,
        Password = Password,
        Ssl = Ssl.Snapshot(),
        AllowInsecureTransport = Authentication.AllowInsecureTransport,
        OAuthTokens = oauthTokens,
        IntegratedSecurity = IntegratedSecurity,
        LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance,
        HeartbeatInterval = HeartbeatInterval,
        MaintenanceInterval = MaintenanceInterval,
        ScopeReset = ScopeReset.Snapshot(),
        DataRowStreamingThreshold = DataRowStreamingThreshold,
        MaxInFlightFlowsPerWire = MaxInFlightOperationsPerWire,
    };

    internal bool Validate()
    {
        ArgumentNullException.ThrowIfNull(Ssl);
        ArgumentNullException.ThrowIfNull(Authentication);
        ArgumentNullException.ThrowIfNull(LoggerFactory);
        if (Name is not null && string.IsNullOrWhiteSpace(Name))
            throw new ArgumentException("Cannot be empty or whitespace.", nameof(Name));
        if ((ConnectionInitializer is null) != (AsyncConnectionInitializer is null))
            throw new ArgumentException(
                "Synchronous and asynchronous connection initializers must be configured together.");
        ArgumentOutOfRangeException.ThrowIfNegative(MaxInFlightOperationsPerWire);
        for (var i = 0; i < TypeLoadingSchemas.Count; i++)
            ArgumentException.ThrowIfNullOrWhiteSpace(TypeLoadingSchemas[i], nameof(TypeLoadingSchemas));
        Ssl.Validate();
        OAuth?.Validate();
        IntegratedSecurity?.Validate();
        // etc
        return true;
    }

    internal SlonDataSourceOptions Snapshot() => this with
    {
        // The BCL endpoint implementations are mutable classes. Unknown custom endpoint types are
        // extension objects and are therefore required to provide immutable configuration semantics.
        EndPoint = EndPoint switch
        {
            IPEndPoint ip => new IPEndPoint(ip.Address, ip.Port),
            DnsEndPoint dns => new DnsEndPoint(dns.Host, dns.Port, dns.AddressFamily),
            UnixDomainSocketEndPoint unix => new UnixDomainSocketEndPoint(unix.ToString()),
            _ => EndPoint
        },
        Ssl = Ssl.Snapshot(),
        Authentication = Authentication.Snapshot(),
        IntegratedSecurity = IntegratedSecurity?.Snapshot(),
        ScopeReset = ScopeReset.Snapshot(),
        TypeLoadingSchemas = [.. (TypeLoadingSchemas
            ?? throw new ArgumentNullException(nameof(TypeLoadingSchemas)))]
    };

    public static EndPoint ParseIpOrDnsEndPoint(string host) => PgClientOptions.ParseIpOrDnsEndPoint(host);

    public override string ToString()
        => $"{nameof(SlonDataSourceOptions)} {{ EndPoint = {EndPoint}, Username = {Username}, " +
           $"Database = {Database}, Password = <redacted> }}";
}

/// <inheritdoc cref="System.Data.Common.DbDataSource" />
public sealed class SlonDataSource : DbDataSource
{
    readonly SlonDataSourceOptions _options;
    readonly PgBackendProvider _backendProvider;
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

    public SlonDataSource(SlonDataSourceOptions options)
    {
        _options = options.Snapshot();
        _options.Validate();
        _loggerFactory = new SlonLoggerFactory(_options.LoggerFactory);
        _adoLogger = _loggerFactory.CreateLogger("Slon");
        DisplayEndpoint = _options.EndPoint.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6 ? $"tcp://{_options.EndPoint}" : _options.EndPoint.ToString()!;
        Name = _options.Name ?? $"{DisplayEndpoint}/{Database}";
        _connectionString = $"Endpoint={DisplayEndpoint};Username={_options.Username};Database={Database}";
        _backendProvider = _options.BackendProvider
            ?? throw new ArgumentNullException(nameof(_options.BackendProvider));
        _userTypeCatalogPlugins = [.. (_options.TypeCatalogPlugins
            ?? throw new ArgumentNullException(nameof(_options.TypeCatalogPlugins)))];
        _lifecycleLock = new(1);
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

    // True for datasources that dispatch commands across different backends.
    // Among other effects this impacts cacheability of state derived from unstable backend type information.
    // Its value should be static for the lifetime of the instance.
    internal bool IsVirtualDataSource => false;
    // This is to get back to the multi-host datasource that owns its host sources.
    // It also helps commands to keep caches intact when switching sources from the same owner.
    internal SlonDataSource DataSourceOwner => this;

    internal TimeSpan ConnectionTimeout => _options.ConnectionTimeout;
    internal TimeSpan DefaultCancellationTimeout => _options.CancellationTimeout;
    internal TimeSpan DefaultCommandTimeout => _options.CommandTimeout;
    internal void ReportTransactionDisposeRollbackFailure(Exception exception)
        => SlonLogMessages.TransactionDisposeRollbackFailed(_adoLogger, exception);
    internal string Database => _options.Database ?? _options.Username;
    internal EndPoint EndPoint => _options.EndPoint;
    public string Name { get; }
    internal string DisplayEndpoint { get; }

    internal string ServerVersion => GetDbDependencies().BackendInfo.ServerVersionString;
    internal PgTypeCatalog TypeCatalog => GetDbDependencies().TypeCatalog;

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
                            LoggerFactory = _loggerFactory,
                            MetricsName = Name,
                        });

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
            Exception? error = null;
            try
            {
                bootstrap = pool is null
                    ? async
                        ? await bootstrapFactory.CreateAsync(shutdownToken).ConfigureAwait(false)
                        : bootstrapFactory.Create(timeout)
                    : async
                        ? await pool.GetAsync(timeout, shutdownToken).ConfigureAwait(false)
                        : pool.Get(timeout);

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

                var context = new PgTypeCatalogFactoryContext(bootstrap.Protocol, shutdownToken);
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
                if (pool is not null && retainBootstrap && !context.FlowQueued)
                    pool.ReturnUnscheduled(bootstrap);
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
                    if (async)
                        await bootstrap.CompleteAsync(error).ConfigureAwait(false);
                    else
                        bootstrap.CompleteAsync(error).GetAwaiter().GetResult();
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

    public void ReloadTypes()
        => ReloadTypesCore(async: false, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    public ValueTask ReloadTypesAsync(CancellationToken cancellationToken = default)
        => ReloadTypesCore(async: true, cancellationToken);

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
            Load = async
                ? factory.CreateAsync(context, plugins, shutdownToken)
                : new(factory.Create(context, plugins));

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
    internal CommandFlow EnqueueCommands<TState>(Func<PgConnection, TState, CommandFlowOptions> createOptions,
        TState state, TimeSpan pendingTimeout)
    {
        var flow = CommandFlow.CreateUninitialized();
        var schedule = new CommandScheduleState<TState>(createOptions, state, flow, async: false);
        try
        {
            _connectionPool.Get(static (ctx, s) => s.TrySchedule(ctx), schedule, pendingTimeout);
            return flow;
        }
        catch
        {
            flow.DiscardUnqueued();
            throw;
        }
    }

    internal async ValueTask<CommandFlow> EnqueueCommandsAsync<TState>(
        Func<PgConnection, TState, CommandFlowOptions> createOptions, TState state, TimeSpan pendingTimeout,
        CancellationToken cancellationToken)
    {
        var flow = CommandFlow.CreateUninitialized();
        var schedule = new CommandScheduleState<TState>(createOptions, state, flow, async: true);
        try
        {
            await _connectionPool.GetAsync(static (ctx, s) => s.TrySchedule(ctx), schedule, pendingTimeout,
                cancellationToken).ConfigureAwait(false);
            return flow;
        }
        catch
        {
            flow.DiscardUnqueued();
            throw;
        }
    }

    readonly struct CommandScheduleState<TState>(Func<PgConnection, TState, CommandFlowOptions> createOptions,
        TState state, CommandFlow flow, bool async)
    {
        public bool TrySchedule(ConnectionCandidate<PgConnection> context)
        {
            var enqueueOptions = context.IsIdleCandidate
                ? FlowEnqueueOptions.None
                : FlowEnqueueOptions.RequireExistingPipeline;
            return context.Connection.TryQueue(
                static (connection, args) => args.Flow.Initialize(
                    args.Async, args.CreateOptions(connection, args.State)),
                (CreateOptions: createOptions, State: state, Flow: flow, Async: async),
                out _,
                context.CancellationToken,
                enqueueOptions);
        }
    }

    public override string ConnectionString => _connectionString;
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

    internal ValueTask<PgDbDependencies> GetDbDependenciesAsync(CancellationToken cancellationToken)
    {
        return _isInitialized ? new(GetDbDependencies()) : Core(cancellationToken);

        async ValueTask<PgDbDependencies> Core(CancellationToken token)
        {
            await EnsureInitializedAsync(ConnectionTimeout, token).ConfigureAwait(false);
            return GetDbDependencies();
        }
    }

    protected override DbConnection CreateDbConnection() => CreateConnection();
    public new SlonConnection CreateConnection() => new(this);

    protected override DbConnection OpenDbConnection() => OpenConnection();
    public new SlonConnection OpenConnection()
    {
        var connection = CreateConnection();
        connection.SetProxy(GetProxy(connection, ConnectionTimeout));
        connection.AcquireExclusiveScope();
        return connection;
    }

    /// <summary>Opens a connection, optionally excluding it from datasource command admission.</summary>
    /// <param name="longRunning">
    /// <see langword="true"/> when newly arriving datasource operations must not be scheduled behind
    /// the returned connection during its tenure.
    /// </param>
    public SlonConnection OpenConnection(bool longRunning)
    {
        var connection = CreateConnection();
        connection.Open(longRunning);
        return connection;
    }


    protected override async ValueTask<DbConnection> OpenDbConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = CreateConnection();
        connection.SetProxy(await GetProxyAsync(connection, ConnectionTimeout, cancellationToken).ConfigureAwait(false));
        await connection.AcquireExclusiveScopeAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }
    public new async ValueTask<SlonConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = CreateConnection();
        connection.SetProxy(await GetProxyAsync(connection, ConnectionTimeout, cancellationToken).ConfigureAwait(false));
        await connection.AcquireExclusiveScopeAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <summary>Opens a connection asynchronously, optionally excluding it from datasource command admission.</summary>
    /// <param name="longRunning">
    /// <see langword="true"/> when newly arriving datasource operations must not be scheduled behind
    /// the returned connection during its tenure.
    /// </param>
    /// <param name="cancellationToken">A token for cancelling the open operation.</param>
    public async ValueTask<SlonConnection> OpenConnectionAsync(bool longRunning,
        CancellationToken cancellationToken = default)
    {
        var connection = CreateConnection();
        await connection.OpenAsync(longRunning, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    public new SlonBatch CreateBatch() => new(this);
    protected override DbBatch CreateDbBatch() => CreateBatch();

    protected override DbCommand CreateDbCommand(string? commandText = null) => CreateCommand(commandText);
    public new SlonCommand CreateCommand(string? commandText = null) => new(null, this, commandText);

    protected override void Dispose(bool disposing)
    {
        // The data source owns its connection pool (created in EnsureInitialized); disposing it closes
        // every wire. Without this a disposed source leaked its whole pool (MinPoolSize warms one eagerly,
        // up to MaxPoolSize), so a process that creates many short-lived sources exhausts max_connections.
        // Guard on `disposing` so the DisposeAsync path (which runs DisposeAsyncCore then Dispose(false))
        // doesn't double-dispose; the pool's own _disposed makes a double harmless regardless.
        if (!disposing || Interlocked.Exchange(ref _disposed, 1) is not 0)
            return;

        _shutdown.Cancel();
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

    protected override async ValueTask DisposeAsyncCore()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is not 0)
            return;

        _shutdown.Cancel();
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

    // Internal for testing.
    internal class PgDbDependencies
    {
        public PgDbDependencies(PgBackendInfo backendInfo, PgTypeCatalog typeCatalog,
            CommandTracker commandTracker, int revision)
        {
            BackendInfo = backendInfo;
            TypeCatalog = typeCatalog;
            SerializerOptions = new(typeCatalog);
            ParameterWriterStrategy = SerializerParameterWriterStrategy.Instance;
            CommandsTracker = commandTracker;
            Revision = revision;
        }

        public PgBackendInfo BackendInfo { get; }
        public PgBackendCapabilities BackendCapabilities => BackendInfo.Capabilities;
        public PgTypeCatalog TypeCatalog { get; }
        public PgSerializerOptions SerializerOptions { get; }
        public ParameterWriterStrategy ParameterWriterStrategy { get; }
        public CommandTracker CommandsTracker { get; }
        public int Revision { get; }

    }

    static Exception NotInitializedException() => new InvalidOperationException("DataSource is not initialized yet, at least one connection needs to be opened first.");

    internal AdoConnectionProxy GetProxy(IAdoConnection connection, TimeSpan timeout)
    {
        EnsureInitialized(timeout);
        return CreateProxy(_connectionPool.Get(timeout), connection);
    }

    internal async ValueTask<AdoConnectionProxy> GetProxyAsync(IAdoConnection connection, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(timeout, cancellationToken).ConfigureAwait(false);
        return CreateProxy(await _connectionPool.GetAsync(timeout, cancellationToken).ConfigureAwait(false), connection);
    }

    internal AdoConnectionProxy GetLongRunningProxy(IAdoConnection connection, TimeSpan timeout)
    {
        EnsureInitialized(timeout);
        var state = new LongRunningProxyScheduleState(this, connection, async: false);
        _connectionPool.Get(static (candidate, state) => state.TrySchedule(candidate), state, timeout);
        state.Proxy!.WaitForExclusiveScope();
        return state.Proxy;
    }

    internal async ValueTask<AdoConnectionProxy> GetLongRunningProxyAsync(IAdoConnection connection,
        TimeSpan timeout, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(timeout, cancellationToken).ConfigureAwait(false);
        var state = new LongRunningProxyScheduleState(this, connection, async: true);
        await _connectionPool.GetAsync(static (candidate, state) => state.TrySchedule(candidate),
            state, timeout, cancellationToken).ConfigureAwait(false);
        await state.Proxy!.WaitForExclusiveScopeAsync(cancellationToken).ConfigureAwait(false);
        return state.Proxy;
    }

    sealed class LongRunningProxyScheduleState(
        SlonDataSource dataSource, IAdoConnection connection, bool async)
    {
        public AdoConnectionProxy? Proxy { get; private set; }

        public bool TrySchedule(ConnectionCandidate<PgConnection> candidate)
        {
            var proxy = dataSource.CreateProxy(candidate.Connection, connection);
            var enqueueOptions = FlowEnqueueOptions.BlockAdmission |
                (candidate.IsIdleCandidate
                    ? FlowEnqueueOptions.None
                    : FlowEnqueueOptions.RequireExistingPipeline);
            if (!proxy.TryQueueExclusiveScope(async, enqueueOptions))
                return false;
            Proxy = proxy;
            return true;
        }
    }
}
