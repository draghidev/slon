using System.Data.Common;
using System.Net;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Types;
using Slon.Pools;
using Slon.Transport;
using AddressFamily = System.Net.Sockets.AddressFamily;

namespace Slon;

interface IFrontendTypeCatalog
{
    DataTypeName GetDataTypeName(PgTypeId pgTypeId);
    bool TryGetIdentifiers(SlonDbType slonDbType, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName);
}

public record SlonDataSourceOptions
{
    internal static TimeSpan DefaultCommandTimeout = TimeSpan.FromSeconds(30);

    public required EndPoint EndPoint { get; init; }
    public required string Username { get; init; }
    public string? Password { get; init; }
    public string? Database { get; init; }
    public TimeSpan ConnectionTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan CancellationTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public int MinPoolSize { get; init; } = 1;
    public int MaxPoolSize { get; init; } = 10;
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
    /// CommandTimeout affects the first IO read after writing out a command.
    /// Default is infinite, where behavior purely relies on read and write timeouts of the underlying protocol.
    /// </summary>
    public TimeSpan CommandTimeout { get; init; } = DefaultCommandTimeout;
    public int AutoPreparationMinimumUses { get; set; } = 5;
    public int MaxActiveAutoPreparations { get; set; }
    /// <summary>Configures which connection state is reset when an exclusive scope is released.</summary>
    public ScopeResetOptions ScopeReset { get; init; } = new();

    // Internal, tests need to override these to drive maintenance flows on a tight loop. Public
    // surface would require thinking through "what's a sensible knob for end users."
    internal TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(1);
    internal TimeSpan MaintenanceInterval { get; init; } = TimeSpan.FromSeconds(1);

    internal PgClientOptions ToPgClientOptions() => new()
    {
        EndPoint = EndPoint,
        Username = Username,
        Database = Database,
        Password = Password,
        HeartbeatInterval = HeartbeatInterval,
        MaintenanceInterval = MaintenanceInterval,
        ScopeReset = ScopeReset.Snapshot(),
    };

    internal bool Validate()
    {
        // etc
        return true;
    }

    public static EndPoint ParseIpOrDnsEndPoint(string host) => PgClientOptions.ParseIpOrDnsEndPoint(host);
}

class PgDatabaseInfo
{
    public PgDatabaseInfo(PgTypeCatalog typeCatalog)
    {
        TypeCatalog = typeCatalog;
        ServerVersion = "PG";
    }

    public string ServerVersion { get; }

    public PgTypeCatalog TypeCatalog { get; }
}


interface ISlonDatabaseInfoProvider
{
    PgDatabaseInfo Get(SlonDataSourceOptions options, TimeSpan timeSpan);
    ValueTask<PgDatabaseInfo> GetAsync(SlonDataSourceOptions options, CancellationToken cancellationToken = default);
}

sealed class DefaultSlonDatabaseInfoProvider: ISlonDatabaseInfoProvider
{
    PgDatabaseInfo Create() => new(PgTypeCatalog.Default);
    public PgDatabaseInfo Get(SlonDataSourceOptions options, TimeSpan timeSpan) => Create();
    public ValueTask<PgDatabaseInfo> GetAsync(SlonDataSourceOptions options, CancellationToken cancellationToken = default) => new(Create());
}

/// <inheritdoc cref="System.Data.Common.DbDataSource" />
public sealed class SlonDataSource: DbDataSource
{
    readonly SlonDataSourceOptions _options;
    readonly ISlonDatabaseInfoProvider _slonDatabaseInfoProvider;
    readonly SemaphoreSlim _lifecycleLock;

    // Initialized on the first real use.
    ConnectionPool<PgConnection> _connectionPool = null!;
    IPoolConnectionFactory<PgConnection> _clientFactory = null!;
    PgDbDependencies _dbDependencies = null!;
    bool _isInitialized;

    public SlonDataSource(SlonDataSourceOptions options) : this(options, null) { }
    internal SlonDataSource(SlonDataSourceOptions options, ISlonDatabaseInfoProvider? pgDatabaseInfoProvider = null)
    {
        options.Validate();
        _options = options;
        DisplayEndpoint = options.EndPoint.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6 ? $"tcp://{options.EndPoint}" : options.EndPoint.ToString()!;
        _slonDatabaseInfoProvider = pgDatabaseInfoProvider ?? new DefaultSlonDatabaseInfoProvider();
        _lifecycleLock = new(1);
    }

    ConnectionPool<PgConnection> ConnectionPool => _connectionPool ?? throw NotInitializedException();

    // Store the result if multiple dependencies are required. The instance may be switched out during reloading.
    // To prevent any inconsistencies without having to obtain a lock on the data we instead use an immutable instance.
    // All relevant depedencies are bundled to provide a consistent view, it's either all new or all old data.
    PgDbDependencies GetDbDependencies() => _dbDependencies ?? throw NotInitializedException();

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
    internal string Database => _options.Database ?? _options.Username;
    internal string DisplayEndpoint { get; }

    internal string ServerVersion => GetDbDependencies().PgDatabaseInfo.ServerVersion;

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
                if (_isInitialized)
                    return;

                // We don't flow cancellationToken past this point, at least one thread has to finish the init.
                // We do DbDeps first as it may throw, otherwise we'd need to cleanup the other dependencies again.
                var deps = await CreateDbDeps(async, timeout, CancellationToken.None).ConfigureAwait(false);

                var connectionInit = _options.ConnectionInitializer;
                var asyncConnectionInit = _options.AsyncConnectionInitializer;
                var clientOptions = _options.ToPgClientOptions();
                var transportFactory = SocketStreamConnection.CreateFactory(clientOptions.EndPoint);

                var factory = new InitializingConnectionFactory<PgConnection>(
                    factory: new PgConnectionFactory(clientOptions, transportFactory, tracker: deps.CommandsTracker),
                    initializer: connectionInit is not null ? (pgConnection, timeout) =>
                        {
                            using var conn = CreateConnection();
                            conn.SetProxy(CreateProxy(pgConnection, conn));
                            connectionInit(conn, timeout);
                        } : null,
                    asyncInitializer:
                        asyncConnectionInit is not null ? async (pgConnection, cancellationToken) =>
                        {
                            var conn = CreateConnection();
                            await using var _ = conn.ConfigureAwait(false);
                            conn.SetProxy(CreateProxy(pgConnection, conn));
                            await asyncConnectionInit(conn, cancellationToken).ConfigureAwait(false);
                        } : null
                );

                // Finally store all the fields
                if (_options.MaxPoolSize > 0)
                    _connectionPool = new ConnectionPool<PgConnection>(factory, new()
                    {
                        MaxConnections = _options.MaxPoolSize,
                        HeartbeatInterval = _options.HeartbeatInterval,
                    });
                _clientFactory = factory;
                _dbDependencies = deps;

                _isInitialized = true;
            }
            finally
            {
                _lifecycleLock.Release();
            }
        }

        async ValueTask<PgDbDependencies> CreateDbDeps(bool async, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var databaseInfo = async
                ? _slonDatabaseInfoProvider.Get(_options, timeout)
                : await _slonDatabaseInfoProvider.GetAsync(_options, cancellationToken).ConfigureAwait(false);

            return new PgDbDependencies(databaseInfo, new CommandTracker(_options.MaxActiveAutoPreparations, _options.AutoPreparationMinimumUses), DbDepsRevision++);
        }
    }

    void EnsureInitialized(TimeSpan timeout) => Initialize(false, timeout, CancellationToken.None).GetAwaiter().GetResult();
    ValueTask EnsureInitializedAsync(TimeSpan timeout, CancellationToken cancellationToken) => Initialize(true, timeout, cancellationToken);

    internal void Enqueue(PgClientFlow flow, TimeSpan connectionTimeout)
    {
        throw new NotImplementedException();
    }

    // The MULTIPLEXED command path: build the flow, then let the pool pick a wire (power-of-two-choices
    // load score) and enqueue the flow onto its protocol, WITHOUT leasing/holding the connection - other
    // flows multiplex onto the same wire concurrently. No proxy bookkeeping (pipeline depth, break-on-fault
    // are connection-lease concerns); auto-prepare completion rides the options' OnCommandResultAction. The
    // schedule callback runs under the pool's stripe walk and returns whether the wire accepted the flow.
    internal CommandFlow EnqueueCommands(CommandFlowOptions options)
    {
        var flow = new CommandFlow(async: false, options);
        try
        {
            _connectionPool.Get(static (ctx, f) => ctx.Connection.TryQueue(f), flow, ConnectionTimeout);
            return flow;
        }
        catch
        {
            flow.DiscardUnqueued();
            throw;
        }
    }

    internal async ValueTask<CommandFlow> EnqueueCommandsAsync(CommandFlowOptions options, CancellationToken cancellationToken)
    {
        var flow = new CommandFlow(async: true, options);
        try
        {
            await _connectionPool.GetAsync(static (ctx, f) => ctx.Connection.TryQueue(f), flow, ConnectionTimeout, cancellationToken).ConfigureAwait(false);
            return flow;
        }
        catch
        {
            flow.DiscardUnqueued();
            throw;
        }
    }

    internal string SensitiveConnectionString => throw new NotImplementedException();
    public override string ConnectionString => ""; //TODO
    internal CommandTracker GetCommandTracker(bool initializedOnly = false)
    {
        if (initializedOnly && !_isInitialized)
            throw NotInitializedException();

        EnsureInitialized(TimeSpan.Zero);
        return GetDbDependencies().CommandsTracker;
    }

    internal ValueTask<CommandTracker> GetCommandTrackerAsync(CancellationToken cancellationToken)
    {
        return _isInitialized ? new(GetDbDependencies().CommandsTracker) : Core(cancellationToken);

        async ValueTask<CommandTracker> Core(CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(TimeSpan.Zero, cancellationToken).ConfigureAwait(false);
            return GetDbDependencies().CommandsTracker;
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
        if (disposing)
            (_connectionPool as IDisposable)?.Dispose();
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        if (_connectionPool is not null)
            await _connectionPool.DisposeAsync().ConfigureAwait(false);
    }

    // Internal for testing.
    internal class PgDbDependencies: IFrontendTypeCatalog
    {
        public PgDbDependencies(PgDatabaseInfo pgDatabaseInfo, CommandTracker commandTracker, int revision)
        {
            PgDatabaseInfo = pgDatabaseInfo;
            CommandsTracker = commandTracker;
            Revision = revision;
        }

        public PgDatabaseInfo PgDatabaseInfo { get; }
        public CommandTracker CommandsTracker { get; }
        public int Revision { get; }

        public PgTypeCatalog TypeCatalog => PgDatabaseInfo.TypeCatalog;

        public bool TryGetIdentifiers(SlonDbType slonDbType, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
            => TryGetIdentifiers(TypeCatalog, slonDbType, out canonicalTypeId, out dataTypeName);

        public DataTypeName GetDataTypeName(PgTypeId typeId) => TypeCatalog.GetDataTypeName(typeId);

        internal static bool TryGetIdentifiers(PgTypeCatalog typeCatalog, SlonDbType slonDbType, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
        {
            if (slonDbType.ResolveArrayType)
                return typeCatalog.TryGetArrayIdentifiers(slonDbType.DataTypeName, out canonicalTypeId, out dataTypeName);

            if (slonDbType.ResolveMultirangeType)
                return typeCatalog.TryGetMultiRangeIdentifiers(slonDbType.DataTypeName, out canonicalTypeId, out dataTypeName);

            return typeCatalog.TryGetIdentifiers(slonDbType.DataTypeName, out canonicalTypeId, out dataTypeName);
        }
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
}
