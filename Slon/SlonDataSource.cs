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

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbDataSource" />
public sealed partial class SlonDataSource : DbDataSource
{
    readonly SlonDataSourceOptions _options;
    readonly PostgreSqlBackendProvider _backendProvider;
    readonly SemaphoreSlim _lifecycleLock = new(1);
    readonly CancellationTokenSource _shutdown = new();
    readonly ILogger _adoLogger;
    readonly ILoggerFactory _loggerFactory;

    // Initialized on the first real use.
    ConnectionPool<PgConnection> _connectionPool = null!;
    IPoolConnectionFactory<PgConnection> _clientFactory = null!;
    PgDbDependencies _dbDependencies = null!;
    int _dbDepsRevision;
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
        DisplayEndpoint = _options.EndPoint.AddressFamily is
            AddressFamily.InterNetwork or AddressFamily.InterNetworkV6
                ? $"tcp://{_options.EndPoint}"
                : _options.EndPoint.ToString()!;
        Name = _options.Name ?? $"{DisplayEndpoint}/{Database}";
        ConnectionString =
            $"Endpoint={DisplayEndpoint};Username={_options.Username};Database={Database}";
        _backendProvider = _options.BackendProvider ?? PgBackendProviders.Create(_options.CompatibilityProfile);
        _userTypeCatalogPlugins = [.. (_options.TypeCatalogPlugins
            ?? throw new ArgumentNullException(nameof(_options.TypeCatalogPlugins)))];
        ProviderFactory = new ProviderFactoryImpl(this);
    }

    // Commands retain one immutable dependency revision while type metadata is reloaded.
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
    /// <inheritdoc />
    public override string ConnectionString { get; }
    /// <summary>Gets the ADO provider factory bound to this datasource.</summary>
    public DbProviderFactory ProviderFactory { get; }
    internal string DisplayEndpoint { get; }

    internal string ServerVersion => GetDbDependencies().BackendInfo.ServerVersionString;

    AdoConnectionProxy CreateProxy(PgConnection pgConnection) => new(this, pgConnection);

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

    internal CommandTracker GetCommandTracker(bool initializedOnly = false)
        => GetDbDependencies(initializedOnly).CommandsTracker;

    internal ValueTask ReleaseOwnedPreparedCommand(object owner, bool awaitable)
        => GetCommandTracker(initializedOnly: true).ReleaseOwned(owner, awaitable);

    internal ValueTask<PgDbDependencies> GetDbDependenciesAsync(CancellationToken cancellationToken)
    {
        return Volatile.Read(ref _dbDependencies) is { } dependencies
            ? new(dependencies)
            : Core(cancellationToken);

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
        ThrowIfDisposed();
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
    public new ValueTask<SlonConnection> OpenConnectionAsync(
        CancellationToken cancellationToken = default)
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
        ThrowIfDisposed();
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
        ThrowIfDisposed();
        return new(null, this, commandText);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        // DisposeAsyncCore is followed by Dispose(false), so only synchronous disposal claims here.
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
    internal sealed class PgDbDependencies(
        PgBackendInfo backendInfo, PgTypeCatalog typeCatalog,
        CommandTracker commandTracker, int revision)
    {
        public PgBackendInfo BackendInfo { get; } = backendInfo;
        public PgBackendCapabilities BackendCapabilities => BackendInfo.Capabilities;
        public PgTypeCatalog TypeCatalog { get; } = typeCatalog;
        public PgSerializerOptions SerializerOptions { get; } = new(typeCatalog);
        public ParameterWriter ParameterWriter { get; } = SerializerParameterWriter.Instance;
        public CommandTracker CommandsTracker { get; } = commandTracker;
        public int Revision { get; } = revision;
    }

    void ThrowIfDisposed()
        => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) is not 0, this);

    static Exception NotInitializedException()
        => new InvalidOperationException(
            "DataSource is not initialized yet, at least one connection needs to be opened first.");

    internal AdoConnectionProxy GetProxy(TimeSpan timeout,
        SlonConnectionOptions options)
    {
        EnsureInitialized(timeout);
        var proxy = new AdoConnectionProxy(this);
        _connectionPool.Get(static (candidate, state) => TryStartExclusiveScope(candidate, state),
            (Proxy: proxy, Async: false, Options: options), timeout);
        proxy.AcquireExclusiveScope();
        return proxy;
    }

    internal async ValueTask<AdoConnectionProxy> GetProxyAsync(
        TimeSpan timeout, CancellationToken cancellationToken, SlonConnectionOptions options)
    {
        await EnsureInitializedAsync(timeout, cancellationToken).ConfigureAwait(false);
        var proxy = new AdoConnectionProxy(this);
        await _connectionPool.GetAsync(static (candidate, state) => TryStartExclusiveScope(candidate, state),
            (Proxy: proxy, Async: true, Options: options), timeout, cancellationToken).ConfigureAwait(false);
        await proxy.AcquireExclusiveScopeAsync(cancellationToken).ConfigureAwait(false);
        return proxy;
    }

    static bool TryStartExclusiveScope(ConnectionCandidate<PgConnection> candidate,
        (AdoConnectionProxy Proxy, bool Async, SlonConnectionOptions Options) state)
    {
        var enqueueOptions = (state.Options.HasFlag(SlonConnectionOptions.LongRunning)
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
