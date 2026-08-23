using System.Net;
using System.Net.Sockets;
using Draghi.Pipelining;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Types;
using Slon.Threading;

namespace Slon;

/// Provides the connection being initialized.
// TODO expose a session-state bag shared with later operations so initialization effects can
// participate in session reset policy.
public readonly struct SlonConnectionInitializerContext
{
    internal SlonConnectionInitializerContext(SlonConnection connection)
        => Connection = connection;

    /// Gets the newly opened physical connection being initialized.
    public SlonConnection Connection { get; }
}

/// Configures a <see cref="SlonDataSource" />.
public sealed record SlonDataSourceOptions
{
    internal static TimeSpan DefaultCommandTimeout = TimeSpan.FromSeconds(30);

    internal static int ToAdoTimeoutSeconds(TimeSpan timeout)
    {
        if (timeout == Timeout.InfiniteTimeSpan)
            return 0;

        return timeout.TotalSeconds >= int.MaxValue ? int.MaxValue : (int)timeout.TotalSeconds;
    }

    /// Gets the PostgreSQL network endpoint.
    public required EndPoint EndPoint { get; init; }
    /// Gets the PostgreSQL username.
    public required string Username { get; init; }
    /// <summary>Identifies this datasource in metrics. Defaults to endpoint/database.</summary>
    public string? Name { get; init; }
    /// Gets the password used for password-based authentication.
    public string? Password { get; init; }
    /// Gets the database name. When omitted, the username is used.
    public string? Database { get; init; }
    /// <summary>Configures PostgreSQL TLS negotiation and server authentication.</summary>
    public PostgreSqlSslOptions Ssl { get; init; } = new();
    /// <summary>Configures authentication policy. The data source snapshots these values when built.</summary>
    public PostgreSqlAuthenticationOptions Authentication { get; init; } = new();
    /// Gets OAuth bearer-token authentication configuration.
    public PostgreSqlOAuthOptions? OAuth { get; init; }
    /// Gets integrated-security authentication configuration.
    public PostgreSqlIntegratedSecurityOptions? IntegratedSecurity { get; init; }
    /// <summary>Describes differences from PostgreSQL for a compatible wire-protocol backend.</summary>
    public PostgreSqlCompatibilityProfile? CompatibilityProfile { get; init; }
    /// Gets the maximum time allowed for establishing and initializing a connection.
    public TimeSpan ConnectionTimeout { get; init; } = TimeSpan.FromSeconds(10);
    /// <summary>
    /// Bounds cancellation convergence after a command has acquired a PostgreSQL wire. The bound
    /// includes backend-cancellation grace and ends by aborting a wire that cannot reach a safe
    /// ReadyForQuery or idle boundary.
    /// </summary>
    public TimeSpan CancellationTimeout { get; init; } = TimeSpan.FromSeconds(10);
    /// <summary>Delay before the one ambiguous backend-cancellation retry.</summary>
    public TimeSpan CancellationRetryInterval { get; init; } = TimeSpan.FromSeconds(1);
    /// Gets the minimum number of physical connections retained by the pool.
    public int MinPoolSize { get; init; } = 1;
    /// Gets the maximum number of physical connections created by the pool.
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

    /// Gets the synchronous initializer invoked for each newly opened physical connection.
    public Action<SlonConnectionInitializerContext, TimeSpan>? ConnectionInitializer { get; init; }
    /// Gets the asynchronous initializer invoked for each newly opened physical connection.
    public Func<SlonConnectionInitializerContext, CancellationToken, ValueTask>? AsyncConnectionInitializer { get; init; }

    /// <summary>
    /// A command's PendingTimeout initially follows CommandTimeout and bounds datasource admission and
    /// response-order waiting. Once executing, CommandTimeout affects the first IO read after writing
    /// each command.
    /// </summary>
    public TimeSpan CommandTimeout { get; init; } = DefaultCommandTimeout;
    /// Gets the number of matching executions required before automatic preparation.
    public int AutoPreparationMinimumUses { get; init; } = 5;
    /// Gets the maximum number of automatically prepared statements tracked by the datasource.
    public int MaxActiveAutoPreparations { get; init; }
    /// <summary>
    /// DataRows larger than this may cross the decoder boundary before their complete body has arrived.
    /// </summary>
    public int DataRowStreamingThreshold { get; init; } = BackendMessageBatch.Segmenter.DefaultDataRowStreamingThreshold;
    /// <summary>Configures which connection state is reset when an exclusive scope is released.</summary>
    internal PgSessionResetOptions SessionReset { get; init; } = new();
    /// <summary>
    /// Optionally restricts ordinary type loading to <c>pg_catalog</c> and the listed schemas.
    /// Category-U extension types and their canonical array counterparts remain discoverable
    /// across schemas. An empty list leaves catalog loading unrestricted.
    /// </summary>
    public IReadOnlyList<string> TypeLoadingSchemas { get; init; } = [];
    /// <summary>Whether table row types should be loaded as composites.</summary>
    public bool LoadTableComposites { get; init; }

    // Keep the provider override internal while its type-loading contracts settle. Public callers
    // select a supported profile through CompatibilityProfile; automatic backend detection is not
    // reliable for servers that deliberately advertise PostgreSQL identity.
    internal PostgreSqlBackendProvider? BackendProvider { get; init; }
    internal IReadOnlyList<PgTypeCatalogPlugin> TypeCatalogPlugins { get; init; } = [];

    // Internal, tests need to override these to drive maintenance flows on a tight loop. Public
    // surface would require thinking through "what's a sensible knob for end users."
    internal TimeSpan HeartbeatInterval { get; init; } = Heartbeat.DefaultInterval;
    internal TimeSpan MaintenanceInterval { get; init; } = TimeSpan.FromSeconds(1);
    internal TimeProvider TimeProvider { get; init; } = TimeProvider.System;
    internal PipelineScheduler? ExecutionScheduler { get; init; }

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
        TimeProvider = TimeProvider,
        CancellationTimeout = CancellationTimeout,
        CancellationRetryInterval = CancellationRetryInterval,
        SessionReset = SessionReset.Snapshot(),
        DataRowStreamingThreshold = DataRowStreamingThreshold,
        MaxInFlightFlowsPerWire = MaxInFlightOperationsPerWire,
        ExecutionScheduler = ExecutionScheduler,
    };

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(EndPoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(Username);
        ArgumentNullException.ThrowIfNull(Ssl);
        ArgumentNullException.ThrowIfNull(Authentication);
        ArgumentNullException.ThrowIfNull(LoggerFactory);
        ArgumentNullException.ThrowIfNull(TypeLoadingSchemas);
        CompatibilityProfile?.Validate();
        if (Name is not null && string.IsNullOrWhiteSpace(Name))
            throw new ArgumentException("Cannot be empty or whitespace.", nameof(Name));
        if ((ConnectionInitializer is null) != (AsyncConnectionInitializer is null))
            throw new ArgumentException(
                "Synchronous and asynchronous connection initializers must be configured together.");
        if (MaxPoolSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxPoolSize), "MaxPoolSize must be positive.");
        if (MinPoolSize < 0 || MinPoolSize > MaxPoolSize)
            throw new ArgumentOutOfRangeException(nameof(MinPoolSize),
                "MinPoolSize must be between zero and MaxPoolSize.");
        ArgumentOutOfRangeException.ThrowIfNegative(MaxInFlightOperationsPerWire);
        ValidateTimeout(ConnectionTimeout, nameof(ConnectionTimeout));
        ValidateTimeout(CommandTimeout, nameof(CommandTimeout));
        ArgumentOutOfRangeException.ThrowIfNegative(DataRowStreamingThreshold);
        ArgumentOutOfRangeException.ThrowIfNegative(MaxActiveAutoPreparations);
        if (AutoPreparationMinimumUses <= 0)
            throw new ArgumentOutOfRangeException(nameof(AutoPreparationMinimumUses),
                "AutoPreparationMinimumUses must be positive.");
        if (ConnectionIdleLifetime < TimeSpan.Zero && ConnectionIdleLifetime != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(ConnectionIdleLifetime),
                "ConnectionIdleLifetime must be non-negative or Timeout.InfiniteTimeSpan.");
        var pruningEnabled = ConnectionIdleLifetime != Timeout.InfiniteTimeSpan && MinPoolSize < MaxPoolSize;
        if (pruningEnabled && ConnectionPruningInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ConnectionPruningInterval),
                "ConnectionPruningInterval must be positive when pruning is enabled.");
        if (pruningEnabled && ConnectionPruningInterval < HeartbeatInterval)
            throw new ArgumentOutOfRangeException(nameof(ConnectionPruningInterval),
                "ConnectionPruningInterval must be at least HeartbeatInterval when pruning is enabled.");
        if (pruningEnabled && ConnectionIdleLifetime < ConnectionPruningInterval)
            throw new ArgumentOutOfRangeException(nameof(ConnectionIdleLifetime),
                "ConnectionIdleLifetime must be at least ConnectionPruningInterval.");
        if (CancellationTimeout <= TimeSpan.Zero || CancellationTimeout == Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(CancellationTimeout),
                "CancellationTimeout must be finite and positive.");
        if (CancellationRetryInterval <= TimeSpan.Zero
            || CancellationRetryInterval == Timeout.InfiniteTimeSpan
            || CancellationRetryInterval >= CancellationTimeout)
            throw new ArgumentOutOfRangeException(nameof(CancellationRetryInterval),
                "CancellationRetryInterval must be finite, positive, and less than CancellationTimeout.");
        for (var i = 0; i < TypeLoadingSchemas.Count; i++)
            ArgumentException.ThrowIfNullOrWhiteSpace(TypeLoadingSchemas[i], nameof(TypeLoadingSchemas));
        Ssl.Validate();
        OAuth?.Validate();
        IntegratedSecurity?.Validate();

        static void ValidateTimeout(TimeSpan timeout, string parameterName)
        {
            if (timeout < TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
                throw new ArgumentOutOfRangeException(parameterName,
                    "The timeout must be non-negative or Timeout.InfiniteTimeSpan.");
        }
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
        SessionReset = SessionReset.Snapshot(),
        TypeLoadingSchemas = [.. (TypeLoadingSchemas
                                  ?? throw new ArgumentNullException(nameof(TypeLoadingSchemas)))]
    };

    /// <summary>Parses a host and optional port into an IP or DNS endpoint.</summary>
    /// <param name="host">The host, optionally followed by a port.</param>
    /// <returns>The parsed endpoint.</returns>
    public static EndPoint ParseIpOrDnsEndPoint(string host) => PgClientOptions.ParseIpOrDnsEndPoint(host);

    /// Returns a redacted description of these options.
    public override string ToString()
        => $"{nameof(SlonDataSourceOptions)} {{ EndPoint = {EndPoint}, Username = {Username}, " +
           $"Database = {Database}, Password = <redacted> }}";
}
