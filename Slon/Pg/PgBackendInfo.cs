using System.Collections.Immutable;

namespace Slon.Pg;

// Immutable backend identity. Capabilities are a separate value because protocol behavior also
// consumes them directly, without depending on the type catalog or serializer layers.
sealed class PgBackendInfo
{
    internal PgBackendInfo(PgBackendInfoBuilder builder)
    {
        ServerVersionString = builder.ServerVersionString;
        ServerVersion = builder.ServerVersion;
        StartupParameters = builder.StartupParameters;
        Capabilities = builder.Capabilities;
    }

    public string ServerVersionString { get; }
    public Version ServerVersion { get; }
    public IImmutableDictionary<string, string> StartupParameters { get; }
    public PgBackendCapabilities Capabilities { get; }
}

readonly record struct PgBackendCapabilities
{
    // Compatibility defaults for the standalone raw protocol, which has no datasource backend
    // provider. Datasource-owned protocols replace this value from their startup-built BackendInfo.
    internal static PgBackendCapabilities PostgreSqlCompatibility { get; }
        = CreatePostgreSql(new Version(int.MaxValue, 0), hasIntegerDateTimes: true);

    public bool SupportsRangeTypes { get; init; }
    public bool SupportsMultirangeTypes { get; init; }
    public bool SupportsEnumTypes { get; init; }
    public bool HasEnumSortOrder { get; init; }
    public bool HasTypeCategory { get; init; }
    public bool HasIntegerDateTimes { get; init; }
    public bool SupportsDiscardTemp { get; init; }
    public bool SupportsUnlisten { get; init; }
    public bool SupportsCloseAll { get; init; }
    public bool SupportsResetAll { get; init; }
    public bool SupportsSessionAuthorization { get; init; }
    public bool SupportsAdvisoryLocks { get; init; }
    public bool SupportsListen { get; init; }
    public bool SupportsNotifications { get; init; }

    internal static PgBackendCapabilities CreatePostgreSql(Version serverVersion, bool hasIntegerDateTimes)
    {
        bool IsAtLeast(int major, int minor = 0)
            => serverVersion.CompareTo(new Version(major, minor)) >= 0;

        return new()
        {
            SupportsRangeTypes = IsAtLeast(9, 2),
            SupportsMultirangeTypes = IsAtLeast(14),
            SupportsEnumTypes = IsAtLeast(8, 3),
            HasEnumSortOrder = IsAtLeast(9, 1),
            HasTypeCategory = IsAtLeast(8, 4),
            HasIntegerDateTimes = hasIntegerDateTimes,
            SupportsDiscardTemp = IsAtLeast(8, 3),
            SupportsUnlisten = IsAtLeast(6, 4),
            SupportsCloseAll = IsAtLeast(8, 3),
            SupportsResetAll = IsAtLeast(7, 2),
            SupportsSessionAuthorization = IsAtLeast(7, 3),
            SupportsAdvisoryLocks = IsAtLeast(8, 2),
            SupportsListen = true,
            SupportsNotifications = true
        };
    }

    internal static PgBackendCapabilities FromCompatibilityFeatures(
        PostgreSqlCompatibilityFeatures features)
    {
        bool Has(PostgreSqlCompatibilityFeatures feature)
            => (features & feature) != PostgreSqlCompatibilityFeatures.None;

        return new()
        {
            SupportsRangeTypes = Has(PostgreSqlCompatibilityFeatures.RangeTypes),
            SupportsMultirangeTypes = Has(PostgreSqlCompatibilityFeatures.MultirangeTypes),
            SupportsEnumTypes = Has(PostgreSqlCompatibilityFeatures.EnumTypes),
            HasEnumSortOrder = Has(PostgreSqlCompatibilityFeatures.EnumSortOrder),
            HasTypeCategory = Has(PostgreSqlCompatibilityFeatures.TypeCategory),
            HasIntegerDateTimes = Has(PostgreSqlCompatibilityFeatures.IntegerDateTimes),
            SupportsDiscardTemp = Has(PostgreSqlCompatibilityFeatures.DiscardTemp),
            SupportsUnlisten = Has(PostgreSqlCompatibilityFeatures.Unlisten),
            SupportsCloseAll = Has(PostgreSqlCompatibilityFeatures.CloseAll),
            SupportsResetAll = Has(PostgreSqlCompatibilityFeatures.ResetAll),
            SupportsSessionAuthorization = Has(PostgreSqlCompatibilityFeatures.SessionAuthorization),
            SupportsAdvisoryLocks = Has(PostgreSqlCompatibilityFeatures.AdvisoryLocks),
            SupportsListen = Has(PostgreSqlCompatibilityFeatures.Listen),
            SupportsNotifications = Has(PostgreSqlCompatibilityFeatures.Notifications)
        };
    }
}

// Only facts baked into the datasource-wide catalog/serializer snapshot participate in pooled
// compatibility. Reset, notification and other protocol behavior remains connection-local.
readonly record struct PgBackendCompatibilityShape(
    bool SupportsRangeTypes,
    bool SupportsMultirangeTypes,
    bool SupportsEnumTypes,
    bool HasEnumSortOrder,
    bool HasTypeCategory,
    bool HasIntegerDateTimes)
{
    public static PgBackendCompatibilityShape From(PgBackendCapabilities capabilities)
        => new(
            capabilities.SupportsRangeTypes,
            capabilities.SupportsMultirangeTypes,
            capabilities.SupportsEnumTypes,
            capabilities.HasEnumSortOrder,
            capabilities.HasTypeCategory,
            capabilities.HasIntegerDateTimes);
}

sealed class PgBackendInfoBuilder
{
    public PgBackendInfoBuilder(IReadOnlyDictionary<string, string> serverParameters)
    {
        ArgumentNullException.ThrowIfNull(serverParameters);
        StartupParameters = Snapshot(serverParameters);
        ServerVersionString = GetRequired(serverParameters, "server_version");
        ServerVersion = ParseServerVersion(ServerVersionString);
        // server_encoding describes database storage. Wire text follows the client_encoding Slon
        // requests and the server subsequently reports, so storage encoding is neither required
        // nor part of pooled compatibility.
        Capabilities = CreateCapabilities(serverParameters, ServerVersion);
    }

    public PgBackendInfoBuilder(
        IReadOnlyDictionary<string, string> serverParameters,
        string serverVersionString,
        Version serverVersion)
    {
        ArgumentNullException.ThrowIfNull(serverParameters);
        ArgumentException.ThrowIfNullOrWhiteSpace(serverVersionString);
        ArgumentNullException.ThrowIfNull(serverVersion);
        StartupParameters = Snapshot(serverParameters);
        ServerVersionString = serverVersionString;
        ServerVersion = serverVersion;

        Capabilities = CreateCapabilities(serverParameters, ServerVersion);
    }

    public string ServerVersionString { get; }
    public Version ServerVersion { get; }
    public IImmutableDictionary<string, string> StartupParameters { get; }
    public PgBackendCapabilities Capabilities { get; set; }

    public PgBackendInfo Build() => new(this);

    static IImmutableDictionary<string, string> Snapshot(
        IReadOnlyDictionary<string, string> serverParameters)
        => serverParameters is IImmutableDictionary<string, string> immutable
            ? immutable
            : serverParameters.ToImmutableDictionary(StringComparer.Ordinal);

    static PgBackendCapabilities CreateCapabilities(
        IReadOnlyDictionary<string, string> serverParameters, Version serverVersion)
        => PgBackendCapabilities.CreatePostgreSql(serverVersion,
            !serverParameters.TryGetValue("integer_datetimes", out var integerDateTimes)
            || integerDateTimes is "on");

    static string GetRequired(IReadOnlyDictionary<string, string> parameters, string name)
        => parameters.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException(
                $"PostgreSQL did not report the required startup parameter '{name}'.");

    static Version ParseServerVersion(string value)
    {
        var span = value.AsSpan().TrimStart();
        var length = 0;
        while (length < span.Length && (char.IsAsciiDigit(span[length]) || span[length] is '.'))
            length++;
        span = span[..length];
        if (span.IsEmpty)
            throw new FormatException($"PostgreSQL reported an invalid server_version value: '{value}'.");

        Version? version;
        if (span.Contains('.'))
        {
            if (!Version.TryParse(span, out version))
                throw InvalidServerVersion(value);
        }
        else if (int.TryParse(span, out var major))
        {
            version = new Version(major, 0);
        }
        else
        {
            throw InvalidServerVersion(value);
        }

        return version;

        static FormatException InvalidServerVersion(string value)
            => new($"PostgreSQL reported an invalid server_version value: '{value}'.");
    }
}

// Low-level backend negotiation seam. This class belongs with the protocol-facing backend
// identity rather than datasource/type-catalog composition, so it can become a stable package
// boundary independently. New optional behavior should be added as virtual methods with defaults.
abstract class PgBackendInfoProvider
{
    public abstract PgBackendInfo CreateBackendInfo(
        IReadOnlyDictionary<string, string> serverParameters);

    public virtual void ValidateConnectionCompatibility(PgBackendInfo expected, PgBackendInfo actual)
        => PgBackendCompatibility.ValidateConnectionCompatibility(expected, actual);

    public virtual string? ResolveSessionResetCommand(
        PgSessionResetOptions options, PgBackendInfo backendInfo)
        => options.ResolveCommand(backendInfo.Capabilities);
}

static class PgBackendCompatibility
{
    internal static void ValidateConnectionCompatibility(PgBackendInfo expected, PgBackendInfo actual)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);
        var expectedShape = PgBackendCompatibilityShape.From(expected.Capabilities);
        var actualShape = PgBackendCompatibilityShape.From(actual.Capabilities);
        if (expectedShape != actualShape)
            throw new InvalidOperationException(
                "The PostgreSQL connection is not compatible with the datasource bootstrap connection. " +
                $"Expected {expectedShape}; actual {actualShape}. " +
                "Recycle the data source after a backend compatibility change.");
    }
}
