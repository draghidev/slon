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
        ServerEncoding = builder.ServerEncoding;
        StartupParameters = builder.StartupParameters;
        Capabilities = builder.Capabilities;
    }

    public string ServerVersionString { get; }
    public Version ServerVersion { get; }
    public string ServerEncoding { get; }
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
            SupportsUnlisten = true,
            SupportsCloseAll = IsAtLeast(8, 3),
            SupportsResetAll = true,
            SupportsSessionAuthorization = true,
            SupportsAdvisoryLocks = IsAtLeast(8, 2),
            SupportsListen = true,
            SupportsNotifications = true
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
        ServerEncoding = GetRequired(serverParameters, "server_encoding");
        Capabilities = CreateCapabilities(serverParameters, ServerVersion);
    }

    public PgBackendInfoBuilder(
        IReadOnlyDictionary<string, string> serverParameters,
        string serverVersionString,
        Version serverVersion,
        string serverEncoding)
    {
        ArgumentNullException.ThrowIfNull(serverParameters);
        ArgumentException.ThrowIfNullOrWhiteSpace(serverVersionString);
        ArgumentNullException.ThrowIfNull(serverVersion);
        ArgumentException.ThrowIfNullOrWhiteSpace(serverEncoding);
        StartupParameters = Snapshot(serverParameters);
        ServerVersionString = serverVersionString;
        ServerVersion = serverVersion;
        ServerEncoding = serverEncoding;

        Capabilities = CreateCapabilities(serverParameters, ServerVersion);
    }

    public string ServerVersionString { get; }
    public Version ServerVersion { get; }
    public string ServerEncoding { get; }
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
}

static class PgBackendCompatibility
{
    internal static void ValidateConnectionCompatibility(PgBackendInfo expected, PgBackendInfo actual)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);
        var expectedShape = PgBackendCompatibilityShape.From(expected.Capabilities);
        var actualShape = PgBackendCompatibilityShape.From(actual.Capabilities);
        if (!StringComparer.OrdinalIgnoreCase.Equals(expected.ServerEncoding, actual.ServerEncoding)
            || expectedShape != actualShape)
            throw new InvalidOperationException(
                "The PostgreSQL connection is not compatible with the datasource bootstrap connection. " +
                $"Expected server encoding '{expected.ServerEncoding}' and {expectedShape}; " +
                $"actual server encoding '{actual.ServerEncoding}' and {actualShape}. " +
                "Recycle the data source after a backend compatibility change.");
    }
}
