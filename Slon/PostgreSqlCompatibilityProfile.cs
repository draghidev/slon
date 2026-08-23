namespace Slon;

/// <summary>
/// Describes differences between PostgreSQL and a PostgreSQL-wire-compatible backend.
/// </summary>
public sealed record PostgreSqlCompatibilityProfile
{
    /// <summary>
    /// Replaces PostgreSQL's version-derived feature set. A null value retains normal inference.
    /// </summary>
    public PostgreSqlCompatibilityFeatures? Features { get; init; }

    /// <summary>
    /// Whether PostgreSQL catalog queries can be used to discover types. Disable this to use Slon's
    /// built-in type catalog.
    /// </summary>
    public bool LoadTypesFromCatalog { get; init; } = true;

    /// <summary>
    /// Replaces the normal reset sequence when every session-reset action is enabled.
    /// </summary>
    public string? SessionResetCommand { get; init; }

    internal void Validate()
    {
        if (SessionResetCommand is not null)
            ArgumentException.ThrowIfNullOrWhiteSpace(SessionResetCommand);
    }
}

/// <summary>PostgreSQL behaviors supported by a compatibility profile.</summary>
[Flags]
public enum PostgreSqlCompatibilityFeatures
{
    /// No optional PostgreSQL behaviors are assumed.
    None = 0,
    /// Range types are supported.
    RangeTypes = 1 << 0,
    /// Multirange types are supported.
    MultirangeTypes = 1 << 1,
    /// Enum types are supported.
    EnumTypes = 1 << 2,
    /// Enum labels expose PostgreSQL sort order.
    EnumSortOrder = 1 << 3,
    /// Type catalogs expose PostgreSQL type categories.
    TypeCategory = 1 << 4,
    /// Timestamps use PostgreSQL's integer representation.
    IntegerDateTimes = 1 << 5,
    /// <c>DISCARD TEMP</c> is supported.
    DiscardTemp = 1 << 6,
    /// <c>UNLISTEN *</c> is supported.
    Unlisten = 1 << 7,
    /// <c>CLOSE ALL</c> is supported.
    CloseAll = 1 << 8,
    /// <c>RESET ALL</c> is supported.
    ResetAll = 1 << 9,
    /// Resetting session authorization is supported.
    SessionAuthorization = 1 << 10,
    /// PostgreSQL advisory locks are supported.
    AdvisoryLocks = 1 << 11,
    /// <c>LISTEN</c> is supported.
    Listen = 1 << 12,
    /// Asynchronous notification messages are supported.
    Notifications = 1 << 13,
    /// All known PostgreSQL compatibility behaviors are supported.
    All = (1 << 14) - 1
}
