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
    /// Replaces the normal reset sequence when every scope-reset action is enabled.
    /// </summary>
    public string? CompleteScopeResetCommand { get; init; }

    internal void Validate()
    {
        if (CompleteScopeResetCommand is not null)
            ArgumentException.ThrowIfNullOrWhiteSpace(CompleteScopeResetCommand);
    }
}

/// <summary>PostgreSQL behaviors supported by a compatibility profile.</summary>
[Flags]
public enum PostgreSqlCompatibilityFeatures
{
    None = 0,
    RangeTypes = 1 << 0,
    MultirangeTypes = 1 << 1,
    EnumTypes = 1 << 2,
    EnumSortOrder = 1 << 3,
    TypeCategory = 1 << 4,
    IntegerDateTimes = 1 << 5,
    DiscardTemp = 1 << 6,
    Unlisten = 1 << 7,
    CloseAll = 1 << 8,
    ResetAll = 1 << 9,
    SessionAuthorization = 1 << 10,
    AdvisoryLocks = 1 << 11,
    Listen = 1 << 12,
    Notifications = 1 << 13,
    All = (1 << 14) - 1
}
