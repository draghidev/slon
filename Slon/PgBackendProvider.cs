using Slon.Pg;
using Slon.Pg.Types;

namespace Slon;

// Datasource composition seam. The protocol consumes the inherited low-level backend-info
// contract; catalog construction belongs to this higher layer that composes protocol and types.
abstract class PgBackendProvider : PgBackendInfoProvider
{
    // Every backend makes the dynamic-vs-prebuilt catalog choice explicitly. Falling back here
    // would let a provider that only customizes identity silently disable type discovery/reload.
    public abstract PgTypeCatalogFactory CreateTypeCatalogFactory(PgBackendInfo backendInfo);

    public virtual IReadOnlyList<PgTypeCatalogPlugin> CreateTypeCatalogPlugins(PgBackendInfo backendInfo)
        => [];
}

sealed class PostgreSqlBackendProvider : PgBackendProvider
{
    public static PostgreSqlBackendProvider Instance { get; } = new();

    PostgreSqlBackendProvider() { }

    public override PgBackendInfo CreateBackendInfo(IReadOnlyDictionary<string, string> serverParameters)
        => new PgBackendInfoBuilder(serverParameters).Build();

    public override PgTypeCatalogFactory CreateTypeCatalogFactory(PgBackendInfo backendInfo)
        => PostgreSqlTypeCatalogFactory.Instance;
}

sealed class ConfiguredBackendProvider(PostgreSqlCompatibilityProfile profile) : PgBackendProvider
{
    public override PgBackendInfo CreateBackendInfo(IReadOnlyDictionary<string, string> serverParameters)
    {
        var builder = new PgBackendInfoBuilder(serverParameters);
        if (profile.Features is { } features)
            builder.Capabilities = PgBackendCapabilities.FromCompatibilityFeatures(features);
        return builder.Build();
    }

    public override PgTypeCatalogFactory CreateTypeCatalogFactory(PgBackendInfo backendInfo)
        => profile.LoadTypesFromCatalog
            ? PostgreSqlTypeCatalogFactory.Instance
            : PgTypeCatalogFactory.FromBaseline(PgTypeCatalog.Default);

    public override string? ResolveScopeResetCommand(
        ScopeResetOptions options, PgBackendInfo backendInfo)
    {
        if (options.HasAllActionsEnabled && profile.CompleteScopeResetCommand is not null)
            return profile.CompleteScopeResetCommand;

        var command = base.ResolveScopeResetCommand(options, backendInfo);
        if (command is null && options.HasEnabledActions && profile.CompleteScopeResetCommand is not null)
            throw new NotSupportedException(
                "The compatibility profile provides complete scope reset but not the configured partial reset.");
        return command;
    }
}

static class PgBackendProviders
{
    public static PgBackendProvider Create(PostgreSqlCompatibilityProfile? profile)
        => profile is null ? PostgreSqlBackendProvider.Instance : new ConfiguredBackendProvider(profile);
}
