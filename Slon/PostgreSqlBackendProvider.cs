using Slon.Pg;
using Slon.Pg.Types;

namespace Slon;

// Datasource composition seam. The protocol consumes the inherited low-level backend-info
// contract; catalog construction belongs to this higher layer that composes protocol and types.
abstract class PostgreSqlBackendProvider : PgBackendInfoProvider
{
    // Every backend makes the dynamic-vs-prebuilt catalog choice explicitly. Falling back here
    // would let a provider that only customizes identity silently disable type discovery/reload.
    public abstract PgTypeCatalogFactory CreateTypeCatalogFactory(PgBackendInfo backendInfo);

    public virtual IReadOnlyList<PgTypeCatalogPlugin> CreateTypeCatalogPlugins(PgBackendInfo backendInfo)
        => [];
}

sealed class DefaultPostgreSqlBackendProvider : PostgreSqlBackendProvider
{
    public static DefaultPostgreSqlBackendProvider Instance { get; } = new();

    DefaultPostgreSqlBackendProvider() { }

    public override PgBackendInfo CreateBackendInfo(IReadOnlyDictionary<string, string> serverParameters)
        => new PgBackendInfoBuilder(serverParameters).Build();

    public override PgTypeCatalogFactory CreateTypeCatalogFactory(PgBackendInfo backendInfo)
        => PostgreSqlTypeCatalogFactory.Instance;
}

sealed class ConfiguredBackendProvider(PostgreSqlCompatibilityProfile profile) : PostgreSqlBackendProvider
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

    public override string? ResolveSessionResetCommand(
        PgSessionResetOptions options, PgBackendInfo backendInfo)
    {
        if (options.HasAllActionsEnabled && profile.CompleteSessionResetCommand is not null)
            return profile.CompleteSessionResetCommand;

        var command = base.ResolveSessionResetCommand(options, backendInfo);
        if (command is null && options.HasEnabledActions && profile.CompleteSessionResetCommand is not null)
            throw new NotSupportedException(
                "The compatibility profile provides complete scope reset but not the configured partial reset.");
        return command;
    }
}

static class PgBackendProviders
{
    public static PostgreSqlBackendProvider Create(PostgreSqlCompatibilityProfile? profile)
        => profile is null ? DefaultPostgreSqlBackendProvider.Instance : new ConfiguredBackendProvider(profile);
}
