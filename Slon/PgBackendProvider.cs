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
