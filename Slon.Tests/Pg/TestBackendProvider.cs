using Slon.Pg;
using Slon.Pg.Types;

namespace Slon.Tests.Pg;

// Canned startup transcripts intentionally contain only the messages needed by the behavior under
// test. Keep the production PostgreSQL provider strict and make their synthetic backend explicit.
sealed class TestBackendProvider : PgBackendProvider
{
    public static TestBackendProvider Instance { get; } = new();

    TestBackendProvider() { }

    public override PgBackendInfo CreateBackendInfo(IReadOnlyDictionary<string, string> serverParameters)
        => new PgBackendInfoBuilder(
            serverParameters, "synthetic", new Version(0, 0), "UTF8").Build();

    public override PgTypeCatalogFactory CreateTypeCatalogFactory(PgBackendInfo backendInfo)
        => PgTypeCatalogFactory.FromBaseline(PgTypeCatalog.Default);
}
