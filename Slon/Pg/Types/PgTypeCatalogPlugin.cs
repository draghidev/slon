namespace Slon.Pg.Types;

/// <summary>
/// Contributes loading requirements and deterministic, wire-free changes to a type catalog.
/// </summary>
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public abstract class PgTypeCatalogPlugin
{
    public virtual void Configure(PgTypeLoadingOptionsBuilder options) { }

    public virtual void Apply(PgTypeCatalogBuilder builder) { }
}
