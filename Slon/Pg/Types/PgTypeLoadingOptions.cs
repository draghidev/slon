using System.Collections.Immutable;

namespace Slon.Pg.Types;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed class PgTypeLoadingOptions
{
    public required ImmutableArray<string> Schemas { get; init; }
    public required bool LoadTableComposites { get; init; }
}

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed class PgTypeLoadingOptionsBuilder
{
    readonly List<string> _schemas = [];
    bool _loadTableComposites;

    public void AddTypeLoadingSchema(string schema)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        if (!_schemas.Contains(schema))
            _schemas.Add(schema);
    }

    public void EnableTableCompositesLoading(bool enable = true)
    {
        // Loading requirements compose monotonically: a later participant cannot revoke work
        // requested by the datasource, dialect, or an earlier plugin.
        if (enable)
            _loadTableComposites = true;
    }

    internal PgTypeLoadingOptions Build()
        => new()
        {
            Schemas = [.. _schemas],
            LoadTableComposites = _loadTableComposites
        };
}
