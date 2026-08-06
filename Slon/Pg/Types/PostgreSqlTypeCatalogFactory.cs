using System.Collections.Immutable;
using System.Text;
using Slon.Pg.Protocol.Flows;

namespace Slon.Pg.Types;

// Query shape and row parsing form one contract. Dialects customize through capabilities and
// catalog plugins, or replace the complete factory; selectively overriding SQL is not safe.
sealed class PostgreSqlTypeCatalogFactory : PgTypeCatalogFactory
{
    public static PostgreSqlTypeCatalogFactory Instance { get; } = new();

    PostgreSqlTypeCatalogFactory() { }

    protected override void Populate(PgTypeCatalogBuilder builder,
        PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options)
        => PopulateCoreAsync(builder, context, options, async: false, context.StoppingToken)
            .GetAwaiter().GetResult();

    protected override ValueTask PopulateAsync(PgTypeCatalogBuilder builder,
        PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options,
        CancellationToken cancellationToken)
        => PopulateCoreAsync(builder, context, options, async: true, cancellationToken);

    static async ValueTask PopulateCoreAsync(PgTypeCatalogBuilder builder,
        PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options, bool async,
        CancellationToken cancellationToken)
    {
        // Reload may be placed behind arbitrary session work. Keep catalog queries independent of
        // search_path and SQL-literal GUCs; execution-time client_encoding is captured below.
        var command = Command.Create(BuildTypeQuery(options, context.Capabilities)) with
        {
            // OIDs are binary; catalog names and one-byte discriminator columns have the
            // same representation in text and binary, so the stock path stays all-binary.
            ResultFormats = [PgFormat.Binary]
        };
        var enumCommand = Command.Create(BuildEnumQuery(options, context.Capabilities)) with
        {
            ResultFormats = [PgFormat.Binary]
        };
        var compositeCommand = Command.Create(BuildCompositeQuery(options, context.Capabilities)) with
        {
            ResultFormats = [PgFormat.Binary]
        };

        var flow = context.Queue(
            new CommandFlow(async, command, enumCommand, compositeCommand), cancellationToken);
        var records = new List<TypeRecord>();
        var enumLabels = new Dictionary<uint, ImmutableArray<string>.Builder>();
        var compositeFields = new Dictionary<uint, ImmutableArray<PgCompositeFieldType>.Builder>();
        var resultIndex = 0;
        Encoding? textEncoding = null;

        var flowEnumerator = async ? flow.GetAsyncEnumerator() : flow.GetEnumerator();
        try
        {
            while (async ? await flowEnumerator.MoveNextAsync().ConfigureAwait(false) : flowEnumerator.MoveNext())
            {
                // Capture only after this flow reaches execution. Pool admission may pipeline reload
                // behind an earlier command which changes client_encoding; a pre-queue snapshot would
                // then decode this execution with its predecessor's conversion state. PostgreSQL
                // reports changes at the query boundary, so this value remains valid through all
                // three results in this load batch.
                textEncoding ??= context.ClientEncoding;
                var result = flowEnumerator.Current;
                var rows = async ? result.GetAsyncEnumerator(cancellationToken) : result.GetEnumerator();
                try
                {
                    while (async ? await rows.MoveNextAsync().ConfigureAwait(false) : rows.MoveNext())
                    {
                        var row = rows.Current;
                        if (resultIndex is 0)
                        {
                            records.Add(new(
                                row.GetValue<uint>(0),
                                new DataTypeName(string.Concat(row.GetValue<string>(1, textEncoding), ".",
                                    row.GetValue<string>(2, textEncoding))),
                                row.GetValue<string>(3, textEncoding)[0],
                                row.GetValue<bool>(4),
                                row.GetValue<uint>(5),
                                row.GetValue<uint>(6),
                                row.GetValue<uint>(7),
                                row.GetValue<uint>(8)));
                        }
                        else if (resultIndex is 1)
                        {
                            var oid = row.GetValue<uint>(0);
                            if (!enumLabels.TryGetValue(oid, out var labels))
                                enumLabels.Add(oid, labels = ImmutableArray.CreateBuilder<string>());
                            labels.Add(row.GetValue<string>(1, textEncoding));
                        }
                        else
                        {
                            var oid = row.GetValue<uint>(0);
                            if (!compositeFields.TryGetValue(oid, out var fields))
                                compositeFields.Add(oid, fields = ImmutableArray.CreateBuilder<PgCompositeFieldType>());
                            fields.Add(new(new Field(
                                row.GetValue<string>(1, textEncoding),
                                new PgTypeId((Oid)row.GetValue<uint>(2)),
                                row.GetValue<int>(3))));
                        }
                    }
                }
                finally
                {
                    if (async)
                        await rows.DisposeAsync().ConfigureAwait(false);
                    else
                        rows.Dispose();
                }
                resultIndex++;
            }
        }
        finally
        {
            if (async)
                await flowEnumerator.DisposeAsync().ConfigureAwait(false);
            else
                flowEnumerator.Dispose();
        }

        BuildCatalog(builder, records, enumLabels, compositeFields);
    }

    internal static string BuildTypeQuery(PgTypeLoadingOptions options, PgBackendCapabilities capabilities)
    {
        var sql = new StringBuilder(
            "SELECT t.oid, n.nspname::text, t.typname::text, t.typtype::text, t.typnotnull, " +
            "CASE WHEN et.typarray = t.oid THEN t.typelem ELSE 0 END, t.typbasetype, ");
        sql.Append(capabilities.SupportsRangeTypes ? "COALESCE(r.rngsubtype, 0), " : "0, ");
        sql.Append(capabilities.SupportsMultirangeTypes ? "COALESCE(m.rngtypid, 0) " : "0 ");
        sql.Append(
            "FROM pg_catalog.pg_type AS t " +
            "JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace " +
            "LEFT JOIN pg_catalog.pg_type AS et ON et.oid = t.typelem " +
            "LEFT JOIN pg_catalog.pg_class AS c ON c.oid = t.typrelid ");
        if (!options.LoadTableComposites)
            sql.Append("LEFT JOIN pg_catalog.pg_class AS ec ON ec.oid = et.typrelid ");
        if (capabilities.SupportsRangeTypes)
            sql.Append("LEFT JOIN pg_catalog.pg_range AS r ON r.rngtypid = t.oid ");
        if (capabilities.SupportsMultirangeTypes)
            sql.Append("LEFT JOIN pg_catalog.pg_range AS m ON m.rngmultitypid = t.oid ");
        sql.Append(
            "WHERE n.nspname <> 'information_schema' " +
            "AND n.nspname !~ '^pg_toast' AND n.nspname !~ '^pg_temp_' ");

        if (!options.LoadTableComposites)
            sql.Append(
                "AND (t.typtype <> 'c' OR c.relkind = 'c') " +
                "AND (t.typelem = 0 OR et.typtype <> 'c' OR ec.relkind = 'c') ");

        AppendConfiguredSchemas(sql, options,
            includeUserDefinedTypes: capabilities.HasTypeCategory,
            includeArraysOfUserDefinedTypes: capabilities.HasTypeCategory);

        sql.Append("ORDER BY t.oid");
        return sql.ToString();
    }

    internal static string BuildEnumQuery(PgTypeLoadingOptions options, PgBackendCapabilities capabilities)
    {
        if (!capabilities.SupportsEnumTypes)
            return "SELECT 0::oid, ''::text WHERE FALSE";

        var sql = new StringBuilder(
            "SELECT e.enumtypid, e.enumlabel::text " +
            "FROM pg_catalog.pg_enum AS e " +
            "JOIN pg_catalog.pg_type AS t ON t.oid = e.enumtypid " +
            "JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace " +
            "WHERE n.nspname <> 'information_schema' " +
            "AND n.nspname !~ '^pg_toast' AND n.nspname !~ '^pg_temp_' ");
        AppendConfiguredSchemas(sql, options, includeUserDefinedTypes: capabilities.HasTypeCategory);
        sql.Append(capabilities.HasEnumSortOrder
            ? "ORDER BY e.enumtypid, e.enumsortorder"
            : "ORDER BY e.enumtypid, e.oid");
        return sql.ToString();
    }

    internal static string BuildCompositeQuery(PgTypeLoadingOptions options, PgBackendCapabilities capabilities)
    {
        var sql = new StringBuilder(
            "SELECT t.oid, a.attname::text, a.atttypid, a.atttypmod " +
            "FROM pg_catalog.pg_type AS t " +
            "JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace " +
            "JOIN pg_catalog.pg_class AS c ON c.oid = t.typrelid " +
            "JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid " +
            "WHERE t.typtype = 'c' AND a.attnum > 0 AND NOT a.attisdropped " +
            "AND n.nspname <> 'information_schema' " +
            "AND n.nspname !~ '^pg_toast' AND n.nspname !~ '^pg_temp_' ");
        if (!options.LoadTableComposites)
            sql.Append("AND c.relkind = 'c' ");
        AppendConfiguredSchemas(sql, options, includeUserDefinedTypes: capabilities.HasTypeCategory);
        sql.Append("ORDER BY t.oid, a.attnum");
        return sql.ToString();
    }

    static void AppendConfiguredSchemas(StringBuilder sql, PgTypeLoadingOptions options,
        bool includeUserDefinedTypes = false, bool includeArraysOfUserDefinedTypes = false)
    {
        if (options.Schemas.IsDefaultOrEmpty)
            return;

        sql.Append("AND (n.nspname = 'pg_catalog' OR n.nspname IN (");
        for (var i = 0; i < options.Schemas.Length; i++)
        {
            if (i > 0)
                sql.Append(',');
            AppendLiteral(sql, options.Schemas[i]);
        }
        sql.Append(')');
        if (includeUserDefinedTypes)
            sql.Append(" OR t.typcategory = 'U'");
        // PostgreSQL assigns arrays category A rather than inheriting their element's category.
        // Keep the array counterpart of a globally-discovered extension type even when that
        // extension's schema was not explicitly selected.
        if (includeArraysOfUserDefinedTypes)
            sql.Append(" OR (et.typarray = t.oid AND et.typcategory = 'U')");
        sql.Append(") ");
    }

    static void AppendLiteral(StringBuilder builder, string value)
    {
        // E'' plus explicit quote/backslash doubling is independent of the session's
        // standard_conforming_strings setting. Reload may be queued behind a command that changes it.
        builder.Append("E'");
        foreach (var c in value)
        {
            if (c is '\'' or '\\')
                builder.Append(c);
            builder.Append(c);
        }
        builder.Append('\'');
    }

    static void BuildCatalog(PgTypeCatalogBuilder builder, List<TypeRecord> records,
        Dictionary<uint, ImmutableArray<string>.Builder> enumLabels,
        Dictionary<uint, ImmutableArray<PgCompositeFieldType>.Builder> compositeFields)
    {
        // Materialize independent identities immediately. Relationship-bearing identities retain
        // only their dependency OID; the catalog seal resolves that small set without imposing
        // query ordering or PostgreSQL object-creation ordering on the loader.
        foreach (var record in records)
        {
            if (record.ElementOid is not 0)
            {
                builder.AddArray(record.Name, record.Oid, new PgTypeId((Oid)record.ElementOid));
                continue;
            }

            switch (record.Kind)
            {
                case 'b':
                    builder.Add(PgType.CreateBase(record.Name, record.Oid));
                    break;
                case 'p':
                    builder.Add(PgType.CreatePseudo(record.Name, record.Oid));
                    break;
                case 'e':
                    builder.Add(PgType.CreateEnum(
                    enumLabels.TryGetValue(record.Oid, out var labels) ? labels.ToImmutable() : [],
                    record.Name, record.Oid));
                    break;
                case 'c':
                    builder.Add(PgType.CreateComposite(
                    compositeFields.TryGetValue(record.Oid, out var fields) ? fields.ToImmutable() : [],
                    record.Name, record.Oid));
                    break;
                case 'd':
                    builder.AddDomain(record.Name, record.Oid, new PgTypeId((Oid)record.BaseTypeOid),
                        record.IsNotNull);
                    break;
                case 'r':
                    builder.AddRange(record.Name, record.Oid, new PgTypeId((Oid)record.RangeSubtypeOid));
                    break;
                case 'm':
                    builder.AddMultirange(record.Name, record.Oid, new PgTypeId((Oid)record.RangeTypeOid));
                    break;
                default:
                    // Deliberately fail the snapshot rather than silently omit a backend type kind
                    // the driver does not understand. A dialect should shape the stock query so
                    // unsupported rows never enter this PostgreSQL materializer.
                    throw new InvalidOperationException(
                        $"PostgreSQL type '{record.Name}' has unsupported kind '{record.Kind}'.");
            }
        }
    }

    readonly record struct TypeRecord(uint Oid, DataTypeName Name, char Kind, bool IsNotNull,
        uint ElementOid, uint BaseTypeOid, uint RangeSubtypeOid, uint RangeTypeOid)
    { }
}
