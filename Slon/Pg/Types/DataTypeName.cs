using System.Diagnostics;

namespace Slon.Pg.Types;

/// <summary>
/// Represents the fully-qualified name of a PostgreSQL type.
/// </summary>
[DebuggerDisplay("{DisplayName,nq}")]
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct DataTypeName : IEquatable<DataTypeName>
{
    const char InvalidIdentifier = '-';
    const string InvalidIdentifierString = "-";
    const string UnspecifiedName = InvalidIdentifierString + "." + InvalidIdentifierString;
    /// <summary>
    /// The maximum length of names in an unmodified PostgreSQL installation.
    /// </summary>
    /// <remarks>
    /// We need to respect this to get to valid names when deriving them (for multirange/arrays etc).
    /// This does not include the namespace.
    /// </remarks>
    internal const int NAMEDATALEN = 64 - 1; // Minus null terminator.

    readonly string _value;

    DataTypeName(string fullyQualifiedDataTypeName, bool validated)
    {
        if (!validated)
        {
            var schemaEndIndex = fullyQualifiedDataTypeName.IndexOf('.');
            if (schemaEndIndex is -1 or 0)
                throw new ArgumentException("Given value does not contain a schema.", nameof(fullyQualifiedDataTypeName));

            // Friendly array syntax is the only fully qualified name quirk that's allowed by postgres (see FromDisplayName).
            if (fullyQualifiedDataTypeName.AsSpan(schemaEndIndex).EndsWith("[]".AsSpan()))
                fullyQualifiedDataTypeName = NormalizeName(fullyQualifiedDataTypeName);

            var typeNameLength = fullyQualifiedDataTypeName.Length - (schemaEndIndex + 1);
            if (typeNameLength > NAMEDATALEN)
                throw new ArgumentException(
                    $"Name is too long and would be truncated to: {fullyQualifiedDataTypeName.Substring(0,
                        fullyQualifiedDataTypeName.Length - typeNameLength + NAMEDATALEN)}");
        }

        _value = fullyQualifiedDataTypeName;
    }

    public DataTypeName(string fullyQualifiedDataTypeName)
        : this(fullyQualifiedDataTypeName, validated: false) { }

    internal static DataTypeName ValidatedName(string fullyQualifiedDataTypeName)
        => new(fullyQualifiedDataTypeName, validated: true);

    bool IsUnqualifiedDisplayName => SchemaSpan is "pg_catalog" || IsUnqualified;

    // Includes schema unless it's pg_catalog or the schema marks an unqualified name.
    public string DisplayName =>
        IsUnqualifiedDisplayName
            ? UnqualifiedDisplayName
            : Schema + "." + UnqualifiedDisplayName;

    public string UnqualifiedDisplayName => ToDisplayName(UnqualifiedNameSpan, mapAliases: IsUnqualifiedDisplayName);

    internal ReadOnlySpan<char> SchemaSpan => Value.AsSpan(0, _value.IndexOf('.'));
    public string Schema => Value.Substring(0, _value.IndexOf('.'));
    internal ReadOnlySpan<char> UnqualifiedNameSpan => Value.AsSpan(_value.IndexOf('.') + 1);
    public string UnqualifiedName => Value.Substring(_value.IndexOf('.') + 1);
    public string Value => _value is null ? ThrowDefaultException() : _value;

    static string ThrowDefaultException() =>
        throw new InvalidOperationException($"This operation cannot be performed on a default value of {nameof(DataTypeName)}.");

    public static implicit operator string(DataTypeName value) => value.Value;

    // This contains two invalid sql identifiers (schema and name are both separate identifiers, and would both have to be quoted to be valid).
    // Given this is an invalid name it's fine for us to represent a fully qualified 'unspecified' name with it.
    public static DataTypeName Unspecified => ValidatedName(UnspecifiedName);

    public static string GetUnqualifiedName(string dataTypeName)
        => dataTypeName.IndexOf('.') is not -1 and var index
            ? dataTypeName.Substring(index + 1) : dataTypeName;

    // The invalid-schema prefix represents an explicitly unqualified name. The two-sentinel
    // Unspecified value is a distinct state and is deliberately excluded.
    public bool IsUnqualified => Value.StartsWith(InvalidIdentifier) && Value != UnspecifiedName;

    public bool IsArray => UnqualifiedNameSpan.StartsWith("_".AsSpan(), StringComparison.Ordinal);

    internal static DataTypeName CreateFullyQualifiedName(string dataTypeName)
        => dataTypeName.IndexOf('.') != -1
            ? new(dataTypeName)
            : new(string.Concat(InvalidIdentifierString, ".", dataTypeName));

    // Static transform as defined by https://www.postgresql.org/docs/current/sql-createtype.html#SQL-CREATETYPE-ARRAY
    // We don't have to deal with [] as we're always starting from a normalized fully qualified name.
    public DataTypeName ToArrayName()
    {
        var unqualifiedNameSpan = UnqualifiedNameSpan;
        if (unqualifiedNameSpan.StartsWith("_".AsSpan(), StringComparison.Ordinal))
            return this;

        if (unqualifiedNameSpan.Length + "_".Length > NAMEDATALEN)
            unqualifiedNameSpan = unqualifiedNameSpan.Slice(0, NAMEDATALEN - "_".Length);

        return new(string.Concat(Schema, "._", unqualifiedNameSpan));
    }

    // Static transform as defined by https://www.postgresql.org/docs/current/sql-createtype.html#SQL-CREATETYPE-RANGE
    // Manual testing on PG confirmed it's only the first occurence of 'range' that gets replaced.
    public DataTypeName ToDefaultMultirangeName()
    {
        var nameSpan = UnqualifiedNameSpan;
        if (nameSpan.IndexOf("multirange".AsSpan(), StringComparison.Ordinal) != -1)
            return this;

        var rangeIndex = nameSpan.IndexOf("range", StringComparison.Ordinal);
        if (rangeIndex != -1)
        {
            nameSpan = string.Concat(nameSpan.Slice(0, rangeIndex), "multirange", nameSpan.Slice(rangeIndex + "range".Length));
            if (nameSpan.Length > NAMEDATALEN)
                nameSpan = nameSpan.Slice(0, NAMEDATALEN);

            return new(string.Concat(Schema, ".", nameSpan));
        }

        if (nameSpan.Length > NAMEDATALEN - "_multirange".Length)
            nameSpan = nameSpan.Slice(0, NAMEDATALEN - "_multirange".Length);

        return new(string.Concat(Schema, ".", nameSpan, "_multirange"));
    }

    // Create a DataTypeName from a broader range of valid names.
    // including SQL aliases like 'timestamp without time zone', trailing facet info etc.
    public static DataTypeName FromDisplayName(string displayName)
    {
        var displayNameSpan = displayName.AsSpan().Trim();

        var schemaEndIndex = displayNameSpan.IndexOf('.');
        ReadOnlySpan<char> schemaSpan;
        if (schemaEndIndex is not -1)
        {
            schemaSpan = displayNameSpan.Slice(0, schemaEndIndex);
            displayNameSpan = displayNameSpan.Slice(schemaEndIndex + 1);
        }
        else
            schemaSpan = InvalidIdentifierString;

        var isArray = false;
        if (displayNameSpan.StartsWith("_", StringComparison.Ordinal))
        {
            isArray = true;
            displayNameSpan = displayNameSpan.Slice(1);
        }
        else if (displayNameSpan.EndsWith("[]", StringComparison.Ordinal))
        {
            isArray = true;
            displayNameSpan = displayNameSpan.Slice(0, displayNameSpan.Length - 2);
        }

        if (schemaEndIndex is not -1)
        {
            return !isArray
                ? new(displayName.Length == schemaEndIndex + 1 + displayNameSpan.Length
                    ? displayName
                    : string.Concat(schemaSpan, ".", displayNameSpan))
                : new(string.Concat(schemaSpan, ".", "_", displayNameSpan));
        }

        var parenIndex = displayNameSpan.IndexOf('(');
        if (parenIndex > -1)
            displayNameSpan = displayNameSpan.Slice(0, parenIndex);

        var mapped = displayNameSpan switch
        {
            "boolean" => "bool",
            "character" => "bpchar",
            "decimal" => "numeric",
            "real" => "float4",
            "double precision" => "float8",
            "smallint" => "int2",
            "integer" => "int4",
            "bigint" => "int8",
            "time without time zone" => "time",
            "timestamp without time zone" => "timestamp",
            "time with time zone" => "timetz",
            "timestamp with time zone" => "timestamptz",
            "bit varying" => "varbit",
            "character varying" => "varchar",
            var value => value
        };

        if (DataTypeNames.IsWellKnownUnqualifiedName(mapped))
            schemaSpan = "pg_catalog".AsSpan();

        return new(string.Concat(schemaSpan, ".", isArray ? "_" : "", mapped));
    }

    // The type names stored in a DataTypeName are usually the actual typname from the pg_type column.
    // There are some canonical aliases defined in the SQL standard which we take into account.
    // Additionally array types have a '_' prefix while for readability their element type should be postfixed with '[]'.
    // See the table for all the aliases https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
    // Alternatively some of the source lives at https://github.com/postgres/postgres/blob/c8e1ba736b2b9e8c98d37a5b77c4ed31baf94147/src/backend/utils/adt/format_type.c#L186
    static string ToDisplayName(ReadOnlySpan<char> unqualifiedName, bool mapAliases)
    {
        var isArray = unqualifiedName.IndexOf('_') == 0;
        var baseTypeName = isArray ? unqualifiedName.Slice(1) : unqualifiedName;

        string? mappedBaseType = null;
        if (mapAliases)
        {
            mappedBaseType = baseTypeName switch
            {
                "bool" => "boolean",
                "bpchar" => "character",
                "decimal" => "numeric",
                "float4" => "real",
                "float8" => "double precision",
                "int2" => "smallint",
                "int4" => "integer",
                "int8" => "bigint",
                "time" => "time without time zone",
                "timestamp" => "timestamp without time zone",
                "timetz" => "time with time zone",
                "timestamptz" => "timestamp with time zone",
                "varbit" => "bit varying",
                "varchar" => "character varying",
                _ => null
            };
        }

        return isArray
            ? string.Concat(mappedBaseType ?? baseTypeName, "[]")
            : mappedBaseType ?? baseTypeName.ToString();
    }

    internal static bool IsFullyQualified(ReadOnlySpan<char> dataTypeName) => dataTypeName.Contains(".".AsSpan(), StringComparison.Ordinal);

    internal static string NormalizeName(string dataTypeName)
    {
        var fqName = FromDisplayName(dataTypeName);
        return IsFullyQualified(dataTypeName.AsSpan()) ? fqName.Value : fqName.UnqualifiedName;
    }

    public override string ToString() => Value;
    public bool Equals(DataTypeName other) => string.Equals(_value, other._value);
    public override bool Equals(object? obj) => obj is DataTypeName other && Equals(other);
    public override int GetHashCode() => _value?.GetHashCode() ?? 0;
    public static bool operator ==(DataTypeName left, DataTypeName right) => left.Equals(right);
    public static bool operator !=(DataTypeName left, DataTypeName right) => !left.Equals(right);
}
