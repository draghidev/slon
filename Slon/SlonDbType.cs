using System.Data;
using Slon.Pg.Types;

namespace Slon;

/// Provides identifiers for built-in PostgreSQL data types.
public static class SlonDbTypes
{
    /// The PostgreSQL <c>int2</c> type.
    public static SlonDbType Int2 => new(DataTypeNames.Int2);
    /// The PostgreSQL <c>int4</c> type.
    public static SlonDbType Int4 => new(DataTypeNames.Int4);
    /// The PostgreSQL <c>int8</c> type.
    public static SlonDbType Int8 => new(DataTypeNames.Int8);
    /// The PostgreSQL <c>float4</c> type.
    public static SlonDbType Float4 => new(DataTypeNames.Float4);
    /// The PostgreSQL <c>float8</c> type.
    public static SlonDbType Float8 => new(DataTypeNames.Float8);
    /// The PostgreSQL <c>numeric</c> type.
    public static SlonDbType Numeric => new(DataTypeNames.Numeric);
    /// The PostgreSQL <c>money</c> type.
    public static SlonDbType Money => new(DataTypeNames.Money);
    /// The PostgreSQL <c>bool</c> type.
    public static SlonDbType Bool => new(DataTypeNames.Bool);
    /// The PostgreSQL <c>box</c> type.
    public static SlonDbType Box => new(DataTypeNames.Box);
    /// The PostgreSQL <c>circle</c> type.
    public static SlonDbType Circle => new(DataTypeNames.Circle);
    /// The PostgreSQL <c>line</c> type.
    public static SlonDbType Line => new(DataTypeNames.Line);
    /// The PostgreSQL <c>lseg</c> type.
    public static SlonDbType Lseg => new(DataTypeNames.LSeg);
    /// The PostgreSQL <c>path</c> type.
    public static SlonDbType Path => new(DataTypeNames.Path);
    /// The PostgreSQL <c>point</c> type.
    public static SlonDbType Point => new(DataTypeNames.Point);
    /// The PostgreSQL <c>polygon</c> type.
    public static SlonDbType Polygon => new(DataTypeNames.Polygon);
    /// The PostgreSQL <c>bpchar</c> type.
    public static SlonDbType Bpchar => new(DataTypeNames.Bpchar);
    /// The PostgreSQL <c>text</c> type.
    public static SlonDbType Text => new(DataTypeNames.Text);
    /// The PostgreSQL <c>varchar</c> type.
    public static SlonDbType Varchar => new(DataTypeNames.Varchar);
    /// The PostgreSQL <c>name</c> type.
    public static SlonDbType Name => new(DataTypeNames.Name);
    /// The PostgreSQL <c>bytea</c> type.
    public static SlonDbType Bytea => new(DataTypeNames.Bytea);
    /// The PostgreSQL <c>date</c> type.
    public static SlonDbType Date => new(DataTypeNames.Date);
    /// The PostgreSQL <c>time</c> type without time zone.
    public static SlonDbType Time => new(DataTypeNames.Time);
    /// The PostgreSQL <c>timestamp</c> type without time zone.
    public static SlonDbType Timestamp => new(DataTypeNames.Timestamp);
    /// The PostgreSQL <c>timestamp with time zone</c> type.
    public static SlonDbType TimestampTz => new(DataTypeNames.TimestampTz);
    /// The PostgreSQL <c>interval</c> type.
    public static SlonDbType Interval => new(DataTypeNames.Interval);
    /// The PostgreSQL <c>time with time zone</c> type.
    public static SlonDbType TimeTz => new(DataTypeNames.TimeTz);
    /// The PostgreSQL <c>inet</c> type.
    public static SlonDbType Inet => new(DataTypeNames.Inet);
    /// The PostgreSQL <c>cidr</c> type.
    public static SlonDbType Cidr => new(DataTypeNames.Cidr);
    /// The PostgreSQL <c>macaddr</c> type.
    public static SlonDbType MacAddr => new(DataTypeNames.MacAddr);
    /// The PostgreSQL <c>macaddr8</c> type.
    public static SlonDbType MacAddr8 => new(DataTypeNames.MacAddr8);
    /// The PostgreSQL <c>bit</c> type.
    public static SlonDbType Bit => new(DataTypeNames.Bit);
    /// The PostgreSQL <c>varbit</c> type.
    public static SlonDbType Varbit => new(DataTypeNames.Varbit);
    /// The PostgreSQL <c>tsvector</c> type.
    public static SlonDbType TsVector => new(DataTypeNames.TsVector);
    /// The PostgreSQL <c>tsquery</c> type.
    public static SlonDbType TsQuery => new(DataTypeNames.TsQuery);
    /// The PostgreSQL <c>regconfig</c> type.
    public static SlonDbType RegConfig => new(DataTypeNames.RegConfig);
    /// The PostgreSQL <c>uuid</c> type.
    public static SlonDbType Uuid => new(DataTypeNames.Uuid);
    /// The PostgreSQL <c>xml</c> type.
    public static SlonDbType Xml => new(DataTypeNames.Xml);
    /// The PostgreSQL <c>json</c> type.
    public static SlonDbType Json => new(DataTypeNames.Json);
    /// The PostgreSQL <c>jsonb</c> type.
    public static SlonDbType Jsonb => new(DataTypeNames.Jsonb);
    /// The PostgreSQL <c>jsonpath</c> type.
    public static SlonDbType Jsonpath => new(DataTypeNames.Jsonpath);
    /// The PostgreSQL <c>refcursor</c> type.
    public static SlonDbType RefCursor => new(DataTypeNames.RefCursor);
    /// The PostgreSQL <c>oidvector</c> type.
    public static SlonDbType OidVector => new(DataTypeNames.OidVector);
    /// The PostgreSQL <c>int2vector</c> type.
    public static SlonDbType Int2Vector => new(DataTypeNames.Int2Vector);
    /// The PostgreSQL <c>oid</c> type.
    public static SlonDbType Oid => new(DataTypeNames.Oid);
    /// The PostgreSQL <c>xid</c> type.
    public static SlonDbType Xid => new(DataTypeNames.Xid);
    /// The PostgreSQL <c>xid8</c> type.
    public static SlonDbType Xid8 => new(DataTypeNames.Xid8);
    /// The PostgreSQL <c>cid</c> type.
    public static SlonDbType Cid => new(DataTypeNames.Cid);
    /// The PostgreSQL <c>regtype</c> type.
    public static SlonDbType RegType => new(DataTypeNames.RegType);
    /// The PostgreSQL <c>tid</c> type.
    public static SlonDbType Tid => new(DataTypeNames.Tid);
    /// The PostgreSQL <c>pg_lsn</c> type.
    public static SlonDbType PgLsn => new(DataTypeNames.PgLsn);
    /// The PostgreSQL <c>unknown</c> pseudo-type.
    public static SlonDbType Unknown => new(DataTypeNames.Unknown);

    /// The SQL <c>bigint</c> alias for <see cref="Int8"/>.
    public static SlonDbType Bigint => Int8;
    /// The SQL <c>bit varying</c> alias for <see cref="Varbit"/>.
    public static SlonDbType BitVarying => Varbit;
    /// The SQL <c>boolean</c> alias for <see cref="Bool"/>.
    public static SlonDbType Boolean => Bool;
    /// The SQL <c>character</c> alias for <see cref="Bpchar"/>.
    public static SlonDbType Character => Bpchar;
    /// The SQL <c>character varying</c> alias for <see cref="Varchar"/>.
    public static SlonDbType CharacterVarying => Varchar;
    /// The SQL <c>decimal</c> alias for <see cref="Numeric"/>.
    public static SlonDbType Decimal => Numeric;
    /// The SQL <c>double precision</c> alias for <see cref="Float8"/>.
    public static SlonDbType DoublePrecision => Float8;
    /// The SQL <c>integer</c> alias for <see cref="Int4"/>.
    public static SlonDbType Integer => Int4;
    /// The SQL <c>real</c> alias for <see cref="Float4"/>.
    public static SlonDbType Real => Float4;
    /// The SQL <c>smallint</c> alias for <see cref="Int2"/>.
    public static SlonDbType Smallint => Int2;
    /// The SQL <c>time with time zone</c> alias for <see cref="TimeTz"/>.
    public static SlonDbType TimeWithTimeZone => TimeTz;
    /// The SQL <c>time without time zone</c> alias for <see cref="Time"/>.
    public static SlonDbType TimeWithoutTimeZone => Time;
    /// The SQL <c>timestamp with time zone</c> alias for <see cref="TimestampTz"/>.
    public static SlonDbType TimestampWithTimeZone => TimestampTz;
    /// The SQL <c>timestamp without time zone</c> alias for <see cref="Timestamp"/>.
    public static SlonDbType TimestampWithoutTimeZone => Timestamp;

    internal static DbType? ToDbType(SlonDbType slonDbType)
        => slonDbType switch
        {
            { IsInfer: true } => null,
            _ when slonDbType == Int2 => DbType.Int16,
            _ when slonDbType == Int4 => DbType.Int32,
            _ when slonDbType == Int8 => DbType.Int64,
            _ when slonDbType == Float4 => DbType.Single,
            _ when slonDbType == Float8 => DbType.Double,
            _ when slonDbType == Numeric => DbType.Decimal,
            _ when slonDbType == Money => DbType.Currency,
            _ when slonDbType == Bool => DbType.Boolean,
            _ when slonDbType == Text => DbType.String,
            _ when slonDbType == Varchar => DbType.String,
            _ when slonDbType == Bytea => DbType.Binary,
            _ when slonDbType == Date => DbType.Date,
            _ when slonDbType == Time => DbType.Time,
            _ when slonDbType == Timestamp => DbType.DateTime2,
            _ when slonDbType == TimestampTz => DbType.DateTime,
            _ when slonDbType == Uuid => DbType.Guid,
            _ when slonDbType == Xml => DbType.Xml,
            _ => DbType.Object
        };

    internal static SlonDbType ToSlonDbType(DbType dbType)
        => dbType switch
        {
            DbType.AnsiString            => Text,
            DbType.Binary                => Bytea,
            DbType.Byte                  => Int2,
            DbType.SByte                 => Int2,
            DbType.Boolean               => Bool,
            DbType.Currency              => Money,
            DbType.Decimal               => Numeric,
            DbType.VarNumeric            => Numeric,
            DbType.Double                => Float8,
            DbType.Guid                  => Uuid,
            DbType.Int16                 => Int2,
            DbType.Int32                 => Int4,
            DbType.Int64                 => Int8,
            DbType.Single                => Float4,
            DbType.String                => Text,
            DbType.AnsiStringFixedLength => Text,
            DbType.StringFixedLength     => Text,
            DbType.Xml                   => Xml,
            DbType.Date                  => Date,
            DbType.Time                  => Time,
            DbType.DateTime              => TimestampTz,
            DbType.DateTime2             => Timestamp,
            DbType.DateTimeOffset        => TimestampTz,

            DbType.Object                => SlonDbType.Infer,
            DbType.UInt16                => SlonDbType.Infer,
            DbType.UInt32                => SlonDbType.Infer,
            DbType.UInt64                => SlonDbType.Infer,

            _ => throw new ArgumentOutOfRangeException(nameof(dbType), dbType, null)
        };
}

// A potentially invalid or unknown type identifier, used in frontend operations like configuring DbParameter types.
// The DbDataSource this is passed to decides on the validity of the contents.
/// Identifies a requested PostgreSQL data type for an ADO parameter or command.
public readonly record struct SlonDbType
{
    readonly string? _dataTypeName;

    internal SlonDbType(DataTypeName dataTypeName)
        : this((string)dataTypeName)
    {
    }

    internal SlonDbType(string dataTypeName)
    {
        _dataTypeName = dataTypeName;
    }

    internal bool IsInfer => _dataTypeName is null;
    internal string DataTypeName => _dataTypeName ?? throw new InvalidOperationException("DbType does not carry a name.");

    internal bool ResolveArrayType { get; init; }
    internal bool ResolveMultirangeType { get; init; }

    /// <summary>
    /// Maps the current <see cref="Slon.SlonDbType"/> to a value that represents the array type over the current type.
    /// </summary>
    /// <returns>The mapped <see cref="Slon.SlonDbType"/>.</returns>
    public SlonDbType MakeArrayType() => this with { ResolveArrayType = true };

    /// <summary>
    /// Maps the current <see cref="Slon.SlonDbType"/> to a value that represents the multi-range type over the current type.
    /// </summary>
    /// <returns>The mapped <see cref="Slon.SlonDbType"/>.</returns>
    public SlonDbType MakeMultirangeType() => this with { ResolveMultirangeType = true };

    /// <inheritdoc />
    public override string ToString()
    {
        if (IsInfer)
            return @"Case = ""Inference""";

        var dataTypeName = Pg.Types.DataTypeName.CreateFullyQualifiedName(DataTypeName);
        if (ResolveMultirangeType)
            dataTypeName = dataTypeName.ToDefaultMultirangeName();
        return $@"Case = ""DataTypeName"", Value = ""{dataTypeName.DisplayName}{(ResolveArrayType ? "[]" : "")}""";
    }

    /// Infer a database type from the parameter value instead of specifying one.
    public static SlonDbType Infer => default;
    /// <summary>
    /// Create a <see cref="Slon.SlonDbType"/> type from a data type name.
    /// </summary>
    /// <param name="dataTypeName">A fully qualified or unqualified data type name for the type.</param>
    /// <returns>The SlonDbType value.</returns>
    public static SlonDbType Create(string dataTypeName) => new(dataTypeName.Trim());

    /// <summary>
    /// Resolve the <see cref="SlonDbType"/> to a <see cref="DbType"/> value.
    /// </summary>
    /// <returns>The DbType, null when this is a <see cref="Slon.SlonDbType.Infer"/> value.</returns>
    public DbType? ToDbType() => SlonDbTypes.ToDbType(this);

    /// <summary>Converts an ADO database type to its PostgreSQL type request.</summary>
    /// <param name="dbType">The ADO database type.</param>
    public static explicit operator SlonDbType(DbType dbType) => SlonDbTypes.ToSlonDbType(dbType);

    /// <summary>Converts an optional ADO database type to its PostgreSQL type request.</summary>
    /// <param name="dbType">The ADO database type, or null to infer the type.</param>
    public static explicit operator SlonDbType(DbType? dbType) => dbType is null ? Infer : SlonDbTypes.ToSlonDbType(dbType.GetValueOrDefault());
}
