using Slon.Pg.Serialization.Converters;
using Slon.Pg.Types;

namespace Slon.Pg.Serialization;

/// <summary>
/// Datasource-scoped serializer resolution over one immutable PostgreSQL type catalog.
/// </summary>
sealed class PgSerializerOptions
{
    readonly Dictionary<Type, Mapping> _byClrType = new();
    readonly Dictionary<PgTypeId, Mapping> _byPgTypeId = new();

    internal PgSerializerOptions(PgTypeCatalog typeCatalog)
    {
        _typeCatalog = typeCatalog;

        Add<bool>(new BoolConverter(), DataTypeNames.Bool);
        Add<short>(new Int2Converter<short>(), DataTypeNames.Int2);
        Add<int>(new Int4Converter<int>(), DataTypeNames.Int4);
        Add<long>(new Int8Converter<long>(), DataTypeNames.Int8);
        Add<float>(new RealConverter<float>(), DataTypeNames.Float4);
        Add<double>(new DoubleConverter<double>(), DataTypeNames.Float8);
        Add<Guid>(new GuidUuidConverter(), DataTypeNames.Uuid);
        Add<string>(TextConverter.CreateStringConverter(), DataTypeNames.Text);
        Add<TextReader>(TextConverter.CreateTextReaderConverter(), DataTypeNames.Text,
            defaultForPgType: false);
        Add<Stream>(new StreamConverter(), DataTypeNames.Bytea, defaultForPgType: false);
    }

    readonly PgTypeCatalog _typeCatalog;
    internal bool PortableTypeIds => _typeCatalog.IsPortable;

    internal PgTypeId GetCanonicalTypeId(PgTypeId typeId)
        => PortableTypeIds
            ? _typeCatalog.GetDataTypeName(typeId)
            : _typeCatalog.GetOid(typeId);

    internal DataTypeName GetDataTypeName(PgTypeId typeId) => _typeCatalog.GetDataTypeName(typeId);

    public PgTypeInfo GetTypeInfo(Type? type, PgTypeId? pgTypeId = null)
    {
        if (type?.IsEnum is true)
        {
            var underlying = type.GetEnumUnderlyingType();
            if (pgTypeId is null && _byClrType.TryGetValue(underlying, out var enumMapping))
                return enumMapping.Create(this, type);
        }

        Mapping? mapping = null;
        if (type is not null && type != typeof(object))
            _byClrType.TryGetValue(type, out mapping);
        if (mapping is null && pgTypeId is { } id)
            _byPgTypeId.TryGetValue(GetCanonicalTypeId(id), out mapping);
        if (mapping is null)
            throw new NotSupportedException(
                $"No serializer mapping exists for CLR type '{type}' and PostgreSQL type '{pgTypeId}'.");

        if (type is not null && type != typeof(object) && type != mapping.ClrType)
            throw new NotSupportedException(
                $"PostgreSQL type '{mapping.DataTypeName}' maps to CLR type '{mapping.ClrType}', not '{type}'.");
        if (pgTypeId is { } requestedTypeId
            && GetCanonicalTypeId(mapping.DataTypeName) != GetCanonicalTypeId(requestedTypeId))
            throw new NotSupportedException(
                $"CLR type '{type}' does not support PostgreSQL type '{pgTypeId}'.");

        return mapping.Create(this, requestedType: null);
    }

    void Add<T>(PgConverter<T> converter, DataTypeName dataTypeName, bool defaultForPgType = true)
    {
        // Synthetic and deliberately restricted catalogs need not contain every built-in.
        // Resolution advertises only mappings whose PostgreSQL identity exists in this snapshot.
        if (!_typeCatalog.TryGetIdentifiers(dataTypeName.Value, out var canonicalTypeId, out _))
            return;

        var mapping = new Mapping(typeof(T), converter, dataTypeName);
        _byClrType.Add(typeof(T), mapping);
        if (defaultForPgType)
            _byPgTypeId.Add(canonicalTypeId, mapping);
    }

    sealed record Mapping(Type ClrType, PgConverter Converter, DataTypeName DataTypeName)
    {
        public PgTypeInfo Create(PgSerializerOptions options, Type? requestedType)
            => new(options, Converter, DataTypeName, requestedType);
    }
}
