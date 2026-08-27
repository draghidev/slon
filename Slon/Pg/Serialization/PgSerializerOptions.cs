using System.Runtime.CompilerServices;
using Slon.Pg.Serialization.Converters;
using Slon.Pg.Types;

namespace Slon.Pg.Serialization;

/// <summary>
/// Datasource-scoped serializer resolution over one immutable PostgreSQL type catalog.
/// </summary>
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed partial class PgSerializerOptions
{
    readonly Dictionary<Type, Mapping> _byClrType = new();
    readonly Dictionary<PgTypeId, Mapping> _byPgTypeId = new();
    readonly ConditionalWeakTable<RowDescription, PgSerializerReadCache> _readCaches = new();

    public PgSerializerOptions(PgTypeCatalog typeCatalog)
    {
        _typeCatalog = typeCatalog;

        AddBuiltInMapping<bool>(new BoolConverter(), DataTypeNames.Bool);
        AddBuiltInMapping<short>(new Int2Converter<short>(), DataTypeNames.Int2);
        AddBuiltInMapping<int>(new Int4Converter<int>(), DataTypeNames.Int4);
        AddBuiltInMapping<long>(new Int8Converter<long>(), DataTypeNames.Int8);
        AddBuiltInMapping<float>(new RealConverter<float>(), DataTypeNames.Float4);
        AddBuiltInMapping<double>(new DoubleConverter<double>(), DataTypeNames.Float8);
        AddBuiltInMapping<Guid>(new GuidUuidConverter(), DataTypeNames.Uuid);
        var stringConverter = TextConverter.CreateStringConverter();
        AddBuiltInMapping<string>(stringConverter, DataTypeNames.Text);
        AddBuiltInMapping<string>(stringConverter, DataTypeNames.Varchar, defaultForClrType: false);
        AddBuiltInMapping<string>(stringConverter, DataTypeNames.Bpchar, defaultForClrType: false);
        AddBuiltInMapping<string>(stringConverter, DataTypeNames.Name, defaultForClrType: false);
        AddBuiltInMapping<TextReader>(TextConverter.CreateTextReaderConverter(), DataTypeNames.Text,
            defaultForPgType: false);
        AddBuiltInMapping<Stream>(new StreamConverter(), DataTypeNames.Bytea, defaultForPgType: false);
    }

    readonly PgTypeCatalog _typeCatalog;
    public PgTypeCatalog TypeCatalog => _typeCatalog;
    internal PgConversionContext ConversionContext { get; } = PgConversionContext.Empty;
    internal bool PortableTypeIds => _typeCatalog.IsPortable;

    internal PgTypeId GetCanonicalTypeId(PgTypeId typeId)
        => PortableTypeIds
            ? _typeCatalog.GetDataTypeName(typeId)
            : _typeCatalog.GetOid(typeId);

    internal DataTypeName GetDataTypeName(PgTypeId typeId) => _typeCatalog.GetDataTypeName(typeId);

    internal PgSerializerReadCache GetReadCache(RowDescription rowDescription)
        => _readCaches.GetValue(rowDescription, static _ => new());

    public PgTypeInfo GetTypeInfo(Type? type, PgTypeId? pgTypeId = null,
        DataFormat? fieldFormat = null)
    {
        if (type is not null && pgTypeId is { } adoTypeId && fieldFormat is { } adoFormat
            && IsAdoFieldProjection(type))
            return GetAdoFieldTypeInfo(type, adoTypeId, adoFormat);

        Mapping? mapping = null;
        if (type?.IsEnum is true)
        {
            var underlying = type.GetEnumUnderlyingType();
            if (_byClrType.TryGetValue(underlying, out var enumMapping)
                && (pgTypeId is null || Matches(enumMapping, pgTypeId.Value)))
                return enumMapping.Create(this, type);
        }

        if (type is not null && type != typeof(object))
        {
            _byClrType.TryGetValue(type, out mapping);
            if (mapping is null && typeof(Stream).IsAssignableFrom(type))
                _byClrType.TryGetValue(typeof(Stream), out mapping);
            if (mapping is null && typeof(TextReader).IsAssignableFrom(type))
                _byClrType.TryGetValue(typeof(TextReader), out mapping);
            if (mapping is not null && pgTypeId is { } specifiedTypeId
                && !Matches(mapping, specifiedTypeId))
                mapping = null;
        }
        if (mapping is null && pgTypeId is { } id)
            _byPgTypeId.TryGetValue(GetCanonicalTypeId(id), out mapping);
        if (mapping is null)
            throw new NotSupportedException(
                $"No serializer mapping exists for CLR type '{type}' and PostgreSQL type '{pgTypeId}'.");

        if (type is not null && type != typeof(object) && type != mapping.ClrType
            && !mapping.ClrType.IsAssignableFrom(type))
            throw new NotSupportedException(
                $"PostgreSQL type '{mapping.DataTypeName}' maps to CLR type '{mapping.ClrType}', not '{type}'.");
        if (pgTypeId is { } requestedTypeId
            && GetCanonicalTypeId(mapping.DataTypeName) != GetCanonicalTypeId(requestedTypeId))
            throw new NotSupportedException(
                $"CLR type '{type}' does not support PostgreSQL type '{pgTypeId}'.");

        return mapping.Create(this, type?.IsEnum is true ? type : null);

        bool Matches(Mapping candidate, PgTypeId requestedTypeId)
            => GetCanonicalTypeId(candidate.DataTypeName) == GetCanonicalTypeId(requestedTypeId);
    }

    private static partial bool IsAdoFieldProjection(Type type);

    public void AddMapping<T>(PgConverter<T> converter, DataTypeName dataTypeName,
        bool defaultForPgType = true,
        bool defaultForClrType = true)
    {
        ArgumentNullException.ThrowIfNull(converter);
        if (!AddMappingCore(converter, dataTypeName, defaultForPgType, defaultForClrType))
            throw new ArgumentException(
                $"PostgreSQL type '{dataTypeName}' does not exist in this serializer's type catalog.",
                nameof(dataTypeName));
    }

    void AddBuiltInMapping<T>(PgConverter<T> converter, DataTypeName dataTypeName,
        bool defaultForPgType = true, bool defaultForClrType = true)
        => AddMappingCore(converter, dataTypeName, defaultForPgType, defaultForClrType);

    bool AddMappingCore<T>(PgConverter<T> converter, DataTypeName dataTypeName,
        bool defaultForPgType, bool defaultForClrType)
    {
        // Synthetic and deliberately restricted catalogs need not contain every built-in.
        // Resolution advertises only mappings whose PostgreSQL identity exists in this snapshot.
        if (!_typeCatalog.TryGetIdentifiers(dataTypeName.Value, out var canonicalTypeId, out _))
            return false;

        var mapping = new Mapping(this, typeof(T), converter, dataTypeName);
        if (defaultForClrType)
            _byClrType.Add(typeof(T), mapping);
        if (defaultForPgType)
            _byPgTypeId.Add(canonicalTypeId, mapping);
        return true;
    }

    sealed class Mapping
    {
        readonly PgTypeInfo _defaultTypeInfo;

        internal Mapping(PgSerializerOptions options, Type clrType, PgConverter converter,
            DataTypeName dataTypeName)
        {
            ClrType = clrType;
            Converter = converter;
            DataTypeName = dataTypeName;
            _defaultTypeInfo = new(options, converter, dataTypeName);
        }

        internal Type ClrType { get; }
        internal PgConverter Converter { get; }
        internal DataTypeName DataTypeName { get; }

        public PgTypeInfo Create(PgSerializerOptions options, Type? requestedType)
            => requestedType is null ? _defaultTypeInfo : new(options, Converter, DataTypeName, requestedType);
    }

}
