using System.Collections.Concurrent;
using Slon.Pg.Types;

namespace Slon.Pg.Serialization;

sealed partial class PgSerializerOptions
{
    readonly ConcurrentDictionary<(Type Type, PgTypeId TypeId, DataFormat Format), PgTypeInfo>
        _adoFieldTypeInfos = new();

    private static partial bool IsAdoFieldProjection(Type type)
        => type == typeof(CharsColumnLease) || type == typeof(ByteColumnLease);

    internal PgTypeInfo GetAdoFieldTypeInfo(Type type, PgTypeId typeId, DataFormat format)
    {
        var canonicalTypeId = GetCanonicalTypeId(typeId);
        var key = (type, canonicalTypeId, format);
        return _adoFieldTypeInfos.GetOrAdd(key, static (projection, options) =>
        {
            PgConverter converter = projection.Type == typeof(CharsColumnLease)
                ? AdoCharsConverters.Get(options.GetDataTypeName(projection.TypeId),
                    projection.Format)
                : projection.Type == typeof(ByteColumnLease)
                    ? new ByteColumnLeaseConverter()
                    : throw new NotSupportedException(
                        $"Unknown ADO field projection '{projection.Type}'.");
            return new(options, converter, projection.TypeId, readFormat: projection.Format);
        }, this);
    }
}
