using System.Collections.Frozen;

namespace Slon.Pg.Types;

sealed partial class PgTypeCatalog
{
    // An OID-backed catalog may be shared across physical endpoints only when their relevant
    // catalog OIDs and type shapes are exactly aligned. Physical cluster membership is neither
    // required nor sufficient: replicated or fleet-managed databases may assert that invariant,
    // while portable catalogs defer backend-specific OID materialization.
    readonly FrozenDictionary<string, PgType> _typesByDataTypeName;
    readonly FrozenDictionary<string, PgType?> _typesByUnqualifiedName;
    readonly FrozenDictionary<uint, PgType>? _typesByOid;
    readonly FrozenDictionary<PgTypeId, PgType> _arrayTypesByElementId;
    readonly FrozenDictionary<PgTypeId, PgType> _multirangeTypesByRangeId;

    internal PgTypeCatalog(
        Dictionary<string, PgType> typesByDataTypeName,
        Dictionary<string, PgType?> typesByUnqualifiedName,
        Dictionary<uint, PgType>? typesByOid,
        Dictionary<PgTypeId, PgType> arrayTypesByElementId,
        Dictionary<PgTypeId, PgType> multirangeTypesByRangeId)
    {
        _typesByDataTypeName = typesByDataTypeName.ToFrozenDictionary(StringComparer.Ordinal);
        _typesByUnqualifiedName = typesByUnqualifiedName.ToFrozenDictionary(StringComparer.Ordinal);
        _typesByOid = typesByOid?.ToFrozenDictionary();
        _arrayTypesByElementId = arrayTypesByElementId.ToFrozenDictionary();
        _multirangeTypesByRangeId = multirangeTypesByRangeId.ToFrozenDictionary();
    }

    public IReadOnlyCollection<PgType> Types => _typesByDataTypeName.Values;
    public bool IsPortable => _typesByOid is null;

    public PgType GetPgType(PgTypeId pgTypeId)
        => pgTypeId.IsOid
            ? GetOidCatalog()[unchecked((uint)pgTypeId.Oid)]
            : _typesByDataTypeName[pgTypeId.DataTypeName];

    public Oid GetOid(PgTypeId pgTypeId)
    {
        if (pgTypeId.IsOid)
        {
            _ = GetOidCatalog()[unchecked((uint)pgTypeId.Oid)];
            return pgTypeId.Oid;
        }

        return _typesByDataTypeName[pgTypeId.DataTypeName].Oid
            ?? throw new InvalidOperationException("A portable type catalog cannot resolve PostgreSQL OIDs.");
    }

    public DataTypeName GetDataTypeName(PgTypeId pgTypeId)
        => pgTypeId.IsDataTypeName
            ? _typesByDataTypeName[pgTypeId.DataTypeName].DataTypeName
            : GetOidCatalog()[unchecked((uint)pgTypeId.Oid)].DataTypeName;

    public Oid GetElementOid(PgTypeId arrayTypeId)
    {
        var type = GetPgType(arrayTypeId);
        if (type.Kind is not PgTypeKind.Array)
            throw new InvalidOperationException("Type is not of kind Array.");

        return type.ElementType.Oid
            ?? throw new InvalidOperationException("A portable type catalog cannot resolve PostgreSQL OIDs.");
    }

    public Oid GetArrayOid(PgTypeId elementTypeId)
        => GetInverse(_arrayTypesByElementId, Canonicalize(elementTypeId), "array").Oid
           ?? throw new InvalidOperationException("A portable type catalog cannot resolve PostgreSQL OIDs.");

    public DataTypeName GetDataTypeName(string name)
    {
        if (!TryGetIdentifiers(name, out _, out var dataTypeName))
        {
            if (!DataTypeName.IsFullyQualified(name))
            {
                var normalizedName = DataTypeName.NormalizeName(name);
                var candidates = Types
                    .Where(type => type.DataTypeName.UnqualifiedName == normalizedName)
                    .Select(type => type.DataTypeName.Value)
                    .Order(StringComparer.Ordinal)
                    .ToArray();
                if (candidates.Length > 1)
                    throw new KeyNotFoundException(
                        $"PostgreSQL type name '{name}' is ambiguous; qualify one of: " +
                        string.Join(", ", candidates) + ".");
            }

            throw new KeyNotFoundException($"PostgreSQL type '{name}' was not found in the catalog.");
        }

        return dataTypeName;
    }

    public DataTypeName GetElementDataTypeName(PgTypeId arrayTypeId)
    {
        var type = GetPgType(arrayTypeId);
        if (type.Kind is not PgTypeKind.Array)
            throw new InvalidOperationException("Type is not of kind Array.");

        return type.ElementType.DataTypeName;
    }

    public DataTypeName GetArrayDataTypeName(PgTypeId elementTypeId)
        => GetInverse(_arrayTypesByElementId, Canonicalize(elementTypeId), "array").DataTypeName;

    public bool TryGetMultiRangeIdentifiers(string rangeDataTypeName, out PgTypeId canonicalTypeId,
        out DataTypeName dataTypeName)
        => TryGetRelatedIdentifiers(rangeDataTypeName, _multirangeTypesByRangeId,
            out canonicalTypeId, out dataTypeName);

    public bool TryGetArrayIdentifiers(string elementDataTypeName, out PgTypeId canonicalTypeId,
        out DataTypeName dataTypeName)
        => TryGetRelatedIdentifiers(elementDataTypeName, _arrayTypesByElementId,
            out canonicalTypeId, out dataTypeName);

    public bool TryGetIdentifiers(string name, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
    {
        var normalizedName = DataTypeName.NormalizeName(name);
        PgType? type;
        if (DataTypeName.IsFullyQualified(name))
        {
            type = _typesByDataTypeName.TryGetValue(normalizedName, out var qualified)
                ? qualified
                : null;
        }
        else if (_typesByUnqualifiedName.TryGetValue(normalizedName, out var unqualified))
        {
            type = unqualified;
        }
        else
        {
            type = null;
        }

        if (type is not { } resolved)
        {
            canonicalTypeId = default;
            dataTypeName = default;
            return false;
        }

        canonicalTypeId = resolved.Oid is { } oid ? new PgTypeId(oid) : resolved.DataTypeName;
        dataTypeName = resolved.DataTypeName;
        return true;
    }

    bool TryGetRelatedIdentifiers(string sourceName, FrozenDictionary<PgTypeId, PgType> inverse,
        out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
    {
        if (!TryGetIdentifiers(sourceName, out var sourceId, out _)
            || !inverse.TryGetValue(sourceId, out var related))
        {
            canonicalTypeId = default;
            dataTypeName = default;
            return false;
        }

        canonicalTypeId = related.Oid is { } oid ? new PgTypeId(oid) : related.DataTypeName;
        dataTypeName = related.DataTypeName;
        return true;
    }

    PgTypeId Canonicalize(PgTypeId typeId)
    {
        var type = GetPgType(typeId);
        return type.Oid is { } oid ? new PgTypeId(oid) : type.DataTypeName;
    }

    static PgType GetInverse(FrozenDictionary<PgTypeId, PgType> inverse, PgTypeId source, string kind)
        => inverse.TryGetValue(source, out var related)
            ? related
            : throw new KeyNotFoundException($"The catalog contains no {kind} type for {source}.");

    FrozenDictionary<uint, PgType> GetOidCatalog()
        => _typesByOid
           ?? throw new InvalidOperationException("A portable type catalog cannot resolve PostgreSQL OIDs.");
}
