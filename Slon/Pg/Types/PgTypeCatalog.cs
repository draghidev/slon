using System.Collections.Concurrent;
using System.Collections.Frozen;

namespace Slon.Pg.Types;

sealed partial class PgTypeCatalog
{
    // Important keys are represented by their underlying primitives for slightly faster lookups.
    readonly FrozenDictionary<string, PgType> _typesByDataTypeName;
    readonly FrozenDictionary<uint, PgType> _typesByOid;
    // This one and future additions for range/multirange serve purely as a lookup cache for inverse relations like element type -> array type etc.
    ConcurrentDictionary<PgTypeId, PgType>? _arrayTypesByElementId;

    public PgTypeCatalog(IEnumerable<PgType> types)
    {
        var typesByOid = new Dictionary<uint, PgType>();
        var typesByDataTypeName = new Dictionary<string, PgType>();
        foreach (var type in types)
        {
            if (type.Oid is not { } oid)
            {
                ThrowHelper.ThrowInvalidOperation("All types passed to a catalog must have an oid.");
                return;
            }

            typesByDataTypeName.Add(type.DataTypeName, type);
            typesByOid.Add((uint)oid, type);
        }
        _typesByDataTypeName = typesByDataTypeName.ToFrozenDictionary();
        _typesByOid = typesByOid.ToFrozenDictionary();
    }

    public IReadOnlyCollection<PgType> Types => _typesByDataTypeName.Values;

    public PgType GetPgType(PgTypeId pgTypeId)
    {
        if (pgTypeId.IsOid)
            return _typesByOid[(uint)pgTypeId.Oid];

        return _typesByDataTypeName[pgTypeId.DataTypeName];
    }

    Oid GetOidCore(PgTypeId pgTypeId, bool validateIfOid = true)
    {
        if (pgTypeId.IsOid)
        {
            if (validateIfOid)
                _ = _typesByOid[(uint)pgTypeId.Oid];
            return pgTypeId.Oid;
        }

        return _typesByDataTypeName[pgTypeId.DataTypeName].Oid.GetValueOrDefault();
    }

    DataTypeName GetDataTypeNameCore(PgTypeId pgTypeId, bool validateIfDataTypeName = true)
    {
        if (pgTypeId.IsDataTypeName)
        {
            if (validateIfDataTypeName)
                _ = _typesByDataTypeName[pgTypeId.DataTypeName];
            return pgTypeId.DataTypeName;
        }

        return _typesByOid[(uint)pgTypeId.Oid].DataTypeName;
    }

    /// Returns whether this type catalog can be used to lookup by oid and whether any returned types are portable by default.
    public bool IsPortable => _typesByOid is null;

    public Oid GetOid(PgTypeId pgTypeId) => GetOidCore(pgTypeId, validateIfOid: true);

    public Oid GetElementOid(PgTypeId arrayTypeId)
    {
        var type = GetPgType(arrayTypeId);
        if (type.Kind is not PgTypeKind.Array)
            throw new InvalidOperationException("Type is not of kind Array.");

        return GetOid(type.ElementType.Oid.GetValueOrDefault());
    }

    public Oid GetArrayOid(PgTypeId elementTypeId)
    {
        if ((_arrayTypesByElementId ??= new()).TryGetValue(elementTypeId, out var cached))
            return GetOidCore(cached.Oid.GetValueOrDefault(), validateIfOid: false);

        // Map it to oid as we have stored non-portable types which must be compared against.
        var elementOid = GetOidCore(elementTypeId, validateIfOid: false);
        foreach (var type in Types)
            if (type.Kind is PgTypeKind.Array && type.ElementType.Oid == elementOid)
            {
                // We need to be able to store both the portable and oid version for the cache to work.
                _arrayTypesByElementId[elementTypeId] = type;
                return elementOid;
            }

        throw new KeyNotFoundException();
    }

    public DataTypeName GetDataTypeName(string name)
    {
        if (!TryGetIdentifiers(name, out _, out var dataTypeName))
            throw new KeyNotFoundException();

        return dataTypeName;
    }

    public DataTypeName GetDataTypeName(PgTypeId pgTypeId) => GetDataTypeNameCore(pgTypeId, validateIfDataTypeName: true);

    public DataTypeName GetElementDataTypeName(PgTypeId arrayTypeId)
    {
        var type = GetPgType(arrayTypeId);
        if (type.Kind is PgTypeKind.Array)
            throw new InvalidOperationException("Type is not a kind of Array.");

        return GetDataTypeName(new PgTypeId(type.ElementType.DataTypeName));
    }

    public DataTypeName GetArrayDataTypeName(PgTypeId elementTypeId)
    {
        if ((_arrayTypesByElementId ??= new()).TryGetValue(elementTypeId, out var cached))
            return cached.DataTypeName;

        var oid = GetOidCore(elementTypeId);
        foreach (var type in Types)
        {
            if (type is { Kind: PgTypeKind.Array, ElementType: { Oid: { } elementOid } elementType } && elementOid == oid)
            {
                _arrayTypesByElementId.TryAdd(elementTypeId, type);
                return elementType.DataTypeName;
            }
        }

        throw new KeyNotFoundException();
    }

    public bool TryGetMultiRangeIdentifiers(string rangeDataTypeName, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
    {
        // TODO
        throw new NotImplementedException();
    }

    public bool TryGetArrayIdentifiers(string elementDataTypeName, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
    {
        // TODO
        throw new NotImplementedException();
    }

    public bool TryGetIdentifiers(string name, out PgTypeId canonicalTypeId, out DataTypeName dataTypeName)
    {
        var hasSchema = name.IndexOf('.') != -1;
        if (hasSchema && _typesByDataTypeName.TryGetValue(name, out var type))
        {
            canonicalTypeId = type.Oid is { } oid ? new PgTypeId(oid) : type.DataTypeName;
            dataTypeName = new DataTypeName(name);
            return true;
        }
        // TODO
        throw new NotImplementedException();
    }
}
