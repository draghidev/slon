namespace Slon.Pg.Types;

sealed class PgTypeCatalogBuilder
{
    readonly Dictionary<string, PgType> _typesByName = new(StringComparer.Ordinal);
    readonly Dictionary<uint, PgType> _typesByOid = [];
    readonly Dictionary<string, PendingType> _pendingByName = new(StringComparer.Ordinal);
    readonly Dictionary<uint, PendingType> _pendingByOid = [];
    bool? _portable;

    public PgTypeCatalogBuilder() { }

    public PgTypeCatalogBuilder(IEnumerable<PgType> baseline)
    {
        ArgumentNullException.ThrowIfNull(baseline);
        foreach (var type in baseline)
            Add(type);
    }

    public PgTypeCatalogBuilder(PgTypeCatalog baseline) : this(baseline.Types) { }

    // Later plugins replace whole entries. OID is authoritative when present; the name indexes
    // follow the surviving entry so a plugin cannot leave two identities for one backend type.
    public void Add(PgType type)
    {
        ValidateIdentity(type.DataTypeName, type.Oid);
        RemoveConflicts(type.DataTypeName, type.Oid);

        _typesByName.Add(type.DataTypeName, type);
        if (type.Oid is { } typeOid)
            _typesByOid.Add(unchecked((uint)typeOid), type);
    }

    public void AddArray(DataTypeName dataTypeName, Oid? oid, PgTypeId elementType)
        => AddPending(new(PgTypeKind.Array, dataTypeName, oid, elementType));

    public void AddDomain(DataTypeName dataTypeName, Oid? oid, PgTypeId underlyingType,
        bool isNotNull = false)
        => AddPending(new(PgTypeKind.Domain, dataTypeName, oid, underlyingType, isNotNull));

    public void AddRange(DataTypeName dataTypeName, Oid? oid, PgTypeId elementType)
        => AddPending(new(PgTypeKind.Range, dataTypeName, oid, elementType));

    public void AddMultirange(DataTypeName dataTypeName, Oid? oid, PgTypeId rangeType)
        => AddPending(new(PgTypeKind.Multirange, dataTypeName, oid, rangeType));

    void AddPending(PendingType type)
    {
        ValidateIdentity(type.DataTypeName, type.Oid);
        if ((type.Oid is null) != type.Dependency.IsDataTypeName)
            throw new InvalidOperationException(
                "A relationship in a portable catalog must use a type name, and an OID-backed catalog must use an OID.");
        RemoveConflicts(type.DataTypeName, type.Oid);

        _pendingByName.Add(type.DataTypeName, type);
        if (type.Oid is { } oid)
            _pendingByOid.Add(unchecked((uint)oid), type);
    }

    public bool Remove(DataTypeName dataTypeName)
    {
        if (_typesByName.Remove(dataTypeName, out var removed))
        {
            if (removed.Oid is { } oid)
                _typesByOid.Remove(unchecked((uint)oid));
            return true;
        }
        if (_pendingByName.Remove(dataTypeName, out var pending))
        {
            if (pending.Oid is { } oid)
                _pendingByOid.Remove(unchecked((uint)oid));
            return true;
        }
        return false;
    }

    public PgTypeCatalog Build()
    {
        // Materialize a snapshot-owned graph. Builder inputs may come from an older/prebuilt
        // catalog and plugins may replace any surviving identity; no relationship in the new
        // snapshot may retain an object (or mutable field link) owned by that input graph.
        var canonicalByName = new Dictionary<string, PgType>(StringComparer.Ordinal);
        var canonicalByOid = new Dictionary<uint, PgType>();
        var pending = new List<PendingType>();

        foreach (var source in _typesByName.Values)
        {
            PgType? canonical = source.Kind switch
            {
                PgTypeKind.Base => PgType.CreateBase(source.DataTypeName, source.Oid),
                PgTypeKind.Pseudo => PgType.CreatePseudo(source.DataTypeName, source.Oid),
                PgTypeKind.Enum => PgType.CreateEnum(source.EnumVariants, source.DataTypeName, source.Oid),
                PgTypeKind.Composite => PgType.CreateComposite(
                    [.. source.CompositeFields.Select(static field => new PgCompositeFieldType(field.Field))],
                    source.DataTypeName, source.Oid),
                _ => null
            };

            if (canonical is { } type)
                AddCanonical(type);
            else
                pending.Add(new(source.Kind, source.DataTypeName, source.Oid, GetDependencyId(source),
                    source.Kind is PgTypeKind.Domain && source.IsDomainNotNull));
        }

        foreach (var source in _pendingByName.Values)
            pending.Add(source);

        while (pending.Count > 0)
        {
            var progressed = false;
            for (var i = pending.Count - 1; i >= 0; i--)
            {
                var source = pending[i];
                if (!TryResolveId(source.Dependency, out var resolved))
                    continue;

                var canonical = source.Kind switch
                {
                    PgTypeKind.Array => PgType.CreateArray(resolved, source.DataTypeName, source.Oid),
                    PgTypeKind.Range => PgType.CreateRange(resolved, source.DataTypeName, source.Oid),
                    PgTypeKind.Multirange => PgType.CreateMultirange(resolved, source.DataTypeName, source.Oid),
                    PgTypeKind.Domain => PgType.CreateDomain(
                        resolved, source.DataTypeName, source.Oid, source.IsDomainNotNull),
                    _ => throw new InvalidOperationException($"Unhandled PostgreSQL type kind {source.Kind}.")
                };
                AddCanonical(canonical);
                pending.RemoveAt(i);
                progressed = true;
            }

            if (!progressed)
            {
                var source = pending[0];
                throw new InvalidOperationException(
                    $"PostgreSQL type '{source.DataTypeName}' refers to unloaded type '{source.Dependency}'.");
            }
        }

        foreach (var type in canonicalByName.Values)
        {
            if (type.Kind is not PgTypeKind.Composite)
                continue;
            foreach (var field in type.CompositeFields)
            {
                if (!TryResolveId(field.Field.PgTypeId, out var resolved))
                    throw new InvalidOperationException(
                        $"PostgreSQL type '{type.DataTypeName}' refers to unloaded type '{field.Field.PgTypeId}'.");
                field.Link(resolved);
            }
        }

        var unqualified = new Dictionary<string, PgType?>(StringComparer.Ordinal);
        var arrays = new Dictionary<PgTypeId, PgType>();
        var multiranges = new Dictionary<PgTypeId, PgType>();

        foreach (var type in canonicalByName.Values)
        {
            var shortName = type.DataTypeName.UnqualifiedName;
            if (!unqualified.TryAdd(shortName, type))
            {
                var current = unqualified[shortName];
                if (type.DataTypeName.SchemaSpan is "pg_catalog")
                    unqualified[shortName] = type;
                else if (current is not { DataTypeName.SchemaSpan: "pg_catalog" })
                    unqualified[shortName] = null;
            }

            switch (type.Kind)
            {
                case PgTypeKind.Array:
                    AddInverse(arrays, type.ElementType, type, "array");
                    break;
                case PgTypeKind.Multirange:
                    AddInverse(multiranges, type.RangeType, type, "multirange");
                    break;
            }
        }

        return new(
            canonicalByName,
            unqualified,
            _portable is true ? null : canonicalByOid,
            arrays,
            multiranges);

        void AddCanonical(PgType type)
        {
            canonicalByName.Add(type.DataTypeName, type);
            if (type.Oid is { } oid)
                canonicalByOid.Add(unchecked((uint)oid), type);
        }

        bool TryResolveId(PgTypeId id, out PgType resolved)
            => id.IsOid
                ? canonicalByOid.TryGetValue(unchecked((uint)id.Oid), out resolved)
                : canonicalByName.TryGetValue(id.DataTypeName, out resolved);

        static void AddInverse(Dictionary<PgTypeId, PgType> inverse, PgType source, PgType related,
            string relationship)
        {
            var sourceId = source.Oid is { } oid ? new PgTypeId(oid) : source.DataTypeName;
            if (!inverse.TryAdd(sourceId, related))
                throw new InvalidOperationException(
                    $"More than one {relationship} type refers to '{source.DataTypeName}'.");
        }
    }

    void ValidateIdentity(DataTypeName dataTypeName, Oid? oid)
    {
        if (dataTypeName.IsUnqualified || dataTypeName == DataTypeName.Unspecified)
            throw new ArgumentException(
                "A PostgreSQL type catalog entry must have a concrete schema-qualified name.",
                nameof(dataTypeName));

        var portable = oid is null;
        if (_portable is { } existing && existing != portable)
            throw new InvalidOperationException("A type catalog cannot mix portable and OID-backed types.");
        _portable ??= portable;
    }

    void RemoveConflicts(DataTypeName dataTypeName, Oid? oid)
    {
        Remove(dataTypeName);
        if (oid is not { } value)
            return;

        var key = unchecked((uint)value);
        if (_typesByOid.TryGetValue(key, out var existing))
            Remove(existing.DataTypeName);
        else if (_pendingByOid.TryGetValue(key, out var pending))
            Remove(pending.DataTypeName);
    }

    static PgTypeId GetDependencyId(PgType type)
    {
        var dependency = type.Kind switch
        {
            PgTypeKind.Array or PgTypeKind.Range => type.ElementType,
            PgTypeKind.Multirange => type.RangeType,
            PgTypeKind.Domain => type.UnderlyingType,
            _ => throw new InvalidOperationException($"Unhandled PostgreSQL type kind {type.Kind}.")
        };
        return dependency.Oid is { } oid ? new PgTypeId(oid) : dependency.DataTypeName;
    }

    readonly record struct PendingType(PgTypeKind Kind, DataTypeName DataTypeName, Oid? Oid,
        PgTypeId Dependency, bool IsDomainNotNull = false);

}
