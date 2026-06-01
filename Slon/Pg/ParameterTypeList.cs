using System.Collections;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Slon.Pg.Types;

namespace Slon.Pg;

// Supports structural equality, for preparation information.
// Discriminated union over prepared and unprepared parameter types.
readonly struct ParameterTypeList : IEquatable<ParameterTypeList>
{
    readonly Array _typesIdsOrParameters;

    public ParameterTypeList(ImmutableArray<PgTypeId> typeIds)
    {
        if (!MemoryMarshal.TryGetArray(typeIds.AsMemory(), out var seg) || seg.Array!.Length != seg.Count)
            ThrowHelper.ThrowArgumentException(nameof(typeIds), "Must be backed by an exact sized array.");

        _typesIdsOrParameters = seg.Array!;
    }

    public ParameterTypeList(ImmutableArray<Parameter> parameters)
    {
        if (!MemoryMarshal.TryGetArray(parameters.AsMemory(), out var seg) || seg.Array!.Length != seg.Count)
            ThrowHelper.ThrowArgumentException(nameof(parameters), "Must be backed by an exact sized array.");

        _typesIdsOrParameters = seg.Array!;
    }

    public ushort PgCount => checked((ushort)Count);
    public int Count => _typesIdsOrParameters?.Length ?? 0;

    public ParameterTypeList Preserve(Func<PgTypeId, Oid>? oidLookup = null)
    {
        // If we already have PgTypeIds, nothing to do.
        if (_typesIdsOrParameters is PgTypeId[])
            return this;

        var builder = ImmutableArray.CreateBuilder<PgTypeId>(Count);
        foreach (var pgTypeId in this)
            builder.Add(pgTypeId.IsDataTypeName && oidLookup is not null ? oidLookup(pgTypeId) : pgTypeId);
        return new(builder.MoveToImmutable());
    }

    public static ParameterTypeList Create(ImmutableArray<Parameter> parameters) => new(parameters);

    // Create a NULL filled parameter list, used to make portal describe easy.
    internal ImmutableArray<Parameter> ToDbNullParameterList()
    {
        if (Count is 0)
            return default;

        var builder = ImmutableArray.CreateBuilder<Parameter>(Count);
        for (var i = 0; i < Count; i++)
            builder.Add(default);
        return builder.MoveToImmutable();
    }

    [UnscopedRef]
    public Enumerator GetEnumerator() => new(in this, null);

    [UnscopedRef]
    public Enumerator GetEnumerator(Func<PgTypeId, Oid> oidLookup) => new(in this, oidLookup);

    public ref struct Enumerator(in ParameterTypeList list, Func<PgTypeId, Oid>? oidLookup) : IEnumerator<PgTypeId>
    {
        readonly ref readonly ParameterTypeList _list = ref list;
        PgTypeId _current;
        int _index = -1;

        public bool MoveNext()
        {
            if (_index is -2)
                return false;

            switch (_list._typesIdsOrParameters)
            {
                case Parameter[] parameters when ++_index < parameters.Length:
                {
                    var pgTypeId = parameters[_index].PgTypeId;
                    _current = pgTypeId.IsDataTypeName && oidLookup is not null ? oidLookup(pgTypeId) : pgTypeId;
                    return true;
                }
                case PgTypeId[] pgTypeIds when ++_index < pgTypeIds.Length:
                {
                    var pgTypeId = pgTypeIds[_index];
                    _current = pgTypeId.IsDataTypeName && oidLookup is not null ? oidLookup(pgTypeId) : pgTypeId;
                    return true;
                }
            }

            _current = default;
            _index = -2;
            return false;
        }

        public PgTypeId Current => _current;

        object IEnumerator.Current => Current;
        void IDisposable.Dispose() {}
        void IEnumerator.Reset() => throw new NotImplementedException();
    }

    public bool OidDeepEquals(ParameterTypeList other, Func<PgTypeId, Oid> oidLookup) => DeepEquals(other, PgTypeIdEquality.Oid, oidLookup);
    public bool DataTypeNameDeepEquals(ParameterTypeList other) => DeepEquals(other, PgTypeIdEquality.DataTypeName, null);
    public bool DeepEquals(ParameterTypeList other, Func<PgTypeId, Oid>? oidLookup = null) => DeepEquals(other, PgTypeIdEquality.Default, oidLookup);

    bool DeepEquals(ParameterTypeList other, PgTypeIdEquality equality, Func<PgTypeId, Oid>? oidLookup)
    {
        if (Equals(other))
            return true;

        if (Count != other.Count)
            return false;

        using var enumerator = GetEnumerator();
        foreach (var value in other)
        {
            var success = enumerator.MoveNext();
            Debug.Assert(success);
            var currentType = enumerator.Current;
            var otherType = value;

            if (oidLookup is not null)
            {
                if (equality is PgTypeIdEquality.Default)
                {
                    if (currentType.IsDataTypeName && !otherType.IsDataTypeName)
                        currentType = oidLookup(currentType);
                    if (otherType.IsDataTypeName && !currentType.IsDataTypeName)
                        otherType = oidLookup(otherType);
                }
                else if (equality is PgTypeIdEquality.Oid)
                {
                    if (currentType.IsDataTypeName)
                        currentType = oidLookup(currentType);
                    if (otherType.IsDataTypeName)
                        otherType = oidLookup(otherType);
                }
            }

            if (!currentType.Equals(otherType, equality))
                return false;
        }

        return true;
    }

    public bool Equals(ParameterTypeList other) => ReferenceEquals(_typesIdsOrParameters, other._typesIdsOrParameters);

    public override bool Equals(object? obj) => obj is ParameterTypeList other && Equals(other);
    public override int GetHashCode() => throw new NotImplementedException();
    public static bool operator ==(ParameterTypeList left, ParameterTypeList right) => left.Equals(right);
    public static bool operator !=(ParameterTypeList left, ParameterTypeList right) => !left.Equals(right);
}
