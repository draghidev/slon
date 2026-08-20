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
    readonly object _source;
    readonly ParameterWriterStrategy? _strategy;

    public ParameterTypeList(ImmutableArray<PgTypeId> typeIds)
    {
        if (!MemoryMarshal.TryGetArray(typeIds.AsMemory(), out var seg) || seg.Array!.Length != seg.Count)
            ThrowHelper.ThrowArgumentException(nameof(typeIds), "Must be backed by an exact sized array.");

        _source = seg.Array!;
        _strategy = null;
    }

    public ParameterTypeList(ImmutableArray<Parameter> parameters)
    {
        if (!MemoryMarshal.TryGetArray(parameters.AsMemory(), out var seg) || seg.Array!.Length != seg.Count)
            ThrowHelper.ThrowArgumentException(nameof(parameters), "Must be backed by an exact sized array.");

        _source = seg.Array!;
        _strategy = null;
    }

    internal ParameterTypeList(object? source, ParameterWriterStrategy strategy)
    {
        _source = source!;
        _strategy = strategy;
    }

    public ushort PgCount => checked((ushort)Count);
    public int Count => _source switch
    {
        null => 0,
        Array array => array.Length,
        var state => _strategy!.GetParameterCount(state)
    };

    public ParameterTypeList Preserve(Func<PgTypeId, Oid>? oidLookup = null)
    {
        // If we already have PgTypeIds, nothing to do.
        if (_source is PgTypeId[])
            return this;

        var array = new PgTypeId[Count];
        var index = 0;
        foreach (var pgTypeId in this)
            array[index++] = pgTypeId.IsDataTypeName && oidLookup is not null ? oidLookup(pgTypeId) : pgTypeId;
        return new(ImmutableCollectionsMarshal.AsImmutableArray(array));
    }

    public static ParameterTypeList Create(ImmutableArray<Parameter> parameters) => new(parameters);

    // Create a NULL filled parameter list, used to make portal describe easy.
    internal ImmutableArray<Parameter> ToDbNullParameterList()
    {
        if (Count is 0)
            return default;

        return ImmutableCollectionsMarshal.AsImmutableArray(new Parameter[Count]);
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

            var index = ++_index;
            switch (_list._source)
            {
                case Parameter[] parameters when index < parameters.Length:
                {
                    var pgTypeId = parameters[index].PgTypeId;
                    _current = pgTypeId.IsDataTypeName && oidLookup is not null ? oidLookup(pgTypeId) : pgTypeId;
                    return true;
                }
                case PgTypeId[] pgTypeIds when index < pgTypeIds.Length:
                {
                    var pgTypeId = pgTypeIds[index];
                    _current = pgTypeId.IsDataTypeName && oidLookup is not null ? oidLookup(pgTypeId) : pgTypeId;
                    return true;
                }
                case not null when _list._strategy is { } strategy && index < strategy.GetParameterCount(_list._source):
                {
                    var pgTypeId = strategy.GetParameterType(_list._source, index);
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

    public bool Equals(ParameterTypeList other)
        => ReferenceEquals(_source, other._source) && ReferenceEquals(_strategy, other._strategy);

    public override bool Equals(object? obj) => obj is ParameterTypeList other && Equals(other);
    public override int GetHashCode() => throw new NotImplementedException();
    public static bool operator ==(ParameterTypeList left, ParameterTypeList right) => left.Equals(right);
    public static bool operator !=(ParameterTypeList left, ParameterTypeList right) => !left.Equals(right);
}
