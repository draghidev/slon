using System.Collections;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Slon.Pg.Types;

namespace Slon.Pg;

// Supports structural equality, for preparation information.
// Discriminated union over prepared and unprepared parameter types.
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct ParameterTypeList : IEquatable<ParameterTypeList>
{
    readonly object _source;
    readonly ParameterWriter? _writer;
    readonly int _count;

    public ParameterTypeList(ImmutableArray<PgTypeId> typeIds)
    {
        _source = ImmutableCollectionsMarshal.AsArray(typeIds)!;
        _writer = null;
        _count = typeIds.Length;
    }

    public ParameterTypeList(ImmutableArray<Parameter> parameters)
    {
        _source = ImmutableCollectionsMarshal.AsArray(parameters)!;
        _writer = null;
        _count = parameters.Length;
    }

    public ParameterTypeList(in ParameterSource source)
    {
        _source = source.State!;
        _writer = source.Writer;
        _count = source.Count;
    }

    internal ushort PgCount => checked((ushort)Count);
    public int Count => _count;

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

    // Create a NULL filled parameter list, used to make portal describe easy.
    internal ParameterSource CreateNullParameters()
    {
        if (Count is 0)
            return default;

        return new(ImmutableCollectionsMarshal.AsImmutableArray(new Parameter[Count]));
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
                    _current = parameters[index].Oid;
                    return true;
                }
                case PgTypeId[] pgTypeIds when index < pgTypeIds.Length:
                {
                    var pgTypeId = pgTypeIds[index];
                    _current = pgTypeId.IsDataTypeName && oidLookup is not null ? oidLookup(pgTypeId) : pgTypeId;
                    return true;
                }
                case not null when _list._writer is { } writer && index < _list._count:
                {
                    var pgTypeId = writer.GetParameterTypeCore(_list._source, index);
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
        => _count == other._count
            && ReferenceEquals(_source, other._source)
            && ReferenceEquals(_writer, other._writer);

    public override bool Equals(object? obj) => obj is ParameterTypeList other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(_source, _writer, _count);
    public static bool operator ==(ParameterTypeList left, ParameterTypeList right) => left.Equals(right);
    public static bool operator !=(ParameterTypeList left, ParameterTypeList right) => !left.Equals(right);
}
