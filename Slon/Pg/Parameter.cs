using System.Buffers;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Slon.Pg.Types;

namespace Slon.Pg;

readonly struct ParameterSource
{
    readonly object? _parametersOrState;

    public ParameterSource(object strategyState)
    {
        ArgumentNullException.ThrowIfNull(strategyState);
        if (strategyState is Parameter[])
            ThrowHelper.ThrowArgumentException(nameof(strategyState),
                $"A {nameof(Parameter)} array must be supplied as an {nameof(ImmutableArray<Parameter>)}.");
        _parametersOrState = strategyState;
    }

    public ParameterSource(ImmutableArray<Parameter> parameters)
    {
        if (parameters.IsDefaultOrEmpty)
            return;
        if (!MemoryMarshal.TryGetArray(parameters.AsMemory(), out var segment)
            || segment.Array!.Length != segment.Count)
            ThrowHelper.ThrowArgumentException(nameof(parameters), "Must be backed by an exact sized array.");
        _parametersOrState = segment.Array!;
    }

    public static implicit operator ParameterSource(ImmutableArray<Parameter> parameters) => new(parameters);

    public int GetCount(ParameterWriterStrategy strategy) => _parametersOrState switch
    {
        null => 0,
        Parameter[] parameters => parameters.Length,
        var state => strategy.GetParameterCount(state)
    };

    internal ParameterTypeList GetParameterTypes(ParameterWriterStrategy strategy)
        => _parametersOrState is Parameter[] parameters
            ? new(ImmutableCollectionsMarshal.AsImmutableArray(parameters))
            : new(_parametersOrState, strategy);

    internal ParameterBufferLease Materialize(ParameterWriterStrategy strategy)
    {
        if (_parametersOrState is null)
            return default;
        if (_parametersOrState is Parameter[] parameters)
            return new(parameters, parameters.Length, rented: false);

        var count = strategy.GetParameterCount(_parametersOrState);
        var rented = ArrayPool<Parameter>.Shared.Rent(count);
        try
        {
            strategy.Materialize(_parametersOrState, rented.AsSpan(0, count));
            return new(rented, count, rented: true);
        }
        catch
        {
            ArrayPool<Parameter>.Shared.Return(rented, clearArray: true);
            throw;
        }
    }
}

readonly struct ParameterBuffer(Parameter[]? array, int count)
{
    public int Length => count;
    public ref Parameter this[int index] => ref array![index];
    public Span<Parameter> Span => array.AsSpan(0, count);
}

struct ParameterBufferLease(Parameter[] array, int count, bool rented) : IDisposable
{
    Parameter[]? _array = array;

    public readonly ParameterBuffer Buffer => new(_array, count);

    public void Dispose()
    {
        var array = _array;
        if (array is null)
            return;
        _array = null;

        foreach (ref readonly var parameter in array.AsSpan(0, count))
            parameter.Release();
        if (rented)
            ArrayPool<Parameter>.Shared.Return(array, clearArray: true);
    }
}

// A transient protocol value. A strategy-backed value carries opaque type resolution until the
// writer binds it against the live wire context and fills its size and write state.
readonly struct Parameter
{
    const int Unbound = int.MinValue;
    readonly int _sizePlusOne;

    public PgTypeId PgTypeId { get; }
    public object? Value { get; }
    // Opaque type-resolution state interpreted by ParameterWriterStrategy.
    internal object? TypeResolution { get; }
    internal object? WriteState { get; }

    Parameter(object? value, PgTypeId pgTypeId, int size, object? typeResolution, object? writeState)
    {
        Value = value;
        PgTypeId = pgTypeId;
        _sizePlusOne = size is Unbound ? Unbound : checked(size + 1);
        TypeResolution = typeResolution;
        WriteState = writeState;
    }

    public static Parameter Create(object? value, PgTypeId pgTypeId)
        => new(value, pgTypeId, GetRawSize(ResolveValueType(value)), typeResolution: null, writeState: null);

    public static Parameter Create(object? value)
    {
        var type = ResolveValueType(value);
        var pgTypeId = type == typeof(int)
            ? new PgTypeId(DataTypeNames.Int4)
            : throw new NotSupportedException("Unknown parameter type.");
        return new(value, pgTypeId, GetRawSize(type), typeResolution: null, writeState: null);
    }

    internal static Parameter CreateUnbound(object? value, PgTypeId pgTypeId, object typeResolution)
        => new(value, pgTypeId, Unbound, typeResolution, writeState: null);

    internal Parameter WithBinding(int size, object? writeState)
        => new(Value, PgTypeId, size, TypeResolution, writeState);

    public int GetSize()
    {
        Debug.Assert(!RequiresBinding);
        return _sizePlusOne - 1;
    }

    internal bool RequiresBinding => _sizePlusOne is Unbound;

    internal Type? ResolvedValueType => ResolveValueType(Value);

    internal void Release() => (WriteState as IDisposable)?.Dispose();

    static int GetRawSize(Type? type) => type switch
    {
        null => -1,
        _ when type == typeof(int) => sizeof(int),
        _ when type == typeof(DBNull) => -1,
        _ => throw new NotSupportedException()
    };

    static Type? ResolveValueType(object? value) => value?.GetType();
}
