using System.Diagnostics;
using Slon.Pg.Types;

namespace Slon.Pg;

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

    static Type? ResolveValueType(object? value) => value is IParameter p
        ? p.StaticValueType is var type && type == typeof(object) ? p.Value?.GetType() : type
        : value?.GetType();
}
