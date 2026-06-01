using Slon.Pg.Types;

namespace Slon.Pg;

readonly struct Parameter
{
    public int GetSize() => GetSize(ResolvedValueType);
    public PgTypeId PgTypeId { get; private init; }
    public object? Value { get; private init; }

    internal Type? ResolvedValueType => ResolveValueType(Value);

    public static Parameter Create(object? value, PgTypeId pgTypeId) => new() { Value = value, PgTypeId = pgTypeId };
    public static Parameter Create(object? value) => new()
    {
        Value = value,
        PgTypeId = ResolveValueType(value) switch
        {
            var t when t == typeof(int) => DataTypeNames.Int4,
            _ => throw new NotSupportedException("Unknown parameter type.")
        }
    };

    // Fixed length only for now.
    static int GetSize(Type? type) => type switch
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
