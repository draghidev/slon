using Slon.Pg.Types;
using Slon.Pg.Serialization;

namespace Slon.Pg;

readonly struct Parameter
{
    PgValueBinding Binding { get; init; }

    public int GetSize() => Binding.Converter is not null
        ? Binding.Size?.Value ?? -1
        : GetLegacySize(ResolvedValueType);
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

    internal static Parameter Create(object? value, PgSerializerOptions options,
        PgConversionContext conversionContext, PgTypeId? pgTypeId = null)
    {
        if (value is IParameter parameter)
        {
            var binder = new ParameterBinder(options, conversionContext, pgTypeId, parameter);
            parameter.ApplyReader(ref binder);
            return binder.Result;
        }

        var typeInfo = options.GetTypeInfo(ResolveValueType(value), pgTypeId);
        return new Parameter
        {
            Value = value,
            PgTypeId = typeInfo.PgTypeId,
            Binding = typeInfo.BindParameterValue(conversionContext, value)
        };
    }

    internal void Write(PgWriter writer)
    {
        if (Binding.Converter is null)
        {
            if (ResolvedValueType == typeof(int))
            {
                writer.WriteInt32((int)Value!);
                return;
            }
            throw new NotSupportedException("Only int parameters are supported without serializer options.");
        }

        if (Binding.IsDbNullBinding)
            return;

        if (Value is IParameter parameter)
        {
            var valueWriter = new ParameterWriter(Binding.Converter, writer);
            parameter.ApplyReader(ref valueWriter);
        }
        else
        {
            Binding.Converter.Write(writer, Value);
        }
    }

    internal ValueTask WriteAsync(PgWriter writer, CancellationToken cancellationToken = default)
    {
        if (Binding.Converter is null)
        {
            Write(writer);
            return default;
        }

        if (Binding.IsDbNullBinding)
            return default;

        if (Value is IParameter parameter)
        {
            var valueWriter = new AsyncParameterWriter(Binding.Converter, writer, cancellationToken);
            parameter.ApplyReader(ref valueWriter);
            return valueWriter.Task;
        }
        return Binding.Converter.WriteAsync(writer, Value, cancellationToken);
    }

    internal object? WriteState => Binding.WriteState;

    internal void Release() => (Binding.WriteState as IDisposable)?.Dispose();

    // Fixed length only for now.
    static int GetLegacySize(Type? type) => type switch
    {
        null => -1,
        _ when type == typeof(int) => sizeof(int),
        _ when type == typeof(DBNull) => -1,
        _ => throw new NotSupportedException()
    };

    static Type? ResolveValueType(object? value) => value is IParameter p
        ? p.StaticValueType is var type && type == typeof(object) ? p.Value?.GetType() : type
        : value?.GetType();

    ref struct ParameterBinder(PgSerializerOptions options, PgConversionContext conversionContext,
        PgTypeId? pgTypeId, IParameter parameter) : IParameterValueReader
    {
        public Parameter Result { get; private set; }

        public void Read<T>(T? value)
        {
            var typeInfo = options.GetTypeInfo(typeof(T), pgTypeId);
            Result = new Parameter
            {
                Value = parameter,
                PgTypeId = typeInfo.PgTypeId,
                Binding = typeInfo.BindParameterValue(conversionContext, value)
            };
        }
    }

    ref struct ParameterWriter(PgConverter converter, PgWriter writer) : IParameterValueReader
    {
        public void Read<T>(T? value) => converter.Write(writer, value);
    }

    ref struct AsyncParameterWriter(PgConverter converter, PgWriter writer,
        CancellationToken cancellationToken) : IParameterValueReader
    {
        public ValueTask Task { get; private set; }
        public void Read<T>(T? value) => Task = converter.WriteAsync(writer, value, cancellationToken);
    }
}
