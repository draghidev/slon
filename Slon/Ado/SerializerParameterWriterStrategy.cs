using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon;

// Composition adapter between Slon's parameter container and the serializer substrate. It lives
// above both layers deliberately: PgWriter and PgSerializerOptions never depend on Parameter.
sealed class SerializerParameterWriterStrategy : ParameterWriterStrategy
{
    public static SerializerParameterWriterStrategy Instance { get; } = new();

    SerializerParameterWriterStrategy() { }

    internal static ParameterTypeResolution ResolveTypeInfo(object? value, PgSerializerOptions options,
        PgTypeId? preparedTypeId = null, bool allowUnspecified = false)
    {
        if (value is SlonDbParameter parameter)
        {
            var (dbType, valueType) = parameter.GetResolutionInput();
            if (allowUnspecified && dbType.IsInfer
                && (valueType is null || valueType == typeof(DBNull)))
                return default;

            PgTypeId? parameterTypeId = null;
            if (!dbType.IsInfer)
            {
                var dataTypeName = DataTypeName.CreateFullyQualifiedName(dbType.DataTypeName);
                if (dbType.ResolveMultirangeType)
                    dataTypeName = dataTypeName.ToDefaultMultirangeName();
                if (dbType.ResolveArrayType)
                    dataTypeName = dataTypeName.ToArrayName();
                parameterTypeId = dataTypeName;
            }

            var typeInfo = options.GetTypeInfo(valueType, parameterTypeId ?? preparedTypeId);
            if (parameterTypeId is not null && preparedTypeId is { } expectedTypeId
                && options.GetCanonicalTypeId(typeInfo.PgTypeId)
                    != options.GetCanonicalTypeId(expectedTypeId))
            {
                throw new InvalidOperationException(
                    $"Parameter type '{typeInfo.PgTypeId}' does not match prepared type '{expectedTypeId}'.");
            }
            return new(typeInfo);
        }

        if (allowUnspecified && value is null or DBNull)
            return default;

        if (value is IParameter protocolParameter)
        {
            var valueType = protocolParameter.StaticValueType;
            if (valueType == typeof(object))
                valueType = protocolParameter.Value?.GetType();
            return new(options.GetTypeInfo(valueType, preparedTypeId));
        }

        return new(options.GetTypeInfo(value?.GetType(), preparedTypeId));
    }

    public override object CreateState(IOutputWriter output, Encoding textEncoding)
        => new PgWriter(output, new() { TextEncoding = textEncoding });

    public override int GetParameterCount(object source) => ((SlonParameters)source).Count;
    public override PgTypeId GetParameterType(object source, int index)
        => ((SlonParameters)source).GetResolvedParameterType(index);

    public override void Materialize(object source, Span<Parameter> destination)
    {
        var parameters = (SlonParameters)source;
        if (parameters.Count != destination.Length)
            ThrowHelper.ThrowInvalidOperation("The parameter source changed during execution.");
        for (var i = 0; i < destination.Length; i++)
            destination[i] = parameters.CreateResolvedParameter(i);
    }

    public override Parameter Bind(object state, int parameterIndex, in Parameter parameter)
    {
        if (!parameter.RequiresBinding)
            return parameter;

        var typeInfo = (PgTypeInfo)parameter.TypeResolution!;
        var conversionContext = ((PgWriter)state).ConversionContext;
        if (parameter.Value is SlonDbParameter value)
        {
            var binder = new ParameterBinder(typeInfo, conversionContext, value);
            value.Bind(ref binder);
            return binder.Result;
        }

        var binding = typeInfo.BindParameterValue(conversionContext, parameter.Value);
        return parameter.WithBinding(binding.Size?.Value ?? -1, binding.WriteState);
    }

    public override void Write(object state, int parameterIndex, in Parameter parameter)
    {
        var writer = (PgWriter)state;
        var converter = ((PgTypeInfo)parameter.TypeResolution!).Converter;
        var size = parameter.GetSize();
        writer.Init(writer.ConversionContext, FlushMode.Blocking, parameter.WriteState);
        try
        {
            if (parameter.Value is SlonDbParameter value)
            {
                var valueWriter = new ParameterWriter(converter, writer);
                value.Write(ref valueWriter);
            }
            else
            {
                converter.Write(writer, parameter.Value);
            }
            writer.EndWrite(size);
        }
        catch
        {
            writer.AbortWrite();
            throw;
        }
    }

    public override async ValueTask WriteAsync(object state, int parameterIndex, Parameter parameter,
        CancellationToken cancellationToken = default)
    {
        var writer = (PgWriter)state;
        var converter = ((PgTypeInfo)parameter.TypeResolution!).Converter;
        var size = parameter.GetSize();
        writer.Init(writer.ConversionContext, FlushMode.NonBlocking, parameter.WriteState);
        try
        {
            if (parameter.Value is SlonDbParameter value)
            {
                var valueWriter = new AsyncParameterWriter(converter, writer, cancellationToken);
                value.WriteAsync(ref valueWriter);
                await valueWriter.Task.ConfigureAwait(false);
            }
            else
            {
                await converter.WriteAsync(writer, parameter.Value, cancellationToken).ConfigureAwait(false);
            }
            writer.EndWrite(size);
        }
        catch
        {
            writer.AbortWrite();
            throw;
        }
    }

    internal ref struct ParameterBinder(PgTypeInfo typeInfo, PgConversionContext conversionContext,
        SlonDbParameter parameter)
    {
        public Parameter Result { get; private set; }

        public void Bind<T>(T? value)
        {
            var binding = typeInfo.BindParameterValue(conversionContext, value);
            Result = Parameter.CreateUnbound(parameter, typeInfo.PgTypeId, typeInfo)
                .WithBinding(binding.Size?.Value ?? -1, binding.WriteState);
        }
    }

    internal ref struct ParameterWriter(PgConverter converter, PgWriter writer)
    {
        public void Write<T>(T? value) => converter.Write(writer, value);
    }

    internal ref struct AsyncParameterWriter(PgConverter converter, PgWriter writer,
        CancellationToken cancellationToken)
    {
        public ValueTask Task { get; private set; }
        public void Write<T>(T? value) => Task = converter.WriteAsync(writer, value, cancellationToken);
    }
}

readonly struct ParameterTypeResolution(PgTypeInfo? typeInfo)
{
    public bool IsResolved => typeInfo is not null;
    public PgTypeId PgTypeId => typeInfo?.PgTypeId ?? default;

    public Parameter CreateParameter(object? value, int parameterIndex)
    {
        if (typeInfo is null)
            ThrowHelper.ThrowInvalidOperation(
                $"Parameter ${parameterIndex + 1} cannot be bound before PostgreSQL resolves its type.");
        return Parameter.CreateUnbound(value, typeInfo.PgTypeId, typeInfo);
    }
}
