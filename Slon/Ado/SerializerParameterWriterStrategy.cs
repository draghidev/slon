using System.Buffers;
using System.Diagnostics;
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
        if (value is SlonParameter parameter)
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

        return new(options.GetTypeInfo(value?.GetType(), preparedTypeId));
    }

    public override object CreateWriterState(IOutputWriter output, Encoding textEncoding)
        => new PgWriter(output, new() { TextEncoding = textEncoding });

    public override PgTypeId GetParameterType(object source, int index)
        => ((SlonParameters)source).GetResolvedParameterType(index);

    public override WriteLease BeginWrite(object source, int count)
    {
        var parameters = (SlonParameters)source;
        if (parameters.Count != count)
            ThrowHelper.ThrowInvalidOperation("The parameter source changed during execution.");
        var bindings = ArrayPool<Binding>.Shared.Rent(count);
        for (var i = 0; i < count; i++)
            bindings[i].Initialize();
        return new(bindings, count, this);
    }

    public override void EndWrite(object writeState, int count)
    {
        var bindings = (Binding[])writeState;
        foreach (ref readonly var binding in bindings.AsSpan(0, count))
            binding.Release();
        ArrayPool<Binding>.Shared.Return(bindings, clearArray: true);
    }

    public override int GetSize(object writeState, int parameterIndex)
        => GetBinding(writeState, parameterIndex).GetSize();

    public override void Bind(object writerState, object source, object writeState, int parameterIndex)
    {
        ref var binding = ref GetBinding(writeState, parameterIndex);
        ((SlonParameters)source).GetResolvedParameter(parameterIndex, out var value, out var typeInfo);
        var conversionContext = ((PgWriter)writerState).ConversionContext;
        if (value is SlonParameter parameter)
        {
            var binder = new ParameterBinder(typeInfo, conversionContext, ref binding);
            parameter.Bind(ref binder);
            return;
        }

        var result = typeInfo.BindParameterValue(conversionContext, value);
        binding.Set(result.Size?.Value ?? -1, result.WriteState);
    }

    public override void Write(object writerState, object source, object writeState, int parameterIndex)
    {
        ((SlonParameters)source).GetResolvedParameter(parameterIndex, out var value, out var typeInfo);
        ref readonly var binding = ref GetBinding(writeState, parameterIndex);
        var writer = (PgWriter)writerState;
        var converter = typeInfo.Converter;
        var size = binding.GetSize();
        writer.Init(writer.ConversionContext, FlushMode.Blocking, binding.WriteState);
        try
        {
            if (value is SlonParameter parameter)
            {
                var valueWriter = new ParameterWriter(converter, writer);
                parameter.Write(ref valueWriter);
            }
            else
            {
                converter.Write(writer, value);
            }
            writer.EndWrite(size);
        }
        catch
        {
            writer.AbortWrite();
            throw;
        }
    }

    public override async ValueTask WriteAsync(object writerState, object source, object writeState,
        int parameterIndex, CancellationToken cancellationToken = default)
    {
        ((SlonParameters)source).GetResolvedParameter(parameterIndex, out var value, out var typeInfo);
        var binding = GetBinding(writeState, parameterIndex);
        var writer = (PgWriter)writerState;
        var converter = typeInfo.Converter;
        var size = binding.GetSize();
        writer.Init(writer.ConversionContext, FlushMode.NonBlocking, binding.WriteState);
        try
        {
            if (value is SlonParameter parameter)
            {
                var valueWriter = new AsyncParameterWriter(converter, writer, cancellationToken);
                parameter.WriteAsync(ref valueWriter);
                await valueWriter.Task.ConfigureAwait(false);
            }
            else
            {
                await converter.WriteAsync(writer, value, cancellationToken).ConfigureAwait(false);
            }
            writer.EndWrite(size);
        }
        catch
        {
            writer.AbortWrite();
            throw;
        }
    }

    internal ref struct ParameterBinder
    {
        readonly PgTypeInfo _typeInfo;
        readonly PgConversionContext _conversionContext;
        ref Binding _binding;

        internal ParameterBinder(PgTypeInfo typeInfo, PgConversionContext conversionContext,
            ref Binding destination)
        {
            _typeInfo = typeInfo;
            _conversionContext = conversionContext;
            _binding = ref destination;
        }

        internal void Bind<T>(T? value)
        {
            var binding = _typeInfo.BindParameterValue(_conversionContext, value);
            _binding.Set(binding.Size?.Value ?? -1, binding.WriteState);
        }
    }

    internal ref struct ParameterWriter(PgConverter converter, PgWriter writer)
    {
        internal void Write<T>(T? value) => converter.Write(writer, value);
    }

    internal ref struct AsyncParameterWriter(PgConverter converter, PgWriter writer,
        CancellationToken cancellationToken)
    {
        internal ValueTask Task { get; private set; }
        internal void Write<T>(T? value) => Task = converter.WriteAsync(writer, value, cancellationToken);
    }

    static ref Binding GetBinding(object writeState, int parameterIndex)
        => ref ((Binding[])writeState)[parameterIndex];

    internal struct Binding
    {
        const int Unbound = int.MinValue;
        int _sizePlusOne;
        object? _writeState;

        internal readonly object? WriteState => _writeState;
        internal void Initialize()
        {
            _sizePlusOne = Unbound;
            _writeState = null;
        }

        internal void Set(int size, object? writeState)
        {
            _sizePlusOne = checked(size + 1);
            _writeState = writeState;
        }

        internal readonly int GetSize()
        {
            Debug.Assert(_sizePlusOne is not Unbound);
            return _sizePlusOne - 1;
        }

        internal readonly void Release() => (WriteState as IDisposable)?.Dispose();
    }
}

readonly struct ParameterTypeResolution(PgTypeInfo? typeInfo)
{
    internal bool IsResolved => typeInfo is not null;
    internal PgTypeId PgTypeId => typeInfo?.PgTypeId ?? default;
    internal PgTypeInfo GetTypeInfo(int parameterIndex)
    {
        if (typeInfo is null)
            ThrowHelper.ThrowInvalidOperation(
                $"Parameter ${parameterIndex + 1} cannot be bound before PostgreSQL resolves its type.");
        return typeInfo;
    }
}
