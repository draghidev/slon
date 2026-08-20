using System.Buffers;
using System.Diagnostics;
using System.Text;
using Slon.Buffers;
using Slon.Pg.Types;

namespace Slon.Pg.Serialization;

enum PgParameterValueOperationKind : byte
{
    Bind,
    Write,
    WriteAsync
}

ref struct PgParameterValueOperation
{
    ref PgParameterValueOperationResult _result;
    readonly PgWriter _writer;
    readonly CancellationToken _cancellationToken;
    readonly PgParameterValueOperationKind _kind;

    PgParameterValueOperation(PgWriter writer, ref PgParameterValueOperationResult result,
        PgParameterValueOperationKind kind,
        CancellationToken cancellationToken = default)
    {
        _result = ref result;
        _writer = writer;
        _kind = kind;
        _cancellationToken = cancellationToken;
    }

    public void Apply<T>(PgTypeInfo typeInfo, T? value)
    {
        switch (_kind)
        {
        case PgParameterValueOperationKind.Bind:
            _result.Size = typeInfo.BindParameterValue(
                _writer.ConversionContext, value, out _result.WriteState);
            break;
        case PgParameterValueOperationKind.Write:
            typeInfo.Converter.Write(_writer, value);
            break;
        case PgParameterValueOperationKind.WriteAsync:
            _result.Task = typeInfo.Converter.WriteAsync(_writer, value, _cancellationToken);
            break;
        default:
            throw new UnreachableException();
        }
    }

    internal static PgParameterValueOperation Bind(PgWriter writer,
        ref PgParameterValueOperationResult result)
        => new(writer, ref result, PgParameterValueOperationKind.Bind);
    internal static PgParameterValueOperation Write(PgWriter writer,
        ref PgParameterValueOperationResult result)
        => new(writer, ref result, PgParameterValueOperationKind.Write);
    internal static PgParameterValueOperation WriteAsync(PgWriter writer,
        ref PgParameterValueOperationResult result, CancellationToken cancellationToken)
        => new(writer, ref result, PgParameterValueOperationKind.WriteAsync, cancellationToken);
}

struct PgParameterValueOperationResult
{
    internal int Size;
    internal object? WriteState;
    internal ValueTask Task;
}

// Serializer-backed parameter writer. Sources provide values and resolved type information;
// this component owns binding storage, PgWriter tenure and converter failure cleanup.
abstract class PgSerializerParameterWriter<TSource> : ParameterWriter
    where TSource : class
{
    public abstract int GetParameterCount(TSource source);
    public abstract PgTypeId GetParameterType(TSource source, int index);

    internal sealed override object CreateWriterStateCore(IOutputWriter output, Encoding textEncoding)
        => new PgWriter(output, new() { TextEncoding = textEncoding });

    internal sealed override int GetParameterCountCore(object source)
        => GetParameterCount((TSource)source);

    internal sealed override PgTypeId GetParameterTypeCore(object source, int index)
        => GetParameterType((TSource)source, index);

    private protected sealed override object BeginWriteStateCore(object source, int count)
        => ArrayPool<PgSerializerParameterBinding>.Shared.Rent(count);

    private protected sealed override void EndWriteCore(object writeState, int count)
    {
        var bindings = (PgSerializerParameterBinding[])writeState;
        var index = 0;
        try
        {
            for (; index < count; index++)
                bindings[index].Release();
        }
        catch (Exception exception)
        {
            ReleaseRemainingAfterFailure(bindings, index + 1, count, exception);
        }
        ArrayPool<PgSerializerParameterBinding>.Shared.Return(bindings, clearArray: false);
    }

    static void ReleaseRemainingAfterFailure(PgSerializerParameterBinding[] bindings,
        int index, int count, Exception exception)
    {
        for (; index < count; index++)
        {
            try
            {
                bindings[index].Release();
            }
            catch
            {
                // Preserve the first disposal failure while still detaching every retained state.
            }
        }
        ArrayPool<PgSerializerParameterBinding>.Shared.Return(bindings, clearArray: false);
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(exception);
    }

    private protected sealed override int GetSizeCore(object writeState, int parameterIndex)
        => ((PgSerializerParameterBinding[])writeState)[parameterIndex].GetSize();

    private protected sealed override void BindCore(object writerState, object source,
        object writeState, int parameterIndex)
    {
        var writer = (PgWriter)writerState;
        var result = new PgParameterValueOperationResult();
        var operation = PgParameterValueOperation.Bind(writer, ref result);
        ApplyParameter((TSource)source, parameterIndex, operation);
        ((PgSerializerParameterBinding[])writeState)[parameterIndex]
            .Set(result.Size, result.WriteState);
    }

    private protected sealed override void WriteCore(object writerState, object source,
        object writeState, int parameterIndex)
    {
        var writer = (PgWriter)writerState;
        var binding = ((PgSerializerParameterBinding[])writeState)[parameterIndex];
        var size = binding.GetSize();
        writer.Init(writer.ConversionContext, FlushMode.Blocking, binding.WriteState);
        try
        {
            var result = new PgParameterValueOperationResult();
            var operation = PgParameterValueOperation.Write(writer, ref result);
            ApplyParameter((TSource)source, parameterIndex, operation);
            writer.EndWrite(size);
        }
        catch
        {
            writer.AbortWrite();
            throw;
        }
    }

    private protected sealed override async ValueTask WriteAsyncCore(object writerState, object source,
        object writeState, int parameterIndex,
        CancellationToken cancellationToken = default)
    {
        var writer = (PgWriter)writerState;
        var binding = ((PgSerializerParameterBinding[])writeState)[parameterIndex];
        var size = binding.GetSize();
        writer.Init(writer.ConversionContext, FlushMode.NonBlocking, binding.WriteState);
        try
        {
            ValueTask writeTask;
            {
                var result = new PgParameterValueOperationResult();
                var operation = PgParameterValueOperation.WriteAsync(writer, ref result,
                    cancellationToken);
                ApplyParameter((TSource)source, parameterIndex, operation);
                writeTask = result.Task;
            }
            await writeTask.ConfigureAwait(false);
            writer.EndWrite(size);
        }
        catch
        {
            writer.AbortWrite();
            throw;
        }
    }

    protected abstract void ApplyParameter(TSource source, int parameterIndex,
        PgParameterValueOperation operation);
}

struct PgSerializerParameterBinding
{
    object? _writeState;
    int _size;

    internal readonly object? WriteState => _writeState;

    internal void Set(int size, object? writeState)
    {
        Debug.Assert(_writeState is null);
        _size = size;
        if (writeState is not null)
            _writeState = writeState;
    }

    internal readonly int GetSize() => _size;

    internal void Release()
    {
        var writeState = _writeState;
        _writeState = null;
        if (writeState is not null && writeState is IDisposable disposable)
            disposable.Dispose();
    }
}
