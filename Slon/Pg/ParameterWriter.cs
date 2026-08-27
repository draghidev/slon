using System.Text;
using Slon.Buffers;
using Slon.Pg.Types;

namespace Slon.Pg;

// Protocol-facing parameter serialization component. The protocol owns Bind framing; the
// component projects parameter types and owns its per-wire writer and per-execution write state.
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public abstract class ParameterWriter
{
    private protected ParameterWriter() { }

    internal struct WriteLease : IDisposable
    {
        readonly object _source;
        object? _state;
        readonly ParameterWriter? _writer;
        readonly int _count;

        internal WriteLease(object source, object state, int count, ParameterWriter writer)
        {
            _source = source;
            _state = state;
            _writer = writer;
            _count = count;
        }

        internal readonly void Bind(object writerState, int parameterIndex)
            => _writer!.BindCore(writerState, _source, _state!, parameterIndex);
        internal readonly int GetSize(int parameterIndex)
            => _writer!.GetSizeCore(_state!, parameterIndex);
        internal readonly void Write(object writerState, int parameterIndex)
            => _writer!.WriteCore(writerState, _source, _state!, parameterIndex);
        internal readonly ValueTask WriteAsync(object writerState, int parameterIndex, CancellationToken cancellationToken)
            => _writer!.WriteAsyncCore(writerState, _source, _state!, parameterIndex, cancellationToken);

        void IDisposable.Dispose()
        {
            var state = _state;
            if (state is null)
                return;
            _state = null;
            _writer!.EndWriteCore(state, _count);
        }
    }

    internal abstract object CreateWriterStateCore(IOutputWriter output, Encoding textEncoding);
    internal abstract int GetParameterCountCore(object source);
    internal abstract PgTypeId GetParameterTypeCore(object source, int index);
    internal WriteLease BeginWriteCore(object source, int count)
        => new(source, BeginWriteStateCore(source, count), count, this);
    private protected abstract object BeginWriteStateCore(object source, int count);
    private protected virtual void EndWriteCore(object writeState, int count) { }
    private protected abstract int GetSizeCore(object writeState, int parameterIndex);
    private protected virtual void BindCore(object writerState, object source, object writeState,
        int parameterIndex) { }
    private protected abstract void WriteCore(object writerState, object source, object writeState,
        int parameterIndex);
    private protected abstract ValueTask WriteAsyncCore(object writerState, object source,
        object writeState, int parameterIndex,
        CancellationToken cancellationToken = default);
}
