using System.Text;
using Slon.Buffers;
using Slon.Pg.Types;

namespace Slon.Pg;

// Protocol-facing parameter serialization component. The protocol owns Bind framing; the
// component projects parameter types and owns its per-wire writer and per-execution write state.
abstract class ParameterWriterStrategy
{
    public struct WriteLease : IDisposable
    {
        object? _state;
        readonly ParameterWriterStrategy? _strategy;
        readonly int _count;

        public WriteLease(object state, int count, ParameterWriterStrategy strategy)
        {
            _state = state;
            _strategy = strategy;
            _count = count;
        }

        public readonly object State => _state!;

        void IDisposable.Dispose()
        {
            var state = _state;
            if (state is null)
                return;
            _state = null;
            _strategy!.EndWrite(state, _count);
        }
    }

    public abstract object CreateWriterState(IOutputWriter output, Encoding textEncoding);
    public abstract PgTypeId GetParameterType(object source, int index);
    public abstract WriteLease BeginWrite(object source, int count);
    public virtual void EndWrite(object writeState, int count) { }
    public abstract int GetSize(object writeState, int parameterIndex);
    public virtual void Bind(object writerState, object source, object writeState, int parameterIndex) { }
    public abstract void Write(object writerState, object source, object writeState, int parameterIndex);
    public abstract ValueTask WriteAsync(object writerState, object source, object writeState, int parameterIndex,
        CancellationToken cancellationToken = default);
}
