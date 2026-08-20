using System.Buffers.Binary;
using System.Text;
using Slon.Buffers;
using Slon.Pg.Types;

namespace Slon.Pg;

// Protocol-facing parameter serialization boundary. A flow captures one strategy with its
// dependency snapshot; the protocol owns Bind framing while the returned session owns value
// encoding, resumable flushes and implementation-specific write state.
abstract class ParameterWriterStrategy
{
    public static ParameterWriterStrategy Raw { get; } = new RawParameterWriterStrategy();

    public abstract object CreateState(IOutputWriter output, Encoding textEncoding);
    public virtual int GetParameterCount(object source)
        => throw new NotSupportedException("This parameter writer strategy does not support deferred sources.");
    public virtual PgTypeId GetParameterType(object source, int index)
        => throw new NotSupportedException("This parameter writer strategy does not support deferred sources.");
    public virtual void Materialize(object source, Span<Parameter> destination)
        => throw new NotSupportedException("This parameter writer strategy does not support deferred sources.");
    public virtual Parameter Bind(object state, int parameterIndex, in Parameter parameter)
        => parameter;
    public abstract void Write(object state, int parameterIndex, in Parameter parameter);
    public abstract ValueTask WriteAsync(object state, int parameterIndex, Parameter parameter,
        CancellationToken cancellationToken = default);

    sealed class RawParameterWriterStrategy : ParameterWriterStrategy
    {
        public override object CreateState(IOutputWriter output, Encoding textEncoding) => output;

        public override void Write(object state, int parameterIndex, in Parameter parameter)
        {
            if (parameter.ResolvedValueType != typeof(int))
                throw new NotSupportedException("Only int parameters are supported without a parameter writer strategy.");

            var output = (IOutputWriter)state;
            var span = output.GetSpan(sizeof(int));
            BinaryPrimitives.WriteInt32BigEndian(span, (int)parameter.Value!);
            output.Advance(sizeof(int));
        }

        public override ValueTask WriteAsync(object state, int parameterIndex, Parameter parameter,
            CancellationToken cancellationToken = default)
        {
            Write(state, parameterIndex, in parameter);
            return default;
        }
    }
}
