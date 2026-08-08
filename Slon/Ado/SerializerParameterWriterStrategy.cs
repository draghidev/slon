using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Serialization;

namespace Slon;

// Composition adapter between Slon's parameter container and the serializer substrate. It lives
// above both layers deliberately: PgWriter and PgSerializerOptions never depend on Parameter.
sealed class SerializerParameterWriterStrategy : ParameterWriterStrategy
{
    public static SerializerParameterWriterStrategy Instance { get; } = new();

    SerializerParameterWriterStrategy() { }

    public override object CreateState(IOutputWriter output, Encoding textEncoding)
        => new PgWriter(output, new() { TextEncoding = textEncoding });

    public override void Write(object state, in Parameter parameter)
    {
        var writer = (PgWriter)state;
        var size = parameter.GetSize();
        writer.Init(writer.ConversionContext, FlushMode.Blocking, parameter.WriteState);
        try
        {
            parameter.Write(writer);
            writer.EndWrite(size);
        }
        catch
        {
            writer.AbortWrite();
            throw;
        }
    }

    public override async ValueTask WriteAsync(object state, Parameter parameter,
        CancellationToken cancellationToken = default)
    {
        var writer = (PgWriter)state;
        var size = parameter.GetSize();
        writer.Init(writer.ConversionContext, FlushMode.NonBlocking, parameter.WriteState);
        try
        {
            await parameter.WriteAsync(writer, cancellationToken).ConfigureAwait(false);
            writer.EndWrite(size);
        }
        catch
        {
            writer.AbortWrite();
            throw;
        }
    }
}
