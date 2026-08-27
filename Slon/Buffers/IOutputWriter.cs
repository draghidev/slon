using System.Buffers;

namespace Slon.Buffers;

// A streaming alternative to a System.IO.Stream, based on the preferable IBufferWriter interface.
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public interface IOutputWriter : IBufferWriter<byte>
{
    long UnflushedBytes { get; }
    void Flush(TimeSpan timeout = default);
    ValueTask FlushAsync(CancellationToken cancellationToken = default);
}
