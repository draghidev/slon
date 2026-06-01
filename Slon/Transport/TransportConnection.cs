using System.IO.Pipelines;

namespace Slon.Transport;

class TransportConnectionOptions
{
    public int ReaderSegmentSize { get; init; } = TransportConnection.DefaultReaderSegmentSize;
    public int WriterSegmentSize { get; init; } = TransportConnection.DefaultWriterSegmentSize;
    public bool UseZeroByteReads { get; init; } = true;
}

abstract class TransportConnection
{
    internal const int DefaultReaderSegmentSize = 65536;
    internal const int DefaultWriterSegmentSize = DefaultReaderSegmentSize;

    public abstract PipeReader Reader { get; }
    public abstract PipeWriter Writer { get; }

    public abstract class Factory(TransportConnectionOptions? options = null)
    {
        public TransportConnectionOptions Options { get; } = options ?? new();
        public abstract bool SupportsSynchronousIO { get; }
        public abstract TransportConnection Connect(TimeSpan timeout = default);
        public abstract ValueTask<TransportConnection> ConnectAsync(CancellationToken cancellationToken = default);
    }
}
