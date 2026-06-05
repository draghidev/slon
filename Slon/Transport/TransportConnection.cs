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

    // Set by ResumableScope to request synchronous non-blocking writes. On WouldBlock, the
    // transport returns a pending task backed by this caller-owned signal; only the caller resumes it.
    [ThreadStatic]
    public static WriteResumeSignal? SyncNonBlockingSignal;

    // Optional deadline for synchronous polling under the same scope. Null means infinite.
    [ThreadStatic]
    public static Deadline? SyncNonBlockingDeadline;

    public abstract PipeReader Reader { get; }
    public abstract PipeWriter Writer { get; }

    // Parks the calling thread until the transport is writable.
    public abstract void WaitWritable();

    public abstract class Factory(TransportConnectionOptions? options = null)
    {
        public TransportConnectionOptions Options { get; } = options ?? new();
        public abstract bool SupportsSynchronousIO { get; }
        public abstract TransportConnection Connect(TimeSpan timeout = default);
        public abstract ValueTask<TransportConnection> ConnectAsync(CancellationToken cancellationToken = default);
    }
}
