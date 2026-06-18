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

    // Forcibly breaks the wire so any parked I/O faults promptly. The async paths unblock off
    // AbortToken, but a sync read/write is in a blocking syscall that no token reaches - only
    // closing the underlying handle gets it out. Called from the forceful-abort path BEFORE the
    // drain awaits the in-flight flows, so a parked sync read faults (into the read translation)
    // and the drain can complete. Distinct from full disposal: it must NOT release the reader's
    // buffers, which a parked read may still be writing into. Default no-op suits in-memory
    // transports whose tests unblock via token cancellation.
    public virtual void Abort() { }

    public abstract class Factory(TransportConnectionOptions? options = null)
    {
        public TransportConnectionOptions Options { get; } = options ?? new();
        public abstract bool SupportsSynchronousIO { get; }
        public abstract TransportConnection Connect(TimeSpan timeout = default);
        public abstract ValueTask<TransportConnection> ConnectAsync(CancellationToken cancellationToken = default);
    }
}
