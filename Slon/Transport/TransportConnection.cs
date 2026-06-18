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

    // Faults parked I/O terminally without blocking and without releasing the reader/writer buffers
    // (a parked read may hold a reserved segment; those go on the later Complete). Generic finalize
    // stays on the reader/writer's Complete - this is only the "break it now, with a reason the other
    // end understands" step that a graceful close can't do safely (it can hang on a wedged peer).
    // Socket transports do a 0-linger abortive close (RST). A pipe transport overrides to Complete its
    // end with a sentinel exception the read end recognizes as an abort - the in-memory analogue of the
    // RST. Default no-op: our async transports unblock off AbortToken and never take the sync path.
    public virtual void Abort() { }

    public abstract class Factory(TransportConnectionOptions? options = null)
    {
        public TransportConnectionOptions Options { get; } = options ?? new();
        public abstract bool SupportsSynchronousIO { get; }
        public abstract TransportConnection Connect(TimeSpan timeout = default);
        public abstract ValueTask<TransportConnection> ConnectAsync(CancellationToken cancellationToken = default);
    }
}
