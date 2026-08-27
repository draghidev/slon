using System.IO.Pipelines;

namespace Slon.Transport;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed class TransportConnectionOptions
{
    public int ReaderSegmentSize { get; init; } = TransportConnection.DefaultReaderSegmentSize;
    public int WriterSegmentSize { get; init; } = TransportConnection.DefaultWriterSegmentSize;
    public bool UseZeroByteReads { get; init; } = true;
}

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public abstract class TransportConnection
{
    public readonly struct ResumableWrite(ResumeSignal? signal, TimeSpan timeout)
    {
        public ResumeSignal? Signal { get; } = signal;
        public TimeSpan Timeout { get; } = timeout;
    }

    protected internal const int DefaultReaderSegmentSize = 65536;
    protected internal const int DefaultWriterSegmentSize = DefaultReaderSegmentSize;

    // Enables the resumable-write path for the current thread. The transport attempts an async write
    // synchronously and parks it with a fresh deadline when the wire blocks.
    [field: ThreadStatic]
    public static ResumableWrite CurrentResumableWrite { get; set; }

    // The protocol completes both endpoints after all borrowed buffers have been returned.
    public abstract PipeReader Reader { get; }
    public abstract PipeWriter Writer { get; }

    // Classifies transport-specific exceptions that mean the established byte stream was lost.
    public virtual bool IsConnectionLost(Exception exception) => false;

    // Waits up to the supplied remaining timeout for the parked resumable write to become writable.
    public abstract void WaitUntilWritable(TimeSpan timeout = default);

    // Breaks outstanding physical I/O without completing the endpoints; endpoint completion remains
    // the protocol's responsibility after borrowed buffers have been returned.
    public virtual void Abort() { }

    public abstract class Factory(TransportConnectionOptions? options = null)
    {
        static readonly Func<Stream, Stream> IdentityTransform = static stream => stream;

        public TransportConnectionOptions Options { get; } = options ?? new();

        public TransportConnection Connect(TimeSpan timeout = default)
            => ConnectTransformed(IdentityTransform, timeout);

        public ValueTask<TransportConnection> ConnectAsync(CancellationToken cancellationToken = default)
            => ConnectTransformedAsync(IdentityTransform, cancellationToken);

        public abstract TransportConnection ConnectTransformed(Func<Stream, Stream> transform, TimeSpan timeout = default);
        public abstract ValueTask<TransportConnection> ConnectTransformedAsync(Func<Stream, Stream> transform, CancellationToken cancellationToken = default);
        public abstract TransportConnection Upgrade(TransportConnection connection, Func<Stream, Stream> transform);
    }
}
