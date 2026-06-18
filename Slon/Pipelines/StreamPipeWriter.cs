using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Slon.Runtime.CompilerServices;

namespace Slon.Pipelines;

abstract class StreamPipeWriter : PipeWriter
{
    readonly ValueTaskSourcePromise<FlushResult> _flushAsyncCorePromise = new();
    bool _isFlushActive;

    // Null when CancelPendingFlush is not supported (conduit mode): the caller's token then threads
    // straight to the underlying stream op, so we allocate neither this source nor a per-flush
    // registration. Only worth backing when the bottom is a non-token-cancelable channel (a pipe).
    protected AutoResetCancellationTokenSource? PendingFlushTokenSource { get; }
    protected bool IsWriterCompleted { get; set; }
    protected SegmentChainBuilder Segments { get; }
    protected bool LeaveOpen { get; }
    protected Stream Stream { get; }
    protected int? WriteTimeout { get; }

    public StreamPipeWriter(Stream writingStream, StreamPipeWriterOptions options, bool supportCancelPending = true)
    {
        ArgumentNullException.ThrowIfNull(writingStream);
        ArgumentNullException.ThrowIfNull(options);

        PendingFlushTokenSource = supportCancelPending ? new() : null;
        Segments = new SegmentChainBuilder(options.Pool, options.MinimumBufferSize);
        Stream = writingStream;
        LeaveOpen = options.LeaveOpen;
        var canTimeout = CanTimeout = writingStream.CanTimeout;
        // Reading this can be somewhat expensive so we cache it if leave open is false, as it conveys some amount of ownership (admittedly it's not perfect).
        WriteTimeout = canTimeout && !LeaveOpen ? writingStream.WriteTimeout : null;
    }

    // TODO create a stream that delegates to sync writes etc.
    public override Stream AsStream(bool leaveOpen = false)
    {
        return base.AsStream(leaveOpen);
    }

    /// <inheritdoc />
    public override void Advance(int bytes)
    {
        Segments.Grow(bytes);
    }

    /// <inheritdoc />
    public override Memory<byte> GetMemory(int sizeHint = 0)
    {
        ThrowIfCompleted();
        return Segments.Reserve(sizeHint, enforceHint: true);
    }

    /// <inheritdoc />
    public override Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

    /// <inheritdoc />
    public override void CancelPendingFlush() => PendingFlushTokenSource?.Cancel();

    public virtual bool CanTimeout { get; }

    /// <inheritdoc />
    public override bool CanGetUnflushedBytes => true;

    /// <inheritdoc />
    public override void Complete(Exception? exception = null)
    {
        if (IsWriterCompleted)
            return;

        IsWriterCompleted = true;
        try
        {
            FlushCore(writeToStream: exception == null, ReadOnlySpan<byte>.Empty, Timeout.InfiniteTimeSpan);
        }
        finally
        {
            PendingFlushTokenSource?.Dispose();
            Segments.Dispose();
            if (!LeaveOpen)
                Stream.Dispose();
        }
    }

    /// <inheritdoc />
    public override ValueTask CompleteAsync(Exception? exception = null)
        => CompleteAsyncCore(writeToStream: exception is null, exception);

    protected async ValueTask CompleteAsyncCore(bool writeToStream, Exception? exception = null)
    {
        if (IsWriterCompleted)
            return;

        IsWriterCompleted = true;
        try
        {
            await FlushAsyncCore(writeToStream, data: ReadOnlyMemory<byte>.Empty, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            PendingFlushTokenSource?.Dispose();
            Segments.Dispose();
            if (!LeaveOpen)
                await Stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    public virtual FlushResult Flush(TimeSpan timeout = default)
    {
        if (Segments.BufferedBytes is 0)
            return new FlushResult(isCanceled: false, isCompleted: false);

        return FlushCore(writeToStream: true, ReadOnlySpan<byte>.Empty, timeout: timeout);
    }

    /// <inheritdoc />
    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
    {
        var canceled = PendingFlushTokenSource?.Token.IsCancellationRequested ?? false;
        if (Segments.BufferedBytes is 0 || canceled)
            return new(new FlushResult(isCanceled: canceled, isCompleted: false));

        return FlushAsyncCore(writeToStream: true, data: ReadOnlyMemory<byte>.Empty, cancellationToken);
    }

    /// <inheritdoc />
    public override long UnflushedBytes => Segments.BufferedBytes;

    public virtual void Write(ReadOnlySpan<byte> source, TimeSpan timeout = default)
    {
        FlushCore(writeToStream: true, source, timeout: timeout);
    }

    public override ValueTask<FlushResult> WriteAsync(ReadOnlyMemory<byte> source, CancellationToken cancellationToken = default)
    {
        if (PendingFlushTokenSource?.Token.IsCancellationRequested == true)
            return new(new FlushResult(isCanceled: true, isCompleted: false));

        return FlushAsyncCore(writeToStream: true, data: source, cancellationToken);
    }

    protected virtual FlushResult FlushCore(bool writeToStream, ReadOnlySpan<byte> data, TimeSpan timeout)
    {
        var deadline = new Deadline(timeout);
        var originalTimeout = writeToStream && CanTimeout ? WriteTimeout ?? Stream.WriteTimeout : 0;

        // To map conceptually to pipelines, only one operation can be active on the stream.
        if (writeToStream && !TryStartFlush())
        {
            ThrowAlreadyFlushing();
        }

        try
        {
            // Now that we have acquired 'exclusive' access we can change the timeout on the stream, if necessary.
            if (writeToStream && CanTimeout)
            {
                Stream.WriteTimeout = (int)deadline.GetRemaining().TotalMilliseconds;
            }

            var didWrite = false;
            var nextSegment = Segments.HeadInfo.Head;
            BufferSegment? segment = null;
            while (nextSegment != null)
            {
                nextSegment = nextSegment.NextSegment;
                segment = Segments.HeadInfo.Head!;

                // TODO all these writes could become one vectored write.
                if (writeToStream && segment.WrittenBytes > 0)
                {
                    var buffer = segment.Memory.Span;
                    Stream.Write(buffer);
                }
            }

            if (segment is not null)
            {
                // We assume that one segment was non empty.
                didWrite = writeToStream;
                Segments.AdvanceTo(new SequencePosition(segment, segment.End));
            }

            if (writeToStream)
            {
                // Write data after the buffered data
                if (!data.IsEmpty)
                {
                    Stream.Write(data);
                }

                if (didWrite || data.Length > 0)
                {
                    Stream.Flush();
                }
            }

            return new FlushResult(isCanceled: false, isCompleted: false);
        }
        catch (IOException ex)
        {
            // TODO for our own streams we could just pretranslate timeouts.
            // We'll assume that if we're past our deadline a timeout was the reason for this exception.
            // Stream has no contract to communicate an IOException was specifically because of a read/write/close timeout.
            // This either means baking in all the different patterns (IOException wrapping SocketException etc.), or doing this.
            // It's not perfect, but it's the best we can do with the existing Stream contract.
            if (deadline.IsElapsed)
                throw new TimeoutException("The operation has timed out", ex);
            throw;
        }
        finally
        {
            if (writeToStream)
            {
                if (CanTimeout)
                {
                    Stream.WriteTimeout = originalTimeout;
                }
                EndStartedFlush();
            }
        }
    }

    protected virtual ValueTask<FlushResult> FlushAsyncCore(bool writeToStream, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        PromiseAsyncValueTaskMethodBuilder<FlushResult>.Promise = _flushAsyncCorePromise;
        try
        {
            return FlushAsyncCore(PendingFlushTokenSource, writeToStream, data, cancellationToken);
        }
        finally
        {
            PromiseAsyncValueTaskMethodBuilder<FlushResult>.Promise = null;
        }

        [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder<>))]
        async ValueTask<FlushResult> FlushAsyncCore(AutoResetCancellationTokenSource? tokenSource, bool writeToStream, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            // Cancellation token was already checked before getting here.

            // To map conceptually to pipelines, only one operation can be active on the stream.
            if (writeToStream && !TryStartFlush())
            {
                ThrowAlreadyFlushing();
            }

            // Conduit mode (no source): the caller's token threads straight to the stream op, no
            // registration. Otherwise hook the caller's token onto the source so CancelPendingFlush
            // and the caller's token both cancel.
            CancellationTokenRegistration reg = default;
            CancellationToken token;
            if (tokenSource is { } src)
            {
                if (cancellationToken.CanBeCanceled)
                    reg = src.UnsafeRegister(cancellationToken);
                token = src.Token;
            }
            else
                token = cancellationToken;
            try
            {
                var didWrite = false;
                var nextSegment = Segments.HeadInfo.Head;
                BufferSegment? segment = null;
                while (nextSegment != null)
                {
                    segment = nextSegment;
                    nextSegment = nextSegment.NextSegment;

                    // TODO all these writes could become one vectored write.
                    if (writeToStream && segment.WrittenBytes > 0)
                    {
                        await Stream.WriteAsync(segment.Memory, token).ConfigureAwait(false);
                    }
                }

                if (segment is not null)
                {
                    // We assume that one segment was non empty.
                    didWrite = writeToStream;
                    Segments.AdvanceTo(new SequencePosition(segment, segment.End));
                }

                if (writeToStream)
                {
                    // Write data after the buffered data
                    if (!data.IsEmpty)
                    {
                        await Stream.WriteAsync(data, token).ConfigureAwait(false);
                        didWrite = true;
                    }

                    if (didWrite)
                    {
                        await Stream.FlushAsync(token).ConfigureAwait(false);
                    }
                }

                return new FlushResult(isCanceled: false, isCompleted: false);
            }
            catch (OperationCanceledException oce)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    // Simulate an OCE triggered directly by the cancellationToken rather than the InternalTokenSource
                    throw new OperationCanceledException(null, oce, cancellationToken);
                }
                else if (token.IsCancellationRequested)
                {
                    // Catch cancellation and translate it into setting isCanceled = true
                    return new FlushResult(isCanceled: true, isCompleted: false);
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                await reg.DisposeAsync().ConfigureAwait(false);

                if (writeToStream)
                {
                    EndStartedFlush();
                }
            }
        }
    }

    // These are just guardrails to ensure correct usage, they are not for correctness.
    // From https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines
    // "Ensure that only one context "owns" a PipeReader or PipeWriter or accesses them. These types are not thread-safe."
    protected bool TryStartFlush()
    {
        if (_isFlushActive)
            return false;

        return _isFlushActive = true;
    }

    protected void EndStartedFlush()
    {
        Debug.Assert(_isFlushActive);
        _isFlushActive = false;
    }

    protected void ThrowIfCompleted()
    {
        if (IsWriterCompleted)
            ThrowCompleted();

        static void ThrowCompleted()
            => throw new InvalidOperationException("Writing is not allowed after writer was completed.");
    }

    protected void ThrowAlreadyFlushing()
        => throw new InvalidOperationException("Concurrent flushes are not supported.");
}

sealed class DefaultStreamPipeWriter(Stream writingStream, StreamPipeWriterOptions options, bool supportCancelPending = true) : StreamPipeWriter(writingStream, options, supportCancelPending);
