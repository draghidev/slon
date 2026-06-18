using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Slon.Runtime.CompilerServices;

namespace Slon.Pipelines;

abstract class StreamPipeReader : PipeReader
{
    readonly ValueTaskSourcePromise<ReadResult> _readAsyncCorePromise = new();
    bool _isReadActive;

    // Null in conduit mode (CancelPendingRead unsupported): the caller's token threads straight to
    // the underlying stream read, so neither this source nor a per-read registration is allocated.
    protected AutoResetCancellationTokenSource? PendingReadTokenSource { get; }
    protected bool IsReaderCompleted { get; set; }
    protected SegmentChainBuilder Segments { get; }
    protected bool LeaveOpen { get; }
    protected bool UseZeroByteReads { get; }
    protected Stream Stream { get; }
    protected bool ExaminedEverything { get; set; }
    protected int? ReadTimeout { get; }

    /// <summary>
    /// Creates a new StreamPipeReader.
    /// </summary>
    /// <param name="readingStream">The stream to read from.</param>
    /// <param name="options">The options to use.</param>
    protected StreamPipeReader(Stream readingStream, StreamPipeReaderOptions options, bool supportCancelPending = true)
    {
        ArgumentNullException.ThrowIfNull(readingStream);
        ArgumentNullException.ThrowIfNull(options);

        Stream = readingStream;
        Segments = new(options.Pool, options.BufferSize, options.MinimumReadSize);
        LeaveOpen = options.LeaveOpen;
        PendingReadTokenSource = supportCancelPending ? new() : null;
        UseZeroByteReads = options.UseZeroByteReads;
        var canTimeout = readingStream.CanTimeout;
        CanTimeout = canTimeout;
        // Reading this can be somewhat expensive so we cache it if leave open is false, as it conveys some amount of ownership (admittedly it's not perfect).
        ReadTimeout = canTimeout && !LeaveOpen ? readingStream.ReadTimeout : null;

    }

    // TODO create a stream that delegates to sync reads etc.
    public override Stream AsStream(bool leaveOpen = false)
    {
        return base.AsStream(leaveOpen);
    }

    /// <inheritdoc />
    public override void AdvanceTo(SequencePosition consumed)
        => AdvanceTo(consumed, consumed);

    /// <inheritdoc />
    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        ThrowIfCompleted();
        var bufferedBytes = Segments.BufferedBytes;
        var examinedBytes = Segments.AdvanceTo(consumed, examined);
        if (examinedBytes == bufferedBytes)
            ExaminedEverything = true;
    }

    /// <inheritdoc />
    public override void CancelPendingRead()
        => PendingReadTokenSource?.Cancel();

    /// <inheritdoc />
    public override void Complete(Exception? exception = null)
    {
        if (IsReaderCompleted)
            return;

        IsReaderCompleted = true;
        PendingReadTokenSource?.Dispose();
        Segments.Dispose();
        if (!LeaveOpen)
            Stream.Dispose();
    }

    /// <inheritdoc />
    public override ValueTask CompleteAsync(Exception? exception = null)
    {
        if (IsReaderCompleted)
            return new();

        IsReaderCompleted = true;
        PendingReadTokenSource?.Dispose();
        Segments.Dispose();
        return !LeaveOpen ? Stream.DisposeAsync() : new();
    }

    public virtual bool CanTimeout { get; }

    public virtual ReadResult Read(TimeSpan timeout = default)
    {
        ThrowIfCompleted();

        if (timeout != default && timeout != Timeout.InfiniteTimeSpan && !CanTimeout)
            ThrowTimeoutNotSupported();

        return TryReadCore(out var readResult) ? readResult : ReadCore(0, timeout);
    }

    /// <inheritdoc />
    public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfCompleted();

        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromException<ReadResult>(new OperationCanceledException(cancellationToken));

        if (PendingReadTokenSource?.Token.IsCancellationRequested == true)
            return new ValueTask<ReadResult>(new ReadResult(default, isCanceled: true, isCompleted: false));

        return TryReadCore(out var readResult) ? new(readResult) : ReadAsyncCore(0, cancellationToken);
    }

    public virtual ReadResult ReadAtLeast(int minimumSize, TimeSpan timeout = default)
    {
        // Put this before ThrowIfCompleted to match ReadAtLeastAsyncCore.
        ArgumentOutOfRangeException.ThrowIfNegative(minimumSize);
        ThrowIfCompleted();

        if (timeout != default && timeout != Timeout.InfiniteTimeSpan && !CanTimeout)
            ThrowTimeoutNotSupported();

        if (Segments.BufferedBytes >= minimumSize && TryReadCore(out var readResult))
        {
            Debug.Assert(!readResult.IsCanceled && !readResult.IsCompleted);
            if (readResult.Buffer.Length >= minimumSize)
                return readResult;
        }

        return ReadCore(minimumSize, timeout);
    }

    protected override ValueTask<ReadResult> ReadAtLeastAsyncCore(int minimumSize, CancellationToken cancellationToken)
    {
        Debug.Assert(minimumSize >= 0, "PipeReader should have validated minimumSize is non-negative.");
        ThrowIfCompleted();

        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromException<ReadResult>(new OperationCanceledException(cancellationToken));

        if (PendingReadTokenSource?.Token.IsCancellationRequested == true)
            return new ValueTask<ReadResult>(new ReadResult(default, isCanceled: true, isCompleted: false));

        if (Segments.BufferedBytes >= minimumSize && TryReadCore(out var readResult))
        {
            Debug.Assert(!readResult.IsCanceled && !readResult.IsCompleted);
            if (readResult.Buffer.Length >= minimumSize)
                return new(readResult);
        }

        return ReadAsyncCore(minimumSize, cancellationToken);
    }

    /// <inheritdoc />
    public override Task CopyToAsync(PipeWriter destination, CancellationToken cancellationToken = default)
    {
        ThrowIfCompleted();

        if (cancellationToken.IsCancellationRequested)
            return Task.FromException(new OperationCanceledException(cancellationToken));

        return CopyToAsyncCore(destination, cancellationToken);
    }

    /// <inheritdoc />
    public override Task CopyToAsync(Stream destination, CancellationToken cancellationToken = default)
    {
        ThrowIfCompleted();

        if (cancellationToken.IsCancellationRequested)
            return Task.FromException(new OperationCanceledException(cancellationToken));

        return CopyToAsyncCore(destination, cancellationToken);
    }

    public override bool TryRead(out ReadResult result)
    {
        ThrowIfCompleted();
        return TryReadCore(out result);
    }

    protected ReadResult ReadCore(int minimumSize, TimeSpan timeout)
    {
        var deadline = new Deadline(timeout);
        var originalTimeout = CanTimeout ? ReadTimeout ?? Stream.ReadTimeout : 0;

        // To map conceptually to pipelines, only one operation can be active.
        if (!TryStartRead())
        {
            ThrowAlreadyReading();
        }

        try
        {
            // Now that we have acquired 'exclusive' access we can change the timeout on the stream, if necessary.
            if (CanTimeout)
            {
                Stream.ReadTimeout = (int)deadline.GetRemaining().TotalMilliseconds;
            }

            // This optimization only makes sense if we don't have anything buffered
            if (Segments.BufferedBytes is 0 && UseZeroByteReads)
            {
                // Wait for data by doing 0 byte read before
                _ = Stream.Read(Span<byte>.Empty);
            }

            int length;
            do
            {
                // We know minimumSize must be null or larger than what we have buffered to get here.
                var segmentSize = minimumSize;
                if (minimumSize is not 0)
                {
                    // We must request a segment that is minimumSize - BufferedBytes.
                    Debug.Assert(Segments.BufferedBytes <= minimumSize);
                    segmentSize -= (int)Segments.BufferedBytes;
                }

                // We don't mind if we get smaller segments, we just want to make progress towards minimumSize.
                var buffer = Segments.Reserve(segmentSize, enforceHint: false);
                length = Stream.Read(buffer.Span);

                if (length is 0)
                {
                    break;
                }

                ExaminedEverything = false;
                Segments.Grow(length);
            } while (Segments.BufferedBytes < minimumSize);

            return new ReadResult(Segments.GetReadOnlySequence(), isCanceled: false, isCompleted: length is 0);
        }
        catch (IOException ex)
        {
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
            if (CanTimeout)
            {
                Stream.ReadTimeout = originalTimeout;
            }
            EndStartedRead();
        }

    }

    protected ValueTask<ReadResult> ReadAsyncCore(int minimumSize, CancellationToken cancellationToken)
    {
        PromiseAsyncValueTaskMethodBuilder<ReadResult>.Promise = _readAsyncCorePromise;
        try
        {
            return ReadAsyncCore(minimumSize, PendingReadTokenSource, cancellationToken);
        }
        finally
        {
            PromiseAsyncValueTaskMethodBuilder<ReadResult>.Promise = null;
        }

        [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder<>))]
        async ValueTask<ReadResult> ReadAsyncCore(int minimumSize, AutoResetCancellationTokenSource? tokenSource, CancellationToken cancellationToken)
        {
            // Cancellation token was already checked before getting here.

            // To map conceptually to pipelines, only one operation can be active.
            if (!TryStartRead())
            {
                ThrowAlreadyReading();
            }

            // Conduit mode (no source): caller's token threads straight to the stream read, no
            // registration. Otherwise hook the caller's token onto the source.
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
                // This optimization only makes sense if we don't have anything buffered
                if (Segments.BufferedBytes is 0 && UseZeroByteReads)
                {
                    // Wait for data by doing 0 byte read before
                    _ = await Stream.ReadAsync(Memory<byte>.Empty, token).ConfigureAwait(false);
                }

                int length;
                do
                {
                    // We know minimumSize must be null or larger than what we have buffered to get here.
                    var segmentSize = minimumSize;
                    if (minimumSize is not 0)
                    {
                        // We must request a segment that is minimumSize - BufferedBytes.
                        Debug.Assert(Segments.BufferedBytes <= minimumSize);
                        segmentSize -= (int)Segments.BufferedBytes;
                    }

                    // We don't mind if we get smaller segments, we just want to make progress towards minimumSize.
                    var buffer = Segments.Reserve(segmentSize, enforceHint: false);
                    length = await Stream.ReadAsync(buffer, token).ConfigureAwait(false);

                    if (length is 0)
                    {
                        break;
                    }

                    ExaminedEverything = false;
                    Segments.Grow(length);
                } while (Segments.BufferedBytes < minimumSize);

                return new ReadResult(Segments.GetReadOnlySequence(), isCanceled: false, isCompleted: length is 0);
            }
            catch (OperationCanceledException ex)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    // Simulate an OCE triggered directly by the cancellationToken rather than the InternalTokenSource
                    throw new OperationCanceledException(null, ex, cancellationToken);
                }
                else if (token.IsCancellationRequested)
                {
                    // Catch cancellation and translate it into setting isCanceled = true
                    return new ReadResult(default, isCanceled: true, isCompleted: false);
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                await reg.DisposeAsync().ConfigureAwait(false);
                EndStartedRead();
            }
        }
    }

    protected async Task CopyToAsyncCore(PipeWriter destination, CancellationToken cancellationToken = default)
    {
        var tokenSource = PendingReadTokenSource;
        CancellationTokenRegistration reg = default;
        CancellationToken token;
        if (tokenSource is { } src)
        {
            token = src.Token;
            if (token.IsCancellationRequested)
                ThrowReadCanceled();
            if (cancellationToken.CanBeCanceled)
                reg = src.UnsafeRegister(cancellationToken);
        }
        else
        {
            token = cancellationToken;
            if (token.IsCancellationRequested)
                ThrowReadCanceled();
        }

        try
        {
            var (segment, segmentIndex) = Segments.HeadInfo;

            try
            {
                while (segment != null)
                {
                    FlushResult flushResult = await destination.WriteAsync(segment.Memory.Slice(segmentIndex), token).ConfigureAwait(false);

                    if (flushResult.IsCanceled)
                    {
                        ThrowFlushCanceled();
                    }

                    segment = segment.NextSegment;
                    segmentIndex = 0;

                    if (flushResult.IsCompleted)
                    {
                        return;
                    }
                }
            }
            finally
            {
                // Advance even if WriteAsync throws so the PipeReader is not left in the
                // currently reading state
                if (segment != null)
                {
                    Segments.AdvanceTo(new(segment, segment.End));
                }
            }

            await Stream.CopyToAsync(destination, token).ConfigureAwait(false);
        }
        finally
        {
            await reg.DisposeAsync().ConfigureAwait(false);
        }

        static void ThrowFlushCanceled()
            => throw new OperationCanceledException("Flush was canceled on underlying PipeWriter.");
    }

    protected async Task CopyToAsyncCore(Stream destination, CancellationToken cancellationToken = default)
    {
        var tokenSource = PendingReadTokenSource;
        CancellationTokenRegistration reg = default;
        CancellationToken token;
        if (tokenSource is { } src)
        {
            token = src.Token;
            if (token.IsCancellationRequested)
                ThrowReadCanceled();
            if (cancellationToken.CanBeCanceled)
                reg = src.UnsafeRegister(cancellationToken);
        }
        else
        {
            token = cancellationToken;
            if (token.IsCancellationRequested)
                ThrowReadCanceled();
        }

        try
        {
            var (segment, segmentIndex) = Segments.HeadInfo;

            try
            {
                while (segment != null)
                {
                    await destination.WriteAsync(segment.Memory.Slice(segmentIndex), token).ConfigureAwait(false);

                    segment = segment.NextSegment;
                    segmentIndex = 0;
                }
            }
            finally
            {
                // Advance even if WriteAsync throws so the PipeReader is not left in the
                // currently reading state
                if (segment != null)
                {
                    Segments.AdvanceTo(new(segment, segment.End));
                }
            }

            await Stream.CopyToAsync(destination, token).ConfigureAwait(false);
        }
        finally
        {
            await reg.DisposeAsync().ConfigureAwait(false);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected bool TryReadCore(out ReadResult result)
    {
        if (Segments.BufferedBytes is 0 || ExaminedEverything)
        {
            result = default;
            return false;
        }

        result = new ReadResult(Segments.GetReadOnlySequence(), isCanceled: false, isCompleted: false);
        return true;
    }

    // These are just guardrails to ensure correct usage, they are not for correctness.
    // From https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines
    // "Ensure that only one context "owns" a PipeReader or PipeWriter or accesses them. These types are not thread-safe."
    protected bool TryStartRead()
    {
        if (_isReadActive)
            return false;

        return _isReadActive = true;
    }

    protected void EndStartedRead()
    {
        Debug.Assert(_isReadActive);
        _isReadActive = false;
    }

    protected void ThrowIfCompleted()
    {
        if (IsReaderCompleted)
            ThrowCompleted();

        static void ThrowCompleted()
            => throw new InvalidOperationException("Reading is not allowed after reader was completed.");
    }

    static void ThrowReadCanceled()
        => throw new OperationCanceledException("Read was canceled on underlying PipeReader.");

    static void ThrowTimeoutNotSupported()
        => throw new ArgumentException("Timeouts are not supported for this pipe reader", "timeout");

    protected static void ThrowAlreadyReading()
        => throw new InvalidOperationException("Concurrent reads are not supported.");
}

sealed class DefaultStreamPipeReader(Stream readingStream, StreamPipeReaderOptions options, bool supportCancelPending = true) : StreamPipeReader(readingStream, options, supportCancelPending);
