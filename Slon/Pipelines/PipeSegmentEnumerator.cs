using System.Buffers;
using System.Collections;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

namespace Slon.Pipelines;

interface IPipeSegmenter<TSegment>
{
    /// <summary>
    /// MinimumSize guarantees CreateSegment won't be called unless there is enough data to examine.
    /// </summary>
    int MinimumSize { get; }

    /// <summary>
    /// Create the next segment from the given buffer, returning segment information on OperationStatus.Done.
    /// </summary>
    /// <param name="buffer">The buffer to try to read the next segment from.</param>
    /// <param name="segmentLength">The length of the segment, this may be larger than the amount buffered at the time of the call.</param>
    /// <param name="segment">Segment to return to the caller.</param>
    /// <returns>Whether the call was successful, requires more data, or invalid data was found, DestinationTooSmall is not supported.</returns>
    OperationStatus CreateSegment(in ReadOnlySequence<byte> buffer, out long segmentLength, out TSegment segment);
}

readonly struct CurrentSegmentBuffer(ReadOnlySequence<byte> buffer, bool isComplete)
{
    public ReadOnlySequence<byte> Buffer { get; } = buffer;
    public bool IsComplete { get; } = isComplete;
}

sealed class PipeSegmentEnumerator<TSegmenter, TSegment>(PipeReader reader, TSegmenter segmenter, bool ownsReader = false)
    : IEnumerator<TSegment>, IAsyncEnumerator<TSegment>
    where TSegmenter: IPipeSegmenter<TSegment>
{
    readonly StreamPipeReader? _directReader = reader as StreamPipeReader;
    TSegmenter _segmenter = segmenter;
    TSegment _current = default!;

    SequencePosition _examinedPosition;
    SequencePosition _currentSegmentStart;
    SequencePosition? _consumePosition;
    long _currentLength = -1;
    byte _currentSegmentReadPending;

    public PipeReader PipeReader => reader;

    public ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken)
        => reader.ReadAsync(cancellationToken);

    public bool TryBeginDirectRead(CancellationToken cancellationToken, out ValueTask<int> task)
    {
        if (_directReader is { SupportsDirectRead: true } directReader)
        {
            task = directReader.BeginDirectRead(cancellationToken);
            return true;
        }
        task = default;
        return false;
    }

    public bool CompleteDirectRead(int length, CancellationToken cancellationToken, out ValueTask<int> next, out bool readFinished, out bool completed)
    {
        if (!_directReader!.CompleteDirectRead(length, cancellationToken, out next, out var result))
        {
            readFinished = false;
            completed = false;
            return false;
        }
        readFinished = true;
        return TryMoveNext(result, cancellationToken, out completed);
    }

    public void AbortDirectRead() => _directReader!.AbortDirectRead();

    public bool TryMoveNext(ReadResult result, CancellationToken cancellationToken, out bool completed)
    {
        if (result.IsCanceled)
            ThrowHelper.ThrowOperationCanceled(cancellationToken);
        return TryMoveNext(result, hasRead: true, out completed);
    }

    // The underlying reader reported completion, so the wire is at EOF. Disarm the deferred advance by
    // clearing the pending-segment sentinel: a re-drive past completion (recovery drain, or any caller
    // that keeps pulling after false) must not re-apply a stale consume position, whose segment and
    // backing array have since been consumed and pool-recycled, driving the buffer accounting negative.
    // Returns false so completion sites read as return EndOfData().
    bool EndOfData()
    {
        _consumePosition = null;
        _examinedPosition = default;
        _currentSegmentStart = default;
        _currentLength = -1;
        _currentSegmentReadPending = 0;
        return false;
    }

    static bool IsEmptyCompletion(in ReadResult result)
        => result.IsCompleted && result.Buffer.IsEmpty;

    static void ThrowIfTruncatedCompletion(in ReadResult result)
    {
        if (result.IsCompleted)
            throw new EndOfStreamException("The pipe completed within a framed segment.");
    }

    ValueTask<bool> IAsyncEnumerator<TSegment>.MoveNextAsync() => MoveNextAsync(CancellationToken.None);

    // Nonblocking counterpart to MoveNext/MoveNextAsync. It consumes every byte already available
    // from the reader and returns false only when another physical read is required. completed
    // distinguishes that would-block from EOF. This is the poll primitive used by read-wake drivers:
    // after one leaf wake they re-enter here and synchronously descend framing again.
    public bool TryMoveNext(out bool completed)
        => TryMoveNext(default, hasRead: false, out completed);

    // Releases a consumed prefix of a partially buffered segment and polls for its next bytes. The
    // returned buffer never crosses the segment boundary; normal MoveNext resumes at that boundary.
    public bool TryContinueCurrentSegment(SequencePosition consumed, long consumedLength, out CurrentSegmentBuffer result)
    {
        PrepareCurrentSegmentRead(consumed, consumedLength, mode: 1);
        if (!reader.TryRead(out var readResult))
        {
            result = default;
            return false;
        }

        result = CompleteCurrentSegmentRead(readResult);
        return true;
    }

    public bool TryExtendCurrentSegment(out CurrentSegmentBuffer result)
    {
        PrepareCurrentSegmentRead(_currentSegmentStart, consumedLength: 0, mode: 2);
        if (!reader.TryRead(out var readResult))
        {
            result = default;
            return false;
        }

        result = CompleteCurrentSegmentRead(readResult);
        return true;
    }

    public ValueTask<CurrentSegmentBuffer> ContinueCurrentSegmentAsync(
        SequencePosition consumed, long consumedLength, CancellationToken cancellationToken = default)
    {
        PrepareCurrentSegmentRead(consumed, consumedLength, mode: 1);
        var task = reader.ReadAsync(cancellationToken);
        return task.IsCompletedSuccessfully
            ? new(CompleteCurrentSegmentRead(task.Result, cancellationToken))
            : Core(task, cancellationToken);

#if !NET11_0_OR_GREATER
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        async ValueTask<CurrentSegmentBuffer> Core(ValueTask<ReadResult> task, CancellationToken cancellationToken)
            => CompleteCurrentSegmentRead(await task.ConfigureAwait(false), cancellationToken);
    }

    public ValueTask<CurrentSegmentBuffer> ExtendCurrentSegmentAsync(CancellationToken cancellationToken = default)
    {
        PrepareCurrentSegmentRead(_currentSegmentStart, consumedLength: 0, mode: 2);
        var task = reader.ReadAtLeastAsync((int)Math.Min(_currentLength, int.MaxValue), cancellationToken);
        return task.IsCompletedSuccessfully
            ? new(CompleteCurrentSegmentRead(task.Result, cancellationToken))
            : Core(task, cancellationToken);

#if !NET11_0_OR_GREATER
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        async ValueTask<CurrentSegmentBuffer> Core(ValueTask<ReadResult> task, CancellationToken cancellationToken)
            => CompleteCurrentSegmentRead(await task.ConfigureAwait(false), cancellationToken);
    }

    public CurrentSegmentBuffer ContinueCurrentSegment(
        SequencePosition consumed, long consumedLength, TimeSpan timeout = default)
    {
        if (reader is not StreamPipeReader syncReader)
            throw new NotSupportedException("Underlying pipe reader does not support synchronous reads.");

        PrepareCurrentSegmentRead(consumed, consumedLength, mode: 1);
        return CompleteCurrentSegmentRead(syncReader.Read(timeout));
    }

    public CurrentSegmentBuffer ExtendCurrentSegment(TimeSpan timeout = default)
    {
        if (reader is not StreamPipeReader syncReader)
            throw new NotSupportedException("Underlying pipe reader does not support synchronous reads.");

        PrepareCurrentSegmentRead(_currentSegmentStart, consumedLength: 0, mode: 2);
        return CompleteCurrentSegmentRead(syncReader.ReadAtLeast((int)Math.Min(_currentLength, int.MaxValue), timeout));
    }

    void PrepareCurrentSegmentRead(SequencePosition consumed, long consumedLength, byte mode)
    {
        if (_currentSegmentReadPending != 0)
        {
            if (_currentSegmentReadPending != mode)
                ThrowHelper.ThrowInvalidOperation("The pending segment read uses a different continuation mode.");
            return;
        }
        if (_currentLength <= 0 || _consumePosition is not null)
            ThrowHelper.ThrowInvalidOperation("The current segment is not awaiting more data.");
        if (mode == 1 && (consumedLength <= 0 || consumedLength >= _currentLength))
            throw new ArgumentOutOfRangeException(nameof(consumedLength));

        reader.AdvanceTo(consumed, _examinedPosition);
        _currentLength -= consumedLength;
        _currentSegmentReadPending = mode;
    }

    CurrentSegmentBuffer CompleteCurrentSegmentRead(ReadResult result,
        CancellationToken cancellationToken = default)
    {
        _currentSegmentReadPending = 0;
        if (result.IsCanceled)
            ThrowHelper.ThrowOperationCanceled(cancellationToken);
        if (result.IsCompleted && result.Buffer.Length < _currentLength)
            throw new EndOfStreamException("The pipe completed within a framed segment.");

        var isComplete = result.Buffer.Length >= _currentLength;
        var buffer = isComplete ? result.Buffer.Slice(0, _currentLength) : result.Buffer;
        _currentSegmentStart = result.Buffer.Start;
        _consumePosition = isComplete ? buffer.End : null;
        _examinedPosition = buffer.End;
        return new(buffer, isComplete);
    }

    bool TryMoveNext(ReadResult suppliedRead, bool hasRead, out bool completed)
    {
        completed = false;

        if (_currentLength is not -1)
        {
            var segmentReadPending = _currentSegmentReadPending != 0;
            _currentSegmentReadPending = 0;
            if (_consumePosition is null)
            {
                // A supplied read is already the result of the advance performed by the poll which
                // returned false. Advancing again would retire that result before we inspect it.
                if (!segmentReadPending && !hasRead)
                    reader.AdvanceTo(_currentSegmentStart, _examinedPosition);
                if (!TryTakeRead(out var consumeResult))
                    return false;
                if (IsEmptyCompletion(consumeResult))
                    ThrowIfTruncatedCompletion(consumeResult);
                if (consumeResult.IsCanceled)
                    ThrowHelper.ThrowOperationCanceled(CancellationToken.None);
                if (consumeResult.Buffer.Length < _currentLength)
                {
                    ThrowIfTruncatedCompletion(consumeResult);
                    reader.AdvanceTo(consumeResult.Buffer.Start, consumeResult.Buffer.End);
                    return false;
                }
                reader.AdvanceTo(consumeResult.Buffer.GetPosition(_currentLength));
                _consumePosition = null;
                _currentLength = -1;
            }
            else
            {
                reader.AdvanceTo(_consumePosition.GetValueOrDefault(), _examinedPosition);
                _consumePosition = null;
                _currentLength = -1;
            }
        }

        if (!TryTakeRead(out var result))
            return false;
        if (IsEmptyCompletion(result))
        {
            completed = true;
            return EndOfData();
        }
        if (result.IsCanceled)
            ThrowHelper.ThrowOperationCanceled(CancellationToken.None);

        var status = _segmenter.CreateSegment(result.Buffer, out _currentLength, out _current);
        switch (status)
        {
            case OperationStatus.NeedMoreData when _currentLength > 0:
            case OperationStatus.Done:
                ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_currentLength, "segmentLength");
                _consumePosition = _currentLength <= result.Buffer.Length ? result.Buffer.GetPosition(_currentLength) : null;
                _currentSegmentStart = result.Buffer.Start;
                _examinedPosition = _consumePosition ?? result.Buffer.End;
                return true;
            case OperationStatus.NeedMoreData:
                ThrowIfTruncatedCompletion(result);
                reader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
                _currentLength = -1;
                return false;
            case OperationStatus.InvalidData:
                reader.Complete(new Exception("Segmenter encountered invalid data."));
                completed = true;
                return false;
            case OperationStatus.DestinationTooSmall:
                ThrowHelper.ThrowInvalidOperation();
                return default;
            case var value:
                ThrowHelper.ThrowUnhandledCase(value);
                return default;
        }

        bool TryTakeRead(out ReadResult result)
        {
            if (hasRead)
            {
                result = suppliedRead;
                suppliedRead = default;
                hasRead = false;
                return true;
            }
            return reader.TryRead(out result);
        }
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        ValueTask<ReadResult> task;
        ReadResult result;

        // Advance past current segment.
        if (_currentLength is not -1)
        {
            var segmentReadPending = _currentSegmentReadPending != 0;
            _currentSegmentReadPending = 0;
            // Not everything was buffered when the segment was returned (e.g. with length prefixed segments).
            if (_consumePosition is null)
            {
                if (!segmentReadPending)
                    reader.AdvanceTo(_currentSegmentStart, _examinedPosition);
                task = reader.ReadAtLeastAsync((int)long.Min(_currentLength, int.MaxValue), cancellationToken);
                if (!task.IsCompletedSuccessfully)
                    return Core(task, cancellationToken, consume: true);
                result = task.Result;
                if (IsEmptyCompletion(result))
                    ThrowIfTruncatedCompletion(result);
                if (result.IsCanceled)
                    return new(Task.FromException<bool>(new OperationCanceledException(cancellationToken)));

                if (result.Buffer.Length < _currentLength)
                {
                    ThrowIfTruncatedCompletion(result);
                    return Core(new(result), cancellationToken, consume: true);
                }
                if (result.Buffer.Length > _currentLength)
                    return Core(new(result), cancellationToken, consume: true);
                reader.AdvanceTo(result.Buffer.GetPosition(_currentLength));
                _consumePosition = null;
            }
            else
            {
                reader.AdvanceTo(_consumePosition.GetValueOrDefault(), _examinedPosition);
                _consumePosition = null;
            }
        }

        task = reader.ReadAtLeastAsync(_segmenter.MinimumSize, cancellationToken);
        if (!task.IsCompletedSuccessfully)
            return Core(task, cancellationToken);

        result = task.Result;
        if (IsEmptyCompletion(result))
            return new(EndOfData());
        if (result.IsCanceled)
            return new(Task.FromException<bool>(new OperationCanceledException(cancellationToken)));

        var status = _segmenter.CreateSegment(result.Buffer, out _currentLength, out _current);
        switch (status)
        {
            case OperationStatus.NeedMoreData when _currentLength > 0:
            case OperationStatus.Done:
                ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_currentLength, "segmentLength");
                _consumePosition = _currentLength <= result.Buffer.Length ? result.Buffer.GetPosition(_currentLength) : null;
                _currentSegmentStart = result.Buffer.Start;
                // Stop examined at the segment boundary so trailing buffered bytes (next segment's data) stay visible to the next ReadAsync.
                _examinedPosition = _consumePosition ?? result.Buffer.End;
                return new(true);
            case OperationStatus.DestinationTooSmall:
                ThrowHelper.ThrowInvalidOperation();
                return default;
            case OperationStatus.NeedMoreData:
                ThrowIfTruncatedCompletion(result);
                return Core(new(result), cancellationToken, needMoreData: true);
            case OperationStatus.InvalidData:
                return InvalidData();
            case var value:
                ThrowHelper.ThrowUnhandledCase(value);
                return default;
        }


#if !NET11_0_OR_GREATER
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        async ValueTask<bool> Core(ValueTask<ReadResult> task, CancellationToken cancellationToken, bool consume = false, bool needMoreData = false)
        {
            while (true)
            {
                var result = await task.ConfigureAwait(false);
                if (IsEmptyCompletion(result))
                {
                    if (consume || needMoreData)
                        ThrowIfTruncatedCompletion(result);
                    return EndOfData();
                }
                if (result.IsCanceled)
                    ThrowHelper.ThrowOperationCanceled(cancellationToken);

                if (consume)
                {
                    if (result.Buffer.Length < _currentLength)
                    {
                        ThrowIfTruncatedCompletion(result);
                        reader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
                        task = reader.ReadAsync(cancellationToken);
                        continue;
                    }
                    reader.AdvanceTo(result.Buffer.GetPosition(_currentLength));
                    task = reader.ReadAtLeastAsync(_segmenter.MinimumSize, cancellationToken);
                    consume = false;
                    continue;
                }
                if (needMoreData)
                {
                    reader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
                    _currentLength = -1;
                    task = reader.ReadAtLeastAsync(_segmenter.MinimumSize, cancellationToken);
                    needMoreData = false;
                    continue;
                }

                var status = _segmenter.CreateSegment(result.Buffer, out _currentLength, out _current);
                switch (status)
                {
                    case OperationStatus.NeedMoreData when _currentLength > 0:
                    case OperationStatus.Done:
                        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_currentLength, "segmentLength");
                        _consumePosition = _currentLength <= result.Buffer.Length ? result.Buffer.GetPosition(_currentLength) : null;
                        _currentSegmentStart = result.Buffer.Start;
                        // Stop examined at the segment boundary so trailing buffered bytes stay visible to the next ReadAsync.
                        _examinedPosition = _consumePosition ?? result.Buffer.End;
                        return true;
                    case OperationStatus.DestinationTooSmall:
                        ThrowHelper.ThrowInvalidOperation();
                        return default;
                    case OperationStatus.NeedMoreData:
                        ThrowIfTruncatedCompletion(result);
                        needMoreData = true;
                        break;
                    case OperationStatus.InvalidData:
                        return await InvalidData().ConfigureAwait(false);
                    case var value:
                        ThrowHelper.ThrowUnhandledCase(value);
                        return default;
                }
            }
        }

        async ValueTask<bool> InvalidData()
        {
            await reader.CompleteAsync(new Exception("Segmenter encountered invalid data.")).ConfigureAwait(false);
            return false;
        }
    }

    bool IEnumerator.MoveNext() => MoveNext(default(TimeSpan));
    public bool MoveNext(TimeSpan timeout = default)
    {
        if (reader is not StreamPipeReader syncReader)
            throw new NotSupportedException("Underlying pipe reader does not support synchronous reads.");

        ReadResult result;
        var consume = false;
        var needMoreData = false;

        // Advance past current segment.
        if (_currentLength is not -1)
        {
            var segmentReadPending = _currentSegmentReadPending != 0;
            _currentSegmentReadPending = 0;
            if (_consumePosition is null)
            {
                if (!segmentReadPending)
                    reader.AdvanceTo(_currentSegmentStart, _examinedPosition);
                result = syncReader.ReadAtLeast((int)long.Min(_currentLength, int.MaxValue), timeout);
                if (IsEmptyCompletion(result))
                    ThrowIfTruncatedCompletion(result);
                if (result.IsCanceled)
                    ThrowHelper.ThrowOperationCanceled(CancellationToken.None);

                if (result.Buffer.Length < _currentLength)
                {
                    ThrowIfTruncatedCompletion(result);
                    consume = true;
                    goto loop;
                }
                if (result.Buffer.Length > _currentLength)
                {
                    consume = true;
                    goto loop;
                }
                reader.AdvanceTo(result.Buffer.GetPosition(_currentLength));
                _consumePosition = null;
            }
            else
            {
                reader.AdvanceTo(_consumePosition.GetValueOrDefault(), _examinedPosition);
                _consumePosition = null;
            }
        }

        result = syncReader.ReadAtLeast(_segmenter.MinimumSize, timeout);

        loop:
        while (true)
        {
            if (IsEmptyCompletion(result))
            {
                if (consume || needMoreData)
                    ThrowIfTruncatedCompletion(result);
                return EndOfData();
            }
            if (result.IsCanceled)
                ThrowHelper.ThrowOperationCanceled(CancellationToken.None);

            if (consume)
            {
                if (result.Buffer.Length < _currentLength)
                {
                    ThrowIfTruncatedCompletion(result);
                    reader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
                    result = syncReader.Read(timeout);
                    continue;
                }
                reader.AdvanceTo(result.Buffer.GetPosition(_currentLength));
                result = syncReader.ReadAtLeast(_segmenter.MinimumSize, timeout);
                consume = false;
                continue;
            }
            if (needMoreData)
            {
                reader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
                _currentLength = -1;
                result = syncReader.ReadAtLeast(_segmenter.MinimumSize, timeout);
                needMoreData = false;
                continue;
            }

            var status = _segmenter.CreateSegment(result.Buffer, out _currentLength, out _current);
            switch (status)
            {
                case OperationStatus.NeedMoreData when _currentLength > 0:
                case OperationStatus.Done:
                    ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_currentLength, "segmentLength");
                    _consumePosition = _currentLength <= result.Buffer.Length ? result.Buffer.GetPosition(_currentLength) : null;
                    _currentSegmentStart = result.Buffer.Start;
                    // Stop examined at the segment boundary so trailing buffered bytes stay visible to the next ReadAsync.
                    _examinedPosition = _consumePosition ?? result.Buffer.End;
                    return true;
                case OperationStatus.DestinationTooSmall:
                    ThrowHelper.ThrowInvalidOperation();
                    return default;
                case OperationStatus.NeedMoreData:
                    ThrowIfTruncatedCompletion(result);
                    needMoreData = true;
                    break;
                case OperationStatus.InvalidData:
                    reader.Complete(new Exception("Segmenter encountered invalid data."));
                    return false;
                case var value:
                    ThrowHelper.ThrowUnhandledCase(value);
                    return default;
            }
        }
    }

    public TSegment Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _current;
    }

    public void Dispose()
    {
        if (ownsReader)
            reader.Complete();
    }

    public ValueTask DisposeAsync()
    {
        if (ownsReader)
            return reader.CompleteAsync();
        return new();
    }

    object? IEnumerator.Current => Current;
    void IEnumerator.Reset() => throw new NotSupportedException();
}
