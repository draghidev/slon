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

sealed class PipeSegmentEnumerator<TSegmenter, TSegment>(PipeReader reader, TSegmenter segmenter, bool ownsReader = false)
    : IEnumerator<TSegment>, IAsyncEnumerator<TSegment>
    where TSegmenter: IPipeSegmenter<TSegment>
{
    TSegmenter _segmenter = segmenter;
    TSegment _current = default!;

    SequencePosition _examinedPosition;
    SequencePosition? _consumePosition;
    long _currentLength = -1;

    public PipeReader PipeReader => reader;

    ValueTask<bool> IAsyncEnumerator<TSegment>.MoveNextAsync() => MoveNextAsync(CancellationToken.None);
    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        ValueTask<ReadResult> task;
        ReadResult result;

        // Advance past current segment.
        if (_currentLength is not -1)
        {
            // Not everything was buffered when the segment was returned (e.g. with length prefixed segments).
            if (_consumePosition is null)
            {
                task = reader.ReadAtLeastAsync(int.Max((int)_currentLength, int.MaxValue), cancellationToken);
                if (!task.IsCompletedSuccessfully)
                    return Core(task, cancellationToken, consume: true);
                result = task.Result;
                if (result.IsCompleted)
                    return new(false);
                if (result.IsCanceled)
                    return new(Task.FromException<bool>(new OperationCanceledException(cancellationToken)));

                if (result.Buffer.Length > _currentLength)
                    return Core(new(result), cancellationToken, consume: true);
                reader.AdvanceTo(result.Buffer.GetPosition(_currentLength));
            }
            else
            {
                reader.AdvanceTo(_consumePosition.GetValueOrDefault(), _examinedPosition);
            }
        }

        task = reader.ReadAtLeastAsync(_segmenter.MinimumSize, cancellationToken);
        if (!task.IsCompletedSuccessfully)
            return Core(task, cancellationToken);

        result = task.Result;
        if (result.IsCompleted)
            return new(false);
        if (result.IsCanceled)
            return new(Task.FromException<bool>(new OperationCanceledException(cancellationToken)));

        var status = _segmenter.CreateSegment(result.Buffer, out _currentLength, out _current);
        switch (status)
        {
            case OperationStatus.NeedMoreData when _currentLength > 0:
            case OperationStatus.Done:
                ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_currentLength, "segmentLength");
                _consumePosition = _currentLength <= result.Buffer.Length ? result.Buffer.GetPosition(_currentLength) : null;
                // Stop examined at the segment boundary so trailing buffered bytes (next segment's data) stay visible to the next ReadAsync.
                _examinedPosition = _consumePosition ?? result.Buffer.End;
                return new(true);
            case OperationStatus.DestinationTooSmall:
                ThrowHelper.ThrowInvalidOperation();
                return default;
            case OperationStatus.NeedMoreData:
                return Core(new(result), cancellationToken, needMoreData: true);
            case OperationStatus.InvalidData:
                return InvalidData();
            case var value:
                ThrowHelper.ThrowUnhandledCase(value);
                return default;
        }


        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        async ValueTask<bool> Core(ValueTask<ReadResult> task, CancellationToken cancellationToken, bool consume = false, bool needMoreData = false)
        {
            while (true)
            {
                var result = await task.ConfigureAwait(false);
                if (result.IsCompleted)
                    return false;
                if (result.IsCanceled)
                    ThrowHelper.ThrowOperationCanceled(cancellationToken);

                if (consume)
                {
                    if (result.Buffer.Length < _currentLength)
                    {
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
                        // Stop examined at the segment boundary so trailing buffered bytes stay visible to the next ReadAsync.
                        _examinedPosition = _consumePosition ?? result.Buffer.End;
                        return true;
                    case OperationStatus.DestinationTooSmall:
                        ThrowHelper.ThrowInvalidOperation();
                        return default;
                    case OperationStatus.NeedMoreData:
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
            if (_consumePosition is null)
            {
                result = syncReader.ReadAtLeast(int.Max((int)_currentLength, int.MaxValue), timeout);
                if (result.IsCompleted)
                    return false;
                if (result.IsCanceled)
                    ThrowHelper.ThrowOperationCanceled(CancellationToken.None);

                if (result.Buffer.Length > _currentLength)
                {
                    consume = true;
                    goto loop;
                }
                reader.AdvanceTo(result.Buffer.GetPosition(_currentLength));
            }
            else
            {
                reader.AdvanceTo(_consumePosition.GetValueOrDefault(), _examinedPosition);
            }
        }

        result = syncReader.ReadAtLeast(_segmenter.MinimumSize, timeout);

        loop:
        while (true)
        {
            if (result.IsCompleted)
                return false;
            if (result.IsCanceled)
                ThrowHelper.ThrowOperationCanceled(CancellationToken.None);

            if (consume)
            {
                if (result.Buffer.Length < _currentLength)
                {
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
                    // Stop examined at the segment boundary so trailing buffered bytes stay visible to the next ReadAsync.
                    _examinedPosition = _consumePosition ?? result.Buffer.End;
                    return true;
                case OperationStatus.DestinationTooSmall:
                    ThrowHelper.ThrowInvalidOperation();
                    return default;
                case OperationStatus.NeedMoreData:
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
