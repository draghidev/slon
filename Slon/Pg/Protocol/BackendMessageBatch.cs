using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Slon.Pipelines;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol;

// Note: both the batch and the segmenter are perf sensitive.
struct BackendMessageBatch(ReadOnlySequence<byte> buffer)
{
    FastReadOnlySequence<byte> _buffer = new(buffer);
    long _initialLength = buffer.Length;

    public readonly long GetCurrentMessageOffset(long currentBufferedLength)
        => _initialLength - _buffer.Length - currentBufferedLength;

    public readonly BackendMessageBatch Slice(long offset)
    {
        var result = new BackendMessageBatch(_buffer.Sequence.Slice(offset));
        result._initialLength = _initialLength;
        return result;
    }

    public bool TryReadNextInPlace(out BackendHeader header, out ReadOnlySequence<byte> buffer, out uint bufferLength)
    {
        if (!Header.TryParse(_buffer.FirstSpan, out var protoHeader) && !Header.TryParseMultiSegment(_buffer.Sequence, out protoHeader))
        {
            // We use default(ROSeq) - which is fully supported - as ROSeq.Empty weirdly enough wraps an empty array.
            _buffer = default;
            buffer = default;
            bufferLength = default;
            header = default;
            return false;
        }

        var fastSeq = _buffer.SplitInPlace(Math.Min(_buffer.Length, protoHeader.MessageLength));
        buffer = fastSeq.Sequence;
        Debug.Assert(fastSeq.Length <= uint.MaxValue);
        bufferLength = unchecked((uint)fastSeq.Length);
        header = BackendHeader.FromHeader(protoHeader);
        return true;
    }

    public readonly bool TryReadNext(out BackendHeader header, out ReadOnlySequence<byte> buffer, out uint bufferLength, out BackendMessageBatch remaining)
    {
        var thisCopy = this;
        var success = thisCopy.TryReadNextInPlace(out header, out buffer, out bufferLength);
        remaining = success ? thisCopy : default;
        return success;
    }

    // Segmenter parses messages and ensures relevant messages are fully buffered before being returned.
    internal struct Segmenter : IPipeSegmenter<BackendMessageBatch>
    {
        public const int DefaultDataRowStreamingThreshold = 16 * 1024;
        const uint MaxMessageLength = 0x3FFF_FFFF;

        readonly int _dataRowStreamingThreshold;
        int _minimumSize;
        public int MinimumSize => _minimumSize;

        public Segmenter() : this(DefaultDataRowStreamingThreshold) {}

        public Segmenter(int dataRowStreamingThreshold)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(dataRowStreamingThreshold);
            _dataRowStreamingThreshold = dataRowStreamingThreshold;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OperationStatus CreateSegment(in ReadOnlySequence<byte> buffer, out long segmentLength, out BackendMessageBatch segment)
        {
            _minimumSize = Header.ByteCount;
            var reader = new SequenceReader<byte>(buffer);
            var messages = 0;
            var needMoreData = false;
            segmentLength = 0;

            // Try span first before accessing the sequence.
            while (Header.TryParse(reader.UnreadSpan, out var header) || Header.TryParseMultiSegment(reader.UnreadSequence, out header))
            {
                var backendType = (BackendType)header.Tag;
                if (header.MessageLength > MaxMessageLength)
                    throw new PgFramingException($"PostgreSQL backend message length {header.MessageLength} exceeds the maximum supported length.");

                if (reader.Remaining < header.MessageLength)
                {
                    var required = RequiredBufferedLength(backendType, header.MessageLength);
                    if (reader.Remaining < required)
                    {
                        // MinimumSize is relative to the entire unconsumed pipe buffer, including messages
                        // already framed before this one.
                        _minimumSize = int.CreateSaturating(segmentLength + required);
                        needMoreData = true;
                        break;
                    }

                    reader.Advance(reader.Remaining);
                }
                else
                {
                    reader.Advance(header.MessageLength);
                }

                messages++;
                segmentLength += header.MessageLength;
            }

            if (messages is 0)
            {
                segment = default;
                return OperationStatus.NeedMoreData;
            }

            segment = new(reader.Length == segmentLength ? buffer : buffer.Slice(0, reader.Position));
            return needMoreData ? OperationStatus.NeedMoreData : OperationStatus.Done;
        }

        uint RequiredBufferedLength(BackendType backendType, uint messageLength) => backendType switch
        {
            BackendType.DataRow => Math.Min(messageLength, (uint)_dataRowStreamingThreshold),
            // BackendType.RowDescription or
            // BackendType.CopyData or
            // BackendType.FunctionCallResponse or
            // BackendType.NotificationResponse or
            // BackendType.ParameterDescription => false,
            _ => messageLength,
        };
    }

    // Keeps the public position components scalar so consuming the first segment does not repeatedly
    // reconstruct and rediscover the backing of a ReadOnlySequence. Materialize one only at API seams.
    struct FastReadOnlySequence<T>
    {
        object? _startObject;
        object? _endObject;
        int _startIndex;
        int _endIndex;
        long _length;

        FastReadOnlySequence(object? startObject, int startIndex,
            object? endObject, int endIndex, long length)
        {
            Debug.Assert(Unsafe.SizeOf<FastReadOnlySequence<T>>() is 32);
            _startObject = startObject;
            _endObject = endObject;
            _startIndex = startIndex;
            _endIndex = endIndex;
            _length = length;
        }

        public FastReadOnlySequence(ReadOnlySequence<T> sequence)
        {
            Debug.Assert(Unsafe.SizeOf<FastReadOnlySequence<T>>() is 32);
            _startObject = sequence.Start.GetObject();
            _endObject = sequence.End.GetObject();
            _startIndex = sequence.Start.GetInteger() & int.MaxValue;
            _endIndex = sequence.End.GetInteger() & int.MaxValue;
            _length = sequence.Length;
        }

        public ReadOnlySequence<T> Sequence
        {
            get
            {
                if (_startObject is null)
                    return default;
                if (_startObject is T[] array)
                {
                    Debug.Assert(ReferenceEquals(_startObject, _endObject));
                    return new(array, _startIndex, _endIndex - _startIndex);
                }
                if (_startObject is MemoryManager<T> manager)
                {
                    Debug.Assert(ReferenceEquals(_startObject, _endObject));
                    return new(manager.Memory.Slice(
                        _startIndex, _endIndex - _startIndex));
                }
                return new((ReadOnlySequenceSegment<T>)_startObject!, _startIndex,
                    (ReadOnlySequenceSegment<T>)_endObject!, _endIndex);
            }
        }
        public long Length => _length;

        public ReadOnlySpan<T> FirstSpan
        {
            get
            {
                if (_startObject is null)
                    return default;
                var memory = FirstMemory;
                var end = ReferenceEquals(_startObject, _endObject)
                    ? _endIndex
                    : memory.Length;
                return memory.Span.Slice(_startIndex, end - _startIndex);
            }
        }

        ReadOnlyMemory<T> FirstMemory => _startObject switch
        {
            T[] array => array,
            MemoryManager<T> manager => manager.Memory,
            ReadOnlySequenceSegment<T> segment => segment.Memory,
            _ => throw new UnreachableException()
        };

        // Returns the sequence before the index, stores the sequence after it in place.
        public FastReadOnlySequence<T> SplitInPlace(long offset)
        {
            var firstEnd = ReferenceEquals(_startObject, _endObject)
                ? _endIndex
                : FirstMemory.Length;
            var firstLength = firstEnd - _startIndex;
            if (offset == _length)
            {
                var exhausted = this;
                this = default;
                return exhausted;
            }
            if (offset == firstLength
                && _startObject is ReadOnlySequenceSegment<T> segment
                && segment.Next is { } next)
            {
                var boundaryPrefix = new FastReadOnlySequence<T>(
                    segment, _startIndex, segment, firstEnd, offset);
                _startObject = next;
                _startIndex = 0;
                _length -= offset;
                return boundaryPrefix;
            }
            if ((ulong)offset < (uint)firstLength)
            {
                var splitIndex = _startIndex + (int)offset;
                var prev = new FastReadOnlySequence<T>(
                    _startObject, _startIndex, _startObject, splitIndex, offset);
                _startIndex = splitIndex;
                _length -= offset;
                return prev;
            }

            var sequence = Sequence;
            var prefix = sequence.Slice(0, offset);
            var remaining = sequence.Slice(offset);
            var result = new FastReadOnlySequence<T>(prefix);
            this = new(remaining);
            return result;
        }

    }
}
