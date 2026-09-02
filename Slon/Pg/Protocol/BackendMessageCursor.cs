using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pipelines;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol;

// The cursor is perf sensitive.
struct BackendMessageCursor(ReadOnlySequence<byte> buffer)
{
    public const int DefaultDataRowStreamingThreshold = 16 * 1024;
    const uint MaxMessageLength = 0x3FFF_FFFF;

    FastReadOnlySequence<byte> _buffer = new(buffer);
    long _initialLength = buffer.Length;
    readonly int _dataRowStreamingThreshold = DefaultDataRowStreamingThreshold;
    long _requiredBufferedLength;

    internal BackendMessageCursor(
        ReadOnlySequence<byte> buffer, int dataRowStreamingThreshold) : this(buffer)
        => _dataRowStreamingThreshold = dataRowStreamingThreshold;

    BackendMessageCursor(ReadOnlySequence<byte> buffer,
        int dataRowStreamingThreshold, long initialLength)
        : this(buffer, dataRowStreamingThreshold)
        => _initialLength = initialLength;

    public readonly long ConsumedLength => _initialLength - _buffer.Length;
    public readonly long RequiredBufferedLength => _requiredBufferedLength;
    public readonly SequencePosition UnreadStart => _buffer.Sequence.Start;

    public readonly long GetCurrentMessageOffset(long currentBufferedLength)
        => _initialLength - _buffer.Length - currentBufferedLength;

    public readonly BackendMessageCursor Slice(long offset)
    {
        return new(_buffer.Sequence.Slice(offset),
            _dataRowStreamingThreshold, _initialLength);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    public bool TryReadNextInPlace(out BackendHeader header, out ReadOnlySequence<byte> buffer, out uint bufferLength)
    {
        if (!Header.TryParse(_buffer.FirstSpan, out var protoHeader) && !Header.TryParseMultiSegment(_buffer.Sequence, out protoHeader))
        {
            _requiredBufferedLength = ConsumedLength + Header.ByteCount;
            buffer = default;
            bufferLength = default;
            header = default;
            return false;
        }

        var backendType = (BackendType)protoHeader.Tag;
        if (protoHeader.MessageLength > MaxMessageLength)
            ThrowMessageTooLong(protoHeader.MessageLength);
        var required = backendType is BackendType.DataRow
            ? Math.Min(protoHeader.MessageLength, (uint)_dataRowStreamingThreshold)
            : protoHeader.MessageLength;
        if (_buffer.Length < required)
        {
            _requiredBufferedLength = ConsumedLength + required;
            buffer = default;
            bufferLength = default;
            header = default;
            return false;
        }

        var fastSeq = _buffer.SplitInPlace(Math.Min(_buffer.Length, protoHeader.MessageLength));
        _requiredBufferedLength = 0;
        buffer = fastSeq.Sequence;
        Debug.Assert(fastSeq.Length <= uint.MaxValue);
        bufferLength = unchecked((uint)fastSeq.Length);
        header = BackendHeader.FromHeader(protoHeader);
        return true;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [DoesNotReturn]
    static void ThrowMessageTooLong(uint messageLength)
        => throw new PgFramingException(
            $"PostgreSQL backend message length {messageLength} exceeds the maximum supported length.");

    public readonly bool TryReadNext(out BackendHeader header, out ReadOnlySequence<byte> buffer, out uint bufferLength, out BackendMessageCursor remaining)
    {
        var thisCopy = this;
        var success = thisCopy.TryReadNextInPlace(out header, out buffer, out bufferLength);
        remaining = success ? thisCopy : default;
        return success;
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
                if (_startObject is T[] array)
                {
                    Debug.Assert(ReferenceEquals(_startObject, _endObject));
                    return array.AsSpan(_startIndex, _endIndex - _startIndex);
                }
                var memory = _startObject is MemoryManager<T> manager
                    ? manager.Memory
                    : ((ReadOnlySequenceSegment<T>)_startObject).Memory;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FastReadOnlySequence<T> SplitInPlace(long offset)
        {
            var firstEnd = ReferenceEquals(_startObject, _endObject)
                ? _endIndex
                : FirstMemory.Length;
            var firstLength = firstEnd - _startIndex;
            if (offset == _length)
            {
                var exhausted = this;
                _startObject = _endObject;
                _startIndex = _endIndex;
                _length = 0;
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

            return SplitSlow(offset);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        FastReadOnlySequence<T> SplitSlow(long offset)
        {
            var sequence = Sequence;
            var prefix = sequence.Slice(0, offset);
            var remaining = sequence.Slice(offset);
            var result = new FastReadOnlySequence<T>(prefix);
            this = new(remaining);
            return result;
        }

    }
}
