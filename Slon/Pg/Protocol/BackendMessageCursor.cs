using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Slon.Pipelines;
using Slon.Runtime.CompilerServices;
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
        var bufferSlot = default(StackValue<FastReadOnlySequence<byte>>);
        if (!TryReadNextBuffer(out header, ref bufferSlot, out bufferLength))
        {
            buffer = default;
            return false;
        }
        buffer = bufferSlot.Value.Sequence;
        return true;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal bool TryReadNextBuffer(out BackendHeader header,
        ref StackValue<FastReadOnlySequence<byte>> buffer, out uint bufferLength)
    {
        var bufferedLength = _buffer.Length;
        if (!Header.TryParse(_buffer.FirstSpan, out var protoHeader)
            && (bufferedLength < Header.ByteCount
                || !Header.TryParseMultiSegment(_buffer.Sequence, out protoHeader)))
        {
            _requiredBufferedLength = _initialLength - bufferedLength + Header.ByteCount;
            bufferLength = default;
            header = default;
            return false;
        }

        var backendType = (BackendType)protoHeader.Tag;
        var messageLength = protoHeader.MessageLength;
        if (messageLength > MaxMessageLength)
            ThrowMessageTooLong(messageLength);
        var required = backendType is BackendType.DataRow
            ? Math.Min(messageLength, (uint)_dataRowStreamingThreshold)
            : messageLength;
        if (bufferedLength < required)
        {
            _requiredBufferedLength = _initialLength - bufferedLength + required;
            bufferLength = default;
            header = default;
            return false;
        }

        var result = _buffer.SplitInPlace(Math.Min(bufferedLength, messageLength));
        _requiredBufferedLength = 0;
        Debug.Assert(result.Length <= uint.MaxValue);
        bufferLength = unchecked((uint)result.Length);
        header = BackendHeader.FromHeader(protoHeader);
        buffer.Value = result;
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
    internal struct FastReadOnlySequence<T>
    {
        const int SegmentFlag = int.MinValue;
        const int IndexMask = int.MaxValue;

        object? _startObject;
        object? _endObject;
        int _startIndex;
        int _endIndex;
        long _length;

        FastReadOnlySequence(object? startObject, int startIndex,
            object? endObject, int endIndex, long length, bool segmentBacked)
        {
            Debug.Assert(Unsafe.SizeOf<FastReadOnlySequence<T>>() is 32);
            _startObject = startObject;
            _endObject = endObject;
            _startIndex = EncodeIndex(startIndex, segmentBacked);
            _endIndex = EncodeIndex(endIndex, segmentBacked);
            _length = length;
        }

        public FastReadOnlySequence(ReadOnlySequence<T> sequence)
        {
            Debug.Assert(Unsafe.SizeOf<FastReadOnlySequence<T>>() is 32);
            _startObject = sequence.Start.GetObject();
            _endObject = sequence.End.GetObject();
            var segmentBacked = _startObject is ReadOnlySequenceSegment<T>;
            _startIndex = EncodeIndex(
                sequence.Start.GetInteger() & IndexMask, segmentBacked);
            _endIndex = EncodeIndex(
                sequence.End.GetInteger() & IndexMask, segmentBacked);
            _length = sequence.Length;
        }

        static int EncodeIndex(int index, bool segmentBacked)
            => segmentBacked ? index | SegmentFlag : index;

        readonly bool IsSegmentBacked => _startIndex < 0;

        public ReadOnlySequence<T> Sequence
        {
            get
            {
                if (_startObject is null)
                    return default;
                var startIndex = StartIndex;
                var endIndex = EndIndex;
                if (IsSegmentBacked)
                {
                    return new((ReadOnlySequenceSegment<T>)_startObject, startIndex,
                        (ReadOnlySequenceSegment<T>)_endObject!, endIndex);
                }
                if (_startObject is T[] array)
                {
                    Debug.Assert(ReferenceEquals(_startObject, _endObject));
                    return new(array, startIndex, endIndex - startIndex);
                }
                var manager = (MemoryManager<T>)_startObject;
                Debug.Assert(ReferenceEquals(_startObject, _endObject));
                return new(manager.Memory.Slice(startIndex, endIndex - startIndex));
            }
        }
        public long Length => _length;
        public object? StartObject => _startObject;
        public object? EndObject => _endObject;
        public int StartIndex => _startIndex & IndexMask;
        public int EndIndex => _endIndex & IndexMask;

        public ReadOnlySpan<T> FirstSpan
        {
            get
            {
                if (_startObject is null)
                    return default;
                var startIndex = StartIndex;
                var endIndex = EndIndex;
                if (IsSegmentBacked)
                {
                    var memory = ((ReadOnlySequenceSegment<T>)_startObject).Memory;
                    var end = ReferenceEquals(_startObject, _endObject)
                        ? endIndex
                        : memory.Length;
                    return memory.Span.Slice(startIndex, end - startIndex);
                }
                if (_startObject is T[] array)
                {
                    Debug.Assert(ReferenceEquals(_startObject, _endObject));
                    return array.AsSpan(startIndex, endIndex - startIndex);
                }
                return ((MemoryManager<T>)_startObject).Memory.Span
                    .Slice(startIndex, endIndex - startIndex);
            }
        }

        ReadOnlyMemory<T> FirstMemory
            => IsSegmentBacked
                ? ((ReadOnlySequenceSegment<T>)_startObject!).Memory
                : _startObject switch
                {
                    T[] array => array,
                    MemoryManager<T> manager => manager.Memory,
                    _ => throw new UnreachableException()
                };

        // Returns the sequence before the index, stores the sequence after it in place.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FastReadOnlySequence<T> SplitInPlace(long offset)
        {
            var startIndex = StartIndex;
            var firstEnd = ReferenceEquals(_startObject, _endObject)
                ? EndIndex
                : FirstMemory.Length;
            var firstLength = firstEnd - startIndex;
            if (offset == _length)
            {
                var exhausted = this;
                _startObject = _endObject;
                _startIndex = _endIndex;
                _length = 0;
                return exhausted;
            }
            if (offset == firstLength
                && IsSegmentBacked
                && ((ReadOnlySequenceSegment<T>)_startObject!).Next is { } next)
            {
                var boundaryPrefix = new FastReadOnlySequence<T>(
                    _startObject, startIndex, _startObject, firstEnd, offset,
                    segmentBacked: true);
                _startObject = next;
                _startIndex = SegmentFlag;
                _length -= offset;
                NormalizeSingleSegmentArray();
                return boundaryPrefix;
            }
            if ((ulong)offset < (uint)firstLength)
            {
                var splitIndex = startIndex + (int)offset;
                var prev = new FastReadOnlySequence<T>(
                    _startObject, startIndex, _startObject, splitIndex, offset,
                    IsSegmentBacked);
                _startIndex = EncodeIndex(splitIndex, IsSegmentBacked);
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
            NormalizeSingleSegmentArray();
            return result;
        }

        // ReadOnlySequence.Slice retains segment positions after a multi-segment sequence has
        // advanced wholly into its final segment. PipeReader-style consumers should not continue
        // paying that historical topology: when the remaining segment exposes array memory, carry
        // the actual array and absolute indices from this point forward.
        [MethodImpl(MethodImplOptions.NoInlining)]
        void NormalizeSingleSegmentArray()
        {
            if (!IsSegmentBacked || !ReferenceEquals(_startObject, _endObject))
                return;

            var memory = ((ReadOnlySequenceSegment<T>)_startObject!).Memory;
            if (!MemoryMarshal.TryGetArray(memory, out ArraySegment<T> array))
                return;

            var startIndex = StartIndex;
            var endIndex = EndIndex;
            _startObject = _endObject = array.Array;
            _startIndex = array.Offset + startIndex;
            _endIndex = array.Offset + endIndex;
        }

    }
}
