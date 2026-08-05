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
    long _consumedLength;

    BackendMessageBatch(ReadOnlySequence<byte> buffer, long consumedLength) : this(buffer)
        => _consumedLength = consumedLength;

    public readonly long ConsumedLength => _consumedLength;

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
        _consumedLength += fastSeq.Length;
        buffer = fastSeq.Sequence;
        Debug.Assert(fastSeq.Length <= uint.MaxValue);
        bufferLength = unchecked((uint)fastSeq.Length);
        header = (BackendHeader)protoHeader;
        return true;
    }

    public readonly bool TryReadNext(out BackendHeader header, out ReadOnlySequence<byte> buffer, out uint bufferLength, out BackendMessageBatch remaining)
    {
        var thisCopy = this;
        var success = thisCopy.TryReadNextInPlace(out header, out buffer, out bufferLength);
        remaining = success ? new(thisCopy._buffer.Sequence, thisCopy._consumedLength) : default;
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
                if (!backendType.IsDefined())
                    throw new PgFramingException($"Unknown PostgreSQL backend message type: {header.Tag}.");
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

    // TODO faster firstspan and splitting should be able to be upstreamed.
    // Optimizes for faster splitting and length checks.
    struct FastReadOnlySequence<T>
    {
        ReadOnlySequence<T> _sequence;
        long _length;

        FastReadOnlySequence(ReadOnlySequence<T> sequence, long length)
        {
            Debug.Assert(Unsafe.SizeOf<FastReadOnlySequence<T>>() is 32);
            _sequence = sequence;
            _length = length;
        }

        public FastReadOnlySequence(ReadOnlySequence<T> sequence)
        {
            Debug.Assert(Unsafe.SizeOf<FastReadOnlySequence<T>>() is 32);
            _sequence = sequence;
            _length = sequence.Length;
        }

        public ReadOnlySequence<T> Sequence => _sequence;
        public long Length => _length;

        public ReadOnlySpan<T> FirstSpan => GetFirstSpan(out _);

        // Returns the sequence before the index, stores the sequence after it in place.
        public FastReadOnlySequence<T> SplitInPlace(long offset)
        {
            FastReadOnlySequence<T> prev;

            // If it's out-of-range of the first, has to resolve next segment, or not an array, let slice handle it.
            if (GetFirstSpan(out var array).Length <= offset || array.Array is null)
            {
                prev = new(_sequence.Slice(0, offset), offset);
                _sequence = _sequence.Slice(offset);
            }
            else
            {
                Debug.Assert(offset <= int.MaxValue);
                Debug.Assert(_sequence.Start.GetInteger() == StartInteger(ref _sequence), "Unexpected flags on single segment start integer.");
                prev = new(new(array.Array, array.Offset, (int)offset), offset);
                StartInteger(ref _sequence) += (int)offset;
            }

            _length -= offset;
            return prev;
        }

        // TODO arrays should not go down the slow path for First and FirstSpan, the SequenceReader variant doesn't either.
        // Inline to remove the write barriers.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<T> GetFirstSpan(out ArraySegment<T> array)
        {
            var startObject = StartObject(ref _sequence);
            if (startObject is not null && startObject.GetType() == typeof(T[]))
            {
                Debug.Assert(_sequence.IsSingleSegment);
                Debug.Assert(_sequence.Start.GetInteger() == StartInteger(ref _sequence), "Unexpected flags on single segment start integer.");
                var offset = StartInteger(ref _sequence);
                array = new ArraySegment<T>(Unsafe.As<T[]>(startObject), offset, GetIndex(default, EndInteger(ref _sequence)) - offset);
                return array.AsSpan();
            }

            array = default;
            return _sequence.FirstSpan;
        }

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_startObject")]
        static extern ref object? StartObject(ref ReadOnlySequence<T> buffer);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_startInteger")]
        static extern ref int StartInteger(ref ReadOnlySequence<T> buffer);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_endInteger")]
        static extern ref int EndInteger(ref ReadOnlySequence<T> buffer);

        [UnsafeAccessor(UnsafeAccessorKind.StaticMethod, Name = "GetIndex")]
        static extern int GetIndex(ReadOnlySequence<T> buffer, int indexAndFlags);
    }
}
