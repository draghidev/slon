using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Slon.Pipelines;

sealed class SegmentChainBuilder : IDisposable
{
    const int InitialSegmentPoolCapacity = 4;
    const int MaxSegmentPoolCapacity = 256;

    const int MaxBufferSize = 2048 * 1024;
    const int DefaultMaxConsolidationSize = 2048; // This must be less than MaxBufferSize.

    int MinimumBufferSize { get; }
    MemoryPool<byte> Pool { get; }
    int MinimumReserveSize { get; }

    // Mutable struct! Don't make this readonly
    BufferSegmentStack _bufferSegmentPool;
    readonly int _maxPooledSize;

    // Used when bytes have been consolidated into a single segment, the previous segment still has to be returned.
    readonly int _maxConsolidationSize;
    BufferSegment? _consolidated;
    int _consolidatedIndex;

    BufferSegment? _head;
    int _index;

    BufferSegment? _tail;
    long _bufferedBytes;
    bool _disposed;

    public SegmentChainBuilder(MemoryPool<byte> memoryPool, int minimumBufferSize, int minimumReserveSize = 0)
    {
        ArgumentNullException.ThrowIfNull(memoryPool);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(minimumBufferSize, MaxBufferSize);
        ArgumentOutOfRangeException.ThrowIfNegative(minimumReserveSize);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(minimumReserveSize, MaxBufferSize);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(minimumReserveSize, minimumBufferSize);

        if (minimumReserveSize is 0)
        {
            minimumReserveSize = 1;
            _maxConsolidationSize = DefaultMaxConsolidationSize;
        }
        else
        {
            _maxConsolidationSize = Math.Max(minimumBufferSize - minimumReserveSize, DefaultMaxConsolidationSize);
        }
        _maxPooledSize = memoryPool != MemoryPool<byte>.Shared ? Math.Min(memoryPool.MaxBufferSize, MaxBufferSize) : -1;
        _bufferSegmentPool = new BufferSegmentStack(InitialSegmentPoolCapacity);
        MinimumBufferSize = minimumBufferSize;
        Pool = memoryPool;
        MinimumReserveSize = minimumReserveSize;
    }

    public int MaximumBufferSize => MaxBufferSize;

    public long BufferedBytes => _bufferedBytes;

    public (BufferSegment? Head, int Index) HeadInfo => (_head, _index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySequence<byte> GetReadOnlySequence()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // If _readHead is null then _readTail is also null
        return _head is null ? default :
            _head == _tail && _head.Array is { } array
                ? new ReadOnlySequence<byte>(array, _index, _head.End - _index)
                : new ReadOnlySequence<byte>(_head, _index, _tail!, _tail!.End);
    }

    public void Grow(int bytes)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (bytes < 0 || bytes > (_tail?.WritableBytes ?? 0))
            ThrowArgumentOutOfRangeException();

        _tail?.End += bytes;
        _bufferedBytes += bytes;

        [DoesNotReturn]
        static void ThrowArgumentOutOfRangeException()
            => throw new ArgumentOutOfRangeException(nameof(bytes));
    }

    public long AdvanceTo(SequencePosition consumed) => AdvanceTo(consumed, consumed);
    [MethodImpl(MethodImplOptions.NoInlining)]
    public long AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var consumedObject = consumed.GetObject();
        var examinedObject = examined.GetObject();

        if (consumedObject is null || examinedObject is null || _head is null)
        {
            ThrowInvalidCursor();
            return 0;
        }

        BufferSegment consumedSegment;
        BufferSegment examinedSegment;
        if (ReferenceEquals(consumedObject, examinedObject))
        {
            // This may be a segment bashed to an array for perf reasons, do a check and take the segment instance.
            if (consumedObject is not BufferSegment segment)
            {
                if (ReferenceEquals(_head.Array, consumedObject))
                {
                    examinedSegment = consumedSegment = _head;
                }
                else
                {
                    // Trigger an invalid cast exception.
                    _ = (BufferSegment)consumedObject;
                    return 0;
                }
            }
            else
            {
                examinedSegment = consumedSegment = segment;
            }
        }
        else
        {
            consumedSegment = (BufferSegment)consumedObject;
            examinedSegment = (BufferSegment)examinedObject;
        }

        return Core(consumedSegment, consumed.GetInteger(), examinedSegment, examined.GetInteger());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long Core(BufferSegment consumedSegment, int consumedIndex, BufferSegment examinedSegment, int examinedIndex)
        {
            var consumedBytes = BufferSegment.GetLength(_head, _index, consumedSegment, consumedIndex);
            var examinedBytes =
                ReferenceEquals(consumedSegment, examinedSegment) && consumedIndex == examinedIndex
                    ? consumedBytes
                    : BufferSegment.GetLength(_head, _index, examinedSegment, examinedIndex);

            // Once we advance past the consolidated data we return the active, unlinked, segment.
            // We could opt to return this segment during consolidation, it just makes the ROSeqs we hand out more error prone for users.
            // It's technically invalid to use previous ROSeqs past an Advance but refetching existing data (especially up to examined) isn't always done.
            // For example storing the backing ROM on a field until it's empty, so we opt to delay the segment return instead, it is fairly inexpensive.
            if (_consolidated is not null)
            {
                if (consumedBytes > _consolidated.End - _consolidatedIndex)
                {
                    ReturnSegment(_consolidated);
                    _consolidated = null;
                }
                else
                {
                    _consolidatedIndex += (int)consumedBytes;
                }
            }

            _bufferedBytes -= consumedBytes;
            Debug.Assert(_bufferedBytes >= 0);

            // Two cases here:
            // 1. All data is consumed. If so, we empty clear everything so we don't hold onto any
            // excess memory.
            // 2. A segment is entirely consumed but there is still more data in nextSegments
            //  We are allowed to remove an extra segment. by setting returnEnd to be the next block.
            // 3. We are in the middle of a segment.
            //  Move _readHead and _readIndex to consumedSegment and index
            var returnStart = _head;
            var returnEnd = consumedSegment;
            if (_bufferedBytes is 0)
            {
                returnEnd = null;
                _head = null;
                _tail = null;
                _index = 0;
            }
            else if (consumedIndex == returnEnd.WrittenBytes)
            {
                var nextSegment = returnEnd.NextSegment;
                _head = nextSegment;
                _index = 0;
                returnEnd = nextSegment;
            }
            else
            {
                _head = consumedSegment;
                _index = consumedIndex;
            }

            // Remove all blocks that are freed (except the last one)
            while (returnStart != returnEnd)
            {
                var nextSegment = returnStart.NextSegment!;
                ReturnSegment(returnStart);
                returnStart = nextSegment;
            }

            return examinedBytes;

            void ReturnSegment(BufferSegment segment)
            {
                segment.Reset();

                if (_bufferSegmentPool.Count < MaxSegmentPoolCapacity)
                {
                    _bufferSegmentPool.Push(segment);
                }
            }
        }
    }

    public Memory<byte> Reserve(int sizeHint, bool enforceHint)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfNegative(sizeHint);

        if (sizeHint is 0)
            sizeHint = MinimumReserveSize;

        if (_tail is null || _tail.WritableBytes < sizeHint)
        {
            if (enforceHint && sizeHint > MaxBufferSize)
                ThrowOutOfMemory();

            AllocateTail(sizeHint);
        }

        var result = _tail.AvailableMemory.Slice(_tail.End);
        Debug.Assert(!enforceHint || result.Length >= sizeHint);
        return result;

        static void ThrowOutOfMemory()
            => throw new OutOfMemoryException("Unable to allocate the requested memory size from the underlying buffer pool.");
    }

    [MemberNotNull(nameof(_tail))]
    [MethodImpl(MethodImplOptions.NoInlining)]
    void AllocateTail(int reserveSize)
    {
        if (_head is null)
        {
            Debug.Assert(_tail is null);
            _head = _tail = AllocateSegment(Math.Max(reserveSize, MinimumBufferSize));
            return;
        }

        Debug.Assert(_tail is not null);
        Debug.Assert(_tail.WritableBytes < reserveSize);

        if (_consolidated is null && _head == _tail && TryConsolidateIntoNewHead(reserveSize))
            return;

        var nextSegment = AllocateSegment(Math.Max(reserveSize, MinimumBufferSize));
        _tail.SetNext(nextSegment);
        _tail = nextSegment;

        bool TryConsolidateIntoNewHead(int reserveSize)
        {
            Debug.Assert(_consolidated is null);
            Debug.Assert(_head == _tail);
            // If we're already at the start, there's nothing to compact.
            if (_index == 0)
                return false;

            var remaining = _head.WrittenBytes - _index;
            if (remaining <= 0)
                return false;

            // Only consolidate small payloads and small next reservations to avoid large copies.
            if (remaining > _maxConsolidationSize)
                return false;

            var consolidatedSize = remaining + reserveSize;
            if (consolidatedSize > MaxBufferSize)
                return false;

            var nextSegment = AllocateSegment(Math.Max(consolidatedSize, MinimumBufferSize));
            _head.Memory.Slice(_index, remaining).CopyTo(nextSegment.AvailableMemory);
            nextSegment.End += remaining;

            _consolidated = _head;
            _consolidatedIndex = _index;
            _head = _tail = nextSegment;
            _index = 0;
            return true;
        }

        BufferSegment AllocateSegment(int minimumSize)
        {
            var nextSegment = _bufferSegmentPool.TryPop(out var segment) ? segment : new();

            if (minimumSize <= _maxPooledSize)
            {
                nextSegment.SetOwnedMemory(Pool.Rent(minimumSize));
            }
            else
            {
                nextSegment.SetOwnedMemory(ArrayPool<byte>.Shared.Rent(Math.Min(minimumSize, MaxBufferSize)));
            }

            return nextSegment;
        }
    }

    [DoesNotReturn]
    static void ThrowInvalidCursor() => throw new InvalidOperationException();

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        var nextSegment = _head;
        while (nextSegment != null)
        {
            var segment = nextSegment;
            nextSegment = nextSegment.NextSegment;

            segment.Reset();
        }
    }
}
