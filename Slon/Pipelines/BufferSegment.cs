using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Slon.Pipelines;

sealed class BufferSegment : ReadOnlySequenceSegment<byte>
{
    IMemoryOwner<byte>? _memoryOwner;
    BufferSegment? _next;
    int _end;

    /// <summary>
    /// The End represents the offset into AvailableMemory where the range of "active" bytes ends. At the point when the block is leased
    /// the End is guaranteed to be equal to Start. The value of Start may be assigned anywhere between 0 and
    /// Buffer.Length, and must be equal to or less than End.
    /// </summary>
    public int End
    {
        get => _end;
        set
        {
            Debug.Assert(value <= AvailableMemory.Length);

            _end = value;
            Memory = AvailableMemory.Slice(0, value);
        }
    }

    /// <summary>
    /// Reference to the next block of data when the overall "active" bytes spans multiple blocks. At the point when the block is
    /// leased Next is guaranteed to be null. Start, End, and Next are used together in order to create a linked-list of discontiguous
    /// working memory. The "active" memory is grown when bytes are copied in, End is increased, and Next is assigned. The "active"
    /// memory is shrunk when bytes are consumed, Start is increased, and blocks are returned to the pool.
    /// </summary>
    public BufferSegment? NextSegment
    {
        get => _next;
        set
        {
            Next = value;
            _next = value;
        }
    }

    // When the memory is backed by a managed array, it can be retrieved here.
    public byte[]? Array { get; private set; }
    public bool IsBaselineAllocation { get; set; }

    public void SetOwnedMemory(IMemoryOwner<byte> memoryOwner)
    {
        _memoryOwner = memoryOwner;
        AvailableMemory = memoryOwner.Memory;
        if (MemoryMarshal.TryGetArray<byte>(AvailableMemory, out var arraySegment))
            Array = arraySegment.Array;
    }

    public void SetOwnedMemory(byte[] arrayPoolBuffer)
    {
        Array = arrayPoolBuffer;
        AvailableMemory = arrayPoolBuffer;
    }

    // Resets memory and internal state, should be called when removing the segment from the linked list
    public void Reset()
    {
        ResetMemory();

        ResetLinks();
    }

    public void ResetForReuse()
    {
        End = 0;
        ResetLinks();
    }

    void ResetLinks()
    {
        Next = null;
        RunningIndex = 0;
        _next = null;
    }

    // Resets memory only, should be called when keeping the BufferSegment in the linked list and only swapping out the memory
    public void ResetMemory()
    {
        IMemoryOwner<byte>? memoryOwner = _memoryOwner;
        if (memoryOwner != null)
        {
            _memoryOwner = null;
            memoryOwner.Dispose();
        }
        else
        {
            Debug.Assert(Array != null);
            ArrayPool<byte>.Shared.Return(Array);
            Array = null;
        }


        Memory = default;
        _end = 0;
        AvailableMemory = default;
        Array = null;
        IsBaselineAllocation = false;
    }

    // Exposed for testing
    internal object? MemoryOwner => (object?)_memoryOwner ?? Array;

    public Memory<byte> AvailableMemory { get; private set; }

    public int WrittenBytes => End;

    public int WritableBytes
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => AvailableMemory.Length - End;
    }

    public void SetNext(BufferSegment segment)
    {
        Debug.Assert(segment != null);
        Debug.Assert(Next == null);

        NextSegment = segment;

        segment = this;

        while (segment.Next != null)
        {
            Debug.Assert(segment.NextSegment != null);
            segment.NextSegment!.RunningIndex = segment.RunningIndex + segment.WrittenBytes;
            segment = segment.NextSegment;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetLength(BufferSegment startSegment, int startIndex, BufferSegment endSegment, int endIndex)
    {
        return (endSegment.RunningIndex + (uint)endIndex) - (startSegment.RunningIndex + (uint)startIndex);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetLength(long startPosition, BufferSegment endSegment, int endIndex)
    {
        return (endSegment.RunningIndex + (uint)endIndex) - startPosition;
    }
}

struct BufferSegmentStack(int initialCapacity)
{
    SegmentAsValueType[] _array = new SegmentAsValueType[initialCapacity];
    int _size = 0;

    public int Count => _size;

    public bool TryPop([NotNullWhen(true)] out BufferSegment? result)
    {
        int size = _size - 1;
        SegmentAsValueType[] array = _array;

        if ((uint)size >= (uint)array.Length)
        {
            result = default;
            return false;
        }

        _size = size;
        result = array[size];
        array[size] = default;
        return true;
    }

    // Pushes an item to the top of the stack.
    public void Push(BufferSegment item)
    {
        int size = _size;
        SegmentAsValueType[] array = _array;

        if ((uint)size < (uint)array.Length)
        {
            array[size] = item;
            _size = size + 1;
        }
        else
        {
            PushWithResize(item);
        }
    }

    // Non-inline from Stack.Push to improve its code quality as uncommon path
    [MethodImpl(MethodImplOptions.NoInlining)]
    void PushWithResize(BufferSegment item)
    {
        Array.Resize(ref _array, 2 * _array.Length);
        _array[_size] = item;
        _size++;
    }

    // TODO should not be necessary on CoreCLR as long as we address the array with a sealed element type.
    /// <summary>
    /// A simple struct we wrap reference types inside when storing in arrays to
    /// bypass the CLR's covariant checks when writing to arrays.
    /// </summary>
    /// <remarks>
    /// We use <see cref="SegmentAsValueType"/> as a wrapper to avoid paying the cost of covariant checks whenever
    /// the underlying array that the <see cref="BufferSegmentStack"/> class uses is written to.
    /// We've recognized this as a perf win in ETL traces for these stack frames:
    /// clr!JIT_Stelem_Ref
    ///   clr!ArrayStoreCheck
    ///     clr!ObjIsInstanceOf
    /// </remarks>
    readonly struct SegmentAsValueType
    {
        readonly BufferSegment _value;
        SegmentAsValueType(BufferSegment value) => _value = value;
        public static implicit operator SegmentAsValueType(BufferSegment s) => new SegmentAsValueType(s);
        public static implicit operator BufferSegment(SegmentAsValueType s) => s._value;
    }
}

static class PipeThrowHelper
{
    [DoesNotReturn]
    internal static void ThrowArgumentOutOfRangeException(ExceptionArgument argument) => throw CreateArgumentOutOfRangeException(argument);
    [MethodImpl(MethodImplOptions.NoInlining)]
    static Exception CreateArgumentOutOfRangeException(ExceptionArgument argument) => new ArgumentOutOfRangeException(argument.ToString());

    [DoesNotReturn]
    internal static void ThrowArgumentNullException(ExceptionArgument argument) => throw CreateArgumentNullException(argument);
    [MethodImpl(MethodImplOptions.NoInlining)]
    static Exception CreateArgumentNullException(ExceptionArgument argument) => new ArgumentNullException(argument.ToString());

    [DoesNotReturn]
    public static void ThrowInvalidOperationException_NoWritingAllowed() => throw CreateInvalidOperationException_NoWritingAllowed();
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static Exception CreateInvalidOperationException_NoWritingAllowed() => new InvalidOperationException("Writing is not allowed after writer was completed.");

    [DoesNotReturn]
    public static void ThrowInvalidOperationException_AlreadyFlushing() => throw CreateInvalidOperationException_AlreadyFlushing();
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static Exception CreateInvalidOperationException_AlreadyFlushing() => new InvalidOperationException("Concurrent flushes are not supported.");

    [DoesNotReturn]
    public static void ThrowInvalidOperationException_AlreadyReading() => throw CreateInvalidOperationException_AlreadyReading();
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static Exception CreateInvalidOperationException_AlreadyReading() => new InvalidOperationException("Concurrent reads are not supported.");

    [DoesNotReturn]
    public static void ThrowInvalidOperationException_InvalidReadAsync() => throw CreateInvalidOperationException_InvalidReadAsync();
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static Exception CreateInvalidOperationException_InvalidReadAsync() => new InvalidOperationException("The PipeReader still went async after its underlying stream completed a zero byte read.");

    public static void ThrowInvalidOperationException_NoReadingAllowed()
    {
        throw new NotImplementedException();
    }

    public static void ThrowOperationCanceledException_ReadCanceled()
    {
        throw new NotImplementedException();
    }

    public static void ThrowOperationCanceledException_FlushCanceled()
    {
        throw new NotImplementedException();
    }

    public static void ThrowInvalidOperationException_AdvanceToInvalidCursor()
    {
        throw new NotImplementedException();
    }
}

enum ExceptionArgument
{
    minimumSize,
    bytes,
    callback,
    options,
    pauseWriterThreshold,
    resumeWriterThreshold,
    sizeHint,
    destination,
    buffer,
    source,
    readingStream,
    writingStream,
}
