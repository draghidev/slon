using System.Buffers;
using System.Runtime.CompilerServices;

namespace Slon.Buffers;

// This type is extremely hard to remove without regressing TE benchmarks and something like this should probably find its way into the BCL.
// On top of that it's also a huge convenience type, being able to write in an advancing manner, potentially backed by a buffer writer.

/// <summary>
/// A ref struct for writing bytes to <see cref="IBufferWriter{T}"/> spans.
/// </summary>
/// <typeparam name="TWriter">The type of buffer writer to use.</typeparam>
/// <typeparam name="T">The type of item being written.</typeparam>
public ref struct SpanWriter<TWriter, T> : IBufferWriter<T> where TWriter : IBufferWriter<T>
{
    TWriter _writer; // don't make readonly, we want to support mutable structs.
    Span<T> _span;
    long _committed;

    /// <summary>
    /// The number of uncommitted items (all the calls to <see cref="Advance(int)"/> since the last call to <see cref="Commit"/>).
    /// </summary>
    public int Buffered { get; private set; }

    /// <summary>
    /// Creates a new instance of the <see cref="SpanWriter{T,TWriter}"/> struct.
    /// </summary>
    /// <param name="writer">The <see cref="IBufferWriter{T}"/> to write to.</param>
    /// <param name="sizeHint">The minimum size for the next buffer, if 0 a non-empty buffer will be returned.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SpanWriter(TWriter writer, int sizeHint = 0)
    {
        Buffered = 0;
        _committed = 0;
        _writer = writer;
        _span = writer.GetSpan(sizeHint: sizeHint);
    }

    public readonly TWriter InnerWriter => _writer;

    /// <summary>
    /// Gets the Span currently in use by the writer.
    /// </summary>
    public Span<T> Span => _span;
    readonly Span<T> Remaining => _span.Slice(Buffered);

    /// <summary>
    /// Gets the total number of items written with this writer.
    /// </summary>
    public readonly long Committed => _committed;

    /// <summary>
    /// Calls <see cref="IBufferWriter{T}.Advance(int)"/> on the buffer writer with the number of uncommitted bytes.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Commit()
    {
        var buffered = Buffered;
        if (buffered > 0)
        {
            _writer.Advance(buffered);
            _committed += buffered;
            Buffered = 0;
            _span = _writer.GetSpan();
        }
    }

    /// <summary>
    /// Used to indicate that part of the buffer has been written to.
    /// </summary>
    /// <param name="count">The number of bytes written to.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(int count)
    {
        Buffered += count;
    }

    // In an ideal world IBufferWriter would be inheriting from an ISpanWriter and just add GetMemory like so:
    // public interface IBufferWriter<T> : ISpanWriter<T>
    // {
    //     Memory<T> GetMemory(int sizeHint = 0);
    // }
    public Memory<T> GetMemory(int sizeHint = 0) => throw new NotSupportedException();

    public Span<T> GetSpan(int sizeHint = 0)
        => Remaining.Length is var length && (length < sizeHint || length is 0) ? Ensure(sizeHint) : Remaining;

    [MethodImpl(MethodImplOptions.NoInlining)]
    Span<T> Ensure(int sizeHint = 0)
    {
        if (Buffered > 0)
            Commit();

        _span = _writer.GetSpan(sizeHint);
        if (_span.Length is 0)
            throw new InvalidOperationException("Empty span returned by IBufferWriter<T>.GetSpan");
        return _span;
    }
}

// Convenience type that delegates to TWriter = IBufferWriter<T>.
// The TWriter variant allows for specialization over structs and supports mutable structs.
public ref struct SpanWriter<T> : IBufferWriter<T>
{
    SpanWriter<IBufferWriter<T>, T> _spanWriter;

    SpanWriter(SpanWriter<IBufferWriter<T>, T> spanWriter) => _spanWriter = spanWriter;

    public SpanWriter(IBufferWriter<T> bufferWriter, int sizeHint = 0)
    {
        _spanWriter = new(bufferWriter, sizeHint);
    }

    public IBufferWriter<T> InnerWriter => _spanWriter.InnerWriter;

    public int Buffered => _spanWriter.Buffered;
    public long Committed => _spanWriter.Committed;

    public void Advance(int count) => _spanWriter.Advance(count);
    public Span<T> GetSpan(int sizeHint = 0) => _spanWriter.GetSpan(sizeHint);
    public Memory<T> GetMemory(int sizeHint = 0) => throw new NotSupportedException();
    public void Commit() => _spanWriter.Commit();

    public static implicit operator SpanWriter<IBufferWriter<T>, T>(SpanWriter<T> writer) => writer._spanWriter;
    public static explicit operator SpanWriter<T>(SpanWriter<IBufferWriter<T>, T> writer) => new(writer);
}

