using System.Runtime.CompilerServices;

namespace Slon.Buffers;

sealed class OutputWriter<TWriter, T> : IOutputWriter<T> where TWriter : IOutputWriter<T>
{
    TWriter _writer; // don't make readonly, we want to support mutable structs.
    Memory<T> _memory;
    long _committed;

    /// <summary>
    /// The number of uncommitted items (all the calls to <see cref="Advance(int)"/> since the last call to <see cref="Commit"/>).
    /// </summary>
    public int Buffered { get; private set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="OutputWriter{TWriter,T}"/> struct.
    /// </summary>
    /// <param name="output">The <see cref="IOutputWriter{T}"/> to be wrapped.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public OutputWriter(TWriter output)
    {
        Buffered = 0;
        _committed = 0;
        _writer = output;
        _memory = output.GetMemory();
    }

    public TWriter InnerWriter => _writer;

    /// <summary>
    /// Gets the Memory currently in use by the writer.
    /// </summary>
    public Memory<T> Memory => _memory;
    Memory<T> Remaining => _memory.Slice(Buffered);

    /// <summary>
    /// Gets the Span currently in use by the writer.
    /// </summary>
    public Span<T> Span => _memory.Span;
    Span<T> RemainingSpan => Span.Slice(Buffered);

    /// <summary>
    /// Gets the total number of items written with this writer.
    /// </summary>
    public long Committed => _committed;

    /// <summary>
    /// Calls <see cref="IOutputWriter{T}.Advance(int)"/> on the buffer writer with the number of uncommitted bytes.
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
            _memory = _writer.GetMemory();
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

    public Memory<T> GetMemory(int sizeHint = 0)
        => Remaining.Length is var length && (length < sizeHint || length is 0) ? Ensure(sizeHint) : Remaining;

    public Span<T> GetSpan(int sizeHint = 0)
        => RemainingSpan.Length is var length && (length < sizeHint || length is 0) ? EnsureSpan(sizeHint) : RemainingSpan;

    [MethodImpl(MethodImplOptions.NoInlining)]
    Memory<T> Ensure(int sizeHint = 0)
    {
        if (Buffered > 0)
            Commit();

        _memory = _writer.GetMemory(sizeHint);
        if (_memory.Length is 0)
            throw new InvalidOperationException("Empty span returned by IBufferWriter<T>.GetSpan");
        return _memory;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    Span<T> EnsureSpan(int sizeHint = 0) => Ensure(sizeHint).Span;

    public long UnflushedBytes => _committed + Buffered;

    public void Flush(TimeSpan timeout = default)
    {
        Commit();
        _writer.Flush(timeout);
        Buffered = 0;
        _committed = 0;
        _memory = _writer.GetMemory();
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        Commit();
        var flushTask = _writer.FlushAsync(cancellationToken);
        if (!flushTask.IsCompletedSuccessfully)
            return Core(flushTask);

        flushTask.GetAwaiter().GetResult();
        Buffered = 0;
        _committed = 0;
        _memory = _writer.GetMemory();
        return new();

        async ValueTask Core(ValueTask flushTask)
        {
            await flushTask.ConfigureAwait(false);
            Buffered = 0;
            _committed = 0;
            _memory = _writer.GetMemory();
        }
    }
}
