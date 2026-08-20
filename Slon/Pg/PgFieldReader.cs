using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Slon.Buffers;
using Slon.Pg.Serialization;

namespace Slon.Pg;

// Reusable, field-bounded transport cursor. Serializer and other higher-level readers wrap this
// state rather than becoming part of Row's tenure machinery.
class PgFieldReader : IDisposable, IAsyncDisposable
{
    ReadOnlySequence<byte> _buffer;
    ReadOnlyMemory<byte> _memory;
    IInputReader? _source;
    long _position;
    int _fieldSize;
    int _released;
    int _releasePrefix;
    byte[]? _pooledArray;
    IColumnLease? _activeView;
    bool _contiguous;
    bool _revoked;

    protected PgFieldReader() { }

    internal PgFieldReader(ReadOnlyMemory<byte> buffer)
    {
        Initialize(buffer);
    }

    internal PgFieldReader(in ReadOnlySequence<byte> buffer)
    {
        Initialize(buffer);
    }

    internal PgFieldReader(IInputReader source, int fieldSize)
        : this(source, source.Buffer, fieldSize, releasePrefix: 0) { }

    internal PgFieldReader(IInputReader source, in ReadOnlySequence<byte> buffer, int fieldSize,
        int releasePrefix)
    {
        Initialize(source, buffer, fieldSize, releasePrefix);
    }

    protected void Initialize(in ReadOnlySequence<byte> buffer)
    {
        Reset(buffer, source: null, checked((int)buffer.Length), releasePrefix: 0);
    }

    protected void Initialize(ReadOnlyMemory<byte> buffer)
    {
        if (_activeView is not null)
            throw new InvalidOperationException("The previous field view is still active.");
        if (!_contiguous)
        {
            _source = null;
            _buffer = default;
        }
        SetMemory(ref _memory, buffer);
        _position = 0;
        _fieldSize = buffer.Length;
        _activeView = null;
        _contiguous = true;
        _revoked = false;
    }

    protected void Initialize(IInputReader source, in ReadOnlySequence<byte> buffer,
        int fieldSize, int releasePrefix)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fieldSize);
        ArgumentOutOfRangeException.ThrowIfNegative(releasePrefix);
        Reset(buffer, source, fieldSize, releasePrefix);
    }

    void Reset(in ReadOnlySequence<byte> buffer, IInputReader? source, int fieldSize,
        int releasePrefix)
    {
        if (_activeView is not null)
            throw new InvalidOperationException("The previous field view is still active.");
        _source = source;
        _buffer = buffer;
        _memory = default;
        _position = 0;
        _fieldSize = fieldSize;
        _released = 0;
        _releasePrefix = releasePrefix;
        _activeView = null;
        _contiguous = false;
        _revoked = false;
    }

    public int CurrentRemaining
        => _fieldSize - checked((_contiguous ? 0 : _released) + (int)_position);
    internal int FieldSize => _fieldSize;
    internal int FieldOffset => checked((_contiguous ? 0 : _released) + (int)_position);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetReadView(out PgReadView view)
    {
        if (_contiguous)
        {
            view = new(_memory.Span);
            return true;
        }
        view = default;
        return false;
    }

    internal void Seek(int offset)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        if (offset > _fieldSize)
            throw new ArgumentOutOfRangeException(nameof(offset));

        var current = FieldOffset;
        if (offset < current)
        {
            if (!_contiguous && (_source is not null || _released != 0))
                throw new InvalidOperationException(
                    "Attempted to read a position in a sequential column which has already been consumed.");
            _position = offset;
            return;
        }

        Consume(offset - current);
    }

    public byte ReadByte()
    {
        if (TryReadBytes(sizeof(byte), out var direct))
            return direct[0];
        Span<byte> bytes = stackalloc byte[sizeof(byte)];
        ReadFixed(bytes);
        return bytes[0];
    }

    public short ReadInt16()
    {
        if (TryReadBytes(sizeof(short), out var direct))
            return BinaryPrimitives.ReadInt16BigEndian(direct);
        Span<byte> bytes = stackalloc byte[sizeof(short)];
        ReadFixed(bytes);
        var value = BinaryPrimitives.ReadInt16BigEndian(bytes);
        return value;
    }

    public int ReadInt32()
    {
        if (TryReadContiguous(sizeof(int), out var direct))
            return BinaryPrimitives.ReadInt32BigEndian(direct);
        return ReadInt32Slow();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    int ReadInt32Slow()
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        ReadFixed(bytes);
        var value = BinaryPrimitives.ReadInt32BigEndian(bytes);
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool TryReadContiguous(int count, out ReadOnlySpan<byte> bytes)
    {
        if (!_contiguous)
        {
            bytes = default;
            return false;
        }
        CheckBounds(count);
        bytes = _memory.Span.Slice(checked((int)_position), count);
        _position += count;
        return true;
    }

    public long ReadInt64()
    {
        if (TryReadBytes(sizeof(long), out var direct))
            return BinaryPrimitives.ReadInt64BigEndian(direct);
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        ReadFixed(bytes);
        var value = BinaryPrimitives.ReadInt64BigEndian(bytes);
        return value;
    }

    public ushort ReadUInt16()
    {
        if (TryReadBytes(sizeof(ushort), out var direct))
            return BinaryPrimitives.ReadUInt16BigEndian(direct);
        Span<byte> bytes = stackalloc byte[sizeof(ushort)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadUInt16BigEndian(bytes);
    }

    public uint ReadUInt32()
    {
        if (TryReadBytes(sizeof(uint), out var direct))
            return BinaryPrimitives.ReadUInt32BigEndian(direct);
        Span<byte> bytes = stackalloc byte[sizeof(uint)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadUInt32BigEndian(bytes);
    }

    public ulong ReadUInt64()
    {
        if (TryReadBytes(sizeof(ulong), out var direct))
            return BinaryPrimitives.ReadUInt64BigEndian(direct);
        Span<byte> bytes = stackalloc byte[sizeof(ulong)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadUInt64BigEndian(bytes);
    }

    public float ReadFloat()
    {
        if (TryReadBytes(sizeof(float), out var direct))
            return BinaryPrimitives.ReadSingleBigEndian(direct);
        Span<byte> bytes = stackalloc byte[sizeof(float)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadSingleBigEndian(bytes);
    }

    public double ReadDouble()
    {
        if (TryReadBytes(sizeof(double), out var direct))
            return BinaryPrimitives.ReadDoubleBigEndian(direct);
        Span<byte> bytes = stackalloc byte[sizeof(double)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadDoubleBigEndian(bytes);
    }

    public ReadOnlySequence<byte> ReadBytes(int count)
    {
        CheckBounds(count);
        if (_contiguous)
        {
            var position = checked((int)_position);
            var memory = _memory.Slice(position, count);
            var result = new ReadOnlySequence<byte>(memory);
            _position += count;
            return result;
        }
        if (_buffer.Length - _position >= count)
        {
            var result = _buffer.Slice(_position, count);
            _position += count;
            return result;
        }

        var array = RentArray(count);
        Read(array.AsSpan(0, count));
        return new(array, 0, count);
    }

    public async ValueTask<ReadOnlySequence<byte>> ReadBytesAsync(int count,
        CancellationToken cancellationToken = default)
    {
        CheckBounds(count);
        if (_contiguous)
            return ReadBytes(count);
        if (_buffer.Length - _position >= count)
        {
            var result = _buffer.Slice(_position, count);
            _position += count;
            return result;
        }

        var array = RentArray(count);
        await ReadBytesAsync(array.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
        return new(array, 0, count);
    }

    public void Read(Span<byte> destination)
    {
        CheckBounds(destination.Length);
        if (_contiguous)
        {
            _memory.Span.Slice(checked((int)_position), destination.Length).CopyTo(destination);
            _position += destination.Length;
            return;
        }
        while (!destination.IsEmpty)
        {
            if (_buffer.Length == _position)
                Refill();
            var available = _buffer.Slice(_position);
            var count = (int)Math.Min(destination.Length, available.Length);
            available.Slice(0, count).CopyTo(destination);
            _position += count;
            destination = destination.Slice(count);
        }
    }

    public ValueTask ReadBytesAsync(Memory<byte> destination,
        CancellationToken cancellationToken = default)
    {
        CheckBounds(destination.Length);
        if (destination.IsEmpty)
            return default;
        if (_contiguous)
        {
            Read(destination.Span);
            return default;
        }
        return Core(destination, cancellationToken);

        async ValueTask Core(Memory<byte> remaining, CancellationToken token)
        {
            while (!remaining.IsEmpty)
            {
                if (_buffer.Length == _position)
                    await RefillAsync(token).ConfigureAwait(false);
                var available = _buffer.Slice(_position);
                var count = (int)Math.Min(remaining.Length, available.Length);
                available.Slice(0, count).CopyTo(remaining.Span);
                _position += count;
                remaining = remaining.Slice(count);
            }
        }
    }

    public bool TryReadBytes(int count, out ReadOnlySpan<byte> bytes)
    {
        CheckBounds(count);
        if (_contiguous)
        {
            bytes = _memory.Span.Slice(checked((int)_position), count);
            _position += count;
            return true;
        }
        if (_buffer.IsSingleSegment && _buffer.Length - _position >= count)
        {
            bytes = _buffer.FirstSpan.Slice(checked((int)_position), count);
            _position += count;
            return true;
        }
        var unread = _buffer.Slice(_position);
        if (unread.FirstSpan.Length < count)
        {
            bytes = default;
            return false;
        }
        bytes = unread.FirstSpan.Slice(0, count);
        _position += count;
        return true;
    }

    public void Consume(int? count = null)
    {
        var remaining = count ?? CurrentRemaining;
        CheckBounds(remaining);
        if (_contiguous)
        {
            _position += remaining;
            return;
        }
        while (remaining > 0)
        {
            if (_buffer.Length == _position)
                Refill();
            var consumed = (int)Math.Min(remaining, _buffer.Length - _position);
            _position += consumed;
            remaining -= consumed;
        }
    }

    public ValueTask ConsumeAsync(int? count = null, CancellationToken cancellationToken = default)
    {
        var remaining = count ?? CurrentRemaining;
        CheckBounds(remaining);
        if (remaining == 0)
            return default;
        if (_contiguous)
        {
            _position += remaining;
            return default;
        }
        return Core(remaining, cancellationToken);

        async ValueTask Core(int bytesRemaining, CancellationToken token)
        {
            while (bytesRemaining > 0)
            {
                if (_buffer.Length == _position)
                    await RefillAsync(token).ConfigureAwait(false);
                var consumed = (int)Math.Min(bytesRemaining, _buffer.Length - _position);
                _position += consumed;
                bytesRemaining -= consumed;
            }
        }
    }

    public void Rewind(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count > _position)
            throw new InvalidOperationException("Cannot rewind into an input window that has already been released.");
        _position -= count;
    }

    internal NestedReadScope BeginNestedRead(int size, bool consumeRemainder)
        => BeginNestedReadCore(size, consumeRemainder, async: false);

    internal ValueTask<NestedReadScope> BeginNestedReadAsync(int size, bool consumeRemainder,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new(BeginNestedReadCore(size, consumeRemainder, async: true));
    }

    NestedReadScope BeginNestedReadCore(int size, bool consumeRemainder, bool async)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(size);
        CheckBounds(size);
        var previousEnd = _fieldSize;
        var nestedEnd = checked(_released + (int)_position + size);
        _fieldSize = nestedEnd;
        return new(this, previousEnd, nestedEnd, consumeRemainder, async);
    }

    ValueTask EndNestedRead(int previousEnd, int nestedEnd, bool consumeRemainder, bool async)
    {
        if (_fieldSize != nestedEnd)
            throw new InvalidOperationException("Nested read scopes must be disposed in reverse order.");

        var remaining = CurrentRemaining;
        _fieldSize = previousEnd;
        if (remaining == 0)
            return default;
        if (!consumeRemainder)
            throw new InvalidOperationException(
                $"Nested converter left {remaining} bytes unread from an exact field value.");
        return async ? ConsumeAsync(remaining) : ConsumeSynchronously(remaining);

        ValueTask ConsumeSynchronously(int count)
        {
            Consume(count);
            return default;
        }
    }

    void ReadFixed(Span<byte> destination)
    {
        Read(destination);
    }

    internal void CheckBounds(int count)
    {
        if ((uint)count > (uint)CurrentRemaining)
            throw new IndexOutOfRangeException("Attempt to read past the end of the field.");
    }

    void Refill()
    {
        var source = _source ?? throw new EndOfStreamException();
        if (source.IsComplete)
            throw new EndOfStreamException();
        ReleaseWindow(source);
        source.Read();
        Publish(source);
    }

    async ValueTask RefillAsync(CancellationToken cancellationToken)
    {
        var source = _source ?? throw new EndOfStreamException();
        if (source.IsComplete)
            throw new EndOfStreamException();
        ReleaseWindow(source);
        await source.ReadAsync(cancellationToken).ConfigureAwait(false);
        Publish(source);
    }

    void ReleaseWindow(IInputReader source)
    {
        var consumed = _position;
        var position = _buffer.GetPosition(consumed);
        source.AdvanceTo(position, checked(_releasePrefix + consumed));
        _released = checked(_released + (int)consumed);
    }

    void Publish(IInputReader source)
    {
        _buffer = source.Buffer;
        _position = 0;
        _releasePrefix = 0;
        if (_buffer.IsEmpty && !source.IsComplete)
            throw new InvalidOperationException("Input source published an empty incomplete window.");
    }

    internal void CompleteField()
    {
        if (CurrentRemaining != 0)
            throw new InvalidOperationException($"Converter left {CurrentRemaining} field bytes unread.");
        if (!_contiguous && _source is { } source)
            ReleaseFieldWindow(source);
    }

    internal ValueTask CompleteFieldAsync()
    {
        if (CurrentRemaining != 0)
            throw new InvalidOperationException($"Converter left {CurrentRemaining} field bytes unread.");
        return !_contiguous && _source is { } source
            ? ReleaseFieldWindowAsync(source)
            : default;
    }

    internal bool HasActiveView => _activeView is not null;
    internal IColumnLease ActiveViewLease
        => _activeView is { } view
            ? view
            : throw new InvalidOperationException("No temporary column view is active.");

    internal void RegisterView(IColumnLease view)
    {
        if (_revoked)
            throw new ObjectDisposedException(nameof(PgFieldReader));
        if (_activeView is not null)
            throw new InvalidOperationException("Only one temporary view may be active for a column.");
        _activeView = view;
    }

    internal void ReleaseView(IColumnLease view)
    {
        if (ReferenceEquals(_activeView, view))
            _activeView = null;
    }

    internal void RevokeField()
    {
        if (_revoked)
            throw new InvalidOperationException("The column lease has already been revoked.");
        _activeView?.Revoke();
        Consume();
        CompleteField();
        _revoked = true;
        Dispose();
    }

    internal async ValueTask RevokeFieldAsync()
    {
        if (_revoked)
            throw new InvalidOperationException("The column lease has already been revoked.");
        if (_activeView is { } view)
            view.Revoke();
        await ConsumeAsync().ConfigureAwait(false);
        await CompleteFieldAsync().ConfigureAwait(false);
        _revoked = true;
        Dispose();
    }

    void ReleaseFieldWindow(IInputReader source)
    {
        ReleaseWindow(source);
        if (source.IsComplete)
            return;
        source.Read();
        Publish(source);
    }

    async ValueTask ReleaseFieldWindowAsync(IInputReader source)
    {
        ReleaseWindow(source);
        if (source.IsComplete)
            return;
        await source.ReadAsync().ConfigureAwait(false);
        Publish(source);
    }

    byte[] RentArray(int count)
    {
        if (_pooledArray is { } previous)
            ArrayPool<byte>.Shared.Return(previous);
        return _pooledArray = ArrayPool<byte>.Shared.Rent(count);
    }

    public void Dispose()
    {
        if (_activeView is IDisposable view)
            view.Dispose();
        if (_pooledArray is { } array)
        {
            _pooledArray = null;
            ArrayPool<byte>.Shared.Return(array);
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_activeView is IAsyncDisposable)
            return DisposeAsyncCore();
        Dispose();
        return default;
    }

    async ValueTask DisposeAsyncCore()
    {
        await ((IAsyncDisposable)_activeView!).DisposeAsync().ConfigureAwait(false);
        Dispose();
    }

    public readonly struct NestedReadScope : IDisposable, IAsyncDisposable
    {
        readonly PgFieldReader _reader;
        readonly int _previousEnd;
        readonly int _nestedEnd;
        readonly bool _consumeRemainder;
        readonly bool _async;

        internal NestedReadScope(PgFieldReader reader, int previousEnd, int nestedEnd,
            bool consumeRemainder, bool async)
        {
            _reader = reader;
            _previousEnd = previousEnd;
            _nestedEnd = nestedEnd;
            _consumeRemainder = consumeRemainder;
            _async = async;
        }

        public void Dispose()
        {
            if (_async)
                throw new InvalidOperationException(
                    "An asynchronous nested read scope must be disposed asynchronously.");
            _reader.EndNestedRead(_previousEnd, _nestedEnd, _consumeRemainder, async: false)
                .GetAwaiter().GetResult();
        }

        public ValueTask DisposeAsync()
            => _reader.EndNestedRead(_previousEnd, _nestedEnd, _consumeRemainder, async: true);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void SetMemory(ref ReadOnlyMemory<byte> destination, ReadOnlyMemory<byte> value)
    {
        // The cursor lives on Row. Avoid issuing a checked write barrier when repeated reads
        // publish another slice over the same buffered backend-message array.
        ref var destinationObject = ref MemoryObject(ref destination);
        var valueObject = MemoryObject(ref value);
        if (!ReferenceEquals(destinationObject, valueObject))
            destinationObject = valueObject;
        MemoryIndex(ref destination) = MemoryIndex(ref value);
        MemoryLength(ref destination) = MemoryLength(ref value);
    }

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_object")]
    static extern ref object? MemoryObject(ref ReadOnlyMemory<byte> memory);

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_index")]
    static extern ref int MemoryIndex(ref ReadOnlyMemory<byte> memory);

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_length")]
    static extern ref int MemoryLength(ref ReadOnlyMemory<byte> memory);
}
