using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Slon.Buffers;

namespace Slon.Pg.Serialization;

// Buffered first adapters for the lifted converter contracts. Their surface deliberately matches the
// substrate; the streaming lift replaces the memory cursor with BackendMessageBodyReader without
// changing converters.
public sealed class PgReader : IDisposable, IAsyncDisposable
{
    ReadOnlySequence<byte> _buffer;
    IInputReader? _source;
    long _position;
    int _fieldSize;
    int _released;
    int _releasePrefix;
    byte[]? _pooledArray;

    internal PgReader(ReadOnlyMemory<byte> buffer, PgConversionContext? conversionContext = null)
        : this(new ReadOnlySequence<byte>(buffer), conversionContext) { }

    internal PgReader(in ReadOnlySequence<byte> buffer, PgConversionContext? conversionContext = null)
    {
        _buffer = buffer;
        _fieldSize = checked((int)buffer.Length);
        ConversionContext = conversionContext ?? PgConversionContext.Empty;
    }

    internal PgReader(IInputReader source, int fieldSize,
        PgConversionContext? conversionContext = null)
        : this(source, source.Buffer, fieldSize, releasePrefix: 0, conversionContext) { }

    internal PgReader(IInputReader source, in ReadOnlySequence<byte> buffer, int fieldSize,
        int releasePrefix, PgConversionContext? conversionContext = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fieldSize);
        ArgumentOutOfRangeException.ThrowIfNegative(releasePrefix);
        _source = source;
        _buffer = buffer;
        _fieldSize = fieldSize;
        _releasePrefix = releasePrefix;
        ConversionContext = conversionContext ?? PgConversionContext.Empty;
    }

    public PgConversionContext ConversionContext { get; }
    public int CurrentRemaining => _fieldSize - checked(_released + (int)_position);

    public byte ReadByte()
    {
        Span<byte> bytes = stackalloc byte[sizeof(byte)];
        ReadFixed(bytes);
        return bytes[0];
    }

    public short ReadInt16()
    {
        Span<byte> bytes = stackalloc byte[sizeof(short)];
        ReadFixed(bytes);
        var value = BinaryPrimitives.ReadInt16BigEndian(bytes);
        return value;
    }

    public int ReadInt32()
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        ReadFixed(bytes);
        var value = BinaryPrimitives.ReadInt32BigEndian(bytes);
        return value;
    }

    public long ReadInt64()
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        ReadFixed(bytes);
        var value = BinaryPrimitives.ReadInt64BigEndian(bytes);
        return value;
    }

    public ushort ReadUInt16()
    {
        Span<byte> bytes = stackalloc byte[sizeof(ushort)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadUInt16BigEndian(bytes);
    }

    public uint ReadUInt32()
    {
        Span<byte> bytes = stackalloc byte[sizeof(uint)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadUInt32BigEndian(bytes);
    }

    public ulong ReadUInt64()
    {
        Span<byte> bytes = stackalloc byte[sizeof(ulong)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadUInt64BigEndian(bytes);
    }

    public float ReadFloat()
    {
        Span<byte> bytes = stackalloc byte[sizeof(float)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadSingleBigEndian(bytes);
    }

    public double ReadDouble()
    {
        Span<byte> bytes = stackalloc byte[sizeof(double)];
        ReadFixed(bytes);
        return BinaryPrimitives.ReadDoubleBigEndian(bytes);
    }

    public ReadOnlySequence<byte> ReadBytes(int count)
    {
        CheckBounds(count);
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

    public Stream GetStream(int? length = null)
    {
        var count = length ?? CurrentRemaining;
        CheckBounds(count);
        return new ReaderStream(this, count);
    }

    public TextReader GetTextReader(Encoding encoding)
        => new StreamReader(GetStream(), encoding, detectEncodingFromByteOrderMarks: false,
            bufferSize: 1024, leaveOpen: false);

    public ValueTask<TextReader> GetTextReaderAsync(Encoding encoding,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new(GetTextReader(encoding));
    }

    public void Consume(int? count = null)
    {
        var remaining = count ?? CurrentRemaining;
        CheckBounds(remaining);
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

    public NestedReadScope BeginNestedRead(int size, Size bufferRequirement)
        => BeginNestedReadCore(size, bufferRequirement, async: false);

    public ValueTask<NestedReadScope> BeginNestedReadAsync(int size, Size bufferRequirement,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new(BeginNestedReadCore(size, bufferRequirement, async: true));
    }

    NestedReadScope BeginNestedReadCore(int size, Size bufferRequirement, bool async)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(size);
        CheckBounds(size);
        var previousEnd = _fieldSize;
        var nestedEnd = checked(_released + (int)_position + size);
        _fieldSize = nestedEnd;
        return new(this, previousEnd, nestedEnd, bufferRequirement, async);
    }

    ValueTask EndNestedRead(int previousEnd, int nestedEnd, Size bufferRequirement, bool async)
    {
        if (_fieldSize != nestedEnd)
            throw new InvalidOperationException("Nested read scopes must be disposed in reverse order.");

        var remaining = CurrentRemaining;
        _fieldSize = previousEnd;
        if (remaining == 0)
            return default;
        if (bufferRequirement.Kind is not SizeKind.UpperBound)
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

    void CheckBounds(int count)
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

    internal int CompleteField()
    {
        if (CurrentRemaining != 0)
            throw new InvalidOperationException($"Converter left {CurrentRemaining} field bytes unread.");
        return ReleaseFieldWindow(async: false).GetAwaiter().GetResult();
    }

    internal ValueTask<int> CompleteFieldAsync()
    {
        if (CurrentRemaining != 0)
            throw new InvalidOperationException($"Converter left {CurrentRemaining} field bytes unread.");
        return ReleaseFieldWindow(async: true);
    }

    async ValueTask<int> ReleaseFieldWindow(bool async)
    {
        var source = _source;
        if (source is null)
            return -1;
        if (source.IsComplete)
            return checked(_releasePrefix + (int)_position);

        ReleaseWindow(source);
        if (async)
            await source.ReadAsync().ConfigureAwait(false);
        else
            source.Read();
        Publish(source);
        return 0;
    }

    byte[] RentArray(int count)
    {
        if (_pooledArray is { } previous)
            ArrayPool<byte>.Shared.Return(previous);
        return _pooledArray = ArrayPool<byte>.Shared.Rent(count);
    }

    public void Dispose()
    {
        if (_pooledArray is { } array)
        {
            _pooledArray = null;
            ArrayPool<byte>.Shared.Return(array);
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    sealed class ReaderStream : Stream
    {
        readonly PgReader _reader;
        readonly int _length;
        int _remaining;
        bool _disposed;

        internal ReaderStream(PgReader reader, int length)
        {
            _reader = reader;
            _length = _remaining = length;
        }

        public override int Read(Span<byte> buffer)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var count = Math.Min(buffer.Length, _remaining);
            if (count == 0)
                return 0;
            _reader.Read(buffer.Slice(0, count));
            _remaining -= count;
            return count;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            ArgumentNullException.ThrowIfNull(buffer);
            return Read(buffer.AsSpan(offset, count));
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var count = Math.Min(buffer.Length, _remaining);
            if (count == 0)
                return 0;
            await _reader.ReadBytesAsync(buffer.Slice(0, count), cancellationToken).ConfigureAwait(false);
            _remaining -= count;
            return count;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count,
            CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        protected override void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _reader.Consume(_remaining);
                _remaining = 0;
            }
            _disposed = true;
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                await _reader.ConsumeAsync(_remaining).ConfigureAwait(false);
                _remaining = 0;
                _disposed = true;
            }
            GC.SuppressFinalize(this);
        }

        public override bool CanRead => !_disposed;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _length;
        public override long Position
        {
            get => _length - _remaining;
            set => throw new NotSupportedException();
        }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    public readonly struct NestedReadScope : IDisposable, IAsyncDisposable
    {
        readonly PgReader _reader;
        readonly int _previousEnd;
        readonly int _nestedEnd;
        readonly Size _bufferRequirement;
        readonly bool _async;

        internal NestedReadScope(PgReader reader, int previousEnd, int nestedEnd,
            Size bufferRequirement, bool async)
        {
            _reader = reader;
            _previousEnd = previousEnd;
            _nestedEnd = nestedEnd;
            _bufferRequirement = bufferRequirement;
            _async = async;
        }

        public void Dispose()
        {
            if (_async)
                throw new InvalidOperationException("An asynchronous nested read scope must be disposed asynchronously.");
            _reader.EndNestedRead(_previousEnd, _nestedEnd, _bufferRequirement, async: false)
                .GetAwaiter().GetResult();
        }

        public ValueTask DisposeAsync()
            => _reader.EndNestedRead(_previousEnd, _nestedEnd, _bufferRequirement, async: true);
    }
}
