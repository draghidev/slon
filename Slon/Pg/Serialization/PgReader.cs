using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Buffers;

namespace Slon.Pg.Serialization;

// Serializer vocabulary over Row's reusable, transport-neutral field cursor. The value wrapper
// keeps serialization out of wire ownership without allocating a cursor for every field.
public readonly struct PgReader : IDisposable, IAsyncDisposable
{
    readonly PgFieldReader _reader;

    internal PgReader(ReadOnlyMemory<byte> buffer, PgConversionContext? conversionContext = null)
        : this(new PgFieldReader(buffer), conversionContext ?? PgConversionContext.Empty) { }

    internal PgReader(in ReadOnlySequence<byte> buffer,
        PgConversionContext? conversionContext = null)
        : this(new PgFieldReader(buffer), conversionContext ?? PgConversionContext.Empty) { }

    internal PgReader(IInputReader source, int fieldSize,
        PgConversionContext? conversionContext = null)
        : this(new PgFieldReader(source, fieldSize), conversionContext ?? PgConversionContext.Empty) { }

    internal PgReader(IInputReader source, in ReadOnlySequence<byte> buffer, int fieldSize,
        int releasePrefix, PgConversionContext? conversionContext = null)
        : this(new PgFieldReader(source, buffer, fieldSize, releasePrefix),
            conversionContext ?? PgConversionContext.Empty) { }

    internal PgReader(PgFieldReader reader, PgConversionContext conversionContext,
        bool sequential = false)
    {
        _reader = reader;
        ConversionContext = conversionContext;
        IsSequential = sequential;
    }

    public PgConversionContext ConversionContext { get; }
    internal bool IsSequential { get; }
    public int CurrentRemaining => _reader.CurrentRemaining;
    internal int FieldSize => _reader.FieldSize;
    internal int FieldOffset => _reader.FieldOffset;
    internal bool TryGetReadView(out PgReadView view) => _reader.TryGetReadView(out view);

    internal void Seek(int offset) => _reader.Seek(offset);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte ReadByte() => _reader.ReadByte();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public short ReadInt16() => _reader.ReadInt16();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadInt32() => _reader.ReadInt32();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadInt64() => _reader.ReadInt64();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ushort ReadUInt16() => _reader.ReadUInt16();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint ReadUInt32() => _reader.ReadUInt32();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong ReadUInt64() => _reader.ReadUInt64();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public float ReadFloat() => _reader.ReadFloat();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double ReadDouble() => _reader.ReadDouble();

    public ReadOnlySequence<byte> ReadBytes(int count) => _reader.ReadBytes(count);
    public ValueTask<ReadOnlySequence<byte>> ReadBytesAsync(int count,
        CancellationToken cancellationToken = default)
        => _reader.ReadBytesAsync(count, cancellationToken);
    public void Read(Span<byte> destination) => _reader.Read(destination);
    public ValueTask ReadBytesAsync(Memory<byte> destination,
        CancellationToken cancellationToken = default)
        => _reader.ReadBytesAsync(destination, cancellationToken);
    public bool TryReadBytes(int count, out ReadOnlySpan<byte> bytes)
        => _reader.TryReadBytes(count, out bytes);
    public Stream GetStream(int? length = null)
    {
        var count = length ?? CurrentRemaining;
        _reader.CheckBounds(count);
        var stream = new ReaderStream(_reader, count);
        _reader.RegisterView(stream);
        return stream;
    }

    public TextReader GetTextReader(Encoding encoding)
    {
        var stream = new ReaderStream(_reader, CurrentRemaining);
        var reader = new ReaderTextReader(stream, encoding);
        _reader.RegisterView(reader);
        return reader;
    }

    public ValueTask<TextReader> GetTextReaderAsync(Encoding encoding,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new(GetTextReader(encoding));
    }
    public void Consume(int? count = null) => _reader.Consume(count);
    public ValueTask ConsumeAsync(int? count = null,
        CancellationToken cancellationToken = default)
        => _reader.ConsumeAsync(count, cancellationToken);
    public void Rewind(int count) => _reader.Rewind(count);

    public NestedReadScope BeginNestedRead(int size, Size bufferRequirement)
        => new(_reader.BeginNestedRead(size,
            consumeRemainder: bufferRequirement.Kind is SizeKind.UpperBound));

    public async ValueTask<NestedReadScope> BeginNestedReadAsync(int size,
        Size bufferRequirement, CancellationToken cancellationToken = default)
        => new(await _reader.BeginNestedReadAsync(size,
                consumeRemainder: bufferRequirement.Kind is SizeKind.UpperBound, cancellationToken)
            .ConfigureAwait(false));

    internal void CompleteField() => _reader.CompleteField();
    internal ValueTask CompleteFieldAsync() => _reader.CompleteFieldAsync();
    internal bool HasActiveView => _reader.HasActiveView;
    internal IColumnLease ActiveViewLease => _reader.ActiveViewLease;

    public void Dispose() => _reader.Dispose();
    public ValueTask DisposeAsync() => _reader.DisposeAsync();

    public readonly struct NestedReadScope : IDisposable, IAsyncDisposable
    {
        readonly PgFieldReader.NestedReadScope _scope;

        internal NestedReadScope(PgFieldReader.NestedReadScope scope) => _scope = scope;

        public void Dispose() => _scope.Dispose();
        public ValueTask DisposeAsync() => _scope.DisposeAsync();
    }

    sealed class ReaderStream : Stream, IColumnLease
    {
        readonly PgFieldReader _reader;
        readonly int _length;
        int _remaining;
        bool _disposed;

        internal ReaderStream(PgFieldReader reader, int length)
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
            await _reader.ReadBytesAsync(buffer.Slice(0, count), cancellationToken)
                .ConfigureAwait(false);
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
            _reader.ReleaseView(this);
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                await _reader.ConsumeAsync(_remaining).ConfigureAwait(false);
                _remaining = 0;
                _disposed = true;
                _reader.ReleaseView(this);
            }
            GC.SuppressFinalize(this);
        }

        void IColumnLease.Revoke()
        {
            _disposed = true;
            _reader.ReleaseView(this);
        }

        internal void ReleaseOwner(IColumnLease owner) => _reader.ReleaseView(owner);

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

    sealed class ReaderTextReader : TextReader, IColumnLease
    {
        readonly ReaderStream _stream;
        readonly StreamReader _reader;
        bool _disposed;

        internal ReaderTextReader(ReaderStream stream, Encoding encoding)
        {
            _stream = stream;
            _reader = new(stream, encoding, detectEncodingFromByteOrderMarks: false,
                bufferSize: 1024, leaveOpen: false);
        }

        void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

        public override int Peek() { ThrowIfDisposed(); return _reader.Peek(); }
        public override int Read() { ThrowIfDisposed(); return _reader.Read(); }
        public override int Read(char[] buffer, int index, int count)
        { ThrowIfDisposed(); return _reader.Read(buffer, index, count); }
        public override int Read(Span<char> buffer)
        { ThrowIfDisposed(); return _reader.Read(buffer); }
        public override Task<int> ReadAsync(char[] buffer, int index, int count)
        { ThrowIfDisposed(); return _reader.ReadAsync(buffer, index, count); }
        public override ValueTask<int> ReadAsync(Memory<char> buffer,
            CancellationToken cancellationToken = default)
        { ThrowIfDisposed(); return _reader.ReadAsync(buffer, cancellationToken); }
        public override string? ReadLine() { ThrowIfDisposed(); return _reader.ReadLine(); }
        public override Task<string?> ReadLineAsync()
        { ThrowIfDisposed(); return _reader.ReadLineAsync(); }
        public override ValueTask<string?> ReadLineAsync(CancellationToken cancellationToken)
        { ThrowIfDisposed(); return _reader.ReadLineAsync(cancellationToken); }
        public override string ReadToEnd() { ThrowIfDisposed(); return _reader.ReadToEnd(); }
        public override Task<string> ReadToEndAsync()
        { ThrowIfDisposed(); return _reader.ReadToEndAsync(); }
        public override Task<string> ReadToEndAsync(CancellationToken cancellationToken)
        { ThrowIfDisposed(); return _reader.ReadToEndAsync(cancellationToken); }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
                _reader.Dispose();
            _disposed = true;
            _stream.ReleaseOwner(this);
            base.Dispose(disposing);
        }

        void IColumnLease.Revoke()
        {
            _disposed = true;
            ((IColumnLease)_stream).Revoke();
            _stream.ReleaseOwner(this);
        }
    }
}
