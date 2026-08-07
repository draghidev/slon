using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Slon.Buffers;

namespace Slon.Pg.Serialization;

enum FlushMode : byte
{
    None,
    Blocking,
    NonBlocking
}

public sealed class PgWriter
{
    readonly IBufferWriter<byte> _writer;
    Memory<byte> _buffer;
    int _position;
    int _committed;
    int _totalBytesWritten;
    PgConversionContext _conversionContext = PgConversionContext.Empty;
    FlushMode _flushMode;

    internal PgWriter(IBufferWriter<byte> writer, PgConversionContext? conversionContext = null)
    {
        _writer = writer;
        Init(conversionContext);
    }

    public PgConversionContext ConversionContext => _conversionContext;

    internal PgWriter Init(PgConversionContext? conversionContext = null,
        FlushMode flushMode = FlushMode.None)
    {
        if (_position != _committed)
            throw new InvalidOperationException("PgWriter still has uncommitted bytes.");

        _conversionContext = conversionContext ?? PgConversionContext.Empty;
        _flushMode = flushMode;
        _totalBytesWritten = 0;
        RequestBuffer(0);
        return this;
    }

    Span<byte> Span => _buffer.Span.Slice(_position);
    int Remaining => _buffer.Length - _position;

    void RequestBuffer(int count)
    {
        _buffer = _writer.GetMemory(count);
        _position = _committed = 0;
    }

    void Ensure(int count)
    {
        if (count <= Remaining)
            return;
        Commit();
        RequestBuffer(count);
    }

    void Advance(int count) => _position += count;

    void Commit()
    {
        var count = _position - _committed;
        if (count == 0)
            return;
        _writer.Advance(count);
        _totalBytesWritten += count;
        _committed = _position;
    }

    internal void EndWrite(Size expectedByteCount)
    {
        Commit();
        var actual = _totalBytesWritten;
        _totalBytesWritten = 0;
        if (actual != expectedByteCount.GetValueOrDefault())
            throw new InvalidOperationException(
                $"Bytes written ({actual}) and expected byte count ({expectedByteCount}) do not match.");
    }

    public void WriteByte(byte value)
    {
        Ensure(sizeof(byte));
        Span[0] = value;
        Advance(sizeof(byte));
    }

    public void WriteInt16(short value)
    {
        Ensure(sizeof(short));
        BinaryPrimitives.WriteInt16BigEndian(Span, value);
        Advance(sizeof(short));
    }

    public void WriteInt32(int value)
    {
        Ensure(sizeof(int));
        BinaryPrimitives.WriteInt32BigEndian(Span, value);
        Advance(sizeof(int));
    }

    public void WriteInt64(long value)
    {
        Ensure(sizeof(long));
        BinaryPrimitives.WriteInt64BigEndian(Span, value);
        Advance(sizeof(long));
    }

    public void WriteUInt16(ushort value)
    {
        Ensure(sizeof(ushort));
        BinaryPrimitives.WriteUInt16BigEndian(Span, value);
        Advance(sizeof(ushort));
    }

    public void WriteUInt32(uint value)
    {
        Ensure(sizeof(uint));
        BinaryPrimitives.WriteUInt32BigEndian(Span, value);
        Advance(sizeof(uint));
    }

    public void WriteUInt64(ulong value)
    {
        Ensure(sizeof(ulong));
        BinaryPrimitives.WriteUInt64BigEndian(Span, value);
        Advance(sizeof(ulong));
    }

    public void WriteFloat(float value)
    {
        Ensure(sizeof(float));
        BinaryPrimitives.WriteSingleBigEndian(Span, value);
        Advance(sizeof(float));
    }

    public void WriteDouble(double value)
    {
        Ensure(sizeof(double));
        BinaryPrimitives.WriteDoubleBigEndian(Span, value);
        Advance(sizeof(double));
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        while (!value.IsEmpty)
        {
            if (Remaining == 0)
            {
                if (_flushMode is FlushMode.None)
                    Ensure(1);
                else
                    Flush();
            }
            var count = Math.Min(value.Length, Remaining);
            value.Slice(0, count).CopyTo(Span);
            Advance(count);
            value = value.Slice(count);
        }
    }

    public void WriteChars(ReadOnlySpan<char> value, Encoding encoding)
    {
        var encoder = encoding.GetEncoder();
        var minBufferSize = encoding.GetMaxByteCount(1);
        bool completed;
        do
        {
            if (ShouldFlush(minBufferSize))
                Flush();
            Ensure(minBufferSize);
            encoder.Convert(value, Span, flush: true, out var charsUsed, out var bytesUsed, out completed);
            value = value.Slice(charsUsed);
            Advance(bytesUsed);
        } while (!completed);
    }

    public ValueTask WriteCharsAsync(ReadOnlyMemory<char> value, Encoding encoding,
        CancellationToken cancellationToken = default)
    {
        if (value.IsEmpty)
            return default;
        return Core(value, encoding, cancellationToken);

        async ValueTask Core(ReadOnlyMemory<char> remaining, Encoding textEncoding,
            CancellationToken token)
        {
            var encoder = textEncoding.GetEncoder();
            var minBufferSize = textEncoding.GetMaxByteCount(1);
            bool completed;
            do
            {
                if (ShouldFlush(minBufferSize))
                    await FlushAsync(token).ConfigureAwait(false);
                Ensure(minBufferSize);
                encoder.Convert(remaining.Span, Span, flush: true,
                    out var charsUsed, out var bytesUsed, out completed);
                remaining = remaining.Slice(charsUsed);
                Advance(bytesUsed);
            } while (!completed);
        }
    }

    public Stream GetStream() => new WriterStream(this);

    public ValueTask WriteBytesAsync(ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default)
    {
        if (value.Length <= Remaining)
        {
            value.Span.CopyTo(Span);
            Advance(value.Length);
            return default;
        }
        return Core(value, cancellationToken);

        async ValueTask Core(ReadOnlyMemory<byte> remaining, CancellationToken token)
        {
            while (!remaining.IsEmpty)
            {
                if (Remaining == 0)
                {
                    if (_flushMode is FlushMode.None)
                        Ensure(1);
                    else
                        await FlushAsync(token).ConfigureAwait(false);
                }
                var count = Math.Min(remaining.Length, Remaining);
                remaining.Span.Slice(0, count).CopyTo(Span);
                Advance(count);
                remaining = remaining.Slice(count);
            }
        }
    }

    public bool ShouldFlush(int byteCount)
        => Remaining < byteCount && _flushMode is not FlushMode.None;

    public void Flush(TimeSpan timeout = default)
    {
        if (_flushMode is FlushMode.None)
            return;
        if (_flushMode is FlushMode.NonBlocking)
            throw new NotSupportedException("Use FlushAsync on a non-blocking PgWriter.");
        if (_writer is not IOutputWriter output)
            throw new NotSupportedException("The underlying writer does not support flushing.");

        Commit();
        output.Flush(timeout);
        RequestBuffer(0);
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_flushMode is FlushMode.None)
            return;
        if (_flushMode is FlushMode.Blocking)
            throw new NotSupportedException("Use Flush on a blocking PgWriter.");
        if (_writer is not IOutputWriter output)
            throw new NotSupportedException("The underlying writer does not support flushing.");

        Commit();
        await output.FlushAsync(cancellationToken).ConfigureAwait(false);
        RequestBuffer(0);
    }

    internal ValueTask Flush(bool async, CancellationToken cancellationToken = default)
    {
        if (async)
            return FlushAsync(cancellationToken);
        Flush();
        return default;
    }

    sealed class WriterStream(PgWriter writer) : Stream
    {
        public override void Write(ReadOnlySpan<byte> buffer) => writer.WriteBytes(buffer);
        public override void Write(byte[] buffer, int offset, int count)
        {
            ArgumentNullException.ThrowIfNull(buffer);
            writer.WriteBytes(buffer.AsSpan(offset, count));
        }
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
            => writer.WriteBytesAsync(buffer, cancellationToken);
        public override Task WriteAsync(byte[] buffer, int offset, int count,
            CancellationToken cancellationToken)
            => writer.WriteBytesAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override void Flush() => writer.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken)
            => writer.FlushAsync(cancellationToken).AsTask();
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
