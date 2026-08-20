using Slon.Pg;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon;

sealed class CharsColumnLease(TextReader reader, bool sequential)
    : IColumnLease
{
    string? _buffered;
    int _charsRead;
    bool _revoked;

    internal int Read(int offset, Span<char> destination, bool countOnly)
    {
        ObjectDisposedException.ThrowIf(_revoked, this);
        if (!sequential)
        {
            _buffered ??= reader.ReadToEnd();
            if (countOnly)
                return _buffered.Length;
            if (offset >= _buffered.Length)
                return 0;
            var count = Math.Min(destination.Length, _buffered.Length - offset);
            _buffered.AsSpan(offset, count).CopyTo(destination);
            return count;
        }

        if (offset < _charsRead)
            throw new InvalidOperationException(
                "Attempted to read a position in a sequential column which has already been consumed.");
        Consume(offset - _charsRead);
        if (countOnly)
        {
            Consume(count: null);
            return _charsRead;
        }

        var read = reader.ReadBlock(destination);
        _charsRead += read;
        return read;
    }

    void Consume(int count)
    {
        if (count == 0)
            return;
        var consumed = Consume((int?)count);
        if (consumed != count)
            throw new EndOfStreamException();
    }

    int Consume(int? count)
    {
        Span<char> scratch = stackalloc char[512];
        var total = 0;
        while (count is null || total < count)
        {
            var requested = count is null ? scratch.Length : Math.Min(scratch.Length, count.Value - total);
            var read = reader.ReadBlock(scratch.Slice(0, requested));
            if (read == 0)
                break;
            total += read;
        }
        _charsRead += total;
        return total;
    }

    void IColumnLease.Revoke()
    {
        ObjectDisposedException.ThrowIf(_revoked, this);
        _revoked = true;
    }
}

static class AdoCharsConverters
{
    internal static readonly PgConverter<CharsColumnLease> Text = new GetCharsTextConverter();
    static readonly PgConverter<CharsColumnLease> VersionedText =
        new VersionPrefixedGetCharsConverter(version: 1, new GetCharsTextConverter());

    internal static PgConverter<CharsColumnLease> Get(DataTypeName dataTypeName,
        DataFormat format)
    {
        if ((dataTypeName == DataTypeNames.Jsonb || dataTypeName == DataTypeNames.Jsonpath)
            && format is DataFormat.Binary)
            return VersionedText;
        if (dataTypeName == DataTypeNames.Text || dataTypeName == DataTypeNames.Varchar
            || dataTypeName == DataTypeNames.Bpchar || dataTypeName == DataTypeNames.Json
            || dataTypeName == DataTypeNames.Jsonb || dataTypeName == DataTypeNames.Jsonpath
            || dataTypeName == DataTypeNames.Xml || dataTypeName == DataTypeNames.Name
            || dataTypeName == DataTypeNames.RefCursor || dataTypeName.UnqualifiedName == "citext")
            return Text;
        throw new InvalidCastException(
            $"PostgreSQL type '{dataTypeName}' does not expose an ADO character projection.");
    }
}

sealed class GetCharsTextConverter : PgStreamingConverter<CharsColumnLease>
{
    internal override bool ResultIsColumnLease => true;
    public override ConverterDescriptor GetDescriptor(in DescriptorContext context)
        => ConverterDescriptor.Invariant with { BufferRequirements = BufferRequirements.Streaming };

    public override CharsColumnLease Read(PgReader reader)
    {
        var textReader = reader.GetTextReader(reader.ConversionContext.TextEncoding);
        return new(textReader, reader.IsSequential);
    }

    public override ValueTask<CharsColumnLease> ReadAsync(PgReader reader,
        CancellationToken cancellationToken = default)
        => new(Read(reader));

    protected override Size BindValue(in BindContext context, CharsColumnLease value,
        ref object? writeState) => throw new NotSupportedException();
    public override void Write(PgWriter writer, CharsColumnLease value)
        => throw new NotSupportedException();
    public override ValueTask WriteAsync(PgWriter writer, CharsColumnLease value,
        CancellationToken cancellationToken = default)
        => ValueTask.FromException(new NotSupportedException());
}

sealed class VersionPrefixedGetCharsConverter(byte version, PgConverter<CharsColumnLease> inner)
    : PgStreamingConverter<CharsColumnLease>
{
    public override ConverterDescriptor GetDescriptor(in DescriptorContext context)
        => ConverterDescriptor.Invariant with { BufferRequirements = BufferRequirements.Streaming };

    public override CharsColumnLease Read(PgReader reader)
    {
        Validate(reader.ReadByte());
        return inner.Read(reader);
    }

    public override async ValueTask<CharsColumnLease> ReadAsync(PgReader reader,
        CancellationToken cancellationToken = default)
    {
        var value = await reader.ReadBytesAsync(1, cancellationToken).ConfigureAwait(false);
        Validate(value.FirstSpan[0]);
        return await inner.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
    }

    void Validate(byte actual)
    {
        if (actual != version)
            throw new InvalidCastException($"Unknown wire format version: {actual}.");
    }

    protected override Size BindValue(in BindContext context, CharsColumnLease value,
        ref object? writeState) => throw new NotSupportedException();
    public override void Write(PgWriter writer, CharsColumnLease value)
        => throw new NotSupportedException();
    public override ValueTask WriteAsync(PgWriter writer, CharsColumnLease value,
        CancellationToken cancellationToken = default)
        => ValueTask.FromException(new NotSupportedException());
}
