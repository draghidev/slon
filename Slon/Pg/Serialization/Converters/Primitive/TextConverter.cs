using System.Buffers;

namespace Slon.Pg.Serialization.Converters;

static class TextConverter
{
    public static PgConverter<string> CreateStringConverter() => new StringTextConverter();
    public static PgConverter<TextReader> CreateTextReaderConverter() => new TextReaderConverter();

    sealed class StringTextConverter : PgStreamingConverter<string>
    {
        public override ConverterDescriptor GetDescriptor(in DescriptorContext context)
            => ConverterDescriptor.Invariant with { BufferRequirements = BufferRequirements.Streaming };

        public override string Read(PgReader reader)
        {
            var bytes = reader.ReadBytes(reader.CurrentRemaining);
            var encoding = reader.ConversionContext.TextEncoding;
            return bytes.IsSingleSegment
                ? encoding.GetString(bytes.FirstSpan)
                : encoding.GetString(bytes.ToArray());
        }

        public override async ValueTask<string> ReadAsync(PgReader reader,
            CancellationToken cancellationToken = default)
        {
            var bytes = await reader.ReadBytesAsync(reader.CurrentRemaining, cancellationToken)
                .ConfigureAwait(false);
            var encoding = reader.ConversionContext.TextEncoding;
            return bytes.IsSingleSegment
                ? encoding.GetString(bytes.FirstSpan)
                : encoding.GetString(bytes.ToArray());
        }

        protected override Size BindValue(in BindContext context, string value, ref object? writeState)
            => context.ConversionContext.TextEncoding.GetByteCount(value);

        public override void Write(PgWriter writer, string value)
            => writer.WriteChars(value.AsSpan(), writer.ConversionContext.TextEncoding);

        public override ValueTask WriteAsync(PgWriter writer, string value,
            CancellationToken cancellationToken = default)
            => writer.WriteCharsAsync(value.AsMemory(), writer.ConversionContext.TextEncoding,
                cancellationToken);
    }

    sealed class TextReaderConverter : PgStreamingConverter<TextReader>
    {
        public override ConverterDescriptor GetDescriptor(in DescriptorContext context)
            => ConverterDescriptor.Invariant with { BufferRequirements = BufferRequirements.Streaming };

        public override TextReader Read(PgReader reader)
            => reader.GetTextReader(reader.ConversionContext.TextEncoding);

        public override ValueTask<TextReader> ReadAsync(PgReader reader,
            CancellationToken cancellationToken = default)
            => reader.GetTextReaderAsync(reader.ConversionContext.TextEncoding, cancellationToken);

        protected override Size BindValue(in BindContext context, TextReader value,
            ref object? writeState)
            => throw new NotSupportedException("TextReader parameter writing is not implemented yet.");

        public override void Write(PgWriter writer, TextReader value)
            => throw new NotSupportedException("TextReader parameter writing is not implemented yet.");

        public override ValueTask WriteAsync(PgWriter writer, TextReader value,
            CancellationToken cancellationToken = default)
            => ValueTask.FromException(
                new NotSupportedException("TextReader parameter writing is not implemented yet."));
    }
}
