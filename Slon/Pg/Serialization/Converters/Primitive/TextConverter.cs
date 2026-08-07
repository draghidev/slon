using System.Buffers;

namespace Slon.Pg.Serialization.Converters;

static class TextConverter
{
    public static PgConverter<string> CreateStringConverter() => new StringTextConverter();

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
}
