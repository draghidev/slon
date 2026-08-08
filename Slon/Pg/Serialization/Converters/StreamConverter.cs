namespace Slon.Pg.Serialization.Converters;

sealed class StreamConverter : PgStreamingConverter<Stream>
{
    public override ConverterDescriptor GetDescriptor(in DescriptorContext context)
        => ConverterDescriptor.Invariant with { BufferRequirements = BufferRequirements.Streaming };

    public override Stream Read(PgReader reader) => reader.GetStream();

    public override ValueTask<Stream> ReadAsync(PgReader reader,
        CancellationToken cancellationToken = default)
        => new(reader.GetStream());

    protected override Size BindValue(in BindContext context, Stream value, ref object? writeState)
    {
        if (value.CanSeek)
            return checked((int)(value.Length - value.Position));

        var buffered = new MemoryStream();
        writeState = buffered;
        value.CopyTo(buffered);
        return checked((int)buffered.Length);
    }

    public override void Write(PgWriter writer, Stream value)
    {
        if (writer.WriteState is MemoryStream buffered)
        {
            if (!buffered.TryGetBuffer(out var segment))
                throw new InvalidOperationException("Buffered stream state is not publicly visible.");
            writer.WriteBytes(segment.AsSpan());
        }
        else if (value is MemoryStream memoryStream && memoryStream.TryGetBuffer(out var segment))
        {
            writer.WriteBytes(segment.AsSpan(checked((int)value.Position)));
            value.Position = value.Length;
        }
        else
        {
            value.CopyTo(writer.GetStream());
        }
    }

    public override ValueTask WriteAsync(PgWriter writer, Stream value,
        CancellationToken cancellationToken = default)
    {
        if (writer.WriteState is MemoryStream buffered)
        {
            if (!buffered.TryGetBuffer(out var segment))
                return ValueTask.FromException(
                    new InvalidOperationException("Buffered stream state is not publicly visible."));
            return writer.WriteBytesAsync(segment.AsMemory(), cancellationToken);
        }
        if (value is MemoryStream memoryStream && memoryStream.TryGetBuffer(out var memorySegment))
            return WriteMemoryStreamAsync(writer, memoryStream,
                memorySegment.AsMemory(checked((int)value.Position)), cancellationToken);
        return new(value.CopyToAsync(writer.GetStream(), cancellationToken));

        static async ValueTask WriteMemoryStreamAsync(PgWriter writer, MemoryStream stream,
            ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken)
        {
            await writer.WriteBytesAsync(bytes, cancellationToken).ConfigureAwait(false);
            stream.Position = stream.Length;
        }
    }
}
