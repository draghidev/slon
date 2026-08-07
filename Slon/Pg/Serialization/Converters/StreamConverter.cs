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
        => throw new NotSupportedException("Stream parameter writing is not implemented yet.");

    public override void Write(PgWriter writer, Stream value)
        => throw new NotSupportedException("Stream parameter writing is not implemented yet.");

    public override ValueTask WriteAsync(PgWriter writer, Stream value,
        CancellationToken cancellationToken = default)
        => ValueTask.FromException(
            new NotSupportedException("Stream parameter writing is not implemented yet."));
}
