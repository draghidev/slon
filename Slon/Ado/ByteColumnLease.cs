using Slon.Pg;
using Slon.Pg.Serialization;

namespace Slon;

sealed class ByteColumnLease(PgReader reader, bool sequential) : IColumnLease
{
    bool _revoked;

    internal int Length => reader.FieldSize;

    internal int Read(int offset, Span<byte> destination)
    {
        ObjectDisposedException.ThrowIf(_revoked, this);
        if (sequential && offset < reader.FieldOffset)
            throw new InvalidOperationException(
                "Attempted to read a position in a sequential column which has already been consumed.");
        reader.Seek(offset);
        var count = Math.Min(destination.Length, reader.CurrentRemaining);
        reader.Read(destination.Slice(0, count));
        return count;
    }

    void IColumnLease.Revoke()
    {
        ObjectDisposedException.ThrowIf(_revoked, this);
        _revoked = true;
    }
}

sealed class ByteColumnLeaseConverter : PgStreamingConverter<ByteColumnLease>
{
    internal override bool ResultIsColumnLease => true;
    public override ConverterDescriptor GetDescriptor(in DescriptorContext context)
        => ConverterDescriptor.Invariant with { BufferRequirements = BufferRequirements.Streaming };

    public override ByteColumnLease Read(PgReader reader) => new(reader, reader.IsSequential);
    public override ValueTask<ByteColumnLease> ReadAsync(PgReader reader,
        CancellationToken cancellationToken = default) => new(Read(reader));
    protected override Size BindValue(in BindContext context, ByteColumnLease value,
        ref object? writeState) => throw new NotSupportedException();
    public override void Write(PgWriter writer, ByteColumnLease value) => throw new NotSupportedException();
    public override ValueTask WriteAsync(PgWriter writer, ByteColumnLease value,
        CancellationToken cancellationToken = default)
        => ValueTask.FromException(new NotSupportedException());
}
