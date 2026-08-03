using System.Buffers;
using Slon.Pg.Protocol;

namespace Slon.Pg;

sealed class RowDescription
{
    public int FieldCount { get; private set; }

    public void Initialize(SequenceReader<byte> reader)
    {
        if (!reader.TryReadBigEndian(out short fieldCount))
            throw PgProtocolException.NotEnoughData();
        FieldCount = fieldCount;
    }

    public RowDescription Preserve() => IsNoData ? this : new() { FieldCount = FieldCount };

    public bool IsNoData => ReferenceEquals(this, NoData);

    // A RowDescription instance indicating that the command returns no data.
    public static RowDescription NoData { get; } = new();
}
