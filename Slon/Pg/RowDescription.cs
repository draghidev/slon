using System.Buffers;
using System.Text;
using Slon.Pg.Protocol;
using Slon.Pg.Types;

namespace Slon.Pg;

readonly record struct RowDescriptionField(
    string Name,
    Oid TableOid,
    short ColumnAttributeNumber,
    Oid TypeOid,
    short TypeSize,
    int TypeModifier,
    PgFormat Format);

sealed class RowDescription
{
    const int MaxRetainedFieldCapacity = 256;
    RowDescriptionField[] _fields = [];
    int _fieldCount;

    public int FieldCount => _fieldCount;
    public ref readonly RowDescriptionField this[int ordinal]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(ordinal, _fieldCount);
            return ref _fields[ordinal];
        }
    }

    public void Initialize(SequenceReader<byte> reader, Encoding? encoding = null)
    {
        if (!reader.TryReadBigEndian(out short fieldCount) || fieldCount < 0)
            throw PgProtocolException.NotEnoughData(nameof(RowDescription));

        encoding ??= Encoding.UTF8;
        if (_fields.Length < fieldCount)
            _fields = new RowDescriptionField[fieldCount];

        for (var i = 0; i < fieldCount; i++)
        {
            if (!reader.TryReadTo(out ReadOnlySequence<byte> name, (byte)0,
                    advancePastDelimiter: true)
                || !reader.TryReadBigEndian(out int tableOid)
                || !reader.TryReadBigEndian(out short columnAttributeNumber)
                || !reader.TryReadBigEndian(out int typeOid)
                || !reader.TryReadBigEndian(out short typeSize)
                || !reader.TryReadBigEndian(out int typeModifier)
                || !reader.TryReadBigEndian(out short format))
                throw PgProtocolException.NotEnoughData(nameof(RowDescription));

            if (format is not ((short)PgFormat.Text) and not ((short)PgFormat.Binary))
                throw new PgProtocolException(
                    $"RowDescription field {i} has unknown format code {format}.");

            _fields[i] = new(
                encoding.GetString(name),
                (Oid)unchecked((uint)tableOid),
                columnAttributeNumber,
                (Oid)unchecked((uint)typeOid),
                typeSize,
                typeModifier,
                (PgFormat)format);
        }

        if (reader.Remaining != 0)
            throw new PgProtocolException("RowDescription contains trailing data.");

        if (fieldCount < _fieldCount)
            _fields.AsSpan(fieldCount, _fieldCount - fieldCount).Clear();
        _fieldCount = fieldCount;
    }

    public RowDescription Preserve()
        => IsNoData
            ? this
            : new()
            {
                _fields = _fields.AsSpan(0, _fieldCount).ToArray(),
                _fieldCount = _fieldCount
            };

    // Called when the owning flow retires, after its final CommandResult tenure has ended. An unusually
    // wide description no longer needs to remain rooted; normal high-water storage is kept for reuse.
    public void PrepareForReuse()
    {
        if (_fields.Length > MaxRetainedFieldCapacity)
            _fields = [];
        else
            _fields.AsSpan(0, _fieldCount).Clear();

        _fieldCount = 0;
    }

    public bool IsNoData => ReferenceEquals(this, NoData);

    // A RowDescription instance indicating that the command returns no data.
    public static RowDescription NoData { get; } = new();
}
