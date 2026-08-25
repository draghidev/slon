using System.Buffers;
using System.Globalization;
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
    Dictionary<string, int>? _nameIndex;
    Dictionary<string, int>? _insensitiveNameIndex;
    bool _preserved;

    static readonly StringComparer InsensitiveNameComparer =
        CultureInfo.InvariantCulture.CompareInfo.GetStringComparer(
            CompareOptions.IgnoreWidth | CompareOptions.IgnoreCase | CompareOptions.IgnoreKanaType);

    public int FieldCount => _fieldCount;
    public ref readonly RowDescriptionField this[int ordinal]
    {
        get
        {
            if ((uint)ordinal >= (uint)_fieldCount)
                throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range.");
            return ref _fields[ordinal];
        }
    }

    public int GetFieldIndex(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var exact = _nameIndex ??= BuildNameIndex(StringComparer.Ordinal);
        if (exact.TryGetValue(name, out var ordinal))
            return ordinal;

        var insensitive = _insensitiveNameIndex ??= BuildNameIndex(InsensitiveNameComparer);
        if (insensitive.TryGetValue(name, out ordinal))
            return ordinal;
        throw new IndexOutOfRangeException($"Column '{name}' does not exist in the result.");
    }

    Dictionary<string, int> BuildNameIndex(StringComparer comparer)
    {
        var result = new Dictionary<string, int>(_fieldCount, comparer);
        for (var i = 0; i < _fieldCount; i++)
            result.TryAdd(_fields[i].Name, i);
        return result;
    }

    public void Initialize(SequenceReader<byte> reader, Encoding? encoding = null)
    {
        _nameIndex = null;
        _insensitiveNameIndex = null;
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
                _fieldCount = _fieldCount,
                _preserved = true
            };

    // Preserved descriptions have statement lifetime and may safely key higher-level caches.
    // The protocol-static instance is reused for unrelated results and must never do so.
    internal bool IsPreserved => _preserved;

    // Called when the owning flow retires, after its final CommandResult tenure has ended. An unusually
    // wide description no longer needs to remain rooted; normal high-water storage is kept for reuse.
    public void Reset()
    {
        if (_fields.Length > MaxRetainedFieldCapacity)
            _fields = [];
        else
            _fields.AsSpan(0, _fieldCount).Clear();

        _fieldCount = 0;
        _nameIndex = null;
        _insensitiveNameIndex = null;
        _preserved = false;
    }

    public bool IsNoData => ReferenceEquals(this, NoData);

    // A RowDescription instance indicating that the command returns no data.
    public static RowDescription NoData { get; } = new();
}
