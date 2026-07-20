using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Buffers;
using Slon.Pg.Protocol;

namespace Slon.Pg;

sealed class Row
{
    BackendMessage.Accessor _messageAccessor;
    RowDescription _rowDescription = null!;

    int _column = -1;
    int _columnOffset;

    BackendMessage Message => _messageAccessor.Message;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    SequenceReader<byte> GetColumnReader(int ordinal, out int columnIndex, out int columnOffset)
    {
        Debug.Assert(_column >= 0);
        if (_column <= ordinal)
        {
            columnIndex = _column;
            columnOffset = _columnOffset;
        }
        else
        {
            columnIndex = 0;
            columnOffset = sizeof(short);
        }

        return new(Message.GetSequence(columnOffset));
    }

    public T GetValue<T>(int ordinal)
    {
        if (TryGetFieldSpan(ordinal, out var field))
            return Decode<T>(field);

        return GetValueSlow<T>(ordinal);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    T GetValueSlow<T>(int ordinal)
    {
        var reader = GetColumnReader(ordinal, out var columnIndex, out var columnOffset);
        _ = TrySeek(ref reader, ref columnIndex, ordinal, ref columnOffset, out var length);
        _column = columnIndex;
        _columnOffset = columnOffset;
        if (typeof(T) == typeof(int))
        {
            if (reader.TryPeekBigEndian(out int value))
                return (T)(object)value;

            ThrowHelper.ThrowInvalidOperation();
        }

        if (typeof(T) == typeof(byte[]))
        {
            if (reader.CurrentSpan.Length - reader.CurrentSpanIndex >= length)
                return (T)(object)reader.CurrentSpan.Slice(reader.CurrentSpanIndex, length).ToArray();

            Debug.Assert(reader.Remaining >= length);
            // if (reader.Remaining >= length)
            return (T)(object)reader.Sequence.Slice(reader.Consumed, length).ToArray();
        }

        if (typeof(T) == typeof(string))
        {
            if (reader.CurrentSpan.Length - reader.CurrentSpanIndex >= length)
                return (T)(object)Encoding.UTF8.GetString(reader.CurrentSpan.Slice(reader.CurrentSpanIndex, length));

            Debug.Assert(reader.Remaining >= length);
            // if (reader.Remaining >= length)
            return (T)(object)Encoding.UTF8.GetString(reader.Sequence.Slice(reader.Consumed, length));
        }

        ThrowHelper.ThrowInvalidOperation();
        return default;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static T Decode<T>(ReadOnlySpan<byte> field)
    {
        if (typeof(T) == typeof(int))
        {
            if (field.Length >= sizeof(int))
                return (T)(object)BinaryPrimitives.ReadInt32BigEndian(field);

            ThrowHelper.ThrowInvalidOperation();
        }

        if (typeof(T) == typeof(byte[]))
            return (T)(object)field.ToArray();

        if (typeof(T) == typeof(string))
            return (T)(object)Encoding.UTF8.GetString(field);

        ThrowHelper.ThrowInvalidOperation();
        return default;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool TryGetFieldSpan(int ordinal, out ReadOnlySpan<byte> field)
    {
        var columnIndex = _column <= ordinal ? _column : 0;
        var columnOffset = _column <= ordinal ? _columnOffset : sizeof(short);
        if (!Message.TryGetFirstSpan(columnOffset, out var remaining))
        {
            field = default;
            return false;
        }

        while (columnIndex++ < ordinal)
        {
            if (remaining.Length < sizeof(int))
            {
                field = default;
                return false;
            }

            var length = BinaryPrimitives.ReadInt32BigEndian(remaining);
            var fieldSize = sizeof(int) + (length <= 0 ? 0 : length);
            if ((uint)fieldSize > (uint)remaining.Length)
            {
                field = default;
                return false;
            }

            remaining = remaining.Slice(fieldSize);
            columnOffset += fieldSize;
        }

        if (remaining.Length < sizeof(int))
        {
            field = default;
            return false;
        }

        var fieldLength = BinaryPrimitives.ReadInt32BigEndian(remaining);
        if (fieldLength < 0 || fieldLength > remaining.Length - sizeof(int))
        {
            field = default;
            return false;
        }

        _column = columnIndex;
        _columnOffset = columnOffset + sizeof(int) + fieldLength;
        field = remaining.Slice(sizeof(int), fieldLength);
        return true;
    }

    public ValueTask<T> GetValueAsync<T>(int ordinal, CancellationToken cancellationToken = default)
        => new(GetValue<T>(ordinal));

    public Reader GetReader() => new(this);

    public ref struct Reader
    {
        readonly Row _row;
        ReadOnlySpan<byte> _remaining;
        int _ordinal;

        internal Reader(Row row)
        {
            _row = row;
            _ordinal = 0;
            if (!row.Message.TryGetFirstSpan(sizeof(short), out _remaining))
                _remaining = default;
        }

        public T Read<T>()
        {
            var ordinal = _ordinal++;
            if (_remaining.IsEmpty)
                return _row.GetValue<T>(ordinal);

            if (_remaining.Length < sizeof(int))
            {
                _remaining = default;
                return _row.GetValue<T>(ordinal);
            }

            var length = BinaryPrimitives.ReadInt32BigEndian(_remaining);
            if (length < 0 || length > _remaining.Length - sizeof(int))
            {
                _remaining = default;
                return _row.GetValue<T>(ordinal);
            }

            var field = _remaining.Slice(sizeof(int), length);
            _remaining = _remaining.Slice(sizeof(int) + length);
            return Decode<T>(field);
        }
    }

    internal void Initialize(RowDescription rowDescription)
    {
        if (!ReferenceEquals(_rowDescription, rowDescription))
            _rowDescription = rowDescription;
    }

    internal void InitializeRow(in BackendMessage row)
    {
        Debug.Assert(row.Buffered, "Column streaming is not implemented yet");
        _column = 0;
        _columnOffset = sizeof(short);
        BackendMessage.Accessor.WriteGranularly(ref _messageAccessor, row.GetAccessor());
    }

    // Returns false when the seek was exhausted, true if positioned correctly, and throws if the seek is invalid.
    static bool TrySeek(ref SequenceReader<byte> reader, ref int columnIndex, int ordinal, ref int columnOffset, out int length)
    {
        length = 0;
        while (columnIndex++ < ordinal)
        {
            if (!reader.TryPeekBigEndian(out length))
                return false;

            var fieldSize = sizeof(int) + (length <= 0 ? 0 : length);
            reader.Advance(fieldSize);
            columnOffset += fieldSize;
        }

        if (!reader.TryPeekBigEndian(out length))
            return false;

        reader.Advance(sizeof(int));
        columnOffset += sizeof(int) + (length <= 0 ? 0 : length);
        return true;
    }

}
