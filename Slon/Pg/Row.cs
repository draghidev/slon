using System.Buffers;
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

    // We don't store the first column as it's always easily derivable.
    // TODO assign once we support seeking backwards.
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value
    int[]? _tailColumnPositions;
#pragma warning restore CS0649 // Field is never assigned to, and will always have its default value
    int _column = -1;

    BackendMessage Message => _messageAccessor.Message;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    SequenceReader<byte> GetColumnReader(out int columnIndex)
    {
        Debug.Assert(_column >= 0);
        var offset = sizeof(short);
        if (_column is 0 || _tailColumnPositions is null)
        {
            columnIndex = 0;
        }
        else
        {
            offset += _tailColumnPositions[_column - 1];
            columnIndex = _column;
        }

        return new(Message.GetSequence(offset));
    }

    public T GetValue<T>(int ordinal)
    {
        var reader = GetColumnReader(out var columnIndex);
        _ = TrySeek(ref reader, columnIndex, ordinal, out var length);
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

    public ValueTask<T> GetValueAsync<T>(int ordinal, CancellationToken cancellationToken = default)
        => new(GetValue<T>(ordinal));

    internal void Initialize(RowDescription rowDescription)
    {
        if (!ReferenceEquals(_rowDescription, rowDescription))
            _rowDescription = rowDescription;
    }

    internal void InitializeRow(in BackendMessage row)
    {
        Debug.Assert(row.Buffered, "Column streaming is not implemented yet");
        _column = 0;
        BackendMessage.Accessor.WriteGranularly(ref _messageAccessor, row.GetAccessor());
    }

    // Returns false when the seek was exhausted, true if positioned correctly, and throws if the seek is invalid.
    static bool TrySeek(ref SequenceReader<byte> reader, int columnIndex, int ordinal, out int length)
    {
        if (columnIndex <= ordinal)
        {
            length = 0;
            while (columnIndex++ < ordinal)
            {
                if (!reader.TryPeekBigEndian(out length))
                    return false;

                reader.Advance(sizeof(int) + (length <= 0 ? 0 : length));
            }

            if (!reader.TryPeekBigEndian(out length))
                return false;

            reader.Advance(sizeof(int));
            return true;
        }

        if (columnIndex > ordinal)
        {
            length = SeekBackwards(ordinal);
            return true;
        }

        ThrowHelper.ThrowInvalidOperation();
        length = default;
        return false;

        // On the first call to SeekBackwards we'll fill up the columns list as we may need seek positions more than once.
        [MethodImpl(MethodImplOptions.NoInlining)]
        int SeekBackwards(int ordinal)
        {
            throw new NotSupportedException();
            // var buffer = Buffer;
            // var columns = _columns;
            //
            // (buffer.ReadPosition, var columnLength) = columns.Count is 0
            //     ? (_columnsStartPos, 0)
            //     : columns[Math.Min(columns.Count -1, ordinal)];
            //
            // while (columns.Count <= ordinal)
            // {
            //     if (columnLength > 0)
            //         buffer.Skip(columnLength);
            //     columnLength = buffer.ReadInt32();
            //     columns.Add((buffer.ReadPosition, columnLength));
            // }
            //
            // return columnLength;
        }
    }

}
