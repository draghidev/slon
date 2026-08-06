using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Buffers;
using Slon.Pg.Protocol;
using Slon.Pg.Serialization;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg;

sealed class Row
{
    BackendMessage.Accessor _messageAccessor;
    RowDescription _rowDescription = null!;
    BackendMessageBodyReader? _bodyReader;

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
        => GetValueCore<T>(ordinal, textEncoding: null);

    // Bootstrap consumers have no serializer but must still bind text decoding to one negotiated
    // encoding snapshot for the lifetime of their operation.
    internal T GetValue<T>(int ordinal, Encoding textEncoding)
        => GetValueCore<T>(ordinal, textEncoding);

    T GetValueCore<T>(int ordinal, Encoding? textEncoding)
    {
        EnsureBuffered();
        if (TryGetFieldSpan(ordinal, out var field))
            return RawFieldDecoder.Read<T>(field, textEncoding);

        return GetValueSlow<T>(ordinal, textEncoding);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    T GetValueSlow<T>(int ordinal, Encoding? textEncoding)
    {
        var reader = GetColumnReader(ordinal, out var columnIndex, out var columnOffset);
        _ = TrySeek(ref reader, ref columnIndex, ordinal, ref columnOffset, out var length);
        _column = columnIndex;
        _columnOffset = columnOffset;
        if (length < 0 || reader.Remaining < length)
            ThrowHelper.ThrowInvalidOperation();
        if (reader.CurrentSpan.Length - reader.CurrentSpanIndex >= length)
            return RawFieldDecoder.Read<T>(reader.CurrentSpan.Slice(reader.CurrentSpanIndex, length), textEncoding);
        var sequence = reader.Sequence.Slice(reader.Consumed, length);
        return RawFieldDecoder.Read<T>(sequence, textEncoding);
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
    {
        if (_bodyReader is null)
            return new(GetValue<T>(ordinal));
        return Core(ordinal, cancellationToken);

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        async ValueTask<T> Core(int ordinal, CancellationToken cancellationToken)
        {
            await _bodyReader.BufferAllAsync(cancellationToken).ConfigureAwait(false);
            _bodyReader = null;
            return GetValue<T>(ordinal);
        }
    }

    public Reader GetReader()
    {
        EnsureBuffered();
        return new(this);
    }

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
            return RawFieldDecoder.Read<T>(field);
        }
    }

    internal void Initialize(RowDescription rowDescription)
    {
        if (!ReferenceEquals(_rowDescription, rowDescription))
            _rowDescription = rowDescription;
    }

    internal void InitializeRow(in BackendMessage row)
    {
        _bodyReader = row.Buffered ? null : row.OpenBodyReader();
        _column = 0;
        _columnOffset = sizeof(short);
        BackendMessage.Accessor.WriteGranularly(ref _messageAccessor, row.GetAccessor());
    }

    void EnsureBuffered()
    {
        if (_bodyReader is null)
            return;
        _bodyReader.BufferAll();
        _bodyReader = null;
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
