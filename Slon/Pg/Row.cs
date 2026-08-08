using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
    IColumnLease? _columnLease;
    int _leasedOrdinal;

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
        RevokeColumnLease();
        EnsureBuffered();
        if (TryGetFieldSpan(ordinal, out var field))
            return RawFieldDecoder.Read<T>(field, textEncoding);

        return GetValueSlow<T>(ordinal, textEncoding);
    }

    internal T GetValue<T, TReader>(int ordinal, ref TReader fieldReader)
        where TReader : struct, IFieldReader<T>
        => fieldReader.Read(new PgField(this, ordinal));

    internal ValueTask<T> GetValueAsync<T, TReader>(int ordinal, TReader fieldReader,
        CancellationToken cancellationToken = default)
        where TReader : struct, IFieldReader<T>
        => fieldReader.ReadAsync(new PgField(this, ordinal), cancellationToken);

    internal ref readonly RowDescriptionField GetFieldMetadata(int ordinal)
        => ref _rowDescription[ordinal];

    internal bool IsColumnPast(int ordinal) => ordinal < _column;

    internal bool IsDBNull(int ordinal)
    {
        RevokeColumnLease();
        if (_bodyReader is null)
            return IsBufferedFieldNull(ordinal);

        while (_column < ordinal)
            SkipLiveField();
        EnsureLiveHeader(_bodyReader);
        return ReadFieldLength(_bodyReader.Buffer, _columnOffset) < 0;
    }

    internal ValueTask<bool> IsDBNullAsync(int ordinal,
        CancellationToken cancellationToken = default)
    {
        if (_columnLease is null && _bodyReader is null)
            return new(IsBufferedFieldNull(ordinal));
        return Core(ordinal, cancellationToken);

        async ValueTask<bool> Core(int fieldOrdinal, CancellationToken token)
        {
            await RevokeColumnLeaseAsync().ConfigureAwait(false);
            if (_bodyReader is null)
                return IsBufferedFieldNull(fieldOrdinal);

            while (_column < fieldOrdinal)
                await SkipLiveFieldAsync(token).ConfigureAwait(false);
            await EnsureLiveHeaderAsync(_bodyReader, token).ConfigureAwait(false);
            return ReadFieldLength(_bodyReader.Buffer, _columnOffset) < 0;
        }
    }

    bool IsBufferedFieldNull(int ordinal)
    {
        var reader = GetColumnReader(ordinal, out var columnIndex, out var columnOffset);
        if (!TrySeek(ref reader, ref columnIndex, ordinal, ref columnOffset, out var length))
            ThrowHelper.ThrowInvalidOperation("Field length is truncated.");
        return length < 0;
    }

    internal ReadOnlySequence<byte> GetBufferedField(int ordinal)
    {
        EnsureBuffered();
        return GetFieldSequence(ordinal);
    }

    internal ValueTask<ReadOnlySequence<byte>> GetBufferedFieldAsync(int ordinal,
        CancellationToken cancellationToken = default)
    {
        if (_bodyReader is null)
            return new(GetFieldSequence(ordinal));
        return Core(ordinal, cancellationToken);

        async ValueTask<ReadOnlySequence<byte>> Core(int fieldOrdinal, CancellationToken token)
        {
            await _bodyReader.BufferAllAsync(token).ConfigureAwait(false);
            _bodyReader = null;
            return GetFieldSequence(fieldOrdinal);
        }
    }

    internal PgReader OpenFieldReader(int ordinal, PgConversionContext conversionContext)
    {
        RevokeColumnLease();
        if (_bodyReader is null)
            return new(GetFieldSequence(ordinal), conversionContext);
        if (ordinal < _column)
            throw new InvalidOperationException(
                "A field preceding the sequential row cursor is no longer available.");

        while (_column < ordinal)
            SkipLiveField();
        return OpenCurrentLiveField(conversionContext);
    }

    internal ValueTask<PgReader> OpenFieldReaderAsync(int ordinal,
        PgConversionContext conversionContext, CancellationToken cancellationToken = default)
    {
        if (_columnLease is not null)
            return RevokeAndOpenFieldReaderAsync(ordinal, conversionContext, cancellationToken);
        if (_bodyReader is null)
            return new(new PgReader(GetFieldSequence(ordinal), conversionContext));
        if (ordinal < _column)
            throw new InvalidOperationException(
                "A field preceding the sequential row cursor is no longer available.");
        return Core(ordinal, conversionContext, cancellationToken);

        async ValueTask<PgReader> Core(int fieldOrdinal, PgConversionContext context,
            CancellationToken token)
        {
            while (_column < fieldOrdinal)
                await SkipLiveFieldAsync(token).ConfigureAwait(false);
            return await OpenCurrentLiveFieldAsync(context, token).ConfigureAwait(false);
        }
    }

    async ValueTask<PgReader> RevokeAndOpenFieldReaderAsync(int ordinal,
        PgConversionContext conversionContext, CancellationToken cancellationToken)
    {
        await RevokeColumnLeaseAsync().ConfigureAwait(false);
        return await OpenFieldReaderAsync(ordinal, conversionContext, cancellationToken)
            .ConfigureAwait(false);
    }

    internal void CompleteFieldReader(int ordinal, PgReader reader)
    {
        var offset = reader.CompleteField();
        if (offset >= 0)
            _columnOffset = offset;
        _column = ordinal + 1;
    }

    internal async ValueTask CompleteFieldReaderAsync(int ordinal, PgReader reader)
    {
        var offset = await reader.CompleteFieldAsync().ConfigureAwait(false);
        if (offset >= 0)
            _columnOffset = offset;
        _column = ordinal + 1;
    }

    internal bool TryGetColumnLease<T>(int ordinal, [NotNullWhen(true)] out T? lease)
        where T : class, IColumnLease
    {
        lease = _leasedOrdinal == ordinal ? _columnLease as T : null;
        return lease is not null;
    }

    internal void LeaseColumn(int ordinal, IColumnLease lease)
    {
        if (_columnLease is not null)
            throw new InvalidOperationException("A column lease is already active.");
        _leasedOrdinal = ordinal;
        _columnLease = lease;
    }

    internal void RevokeColumnLease()
    {
        if (_columnLease is not { } lease)
            return;
        _columnLease = null;
        var ordinal = _leasedOrdinal;
        var offset = lease.Revoke();
        if (offset >= 0)
            _columnOffset = offset;
        _column = ordinal + 1;
    }

    internal async ValueTask RevokeColumnLeaseAsync()
    {
        if (_columnLease is not { } lease)
            return;
        _columnLease = null;
        var ordinal = _leasedOrdinal;
        var offset = await lease.RevokeAsync().ConfigureAwait(false);
        if (offset >= 0)
            _columnOffset = offset;
        _column = ordinal + 1;
    }

    internal bool HasColumnLease => _columnLease is not null;

    PgReader OpenCurrentLiveField(PgConversionContext conversionContext)
    {
        var source = _bodyReader!;
        EnsureLiveHeader(source);
        var buffer = source.Buffer;
        var length = ReadFieldLength(buffer, _columnOffset);
        if (length < 0)
            ThrowHelper.ThrowInvalidOperation("Field is null.");
        var dataOffset = checked(_columnOffset + sizeof(int));
        return new(source, buffer.Slice(dataOffset), length, dataOffset, conversionContext);
    }

    async ValueTask<PgReader> OpenCurrentLiveFieldAsync(PgConversionContext conversionContext,
        CancellationToken cancellationToken)
    {
        var source = _bodyReader!;
        await EnsureLiveHeaderAsync(source, cancellationToken).ConfigureAwait(false);
        var buffer = source.Buffer;
        var length = ReadFieldLength(buffer, _columnOffset);
        if (length < 0)
            ThrowHelper.ThrowInvalidOperation("Field is null.");
        var dataOffset = checked(_columnOffset + sizeof(int));
        return new(source, buffer.Slice(dataOffset), length, dataOffset, conversionContext);
    }

    void SkipLiveField()
    {
        var source = _bodyReader!;
        EnsureLiveHeader(source);
        var buffer = source.Buffer;
        var length = ReadFieldLength(buffer, _columnOffset);
        var dataOffset = checked(_columnOffset + sizeof(int));
        using var reader = new PgReader(source, buffer.Slice(dataOffset), Math.Max(0, length),
            dataOffset, PgConversionContext.Empty);
        reader.Consume();
        CompleteFieldReader(_column, reader);
    }

    async ValueTask SkipLiveFieldAsync(CancellationToken cancellationToken)
    {
        var source = _bodyReader!;
        await EnsureLiveHeaderAsync(source, cancellationToken).ConfigureAwait(false);
        var buffer = source.Buffer;
        var length = ReadFieldLength(buffer, _columnOffset);
        var dataOffset = checked(_columnOffset + sizeof(int));
        var reader = new PgReader(source, buffer.Slice(dataOffset), Math.Max(0, length),
            dataOffset, PgConversionContext.Empty);
        try
        {
            await reader.ConsumeAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            await CompleteFieldReaderAsync(_column, reader).ConfigureAwait(false);
        }
        finally
        {
            await reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    static int ReadFieldLength(in ReadOnlySequence<byte> buffer, int offset)
    {
        var reader = new SequenceReader<byte>(buffer.Slice(offset));
        if (!reader.TryReadBigEndian(out int length))
            ThrowHelper.ThrowInvalidOperation("Field length is truncated.");
        return length;
    }

    void EnsureLiveHeader(BackendMessageBodyReader source)
    {
        while (source.Buffer.Length - _columnOffset < sizeof(int))
        {
            if (source.IsComplete)
                ThrowHelper.ThrowInvalidOperation("Field length is truncated.");
            source.Extend();
        }
    }

    async ValueTask EnsureLiveHeaderAsync(BackendMessageBodyReader source,
        CancellationToken cancellationToken)
    {
        while (source.Buffer.Length - _columnOffset < sizeof(int))
        {
            if (source.IsComplete)
                ThrowHelper.ThrowInvalidOperation("Field length is truncated.");
            await source.ExtendAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    ReadOnlySequence<byte> GetFieldSequence(int ordinal)
    {
        var columnIndex = _column <= ordinal ? _column : 0;
        var columnOffset = _column <= ordinal ? _columnOffset : sizeof(short);
        var reader = new SequenceReader<byte>(Message.GetSequence(columnOffset));
        while (columnIndex < ordinal)
        {
            if (!reader.TryReadBigEndian(out int skippedLength) || skippedLength < -1
                || reader.Remaining < Math.Max(0, skippedLength))
                ThrowHelper.ThrowInvalidOperation("Field is null, truncated, or unavailable.");
            var skippedDataLength = Math.Max(0, skippedLength);
            reader.Advance(skippedDataLength);
            columnOffset += sizeof(int) + skippedDataLength;
            columnIndex++;
        }

        if (!reader.TryReadBigEndian(out int length) || length < 0 || reader.Remaining < length)
            ThrowHelper.ThrowInvalidOperation("Field is null, truncated, or unavailable.");

        _column = columnIndex + 1;
        _columnOffset = columnOffset + sizeof(int) + length;
        return reader.Sequence.Slice(reader.Position, length);
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

    internal ValueTask BufferAllAsync(CancellationToken cancellationToken = default)
    {
        if (_bodyReader is null)
            return default;
        return Core(cancellationToken);

        async ValueTask Core(CancellationToken token)
        {
            await _bodyReader.BufferAllAsync(token).ConfigureAwait(false);
            _bodyReader = null;
        }
    }

    public Reader GetReader()
    {
        RevokeColumnLease();
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
        if (_columnLease is not null)
            throw new InvalidOperationException("The previous column lease must be revoked before advancing the row.");
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
