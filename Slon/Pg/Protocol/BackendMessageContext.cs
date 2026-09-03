using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pipelines;

namespace Slon.Pg.Protocol;

// Context to manage message streaming, and to limit incurred write barriers per message to the minimum.
sealed class BackendMessageContext
{
    PgDecoder _decoder = null!;
    BackendMessageCursor _cursor;
    bool _hasCursor;
    BackendMessage _current;
    FallbackBuffer _currentFallbackBuffer;
    short _version;
    const byte PriorCancellationExposure = 1 << 0;
    const byte BackendTermination = 1 << 1;
    const byte ErrorObserved = 1 << 2;
    const byte BodyWindowAdvanced = 1 << 3;
    const byte MessageOffsetCaptured = 1 << 4;
    byte _messageState;

    enum PublicationState : byte { None, Current, Peeked }
    PublicationState _publicationState;
    long _currentMessageOffset;
    ContiguousProjection? _contiguousProjections;
    struct FallbackBuffer
    {
        ReadOnlySequenceSegment<byte>? _start;
        ReadOnlySequenceSegment<byte>? _end;
        int _startIndex;
        int _endIndex;

        public readonly bool IsEmpty => _start is null;

        public void Set(in ReadOnlySequence<byte> buffer)
        {
            var start = (ReadOnlySequenceSegment<byte>)buffer.Start.GetObject()!;
            var end = (ReadOnlySequenceSegment<byte>)buffer.End.GetObject()!;
            Set(start, buffer.Start.GetInteger() & int.MaxValue,
                end, buffer.End.GetInteger() & int.MaxValue);
        }

        public void Set(ReadOnlySequenceSegment<byte> start, int startIndex,
            ReadOnlySequenceSegment<byte> end, int endIndex)
        {
            if (!ReferenceEquals(_start, start))
                _start = start;
            if (!ReferenceEquals(_end, end))
                _end = end;
            _startIndex = startIndex;
            _endIndex = endIndex;
        }

        public void Clear()
        {
            if (_start is not null)
                _start = null;
            if (_end is not null)
                _end = null;
            _startIndex = 0;
            _endIndex = 0;
        }

        public readonly ReadOnlySequence<byte> Sequence
            => new(_start!, _startIndex, _end!, _endIndex);
    }

    sealed class ContiguousProjection
    {
        public required byte[] Buffer { get; init; }
        public required SequencePosition Start { get; init; }
        public required int Length { get; init; }
        public ContiguousProjection? Next { get; init; }
    }


    public BackendMessage Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            var current = _current;
            if (_publicationState is not PublicationState.Current)
                ThrowHelper.ThrowInvalidOperation("The decoder has no current backend message.");
            return current;
        }
    }

    public BackendMessage.Accessor CurrentAccessor
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if (_publicationState is not PublicationState.Current)
                ThrowHelper.ThrowInvalidOperation("The decoder has no current backend message.");
            return new(this, _version, _current.Header.Type, _current.Buffered);
        }
    }

    public bool CurrentIsError
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            Debug.Assert(_publicationState is PublicationState.Current);
            return _current.Header.Type is PgTypes.BackendType.ErrorResponse;
        }
    }

    public PgTypes.BackendType CurrentType
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            Debug.Assert(_publicationState is PublicationState.Current);
            return _current.Header.Type;
        }
    }

    public bool CurrentBuffered
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            Debug.Assert(_publicationState is PublicationState.Current);
            return _current.Buffered;
        }
    }

    public ReadOnlyMemory<byte> CurrentBufferedBody
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            Debug.Assert(_publicationState is PublicationState.Current);
            if (_current.TryGetBufferedArrayMemory(0, out var body)
                && body.Length == _current.Header.BodyLength)
                return body;
            return GetCurrentBufferedBodySlow();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    ReadOnlyMemory<byte> GetCurrentBufferedBodySlow()
    {
        if (_current.TryGetBufferedFirstMemory(0, out var body)
            && body.Length == _current.Header.BodyLength)
            return body;

        var sequence = _current.GetSequence();
        return _current.GetContiguousMemory(sequence);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetCurrent(out BackendMessage current)
    {
        current = _current;
        return _publicationState is PublicationState.Current;
    }

    public BackendMessage GetCurrent(short token)
    {
        if (_publicationState is not PublicationState.Current || _version != token)
            ThrowHelper.ThrowInvalidOperation("Backend message has been invalidated by moving to the next message.");
        return _current;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal BackendMessageBodyReader OpenCurrentBodyReader(short token)
    {
        Validate(token);
        return _current.OpenBodyReader();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetCurrentBufferedFirstMemory(short token, int offset,
        out ReadOnlyMemory<byte> memory)
    {
        Validate(token);
        return _current.TryGetBufferedFirstMemory(offset, out memory);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetCurrentBufferedArray(short token, int offset,
        [NotNullWhen(true)] out byte[]? array,
        out int arrayOffset, out int length)
    {
        Validate(token);
        return _current.TryGetBufferedArray(offset, out array, out arrayOffset, out length);
    }

    internal void SetCurrentFallbackBuffer(
        in ReadOnlySequence<byte> buffer, bool required)
    {
        if (required)
            _currentFallbackBuffer.Set(in buffer);
        else if (!_currentFallbackBuffer.IsEmpty)
            _currentFallbackBuffer.Clear();
    }

    internal void SetCurrentFallbackBuffer(
        in BackendMessageCursor.FastReadOnlySequence<byte> buffer)
    {
        if (buffer.StartObject is ReadOnlySequenceSegment<byte> start)
        {
            _currentFallbackBuffer.Set(start, buffer.StartIndex,
                (ReadOnlySequenceSegment<byte>)buffer.EndObject!, buffer.EndIndex);
        }
        else if (!_currentFallbackBuffer.IsEmpty)
        {
            _currentFallbackBuffer.Clear();
        }
    }

    internal ReadOnlySequence<byte> GetFallbackBuffer(short token)
    {
        Validate(token);
        return _currentFallbackBuffer.Sequence;
    }

    public long GetCurrentMessageOffset(short token)
    {
        Validate(token);
        if ((_messageState & MessageOffsetCaptured) == 0)
        {
            _currentMessageOffset = _cursor.GetCurrentMessageOffset(
                _current.BufferedLength);
            _messageState |= MessageOffsetCaptured;
        }
        return _currentMessageOffset;
    }

    public ReadOnlyMemory<byte> GetContiguousMemory(
        short token, ReadOnlyMemory<byte> source)
    {
        Validate(token);
        return source;
    }

    public ReadOnlyMemory<byte> GetContiguousMemory(
        short token, in ReadOnlySequence<byte> source)
    {
        Validate(token);
        if (source.IsSingleSegment)
            return source.First;
        if (source.Length > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(source));

        var length = (int)source.Length;
        for (var projection = _contiguousProjections;
             projection is not null;
             projection = projection.Next)
        {
            if (projection.Start.Equals(source.Start)
                && projection.Length == length)
                return projection.Buffer.AsMemory(0, length);
        }

        var buffer = ArrayPool<byte>.Shared.Rent(length);
        source.CopyTo(buffer);
        _contiguousProjections = new()
        {
            Buffer = buffer,
            Start = source.Start,
            Length = length,
            Next = _contiguousProjections
        };
        return buffer.AsMemory(0, length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReleaseContiguousProjections()
    {
        var projection = _contiguousProjections;
        if (projection is null)
            return;
        ReleaseContiguousProjectionsCore(projection);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void ReleaseContiguousProjectionsCore(ContiguousProjection projection)
    {
        _contiguousProjections = null;
        do
        {
            ArrayPool<byte>.Shared.Return(projection.Buffer);
            projection = projection.Next!;
        }
        while (projection is not null);
    }

    public void BindDecoder(PgDecoder decoder)
    {
        if (!ReferenceEquals(_decoder, decoder))
            _decoder = decoder;
    }

    public bool TrySlide(short token, SequencePosition consumed, long consumedLength,
        out CurrentMessageBuffer result)
    {
        MarkBodyWindowAdvanced(token);
        return _decoder.TrySlideCurrentMessage(consumed, consumedLength, out result);
    }

    public ValueTask<CurrentMessageBuffer> SlideAsync(short token, SequencePosition consumed,
        long consumedLength, CancellationToken cancellationToken)
    {
        MarkBodyWindowAdvanced(token);
        return _decoder.SlideCurrentMessageAsync(consumed, consumedLength, cancellationToken);
    }

    public CurrentMessageBuffer Slide(short token, SequencePosition consumed, long consumedLength)
    {
        MarkBodyWindowAdvanced(token);
        return _decoder.SlideCurrentMessage(consumed, consumedLength);
    }

    public bool TryExtend(short token, out CurrentMessageBuffer result)
    {
        EnsureBodyWindowAvailable(token);
        if (!_decoder.TryExtendCurrentMessage(out result))
            return false;
        result = GetBodyBuffer(token, result);
        return true;
    }

    public ValueTask<CurrentMessageBuffer> BeginExtendAsync(short token, CancellationToken cancellationToken)
    {
        EnsureBodyWindowAvailable(token);
        return _decoder.ExtendCurrentMessageAsync(cancellationToken);
    }

    public CurrentMessageBuffer CompleteExtend(short token, CurrentMessageBuffer result)
        => GetBodyBuffer(token, result);

    public CurrentMessageBuffer Extend(short token)
    {
        EnsureBodyWindowAvailable(token);
        return GetBodyBuffer(token, _decoder.ExtendCurrentMessage());
    }

    CurrentMessageBuffer GetBodyBuffer(short token, CurrentMessageBuffer result)
    {
        Validate(token);
        var bodyOffset = _currentMessageOffset + BackendHeader.ByteCount;
        var bodyLength = _current.Header.MessageLength - BackendHeader.ByteCount;
        var bufferedLength = Math.Min(bodyLength, result.Buffer.Length - bodyOffset);
        var body = result.Buffer.Slice(bodyOffset, bufferedLength);
        if (result.IsComplete)
        {
            _decoder.CompleteCurrentMessage();
            var messageLength = _current.Header.MessageLength;
            var message = result.Buffer.Slice(_currentMessageOffset, messageLength);
            BackendMessage.Initialize(
                ref _current, _current.Header, message, this, token, buffered: true);
            var messageEnd = _currentMessageOffset + messageLength;
            _cursor = new BackendMessageCursor(result.Buffer).Slice(messageEnd);
        }
        return new(body, result.IsComplete);
    }

    internal void MarkBodyWindowAdvanced(short token)
    {
        _ = GetCurrentMessageOffset(token);
        _messageState |= BodyWindowAdvanced;
    }

    public void EnsureBodyWindowAvailable(short token)
    {
        Validate(token);
        if ((_messageState & BodyWindowAdvanced) != 0)
            ThrowHelper.ThrowInvalidOperation("The original message body is unavailable after streaming has advanced its window.");
        if (!_current.Buffered)
            _ = GetCurrentMessageOffset(token);
    }

    public void BufferCurrentMessage(short token)
    {
        EnsureBodyWindowAvailable(token);
        CurrentMessageBuffer result;
        do result = Extend(token);
        while (!result.IsComplete);
    }

    public ValueTask BufferCurrentMessageAsync(short token, CancellationToken cancellationToken)
    {
        EnsureBodyWindowAvailable(token);
        return Core(token, cancellationToken);

        async ValueTask Core(short token, CancellationToken cancellationToken)
        {
            CurrentMessageBuffer result;
            do
            {
                result = CompleteExtend(token,
                    await BeginExtendAsync(token, cancellationToken).ConfigureAwait(false));
            }
            while (!result.IsComplete);
        }
    }

    void Validate(short token)
    {
        if (_publicationState is PublicationState.Peeked || _version != token)
            ThrowHelper.ThrowInvalidOperation("Backend message has been invalidated by moving to the next message.");
    }

    public void MarkPriorCancellationExposure(short token)
    {
        if (_version != token)
            ThrowHelper.ThrowInvalidOperation("Backend message has been invalidated by moving to the next message.");
        _messageState |= PriorCancellationExposure;
    }

    public bool HasPriorCancellationExposure(short token)
    {
        if (_version != token)
            ThrowHelper.ThrowInvalidOperation("Backend message has been invalidated by moving to the next message.");
        return (_messageState & PriorCancellationExposure) != 0;
    }

    public void MarkBackendTermination(short token)
    {
        Validate(token);
        _messageState |= BackendTermination;
    }

    public bool IsBackendTermination(short token)
    {
        Validate(token);
        return (_messageState & BackendTermination) != 0;
    }

    public bool TryObserveError(short token)
    {
        Validate(token);
        if ((_messageState & ErrorObserved) != 0)
            return false;
        _messageState |= ErrorObserved;
        return true;
    }

    public bool TryMoveNext()
    {
        if (_publicationState is PublicationState.Peeked)
        {
            PublishPeeked();
            return true;
        }
        if (!_cursor.TryReadNextBuffer(out var header, out var buffer, out var bufferLength))
            return false;
        ResetMessageState();
        if (bufferLength < header.MessageLength)
            _decoder.SetCurrentMessageLength(
                _cursor.ConsumedLength - bufferLength
                + header.MessageLength);
        BackendMessage.Initialize(ref _current, header, buffer, this, ++_version,
            bufferLength >= header.MessageLength);
        _publicationState = PublicationState.Current;
        return true;

        void ResetMessageState()
        {
            _messageState = 0;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void PublishPeeked()
    {
        Debug.Assert(_publicationState is PublicationState.Peeked);
        _publicationState = PublicationState.Current;
    }

    public void RetireCursor(bool retainProjections = false)
    {
        if (!retainProjections)
            ReleaseContiguousProjections();
        // Advancing the read grant may return or refill the memory backing every view held here.
        // A failed message poll preserves Current, but crossing this ownership boundary cannot.
        var invalidateToken = _publicationState is not PublicationState.None;
        _current = default;
        _currentFallbackBuffer.Clear();
        _publicationState = PublicationState.None;
        _cursor = default;
        _hasCursor = false;
        _currentMessageOffset = 0;
        _messageState = 0;
        if (invalidateToken)
            _version++;
    }

    public bool TryGetReadRequirement(
        out long consumedLength, out long requiredLength)
    {
        if (!_hasCursor || _cursor.RequiredBufferedLength <= 0)
        {
            consumedLength = 0;
            requiredLength = 0;
            return false;
        }

        consumedLength = _cursor.ConsumedLength;
        requiredLength = _cursor.RequiredBufferedLength
            - consumedLength;
        return true;
    }

    public bool TryGetCursorUnread(out SequencePosition unread)
    {
        if (!_hasCursor)
        {
            unread = default;
            return false;
        }
        unread = _cursor.UnreadStart;
        return true;
    }

    // Reads the next message WITHOUT publishing it as Current. The message cursor
    // really advances past the header, but the parsed (header, buffer) lands in the peek
    // slot and the follow-up TryMoveNext picks it up without re-parsing. The returned
    // BackendMessage is valid until the next TryMoveNext (which bumps the version token);
    // use it immediately, don't store it.
    [MethodImpl(MethodImplOptions.NoInlining)]
    public bool TryPeekNext(out BackendHeader header)
    {
        if (_publicationState is PublicationState.Peeked)
        {
            header = _current.Header;
            return true;
        }
        if (!_cursor.TryReadNextBuffer(
                out header, out var buffer, out var bufferLength))
        {
            return false;
        }
        if (bufferLength < header.MessageLength)
            _decoder.SetCurrentMessageLength(
                _cursor.ConsumedLength - bufferLength
                + header.MessageLength);
        _messageState = 0;
        BackendMessage.Initialize(ref _current, header, buffer, this, ++_version,
            bufferLength >= header.MessageLength);
        _publicationState = PublicationState.Peeked;
        return true;
    }

    public BackendMessage Peeked
    {
        get
        {
            Debug.Assert(_publicationState is PublicationState.Peeked);
            return _current;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetCursor(BackendMessageCursor cursor)
    {
        Debug.Assert(_publicationState is PublicationState.None,
            "The prior cursor must be retired before publishing replacement storage.");
        _publicationState = PublicationState.None;
        _cursor = cursor;
        _hasCursor = true;
    }

}
