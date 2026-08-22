using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Slon.Pipelines;

namespace Slon.Pg.Protocol;

// Context to manage message streaming, and to limit incurred write barriers per message to the minimum.
sealed class BackendMessageContext
{
    PgDecoder _decoder = null!;
    BackendMessageBatch _remainingBatch;
    BackendMessage _current;
    short _version;
    const byte PriorCancellationExposure = 1 << 0;
    const byte BackendTermination = 1 << 1;
    const byte ErrorObserved = 1 << 2;
    const byte BodyWindowAdvanced = 1 << 3;
    const byte MessageOffsetCaptured = 1 << 4;
    byte _messageState;

    // Peek slot: TryPeekNext advances the real batch cursor into here, so the header parse
    // happens at peek time and a follow-up TryMoveNext can publish without re-parsing. _hasPeeked
    // alone owns validity; leaving the inactive buffer populated avoids a redundant clear and lets
    // the next peek usually reuse the same backing objects without write barriers.
    bool _hasPeeked;
    BackendHeader _peekedHeader;
    ReadOnlySequence<byte> _peekedBuffer;
    long _currentMessageOffset;

    public BackendMessage Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            var current = _current;
            if (current.IsDefault)
                ThrowHelper.ThrowInvalidOperation("The decoder has no current backend message.");
            return current;
        }
    }

    public bool CurrentIsError
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _current.Header.Type is PgTypes.BackendType.ErrorResponse;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetCurrent(out BackendMessage current)
    {
        current = _current;
        return !current.IsDefault;
    }

    public BackendMessage GetCurrent(short token)
    {
        if (_version != token)
            ThrowHelper.ThrowInvalidOperation("Backend message has been invalidated by moving to the next message.");
        return _current;
    }

    public long GetCurrentMessageOffset(short token)
    {
        Validate(token);
        if ((_messageState & MessageOffsetCaptured) == 0)
        {
            // Fully buffered messages never need their batch-relative offset. Capture it only
            // before a streaming operation can replace the segment used to derive it.
            Debug.Assert(!_current.Buffered && !_hasPeeked);
            _currentMessageOffset = _remainingBatch.ConsumedLength - _current.BufferedLength;
            _messageState |= MessageOffsetCaptured;
        }
        return _currentMessageOffset;
    }

    public void BindDecoder(PgDecoder decoder)
    {
        if (!ReferenceEquals(_decoder, decoder))
            _decoder = decoder;
    }

    public bool TryContinue(short token, SequencePosition consumed, long consumedLength,
        out CurrentSegmentBuffer result)
    {
        MarkBodyWindowAdvanced(token);
        return _decoder.TryContinueCurrentMessage(consumed, consumedLength, out result);
    }

    public ValueTask<CurrentSegmentBuffer> ContinueAsync(short token, SequencePosition consumed,
        long consumedLength, CancellationToken cancellationToken)
    {
        MarkBodyWindowAdvanced(token);
        return _decoder.ContinueCurrentMessageAsync(consumed, consumedLength, cancellationToken);
    }

    public CurrentSegmentBuffer Continue(short token, SequencePosition consumed, long consumedLength)
    {
        MarkBodyWindowAdvanced(token);
        return _decoder.ContinueCurrentMessage(consumed, consumedLength);
    }

    public bool TryExtend(short token, out CurrentSegmentBuffer result)
    {
        EnsureBodyWindowAvailable(token);
        if (!_decoder.TryExtendCurrentMessage(out result))
            return false;
        result = GetBodyBuffer(token, result);
        return true;
    }

    public async ValueTask<CurrentSegmentBuffer> ExtendAsync(short token, CancellationToken cancellationToken)
    {
        EnsureBodyWindowAvailable(token);
        return GetBodyBuffer(token, await _decoder.ExtendCurrentMessageAsync(cancellationToken).ConfigureAwait(false));
    }

    public CurrentSegmentBuffer Extend(short token)
    {
        EnsureBodyWindowAvailable(token);
        return GetBodyBuffer(token, _decoder.ExtendCurrentMessage());
    }

    CurrentSegmentBuffer GetBodyBuffer(short token, CurrentSegmentBuffer result)
    {
        Validate(token);
        var bodyOffset = _currentMessageOffset + BackendHeader.ByteCount;
        var bodyLength = _current.Header.MessageLength - BackendHeader.ByteCount;
        var bufferedLength = Math.Min(bodyLength, result.Buffer.Length - bodyOffset);
        var body = result.Buffer.Slice(bodyOffset, bufferedLength);
        if (result.IsComplete)
            SetCurrentFromSegment(token, result.Buffer);
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

    public void SetBuffered(short token, ReadOnlySequence<byte> buffer)
    {
        Validate(token);
        BackendMessage.Initialize(ref _current, _current.Header, buffer, this, token, buffered: true);
    }

    public void BufferCurrentMessage(short token)
    {
        EnsureBodyWindowAvailable(token);
        CurrentSegmentBuffer result;
        do result = Extend(token);
        while (!result.IsComplete);
    }

    public ValueTask BufferCurrentMessageAsync(short token, CancellationToken cancellationToken)
    {
        EnsureBodyWindowAvailable(token);
        return Core(token, cancellationToken);

        async ValueTask Core(short token, CancellationToken cancellationToken)
        {
            CurrentSegmentBuffer result;
            do result = await ExtendAsync(token, cancellationToken).ConfigureAwait(false);
            while (!result.IsComplete);
        }
    }

    void SetCurrentFromSegment(short token, ReadOnlySequence<byte> segment)
    {
        var message = segment.Slice(_currentMessageOffset, _current.Header.MessageLength);
        SetBuffered(token, message);
    }

    void Validate(short token)
    {
        if (_version != token)
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
        if (_hasPeeked)
        {
            _hasPeeked = false;
            ResetMessageState();
            BackendMessage.Initialize(ref _current, _peekedHeader, _peekedBuffer, this, ++_version,
                _peekedBuffer.Length >= _peekedHeader.MessageLength);
            return true;
        }
        if (!_remainingBatch.TryReadNextInPlace(out var header, out var buffer, out var bufferLength))
            return false;
        ResetMessageState();
        BackendMessage.Initialize(ref _current, header, buffer, this, ++_version,
            bufferLength >= header.MessageLength);
        return true;

        void ResetMessageState()
        {
            _messageState = 0;
        }
    }

    public void RetireCurrentBatch()
    {
        // Moving the batch enumerator may return or refill the memory backing every view held here.
        // A failed message poll preserves Current, but crossing this ownership boundary cannot.
        var invalidateToken = !_current.IsDefault || _hasPeeked;
        _current = default;
        _hasPeeked = false;
        _peekedHeader = default;
        _peekedBuffer = default;
        _remainingBatch = default;
        _currentMessageOffset = 0;
        _messageState = 0;
        if (invalidateToken)
            _version++;
    }

    // Reads the next message WITHOUT publishing it as Current. The remaining batch cursor
    // really advances past the header, but the parsed (header, buffer) lands in the peek
    // slot and the follow-up TryMoveNext picks it up without re-parsing. The returned
    // BackendMessage is valid until the next TryMoveNext (which bumps the version token);
    // use it immediately, don't store it.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryPeekNextType(out PgTypes.BackendType type)
    {
        if (_hasPeeked)
        {
            type = _peekedHeader.Type;
            return true;
        }
        return _remainingBatch.TryPeekType(out type);
    }

    public bool TryPeekNext(out BackendHeader header)
    {
        if (_hasPeeked)
        {
            header = _peekedHeader;
            return true;
        }
        if (!_remainingBatch.TryReadNextInPlace(out _peekedHeader, out var buffer, out _))
        {
            header = default;
            return false;
        }
        BackendMessage.SetSequence(ref _peekedBuffer, in buffer);
        _hasPeeked = true;
        header = _peekedHeader;
        return true;
    }

    public BackendMessage Peeked
    {
        get
        {
            Debug.Assert(_hasPeeked);
            return new(_peekedHeader, _peekedBuffer, this, _version);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetBatch(BackendMessageBatch batch)
    {
        Debug.Assert(_current.IsDefault && !_hasPeeked,
            "The prior batch must be retired before publishing replacement storage.");
        // Keep release behavior defensive. The inactive buffer may stay populated because
        // _hasPeeked owns validity and the next peek overwrites it.
        _hasPeeked = false;
        _remainingBatch = batch;
    }

}
