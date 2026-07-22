using System.Buffers;
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
    bool _hasPriorCancellationExposure;
    bool _bodyWindowAdvanced;

    // Peek slot: TryPeekNext advances the real batch cursor into here, so the header parse
    // happens at peek time and a follow-up TryMoveNext can publish without re-parsing. _hasPeeked
    // alone owns validity; leaving the inactive buffer populated avoids a redundant clear and lets
    // the next peek usually reuse the same backing objects without write barriers.
    bool _hasPeeked;
    BackendHeader _peekedHeader;
    ReadOnlySequence<byte> _peekedBuffer;
    long _currentMessageOffset;
    long _peekedMessageOffset;

    public BackendMessage Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _current;
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
        Validate(token);
        if (!_decoder.TryExtendCurrentMessage(out result))
            return false;
        result = GetBodyBuffer(token, result);
        return true;
    }

    public async ValueTask<CurrentSegmentBuffer> ExtendAsync(short token, CancellationToken cancellationToken)
    {
        Validate(token);
        return GetBodyBuffer(token, await _decoder.ExtendCurrentMessageAsync(cancellationToken).ConfigureAwait(false));
    }

    public CurrentSegmentBuffer Extend(short token)
    {
        Validate(token);
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
        Validate(token);
        _bodyWindowAdvanced = true;
    }

    public void EnsureBodyWindowAvailable(short token)
    {
        Validate(token);
        if (_bodyWindowAdvanced)
            ThrowHelper.ThrowInvalidOperation("The original message body is unavailable after streaming has advanced its window.");
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
        _hasPriorCancellationExposure = true;
    }

    public bool HasPriorCancellationExposure(short token)
    {
        if (_version != token)
            ThrowHelper.ThrowInvalidOperation("Backend message has been invalidated by moving to the next message.");
        return _hasPriorCancellationExposure;
    }

    public bool TryMoveNext()
    {
        _hasPriorCancellationExposure = false;
        _bodyWindowAdvanced = false;
        if (_hasPeeked)
        {
            _hasPeeked = false;
            _currentMessageOffset = _peekedMessageOffset;
            BackendMessage.Initialize(ref _current, _peekedHeader, _peekedBuffer, this, ++_version,
                _peekedBuffer.Length >= _peekedHeader.MessageLength);
            return true;
        }
        _currentMessageOffset = _remainingBatch.ConsumedLength;
        return BackendMessage.TryCreateFromBatch(ref _remainingBatch, this, ++_version, out _current);
    }

    // Reads the next message WITHOUT publishing it as Current. The remaining batch cursor
    // really advances past the header, but the parsed (header, buffer) lands in the peek
    // slot and the follow-up TryMoveNext picks it up without re-parsing. The returned
    // BackendMessage is valid until the next TryMoveNext (which bumps the version token);
    // use it immediately, don't store it.
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
        _peekedMessageOffset = _remainingBatch.ConsumedLength - buffer.Length;
        BackendMessage.SetSequence(ref _peekedBuffer, in buffer);
        _hasPeeked = true;
        header = _peekedHeader;
        return true;
    }

    public BackendMessage Peeked => new(_peekedHeader, _peekedBuffer, this, _version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetBatch(BackendMessageBatch batch)
    {
        // A fresh batch retires the prior peek. The inactive buffer may stay populated because
        // _hasPeeked owns validity and the next peek overwrites it.
        _hasPeeked = false;
        _remainingBatch = batch;
    }
}
