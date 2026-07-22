using System.Buffers;
using System.Runtime.CompilerServices;

namespace Slon.Pg.Protocol;

// Context to manage message streaming, and to limit incurred write barriers per message to the minimum.
sealed class BackendMessageContext
{
    BackendMessageBatch _remainingBatch;
    BackendMessage _current;
    short _version;
    bool _hasPriorCancellationExposure;

    // Peek slot: TryPeekNext advances the real batch cursor into here, so the header parse
    // happens at peek time and a follow-up TryMoveNext can publish without re-parsing. _hasPeeked
    // alone owns validity; leaving the inactive buffer populated avoids a redundant clear and lets
    // the next peek usually reuse the same backing objects without write barriers.
    bool _hasPeeked;
    BackendHeader _peekedHeader;
    ReadOnlySequence<byte> _peekedBuffer;

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
        if (_hasPeeked)
        {
            _hasPeeked = false;
            BackendMessage.Initialize(ref _current, _peekedHeader, _peekedBuffer, this, ++_version,
                _peekedBuffer.Length >= _peekedHeader.Length);
            return true;
        }
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

