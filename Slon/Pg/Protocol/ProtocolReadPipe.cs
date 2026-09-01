using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Slon.Pipelines;

namespace Slon.Pg.Protocol;

// Shared per-protocol read-side wire state. One cursor parses each backend message once, and an
// incomplete message directly drives the next PipeReader grant.
sealed class ProtocolReadPipe(
    PipeReader reader, int dataRowStreamingThreshold, bool ownsReader = false)
{
    enum PendingRead : byte { None, Messages, Slide, Extend }

    readonly BackendMessageContext _messageContext = new();
    ReadOnlySequence<byte> _activeBuffer;
    SequencePosition _examined;
    SequencePosition _retainedStart;
    long _currentMessageOffset;
    long _currentMessageLength = -1;
    long _pendingCursorOffset;
    long _pendingSkipLength;
    int _minimumReadSize;
    PendingRead _pendingRead;
    bool _hasActiveRead;

    public PipeReader PipeReader => reader;
    public BackendMessage Current => _messageContext.Current;
    public bool CurrentIsError => _messageContext.CurrentIsError;
    public bool TryGetCurrent(out BackendMessage message)
        => _messageContext.TryGetCurrent(out message);

    public bool TryMoveNext() => _messageContext.TryMoveNext();
    public bool TryPeekNext(out BackendHeader header)
        => _messageContext.TryPeekNext(out header);
    public BackendMessage Peeked => _messageContext.Peeked;

    public void BindDecoder(PgDecoder decoder) => _messageContext.BindDecoder(decoder);

    public void PrepareRead()
    {
        if (_pendingRead is PendingRead.Messages)
            return;
        if (_pendingRead is not PendingRead.None)
            ThrowHelper.ThrowInvalidOperation(
                "The current message still has a pending read.");

        if (!_hasActiveRead)
        {
            _pendingCursorOffset = 0;
            _minimumReadSize = BackendHeader.ByteCount;
            _pendingRead = PendingRead.Messages;
            return;
        }

        if (_currentMessageLength > 0)
        {
            PrepareAfterPartialMessage();
            return;
        }

        if (!_messageContext.TryGetReadRequirement(
                out var unread, out var requiredLength))
            ThrowHelper.ThrowInvalidOperation(
                "The current backend-message cursor has not been exhausted.");

        _pendingCursorOffset = 0;
        _messageContext.RetireCursor();
        reader.AdvanceTo(unread, _examined);
        _hasActiveRead = false;
        _activeBuffer = default;
        _currentMessageLength = -1;
        _currentMessageOffset = 0;
        _minimumReadSize = int.CreateSaturating(requiredLength);
        _pendingRead = PendingRead.Messages;
    }

    void PrepareAfterPartialMessage()
    {
        var current = _activeBuffer.Slice(_currentMessageOffset);
        _messageContext.RetireCursor();
        if (current.Length >= _currentMessageLength)
        {
            var unread = current.GetPosition(_currentMessageLength);
            reader.AdvanceTo(unread, unread);
            _pendingCursorOffset = 0;
            _minimumReadSize = BackendHeader.ByteCount;
        }
        else
        {
            _pendingSkipLength = _currentMessageLength - current.Length;
            reader.AdvanceTo(_activeBuffer.End, _examined);
            _pendingCursorOffset = _pendingSkipLength;
            _minimumReadSize = int.CreateSaturating(
                _pendingSkipLength + BackendHeader.ByteCount);
        }

        _hasActiveRead = false;
        _activeBuffer = default;
        _currentMessageLength = -1;
        _currentMessageOffset = 0;
        _pendingRead = PendingRead.Messages;
    }

    public ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken)
        => _minimumReadSize > 0
            ? reader.ReadAtLeastAsync(_minimumReadSize, cancellationToken)
            : reader.ReadAsync(cancellationToken);

    public bool CompleteRead(
        in ReadResult result, CancellationToken cancellationToken,
        out bool completed)
    {
        if (_pendingRead is not PendingRead.Messages)
            ThrowHelper.ThrowInvalidOperation("No protocol read is pending.");
        _pendingRead = PendingRead.None;
        _minimumReadSize = 0;
        if (result.IsCanceled)
            ThrowHelper.ThrowOperationCanceled(cancellationToken);
        if (result.Buffer.IsEmpty && result.IsCompleted)
        {
            completed = true;
            _messageContext.RetireCursor();
            return false;
        }
        if (result.Buffer.IsEmpty)
        {
            completed = false;
            return false;
        }

        _activeBuffer = result.Buffer;
        _examined = result.Buffer.End;
        _retainedStart = result.Buffer.Start;
        _hasActiveRead = true;
        if (_pendingSkipLength > 0)
        {
            if (result.Buffer.Length < _pendingSkipLength)
            {
                if (result.IsCompleted)
                    throw new EndOfStreamException(
                        "The pipe completed within a backend message.");
                _pendingSkipLength -= result.Buffer.Length;
                reader.AdvanceTo(result.Buffer.End, result.Buffer.End);
                _hasActiveRead = false;
                _activeBuffer = default;
                _pendingCursorOffset = _pendingSkipLength;
                _minimumReadSize = int.CreateSaturating(
                    _pendingSkipLength + BackendHeader.ByteCount);
                _pendingRead = PendingRead.Messages;
                completed = false;
                return false;
            }
            _pendingCursorOffset = _pendingSkipLength;
            _pendingSkipLength = 0;
        }
        _currentMessageOffset = _pendingCursorOffset;
        var cursorBuffer = _pendingCursorOffset is 0
            ? result.Buffer
            : result.Buffer.Slice(_pendingCursorOffset);
        if (cursorBuffer.IsEmpty)
        {
            completed = result.IsCompleted;
            if (completed)
                _messageContext.RetireCursor();
            if (!completed)
            {
                reader.AdvanceTo(result.Buffer.End, result.Buffer.End);
                _hasActiveRead = false;
                _activeBuffer = default;
                _pendingCursorOffset = 0;
                _minimumReadSize = BackendHeader.ByteCount;
                _pendingRead = PendingRead.Messages;
            }
            return false;
        }
        _currentMessageLength = -1;
        var cursor = new BackendMessageCursor(
            cursorBuffer, dataRowStreamingThreshold);
        _messageContext.SetCursor(cursor);
        completed = false;
        return true;
    }

    public bool TrySlideCurrentMessage(
        SequencePosition consumed, long consumedLength,
        out CurrentMessageBuffer result)
    {
        PrepareCurrentMessageRead(consumed, consumedLength, PendingRead.Slide);
        if (!reader.TryRead(out var read))
        {
            result = default;
            return false;
        }
        result = CompleteCurrentMessageRead(read);
        return true;
    }

    public ValueTask<CurrentMessageBuffer> SlideCurrentMessageAsync(
        SequencePosition consumed, long consumedLength,
        CancellationToken cancellationToken)
    {
        PrepareCurrentMessageRead(consumed, consumedLength, PendingRead.Slide);
        return CompleteCurrentMessageReadAsync(
            reader.ReadAsync(cancellationToken), cancellationToken);
    }

    public CurrentMessageBuffer SlideCurrentMessage(
        SequencePosition consumed, long consumedLength, TimeSpan timeout)
    {
        if (reader is not StreamPipeReader syncReader)
            throw new NotSupportedException(
                "Underlying pipe reader does not support synchronous reads.");
        PrepareCurrentMessageRead(consumed, consumedLength, PendingRead.Slide);
        return CompleteCurrentMessageRead(syncReader.Read(timeout));
    }
    public bool TryExtendCurrentMessage(out CurrentMessageBuffer result)
    {
        PrepareCurrentMessageRead(
            _retainedStart, consumedLength: 0, PendingRead.Extend);
        if (!reader.TryRead(out var read))
        {
            result = default;
            return false;
        }
        result = CompleteCurrentMessageRead(read);
        return true;
    }

    public ValueTask<CurrentMessageBuffer> ExtendCurrentMessageAsync(
        CancellationToken cancellationToken)
    {
        PrepareCurrentMessageRead(
            _retainedStart, consumedLength: 0, PendingRead.Extend);
        return CompleteCurrentMessageReadAsync(
            reader.ReadAsync(cancellationToken), cancellationToken);
    }

    public CurrentMessageBuffer ExtendCurrentMessage(TimeSpan timeout)
    {
        if (reader is not StreamPipeReader syncReader)
            throw new NotSupportedException(
                "Underlying pipe reader does not support synchronous reads.");
        PrepareCurrentMessageRead(
            _retainedStart, consumedLength: 0, PendingRead.Extend);
        return CompleteCurrentMessageRead(syncReader.Read(timeout));
    }

    void PrepareCurrentMessageRead(
        SequencePosition consumed, long consumedLength, PendingRead mode)
    {
        if (_pendingRead == mode)
            return;
        if (_pendingRead is not PendingRead.None || !_hasActiveRead
            || _currentMessageLength <= 0)
            ThrowHelper.ThrowInvalidOperation(
                "The current message is not awaiting more data.");
        if (mode is PendingRead.Slide
            && (consumedLength <= 0 || consumedLength >= _currentMessageLength))
            throw new ArgumentOutOfRangeException(nameof(consumedLength));

        reader.AdvanceTo(
            mode is PendingRead.Slide ? consumed : _retainedStart,
            _examined);
        if (mode is PendingRead.Slide)
        {
            _retainedStart = consumed;
            _currentMessageOffset = 0;
            _currentMessageLength -= consumedLength;
        }
        _hasActiveRead = false;
        _activeBuffer = default;
        _pendingRead = mode;
    }

    ValueTask<CurrentMessageBuffer> CompleteCurrentMessageReadAsync(
        ValueTask<ReadResult> task, CancellationToken cancellationToken)
        => task.IsCompletedSuccessfully
            ? new(CompleteCurrentMessageRead(task.Result, cancellationToken))
            : Core(task, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<CurrentMessageBuffer> Core(
        ValueTask<ReadResult> task, CancellationToken cancellationToken)
        => CompleteCurrentMessageRead(
            await task.ConfigureAwait(false), cancellationToken);

    CurrentMessageBuffer CompleteCurrentMessageRead(
        in ReadResult result, CancellationToken cancellationToken = default)
    {
        if (_pendingRead is not (PendingRead.Slide or PendingRead.Extend))
            ThrowHelper.ThrowInvalidOperation(
                "No current-message read is pending.");
        _pendingRead = PendingRead.None;
        if (result.IsCanceled)
            ThrowHelper.ThrowOperationCanceled(cancellationToken);

        _activeBuffer = result.Buffer;
        _examined = result.Buffer.End;
        _retainedStart = result.Buffer.Start;
        _hasActiveRead = true;
        var current = result.Buffer.Slice(_currentMessageOffset);
        if (result.IsCompleted && current.Length < _currentMessageLength)
            throw new EndOfStreamException(
                "The pipe completed within a backend message.");
        var complete = current.Length >= _currentMessageLength;
        var buffer = complete
            ? current.Slice(0, _currentMessageLength)
            : current;
        _examined = complete ? buffer.End : result.Buffer.End;
        return new(buffer, complete);
    }

    public async ValueTask<bool> MoveNextAsync(
        CancellationToken cancellationToken)
    {
        while (true)
        {
            PrepareRead();
            var read = await ReadAsync(cancellationToken).ConfigureAwait(false);
            if (CompleteRead(
                    read, cancellationToken, out var completed))
                return true;
            if (completed)
                return false;
        }
    }

    public bool MoveNext(TimeSpan timeout)
    {
        if (reader is not StreamPipeReader syncReader)
            throw new NotSupportedException(
                "Underlying pipe reader does not support synchronous reads.");
        while (true)
        {
            PrepareRead();
            var read = _minimumReadSize > 0
                ? syncReader.ReadAtLeast(_minimumReadSize, timeout)
                : syncReader.Read(timeout);
            if (CompleteRead(
                    read, CancellationToken.None, out var completed))
                return true;
            if (completed)
                return false;
        }
    }

    public void SetCurrentMessageLength(long messageLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(messageLength);
        _currentMessageLength = messageLength;
    }

    public void CompleteCurrentMessage()
        => _currentMessageLength = -1;

    public void Dispose()
    {
        _messageContext.RetireCursor();
        if (ownsReader)
            reader.Complete();
    }

    public ValueTask DisposeAsync()
    {
        _messageContext.RetireCursor();
        return ownsReader ? reader.CompleteAsync() : default;
    }
}
