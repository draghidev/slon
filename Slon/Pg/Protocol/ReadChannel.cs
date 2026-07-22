using System.Buffers;
using System.IO.Pipelines;
using Slon.Pipelines;

namespace Slon.Pg.Protocol;

// Shared per-protocol read-side wire state. One instance per protocol, behind any number of
// PgDecoder shells (the base protocol shell plus a per-exclusive-scope shell). The single-pump
// invariant means only one shell ever drives this channel at a time, so the batch enumerator and
// message context are safe to share. Token-bearing concerns (CTS, abort translation, read-timeout
// countdown, CurrentExecutionControl, the framing/handler loops) live in the shell; the channel
// exposes only the raw read primitives and message iteration.
sealed class ReadChannel
{
    readonly PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> _messageBatchEnumerator;
    readonly BackendMessageContext _messageContext;

    public ReadChannel(PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> messageBatchEnumerator)
    {
        _messageBatchEnumerator = messageBatchEnumerator;
        _messageContext = new();
    }

    public BackendMessage Current => _messageContext.Current;

    public bool TryMoveNext() => _messageContext.TryMoveNext();
    public bool TryPeekNext(out BackendHeader header) => _messageContext.TryPeekNext(out header);
    public BackendMessage Peeked => _messageContext.Peeked;

    public void BindDecoder(PgDecoder decoder) => _messageContext.BindDecoder(decoder);

    public bool TryMoveNextBatch(out bool completed)
    {
        if (!_messageBatchEnumerator.TryMoveNext(out completed))
            return false;
        CommitBatch();
        return true;
    }

    public ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken)
        => _messageBatchEnumerator.ReadAsync(cancellationToken);

    public bool TryBeginConduitRead(CancellationToken cancellationToken, out ValueTask<int> task)
        => _messageBatchEnumerator.TryBeginConduitRead(cancellationToken, out task);

    public bool CompleteConduitRead(int length, CancellationToken cancellationToken, out ValueTask<int> next, out bool readFinished, out bool completed)
    {
        if (!_messageBatchEnumerator.CompleteConduitRead(length, cancellationToken, out next, out readFinished, out completed))
            return false;
        CommitBatch();
        return true;
    }

    public void AbortConduitRead() => _messageBatchEnumerator.AbortConduitRead();

    public bool TryMoveNextBatch(ReadResult result, CancellationToken cancellationToken, out bool completed)
    {
        if (!_messageBatchEnumerator.TryMoveNext(result, cancellationToken, out completed))
            return false;
        CommitBatch();
        return true;
    }

    public bool TryContinueCurrentMessage(SequencePosition consumed, long consumedLength, out CurrentSegmentBuffer result)
        => _messageBatchEnumerator.TryContinueCurrentSegment(consumed, consumedLength, out result);

    public ValueTask<CurrentSegmentBuffer> ContinueCurrentMessageAsync(
        SequencePosition consumed, long consumedLength, CancellationToken cancellationToken)
        => _messageBatchEnumerator.ContinueCurrentSegmentAsync(consumed, consumedLength, cancellationToken);

    public CurrentSegmentBuffer ContinueCurrentMessage(
        SequencePosition consumed, long consumedLength, TimeSpan timeout)
        => _messageBatchEnumerator.ContinueCurrentSegment(consumed, consumedLength, timeout);

    public bool TryExtendCurrentMessage(out CurrentSegmentBuffer result)
        => _messageBatchEnumerator.TryExtendCurrentSegment(out result);

    public ValueTask<CurrentSegmentBuffer> ExtendCurrentMessageAsync(CancellationToken cancellationToken)
        => _messageBatchEnumerator.ExtendCurrentSegmentAsync(cancellationToken);

    public CurrentSegmentBuffer ExtendCurrentMessage(TimeSpan timeout)
        => _messageBatchEnumerator.ExtendCurrentSegment(timeout);

    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken) => _messageBatchEnumerator.MoveNextAsync(cancellationToken);
    public bool MoveNext(TimeSpan timeout) => _messageBatchEnumerator.MoveNext(timeout);

    // Publishes the just-read batch as the current batch the message context iterates.
    public void CommitBatch() => _messageContext.SetBatch(_messageBatchEnumerator.Current);

    public void Dispose() => _messageBatchEnumerator.Dispose();
    public ValueTask DisposeAsync() => _messageBatchEnumerator.DisposeAsync();
}
