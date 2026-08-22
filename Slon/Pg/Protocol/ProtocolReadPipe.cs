using System.IO.Pipelines;
using Slon.Pipelines;

namespace Slon.Pg.Protocol;

// Shared per-protocol read-side wire state. One instance per protocol, behind any number of
// PgDecoder shells (the base protocol shell plus a per-exclusive-scope shell). The single-pump
// invariant means only one shell ever drives this pipe at a time, so the batch enumerator and
// message context are safe to share. Token-bearing concerns (CTS, abort translation, read-timeout
// countdown, CurrentExecutionControl, and framing/handler loops) live in the shell; the pipe
// exposes only the raw read primitives and message iteration.
sealed class ProtocolReadPipe(PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> messageBatchEnumerator)
{
    readonly BackendMessageContext _messageContext = new();

    public BackendMessage Current => _messageContext.Current;
    public bool CurrentIsError => _messageContext.CurrentIsError;
    public bool TryGetCurrent(out BackendMessage message) => _messageContext.TryGetCurrent(out message);

    public bool TryMoveNext() => _messageContext.TryMoveNext();
    public bool TryPeekNextType(out PgTypes.BackendType type) => _messageContext.TryPeekNextType(out type);
    public bool TryPeekNext(out BackendHeader header) => _messageContext.TryPeekNext(out header);
    public BackendMessage Peeked => _messageContext.Peeked;

    public void BindDecoder(PgDecoder decoder) => _messageContext.BindDecoder(decoder);

    public bool TryMoveNextBatch(out bool completed)
    {
        _messageContext.RetireCurrentBatch();
        if (!messageBatchEnumerator.TryMoveNext(out completed))
            return false;
        CommitBatch();
        return true;
    }

    public ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken)
        => messageBatchEnumerator.ReadAsync(cancellationToken);

    public bool TryBeginDirectRead(CancellationToken cancellationToken, out ValueTask<int> task)
        => messageBatchEnumerator.TryBeginDirectRead(cancellationToken, out task);

    public bool CompleteDirectRead(int length, CancellationToken cancellationToken, out ValueTask<int> next, out bool readFinished, out bool completed)
    {
        if (!messageBatchEnumerator.CompleteDirectRead(length, cancellationToken, out next, out readFinished, out completed))
            return false;
        CommitBatch();
        return true;
    }

    public void AbortDirectRead() => messageBatchEnumerator.AbortDirectRead();

    public bool TryMoveNextBatch(ReadResult result, CancellationToken cancellationToken, out bool completed)
    {
        _messageContext.RetireCurrentBatch();
        if (!messageBatchEnumerator.TryMoveNext(result, cancellationToken, out completed))
            return false;
        CommitBatch();
        return true;
    }

    public bool TryContinueCurrentMessage(SequencePosition consumed, long consumedLength, out CurrentSegmentBuffer result)
        => messageBatchEnumerator.TryContinueCurrentSegment(consumed, consumedLength, out result);

    public ValueTask<CurrentSegmentBuffer> ContinueCurrentMessageAsync(
        SequencePosition consumed, long consumedLength, CancellationToken cancellationToken)
        => messageBatchEnumerator.ContinueCurrentSegmentAsync(consumed, consumedLength, cancellationToken);

    public CurrentSegmentBuffer ContinueCurrentMessage(
        SequencePosition consumed, long consumedLength, TimeSpan timeout)
        => messageBatchEnumerator.ContinueCurrentSegment(consumed, consumedLength, timeout);

    public bool TryExtendCurrentMessage(out CurrentSegmentBuffer result)
        => messageBatchEnumerator.TryExtendCurrentSegment(out result);

    public ValueTask<CurrentSegmentBuffer> ExtendCurrentMessageAsync(CancellationToken cancellationToken)
        => messageBatchEnumerator.ExtendCurrentSegmentAsync(cancellationToken);

    public CurrentSegmentBuffer ExtendCurrentMessage(TimeSpan timeout)
        => messageBatchEnumerator.ExtendCurrentSegment(timeout);

    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
    {
        _messageContext.RetireCurrentBatch();
        return messageBatchEnumerator.MoveNextAsync(cancellationToken);
    }

    public bool MoveNext(TimeSpan timeout)
    {
        _messageContext.RetireCurrentBatch();
        return messageBatchEnumerator.MoveNext(timeout);
    }

    // Publishes the just-read batch as the current batch the message context iterates.
    public void CommitBatch() => _messageContext.SetBatch(messageBatchEnumerator.Current);

    public void Dispose()
    {
        _messageContext.RetireCurrentBatch();
        messageBatchEnumerator.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        _messageContext.RetireCurrentBatch();
        return messageBatchEnumerator.DisposeAsync();
    }
}
