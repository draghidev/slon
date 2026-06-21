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
    public bool TryPeekNext(out BackendMessage message) => _messageContext.TryPeekNext(out message);

    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken) => _messageBatchEnumerator.MoveNextAsync(cancellationToken);
    public bool MoveNext(TimeSpan timeout) => _messageBatchEnumerator.MoveNext(timeout);

    // Publishes the just-read batch as the current batch the message context iterates.
    public void CommitBatch() => _messageContext.SetBatch(_messageBatchEnumerator.Current);

    public void Dispose() => _messageBatchEnumerator.Dispose();
    public ValueTask DisposeAsync() => _messageBatchEnumerator.DisposeAsync();
}
