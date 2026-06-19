namespace Slon.Pg.Protocol;

// Wire-flush coordination for a pipeline source over the shared PgClientProtocol writer. Both the
// protocol's own source and the exclusive-flow source compose this so the flush policy lives in one
// place. Two concerns, both reading the protocol's cumulative UnflushedBytes:
//   - Periodic mid-stream flush: above the writer's threshold, gate a consume so a WaitForNextAsync
//     round (which flushes) lands between items, bounding accumulation during a busy stream.
//   - Pre-park flush: before the executor parks, flush any unflushed bytes so in-flight writes reach
//     the wire (else their read phase hangs).
sealed class FlushGate
{
    readonly PgClientProtocol _protocol;
    // Arm gate for the consuming pull. Normally true (fast path). Consumed (false) when a pull hands
    // out an item while over the flush threshold, so the next pull is gated until a wait round re-arms
    // after its flush has run.
    bool _armed = true;

    public FlushGate(PgClientProtocol protocol) => _protocol = protocol;

    // Accumulation has crossed the writer's periodic-flush threshold.
    public bool NeedsArm => _protocol.UnflushedBytes >= PgProtocolDataWriter.UnflushedBytesFlushThreshold;

    public bool Armed => Volatile.Read(ref _armed);

    public void ConsumeArm() => Volatile.Write(ref _armed, false);

    public void Rearm() => Volatile.Write(ref _armed, true);

    // Pre-park flush. Returns null when there is nothing to flush or the flush completed inline (the
    // common case: the socket send buffer has room), so the caller proceeds straight to its wait.
    // Returns the in-flight task only under genuine write backpressure, which the caller awaits.
    // CancellationToken.None on purpose: abort is gated inside the writer (its flush runs on its own
    // _cts linked to AbortToken and translates to closed on fire), so a faulted flush surfaces as
    // not-completed-successfully and rethrows when awaited.
    public ValueTask? FlushBeforePark()
    {
        if (_protocol.UnflushedBytes is 0)
            return null;
        var flushTask = _protocol.FlushAsync(CancellationToken.None);
        if (!flushTask.IsCompletedSuccessfully)
            return flushTask;
        flushTask.GetAwaiter().GetResult();
        return null;
    }
}
