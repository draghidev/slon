using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol.Flows;

// Substitute item returned by PgClientProtocol.Control.TryRecoverItemFailure to resync the wire
// after a flow fails (a decode fault, or a write/execute fault). Inherits the failed flow's
// outstanding RFQ obligation, injects a terminating Sync, flushes the failed flow's buffered work
// plus the Sync, then drains the resulting RFQs (inherited + 1).
//
// Substitution-substrate contract: recovery REPLACES the failed flow's executor slot (ExecutingItem
// follows the substitute), but the failed flow's lifetime is extended through this substitute via
// FailedFlow + the gate permissivity in ThrowIfCannotWrite. When the failed flow's PIPELINE task
// faulted while its TRAILING task is still in-flight (PipelineTask failure kind), the framework
// passes the trailing through PipelineItemFailureContext.OutstandingPhaseTask and we capture it
// here; ExecuteAuto awaits it before touching the encoder so this recovery's writes don't collide
// with the still-running trailing flush on the single-producer writer.
//
// Scope: resyncs non-COPY, non-torn wire state. COPY-mode terminators (CopyFail / copy-out drain)
// and torn-frame repair (truncate / pad) are future work tied to the COPY and writer-gate
// features respectively; see the markers in ExecuteAuto.
sealed class RecoveryDrainFlow : PgClientFlow
{
    int _drainCount;
    ValueTask _outstandingTrailing;

    /// The flow this recovery supplanted, carried so the policy can complete it when the
    /// recovery completes. The failed item's lifetime deliberately extends as far as the
    /// recovery does: completion is the reuse gate (completion actions Reset and re-enqueue
    /// instances), and reuse must be causally after the protocol stops referencing the failed
    /// tenure's machinery (parked dispatch state, inherited RFQ bookkeeping, registrations).
    /// Ordering of anything enqueued after the failure is already handled by the pipeline.
    public PgClientFlow? FailedFlow { get; private set; }
    public Exception? FailureException { get; private set; }

    public void BindFailedFlow(PgClientFlow failedFlow, Exception exception, ValueTask outstandingTrailing)
    {
        FailedFlow = failedFlow;
        FailureException = exception;
        _outstandingTrailing = outstandingTrailing;
    }

    // drainCount: total RFQs to drain - the failed flow's inherited outstanding RFQs plus 1 for
    // the terminating Sync this flow always injects (inherited + 1; computed in
    // TryRecoverItemFailure). The inherited count itself is transferred onto this flow via
    // TransferInheritedRfqCount at the recovery seam, not here, so the inheritance setup is
    // visible at the seam rather than hidden in construction.
    public RecoveryDrainFlow(bool async, int drainCount)
        : base(supportsPipelining: true)
    {
        _drainCount = drainCount;
        IsAsync = async;
    }

    public new void Reset()
    {
        base.Reset();
        _drainCount = 0;
        _outstandingTrailing = default;
        FailedFlow = null;
        FailureException = null;
    }

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        // Resync unconditionally: inject a terminating Sync, flush it (with the failed flow's
        // buffered work), and drain to the RFQ it induces plus the failed flow's inherited
        // outstanding RFQs. No discrimination on the failed flow's termination state - worst
        // case is one extra, harmless RFQ on an idle wire (a redundant resync).
        //
        // TODO(writer-gate): a TORN frame (a partial message whose declared length the server
        // would read past) is NOT resynced by this Sync - the server consumes the Sync's bytes
        // as the torn frame's body. Truncate-if-buffered / pad-to-length-if-flushed lands with
        // PgEncoder message-tracking; until then recovery is correct only for non-torn writes.
        //
        // TODO(copy): when COPY lands, branch on copy-mode here - a bare Sync is IGNORED during
        // copy-in (the backend waits for CopyData/CopyDone/CopyFail), so copy-in recovery must
        // emit CopyFail (+ Sync for the extended-query case), and copy-out recovery must drain
        // the CopyData stream to CopyDone before the RFQ. COPY isn't implemented, so the wire
        // cannot be in copy-mode at recovery time and the bare Sync is correct today.
        //
        // Sequencing against the failed flow's still-in-flight trailing: the trailing owns the
        // writer, and our WriteSync + FlushAsync would collide on the single-producer writer
        // if they overlap. When outstanding is already synchronously completed (the common
        // case - default ValueTask sentinel for non-PipelineTask kinds, or a flush that
        // sync-finished), inline this method's writes; otherwise move them into our trailing
        // phase so the framework's main loop progresses past ExecuteAuto into the well-defined
        // tail-waiter state before we await outstanding. TWO reasons this matters:
        //   (a) TCP send-window deadlock: outstanding's flush is parked on the client-side
        //       send buffer draining, which requires the peer to ACK pending bytes, which
        //       requires the peer's send buffer to drain, which requires US to read server
        //       responses. With ExecuteAuto-inline await, the DrainPhase (our read leg)
        //       cannot start because ExecuteAuto hasn't returned FlowTasks yet - the
        //       framework is parked in ExecuteItemAsync. Trailing-phase await runs the
        //       drain concurrently with the wait on outstanding, so the read leg breaks
        //       the cycle: drain reads server messages, our recv ACKs flow back, peer's
        //       send queue drains, peer reads our pending writes, our send window opens,
        //       outstanding's flush completes, then our WriteSync runs.
        //   (b) Inline-blocking wedges the executor pump in a half-committed state (no
        //       tail-waiter commit, no successor dispatch); trailing-phase await leaves
        //       ExecutingItem invariant intact through the wait and is recoverable via the
        //       framework's TrailingExecutionTask path.
        if (_outstandingTrailing.IsCompletedSuccessfully)
        {
            var encoder = context.GetEncoder();
            encoder.WriteSync();
            var flushTask = encoder.FlushAsync();
            return new(new FlowTasks(trailingExecutionTask: flushTask, pipelineTask: DrainPhase()));
        }

        return new(new FlowTasks(trailingExecutionTask: TrailingPhase(), pipelineTask: DrainPhase()));

        async ValueTask TrailingPhase()
        {
            // Observe-and-discard outstanding: the failed flow's fault is already captured in
            // FailureException, and the trailing's outcome is subordinate. Single await, no
            // double-consume.
            try { await _outstandingTrailing.ConfigureAwait(false); }
            catch { /* subordinate to the failure we're recovering from */ }
            // ExecutingItem invariant: the pipeline keeps the recovery slot through the
            // trailing's tail (see Pipeline.ExecutingItem contract), so GetEncoder's gate
            // accepts our writes here. WriteSync + FlushAsync run sequentially with the
            // failed flow's (now-completed) trailing, single producer preserved.
            var encoder = context.GetEncoder();
            encoder.WriteSync();
            await encoder.FlushAsync().ConfigureAwait(false);
        }

        async ValueTask DrainPhase()
        {
            var decoder = await context.GetDecoderAsync().ConfigureAwait(false);
            int remaining = _drainCount;
            while (remaining > 0)
            {
                var message = await decoder.GetNextAsync().ConfigureAwait(false);
                // HandleMessageAutoCore decrements this flow's _rfqCount internally; the local
                // counter drives the loop exit and stays robust against any future change to the
                // auto-handler's count semantics.
                if (message.Header.Type is BackendType.ReadyForQuery)
                    remaining--;
            }
        }
    }
}
