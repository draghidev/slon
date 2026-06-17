using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol.Flows;

// Substitute item returned by PgClientProtocol.Control.TryRecoverItemFailure to resync the wire
// after a flow fails (decode, write, or execute fault). Inherits the failed flow's outstanding RFQ
// obligation, injects a terminating Sync, flushes the failed flow's buffered work plus the Sync,
// then drains the resulting RFQs (inherited + 1).
//
// Substitution-substrate contract: recovery REPLACES the failed flow's executor slot, but the
// failed flow's lifetime extends through this substitute via FailedFlow and the gate permissivity
// in ThrowIfCannotWrite. When the failed flow's pipeline task faulted with its trailing still
// in-flight, the framework passes that trailing through OutstandingPhaseTask and ExecuteAuto awaits
// it before touching the encoder, so recovery's writes don't collide with the trailing flush on
// the single-producer writer.
//
// Scope: resyncs non-COPY, non-torn wire state. COPY terminators and torn-frame repair are future
// work; see the markers in ExecuteAuto.
sealed class ResyncRecoveryFlow : PgClientFlow
{
    int _drainCount;
    ValueTask _outstandingTrailing;
    bool _outstandingIsRead;
    bool _canWriteSync;
    PgClientProtocol.Control? _control;

    /// The flow this recovery supplanted, carried so the policy can complete it when the
    /// recovery completes. The failed item's lifetime deliberately extends as far as the
    /// recovery does: completion is the reuse gate (completion actions Reset and re-enqueue
    /// instances), and reuse must be causally after the protocol stops referencing the failed
    /// tenure's machinery (parked dispatch state, inherited RFQ bookkeeping, registrations).
    /// Ordering of anything enqueued after the failure is already handled by the pipeline.
    public PgClientFlow? FailedFlow { get; private set; }
    public Exception? FailureException { get; private set; }

    /// Whether recovery may write a terminating Sync. True only while the failed flow's write
    /// window is still open. When closed, ExecuteAuto skips the Sync and drainCount drops its +1.
    public bool CanWriteSync => _canWriteSync;

    /// True while the failed flow still has an in-flight read. The decoder permit resolves to
    /// FailedFlow while this holds so that read finishes on its own read-state, not the recovery's.
    /// The read-side inverse of ThrowIfCannotWrite. DrainPhase awaits it before taking the read turn.
    public bool FailedReadOutstanding => _outstandingIsRead && !_outstandingTrailing.IsCompleted;

    // A recovery is always bound to a failed flow. drainCount = inheritedRfqCount plus the
    // recovery's own Sync when canWriteSync. The rfq transfer routes through ExecutionControl.
    public static ResyncRecoveryFlow Create(
        PgClientProtocol.Control control,
        PgClientFlow failedFlow,
        Exception exception,
        ValueTask outstandingTrailing,
        bool outstandingIsRead,
        int inheritedRfqCount,
        bool canWriteSync)
    {
        var recovery = new ResyncRecoveryFlow(supportsPipelining: true) { IsAsync = failedFlow.IsAsyncAtBind };
        recovery.GetExecutionControl(control).TransferInheritedRfqCount(inheritedRfqCount);
        recovery._control = control;
        recovery.FailedFlow = failedFlow;
        recovery.FailureException = exception;
        recovery._outstandingTrailing = outstandingTrailing;
        recovery._outstandingIsRead = outstandingIsRead;
        recovery._canWriteSync = canWriteSync;
        recovery._drainCount = inheritedRfqCount + (canWriteSync ? 1 : 0);
        return recovery;
    }

    ResyncRecoveryFlow(bool supportsPipelining) : base(supportsPipelining) { }

    public new void Reset()
    {
        base.Reset();
        _drainCount = 0;
        _outstandingTrailing = default;
        _outstandingIsRead = false;
        _canWriteSync = false;
        _control = null;
        FailedFlow = null;
        FailureException = null;
    }

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        // Resync unconditionally: inject a terminating Sync, flush it with the failed flow's
        // buffered work, and drain to the RFQ it induces plus the inherited RFQs. Worst case is
        // one harmless extra RFQ on an idle wire.
        //
        // Torn-frame defense: if the failed flow faulted mid-message, pad with zero bytes so the
        // server reads exactly the declared body and exits at the framing boundary, leaving the
        // following Sync on a clean RFQ state.
        //
        // TODO(copy): COPY needs CopyFail/CopyDone handling instead of a bare Sync. COPY isn't
        // implemented, so the wire can't be in copy-mode at recovery time and a bare Sync is correct.
        //
        // Sequencing against the failed flow's in-flight trailing (it owns the single-producer
        // writer): if outstanding already completed, write inline. Otherwise move the writes to our
        // trailing phase so the framework reaches the tail-waiter state before we await outstanding.
        // Required for two reasons:
        //   (a) TCP send-window deadlock: outstanding's flush waits on the send buffer draining,
        //       which needs the peer to read, which needs US to read its responses. Inline await
        //       can't start DrainPhase (ExecuteAuto hasn't returned), so the read leg never breaks
        //       the cycle. Trailing-phase await runs the drain concurrently and unblocks the flush.
        //   (b) Inline-blocking wedges the executor pump half-committed; trailing-phase await keeps
        //       the ExecutingItem invariant intact and recoverable via TrailingExecutionTask.
        // Write window closed: pure read-drain of the failed flow's inherited RFQs. No Sync is
        // written, so drainCount is inheritedRfqCount with no +1.
        if (!_canWriteSync)
            return new(new FlowTasks(trailingExecutionTask: default, pipelineTask: DrainPhase()));

        // Read-outstanding or write-already-finished: write inline. For read-outstanding the failed
        // read needs our flush to receive its responses, so we must NOT await it first - DrainPhase
        // awaits it instead. Write-outstanding moves to the trailing phase below.
        if (_outstandingIsRead || _outstandingTrailing.IsCompletedSuccessfully)
        {
            var encoder = context.GetEncoder();
            encoder.PadCurrentMessage();
            encoder.WriteSync();
            var flushTask = encoder.FlushAsync();
            return new(new FlowTasks(trailingExecutionTask: flushTask, pipelineTask: DrainPhase()));
        }

        return new(new FlowTasks(trailingExecutionTask: TrailingPhase(), pipelineTask: DrainPhase()));

        async ValueTask TrailingPhase()
        {
            // Observe-and-discard outstanding: the failed flow's fault is already captured in
            // FailureException, and the trailing's outcome is subordinate.
            try { await _outstandingTrailing.ConfigureAwait(false); }
            catch { /* subordinate to the failure we're recovering from */ }
            // ExecutingItem invariant: the pipeline keeps the recovery slot through the trailing's
            // tail, so GetEncoder's gate accepts our writes here, sequential with the now-completed
            // trailing and single-producer-preserved.
            var encoder = context.GetEncoder();
            encoder.PadCurrentMessage();
            encoder.WriteSync();
            await encoder.FlushAsync().ConfigureAwait(false);
        }

        async ValueTask DrainPhase()
        {
            if (_outstandingIsRead)
            {
                // Wait for the failed flow's in-flight read to finish before taking the read turn,
                // or we'd resolve it out from under it. Observe-and-discard, subordinate to our failure.
                try { await _outstandingTrailing.ConfigureAwait(false); }
                catch { /* subordinate to the failure we're recovering from */ }

                // The read may have crossed inherited RFQs after Create snapshotted the count,
                // decrementing the failed flow's own counter. Reconcile against its now-final live
                // count so we drain only what remains, not what the read already consumed.
                _drainCount = FailedFlow!.GetExecutionControl(_control!).RfqCount + (_canWriteSync ? 1 : 0);
            }
            var decoder = await context.GetDecoderAsync().ConfigureAwait(false);
            int remaining = _drainCount;
            while (remaining > 0)
            {
                var message = await decoder.GetNextAsync().ConfigureAwait(false);
                // HandleMessageAutoCore decrements _rfqCount internally; the local counter drives
                // the loop exit independently of the auto-handler's count semantics.
                if (message.Header.Type is BackendType.ReadyForQuery)
                    remaining--;
            }
        }
    }
}
