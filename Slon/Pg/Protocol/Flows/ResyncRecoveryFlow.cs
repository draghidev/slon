using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol.Flows;

// Substitute item returned by PgClientProtocol.Control.TryRecoverItemFailure to resync the wire
// after a flow fails (decode, write, or execute fault). Inherits the failed flow's outstanding RFQ
// obligation, pads any torn outgoing frame, then runs the resync move (realigning Sync when needed +
// a ROLLBACK to close any open transaction; see WriteResyncAsync), flushing it with the failed flow's
// buffered work and draining the resulting RFQs (inherited + the resync move's, 0-2).
// Recovery of an ExclusiveAccessFlow appends its checked session reset to the same write batch.
//
// Substitution-substrate contract: recovery REPLACES the failed flow's executor slot, but the
// failed flow's lifetime extends through this substitute via FailedFlow and the gate permissivity
// in ThrowIfCannotWrite. When the failed flow's pipeline task faulted with its trailing still
// in-flight, the framework passes that trailing through OutstandingPhaseTask and ExecuteAuto awaits
// it before touching the encoder, so recovery's writes don't collide with the trailing flush on
// the single-producer writer.
//
// Scope: write-side torn frames are padded (WriteResync step 1); COPY-mode terminators
// (CopyDone/CopyFail) are future work - see the TODO(copy) marker in ExecuteAuto. COPY isn't
// implemented, so the wire can't be in copy-mode at recovery time.
sealed class ResyncRecoveryFlow : PgClientFlow
{
    // Normal large parameter writes remain worth salvaging: padding is streamed in bounded chunks,
    // so its declared size no longer implies equivalent client memory. This cap only rejects an
    // implausibly large/corrupt frame whose wire repair would monopolize the connection indefinitely.
    internal const int MaxRecoveryPaddingBytes = 256 * 1024 * 1024;
    // Keep recovery resumable without turning the 256 MiB salvage ceiling into thousands of
    // flush/await rounds. The writer may subdivide this bounded window into normal segments.
    const int RecoveryPaddingChunkBytes = 4 * 1024 * 1024;
    int _drainCount;
    ValueTask _outstandingTrailing;
    bool _outstandingIsRead;
    bool _canWriteSync;
    bool _canWrite;
    // Captured with the recovery tenure so the written query and its RFQ accounting cannot observe
    // different reset-plan revisions.
    string? _scopeResetCommand;
    PgClientProtocol.Control? _control;

    /// The flow this recovery supplanted, carried so the policy can complete it when the
    /// recovery completes. The failed item's lifetime deliberately extends as far as the
    /// recovery does: completion is the reuse gate (completed observers Reset and re-enqueue
    /// instances), and reuse must be causally after the protocol stops referencing the failed
    /// tenure's machinery (parked dispatch state, inherited RFQ bookkeeping, registrations).
    /// Ordering of anything enqueued after the failure is already handled by the pipeline.
    public PgClientFlow? FailedFlow { get; private set; }
    public Exception? FailureException { get; private set; }

    /// Whether recovery injects a realigning Sync. True only when the failed flow ended mid extended-
    /// query (no RFQ induced) AND the write window is open. Distinct from _canWrite, which gates the
    /// always-written ROLLBACK on just the write window being open.
    public bool CanWriteSync => _canWriteSync;

    // A recovery without the write window can only drain boundaries left by the failed flow. Stop
    // admitting new work until that premise has been validated by reaching those boundaries.
    public bool BlocksAdmission => !_canWrite;

    /// True while the failed flow still has an in-flight read. The decoder permit resolves to
    /// FailedFlow while this holds so that read finishes on its own read-state, not the recovery's.
    /// The read-side inverse of ThrowIfCannotWrite. DrainPhase awaits it before taking the read turn.
    public bool FailedReadOutstanding => _outstandingIsRead && !_outstandingTrailing.IsCompleted;

    // A recovery is always bound to a failed flow. The rfq transfer routes through ExecutionControl.
    public static ResyncRecoveryFlow Create(
        PgClientProtocol.Control control,
        PgClientFlow failedFlow,
        Exception exception,
        ValueTask outstandingTrailing,
        bool outstandingIsRead,
        int inheritedRfqCount,
        bool canWriteSync,
        bool canWrite)
    {
        var recovery = new ResyncRecoveryFlow { IsAsync = failedFlow.IsAsyncAtDispatch };
        recovery.GetExecutionControl(control).TransferInheritedRfqCount(inheritedRfqCount);
        recovery._control = control;
        recovery.FailedFlow = failedFlow;
        recovery.FailureException = exception;
        recovery._outstandingTrailing = outstandingTrailing;
        recovery._outstandingIsRead = outstandingIsRead;
        recovery._canWriteSync = canWriteSync;
        recovery._canWrite = canWrite;
        recovery._scopeResetCommand = failedFlow is ExclusiveAccessFlow ? control.ScopeResetCommand : null;
        // drainCount = inheritedRfqCount + the resync move's RFQs (WriteResyncAsync): the realigning Sync
        // when canWriteSync, the always-written ROLLBACK when canWrite, and the reset query when the
        // failed item was an exclusive scope.
        recovery._drainCount = inheritedRfqCount + (canWriteSync ? 1 : 0) + (canWrite ? 1 : 0)
            + (canWrite && recovery._scopeResetCommand is not null ? 1 : 0);
        return recovery;
    }

    ResyncRecoveryFlow() : base(supportsDeferredFlush: true) { }

    public new void Reset()
    {
        base.Reset();
        _drainCount = 0;
        _outstandingTrailing = default;
        _outstandingIsRead = false;
        _canWriteSync = false;
        _canWrite = false;
        _scopeResetCommand = null;
        _control = null;
        FailedFlow = null;
        FailureException = null;
    }

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        // Resync with WriteResyncAsync: pad any torn frame, then BEGIN (mid extended-query only) + ROLLBACK to
        // discard whatever the failed flow left uncommitted. Mid extended-query its executed commands sit
        // in an OPEN implicit block that PG holds to the Sync (NOT per-statement commit), so a BEGIN
        // upgrades that block to explicit and the ROLLBACK unwinds it; otherwise the ROLLBACK just closes
        // an open explicit transaction (an unfinished BEGIN, or an exclusive scope's, propagated to root).
        //
        // TODO(copy): COPY needs CopyFail/CopyDone handling instead of a bare Sync. COPY isn't
        // implemented, so the wire can't be in copy-mode at recovery time and this is correct.
        //
        // Sequencing against the failed flow's in-flight trailing (it owns the single-producer
        // writer): if outstanding already completed, write inline. Otherwise move the writes to our
        // trailing phase so the framework reaches the pending-tail state before we await outstanding.
        // Required for two reasons:
        //   (a) TCP send-window deadlock: outstanding's flush waits on the send buffer draining,
        //       which needs the peer to read, which needs US to read its responses. Inline await
        //       can't start DrainPhase (ExecuteAuto hasn't returned), so the read leg never breaks
        //       the cycle. Trailing-phase await runs the drain concurrently and unblocks the flush.
        //   (b) Inline-blocking wedges the executor pump half-committed; trailing-phase await keeps
        //       the ExecutingItem invariant intact and recoverable via TrailingExecutionTask.
        // Write window closed (PipelineTask: identity released from the writer): pure read-drain
        // of the failed flow's inherited RFQs, no resync write. An open transaction left here is
        // backstopped by the next flow's wire-handoff guard.
        if (!_canWrite)
            return new(new FlowTasks(trailingExecutionTask: default, pipelineTask: DrainPhase()));

        // Read-outstanding or write-already-finished: write inline. For read-outstanding the failed
        // read needs our flush to receive its responses, so we must NOT await it first - DrainPhase
        // awaits it instead. Write-outstanding moves to the trailing phase below.
        if (_outstandingIsRead || _outstandingTrailing.IsCompletedSuccessfully)
        {
            var encoder = context.GetEncoder();
            return new(new FlowTasks(
                trailingExecutionTask: StartWriteResync(encoder),
                pipelineTask: DrainPhase()));
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
            await StartWriteResync(encoder).ConfigureAwait(false);
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
                _drainCount = FailedFlow!.GetExecutionControl(_control!).RfqCount
                    + (_canWriteSync ? 1 : 0)
                    + (_canWrite ? 1 : 0)
                    + (_canWrite && _scopeResetCommand is not null ? 1 : 0);
            }
            var decoder = await context.GetDecoderAsync().ConfigureAwait(false);
            int remaining = _drainCount;
            PgError? resetError = null;
            while (remaining > 0)
            {
                var message = await decoder.GetNextAsync().ConfigureAwait(false);
                if (FailedFlow is ExclusiveAccessFlow && remaining == 1 && message.TryCreateError(out var error))
                    resetError ??= error.Preserve();
                // HandleMessageAutoCore decrements _rfqCount internally; the local counter drives
                // the loop exit independently of the auto-handler's count semantics.
                if (message.Header.Type is BackendType.ReadyForQuery)
                    remaining--;
            }
            ExclusiveAccessFlow.ThrowSessionResetError(resetError);
        }
    }

    ValueTask StartWriteResync(PgEncoder encoder)
    {
        if (IsAsync)
            return WriteResyncAsync(encoder);

        ValueTask task;
        using (encoder.BeginResumableScope())
            task = WriteResyncAsync(encoder);
        return task.IsCompleted ? task : encoder.RunResumableTask(task);
    }

    // Realign the wire and discard whatever the failed flow left uncommitted (taken whenever the write
    // window is open):
    //   1. PadCurrentMessage - complete any torn outgoing frame so the server exits at a framing boundary.
    //   2. Parse/Bind/Execute("BEGIN") + Sync (only when canWriteSync) - the failed flow ended mid
    //      extended-query, so the server discards frontend messages through the next Sync after an
    //      error. If the padded frame errors, BEGIN is discarded and Sync rolls back the failed implicit
    //      block. If it does not, the executed commands sit in an OPEN implicit block PG holds to the Sync
    //      (verified: a bare Sync COMMITS them - see Recovery_PipelinedImplicitBlock_SurvivorCount), and
    //      BEGIN upgrades that block to explicit before Sync so the ROLLBACK can unwind it. When the flow
    //      already opened an explicit transaction, BEGIN-in-transaction is a harmless WARNING (NOT an
    //      error), leaving the open transaction for the ROLLBACK.
    //   3. Query("ROLLBACK") - rolls back the now-explicit (or already-explicit) transaction; a
    //      no-op-with-notice when already Idle. When canWriteSync is false the flow's own Query/Sync
    //      already terminated its block, so this is the only message - closing any open BEGIN it left.
    //   4. For an exclusive flow, append the session reset after ROLLBACK. It needs no response-dependent
    //      write, so DrainPhase can consume its additional RFQ and preserve any reset error.
    // BEGIN/ROLLBACK notices and CommandCompletes are non-RFQ and discarded by DrainPhase (counts RFQs).
    async ValueTask WriteResyncAsync(PgEncoder encoder)
    {
        var paddingLength = encoder.CurrentMessagePaddingLength;
        if (paddingLength > MaxRecoveryPaddingBytes)
            throw new PgProtocolException(
                $"Recovering the PostgreSQL wire would require padding {paddingLength} bytes, " +
                $"exceeding the {MaxRecoveryPaddingBytes}-byte safety limit.");

        // A torn streamed message may be much larger than the writer's normal buffer. Pad and
        // physically flush it in bounded pieces so recovery never materializes the entire missing
        // body synchronously. FlushResumable bypasses ordinary pipeline deferral: each chunk must
        // leave before the next one is produced.
        while (paddingLength > 0)
        {
            var padded = encoder.PadCurrentMessage(Math.Min(paddingLength, RecoveryPaddingChunkBytes));
            paddingLength -= padded;
            if (paddingLength > 0)
                await encoder.FlushResumable().ConfigureAwait(false);
        }
        if (_canWriteSync)
        {
            // BEGIN must be in the extended stream before the realigning Sync. A simple Query is itself
            // discarded while the server is recovering from an extended-query error and therefore cannot
            // provide either the transaction upgrade or the boundary.
            encoder.WriteParse("BEGIN -- Slon connection recovery");
            encoder.WriteBind();
            encoder.WriteExecute();
            encoder.WriteSync();
        }
        // Closes the now-explicit (or already-explicit) transaction; a no-op-with-notice when Idle.
        encoder.WriteQuery("ROLLBACK -- Slon connection recovery");
        ExclusiveAccessFlow.WriteScopeReset(encoder, _scopeResetCommand);
        await encoder.FlushResumable().ConfigureAwait(false);
    }
}
