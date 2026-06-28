using System.Runtime.CompilerServices;
using Draghi.Pipelining;
using Draghi.Pipelining.Internal;

// Composes Draghi.Pipelining.Internal SPSC primitives. The Experimental tag is deliberate. This
// is the protocol's idle-flush + sync-flow handoff seam.
#pragma warning disable DRAGHI001

namespace Slon.Pg.Protocol;

/// Pipeline source for PgClientFlow. SPSC primary queue on the thin TryGetNext/WaitForNextAsync pull
/// seam (a WakeSignal wait, no value-task source), plus a sync-flow handoff slot that lets a sync
/// producer's thread drive the executor for its own item without TP dispatch in the rendezvous.
///
/// Idle handoff: the sync producer publishes its flow into HandoffSlot and sets HandoffActive, which
/// gates async producers' signals and completion wakes from racing the rendezvous. It then blocks in
/// WakeSignal.WaitForSuspended until the executor's wait is armed, acks the slot, and claims the wait
/// inline: the executor's continuation runs on the producer's thread, its TryGetNext retry takes the
/// acked slot, and the flow is processed synchronously before EnqueueSyncWithHandoff returns. No TP
/// work item at any point.
///
/// Async producers arriving during HandoffActive enqueue without signalling. After the handoff the
/// sync caller wakes the executor on TP only if items or a deferred completion accrued, zero TP otherwise.
readonly struct PgClientFlowSource : IPipelineSource<PgClientFlow, PgClientFlowSource.Enumerator>
{
    readonly State _state;

    PgClientFlowSource(State state) => _state = state;

    public static PgClientFlowSource Create(PgClientProtocol protocol, PipelineScheduler? executionScheduler = null)
        => new(new State(protocol, executionScheduler ?? PipelineScheduler.ThreadPool));

    /// Enqueues an async-mode flow. The caller dispatches via the returned <see cref="EnqueueResult"/>.
    /// During a sync-flow handoff, the item is queued but the dispatch is a no-op. The executor will
    /// pick it up after the handoff window closes. Throws InvalidOperationException if the source has
    /// been completed.
    public EnqueueResult Enqueue(PgClientFlow flow)
    {
        if (Volatile.Read(ref _state.IsCompleted))
            ThrowCompleted();

        _state.EnqueueItem(flow);
        // Release store suffices: the flag's only cross-thread reader is the handoff close-out's
        // compensation, and EnqueueResult.Execute's under-lock gate is the full fence that publishes
        // it in time. A stale TRUE costs at most a spurious compensation wake (the wait re-check peeks
        // the queue, not this flag).
        Volatile.Write(ref _state.QueueNotEmpty, true);

        return new(_state);
    }

    /// Synchronously enqueues a sync-mode flow and blocks until the executor processes it on the
    /// caller's thread, which drives the rendezvous and the body. No TP work item at any point.
    /// Single producer per source (serialized by the protocol's submission lock). 1: open the handoff
    /// window (HandoffActive) and publish the flow into the slot, gating async signals. 2: claim the
    /// executor's parked wait under the wake lock, acking the slot only on a winning claim so a racing
    /// async wake can't snipe it; re-wait and retry if the executor was busy. The claim dispatches the
    /// executor's continuation inline here to pull the flow and process it. If async producers deferred
    /// items during the window, wake the executor on TP afterward, else zero TP.
    // Combined sync handoff for callers without an outer enqueue lock (the exclusive scope's inner
    // source, a single sync producer): append at the FIFO tail and run the blocking rendezvous.
    public void EnqueueSyncWithHandoff(PgClientFlow flow)
        => WaitForExecutor(EnqueueSyncWaiter(flow));

    // Two-phase split for callers that enqueue under an outer lock (TryQueueFlow under _syncRoot):
    // EnqueueSyncWaiter appends the flow under the lock (FIFO order), WaitForExecutor runs the blocking
    // rendezvous (parking on the flow's own handoff MRES) OUTSIDE it.
    public PgClientFlow EnqueueSyncWaiter(PgClientFlow flow)
    {
        if (Volatile.Read(ref _state.IsCompleted))
            ThrowCompleted();
        return _state.EnqueueSyncWaiter(flow);
    }

    public void WaitForExecutor(PgClientFlow flow) => _state.WaitForExecutor(flow);

    // Drain the inert head of the source: items enqueued but never picked up by the executor.
    // CompleteAsync only sees dispatched flows, so anything still in the SPSC queue needs separate
    // disposition - the caller's handler faults each via ExecutionControl.Complete (future migration
    // rebinds them onto a new protocol here instead). Call only after the executor has stopped
    // pulling (Shutdown awaits DrainSignal first) so this is the sole consumer.
    public void DrainInertItems(Action<PgClientFlow> onInert)
    {
        _state.DrainInert(onInert);
        Volatile.Write(ref _state.QueueNotEmpty, false);
    }

    // Arm the drain gate (see State.DrainSignal). Set before Complete is triggered.
    public void SetDrainSignal(TaskCompletionSource drainSignal) => _state.DrainSignal = drainSignal;

    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return new(_state, cancellationToken);
    }

    /// Backlog: flows enqueued but not yet dispatched. With Pipeline.Depth (in-flight = dispatched -
    /// completed), Depth + Backlog is the total outstanding. Lock-free read, may be stale.
    public int Backlog => _state.Backlog;

    static void ThrowCompleted() => throw new InvalidOperationException("The source has been completed.");

    internal sealed class State
    {
        public readonly FlushGate FlushGate;
        // Primary storage: inline slot fast path + lazy one-way SPSC escalation (SlotEscalatingQueue).
        // The sequential common case - one in-flight flow, or a nested exclusive scope's serial
        // subflows - stays on the slot with no SPSC allocation; a pipelining connection escalates on
        // first overlap. Same SPSC contract as the bare queue it replaces (single producer = Enqueue,
        // single consumer = the executor's pull), so it is a drop-in. Non-readonly: mutated by ref this.
        SlotEscalatingQueue<PgClientFlow> _storage;
        // The wait-protocol WakeSignal with suspension observation enabled: the executor's waits
        // arm against it (thin path, no value-task source), and EnqueueSyncWithHandoff
        // rendezvouses on WaitForSuspended. Scheduler-routed async wakes come with it.
        public readonly WakeSignal WakeSignal;
        public bool QueueNotEmpty;
        public PgClientFlow Current = null!;
        // Drain gate. Fired once from WaitCore's completed-resolution when the executor's pull
        // resolves completed (WaitForNextAsync delivers false). Shutdown awaits it before draining
        // the residual so the drain is the sole consumer of the SPSC queue (a concurrent executor
        // dequeue would tear the read). Set before Complete.
        public TaskCompletionSource? DrainSignal;

        // Sync-handoff FIFO is just _storage (sync+async in submission order); the current sync head the
        // executor is holding for its caller is HeldSyncFlow. No separate wait-list: each parked caller
        // parks on its OWN flow's MRES (PgClientFlow.GetHandoffMres), which the executor signals when it
        // holds that flow. The old intrusive-list-of-wait-nodes (and its lagging-link spin) is gone.
        // One-shot takeover. The head caller's inline claim re-enters the pump on the caller's thread:
        // _takeoverPending makes that pull dequeue the head sync flow (its own), then _takeoverActive
        // makes the NEXT pull fake-miss so the pump parks (hands back to TP) instead of draining the
        // following flow on the caller's thread.
        public bool TakeoverPending;
        public bool TakeoverActive;
        public bool IsCompleted;

        // Set under the wake lock by OnExecutorSuspended when the executor parks AT a sync head: that
        // park is reserved for the head sync caller's takeover, so TryClaim (async / Complete) must NOT
        // steal it. Set to false on any non-sync-head park (idle / async head). A producer-readable
        // substitute for the SPSC-illegal queue peek a producer can't do itself.
        public bool ParkedAtSyncHead;

        public State(PgClientProtocol protocol, PipelineScheduler scheduler)
        {
            FlushGate = new(protocol);
            WakeSignal = new(runContinuationsAsynchronously: true, scheduler, enableWaitForSuspended: true);
            WakeSignal.OnSuspended = OnExecutorSuspended;
        }

        // Invoked under the wake lock when the executor parks. If it dequeued a sync flow and is HOLDING
        // it (HeldSyncFlow) for that flow's caller, signal THAT flow's own handoff MRES so its caller takes
        // over and runs the held flow on its own thread. Idle / async-drained parks are not a sync rendezvous.
        void OnExecutorSuspended()
        {
            var held = HeldSyncFlow;
            // Reserve this park for the held sync caller (TryClaim reads this under the same lock) BEFORE
            // signalling, so a producer's TryClaim can't steal the park between the signal and the gate.
            Volatile.Write(ref ParkedAtSyncHead, held is not null);
            if (held is not null)
            {
                held.GetHandoffMres()?.Set();   // exactly the just-held caller, parked on its own flow's MRES
                return;
            }
            // Parking idle / async-headed with work still queued: re-drive the pump on TP rather than
            // idle-park on pending work. The close-out re-signal and a producer's kick are both lost when
            // they land while the pump is OFF the wake-park - which happens when a sync flow takes the
            // executor INLINE on its caller's thread and then faults: RecoverItem awaits the trailing,
            // suspending the pump off-park, and resumes it on the trailing's TP thread, so the one-shot
            // hand-back arms here with the next caller's flow already queued but unsignalled. Self-healing
            // here keeps all of that contained in the source. Claim the park we just armed and dispatch
            // its continuation on TP; an async dispatch only schedules, so it is safe under this wake lock.
            if (HasItem() && WakeSignal.TryClaimLocked())
                WakeSignal.DispatchClaimed(runContinuationsAsynchronously: true);
        }

        // Register Complete as the completion-token callback. Done here (not in Enumerator) so Complete
        // can stay private: its single-writer safety depends on the CTS firing it at most once, so the
        // sole entry point is this registration.
        public void RegisterCompletion(CancellationToken completionToken)
            => completionToken.UnsafeRegister(static state => ((State)state!).Complete(), this);

        // Private and single-writer by construction: the ONLY caller is the CompletionToken registration
        // above, which the CTS fires at most once however many threads race _cts.Cancel (external
        // CompleteAsync + the executor's terminal DisposeAsync). That once-only guarantee is what lets the
        // IsCompleted store below be a plain release write (no Interlocked) - do not add a direct caller.
        void Complete()
        {
            Volatile.Write(ref IsCompleted, true);
            // Wake the executor so its next wait resolves Completed. During completion TryClaim is allowed
            // to claim a sync-head park too (see below): if the executor is parked at a sync head whose
            // caller is about to bail, that park must be un-parked here or it strands (DrainSignal never
            // fires). Un-parking the executor is ALL Complete does for sync callers now: each parked caller
            // wakes when ITS OWN flow is drained inert (DrainInert -> ExecutionControl.Complete -> OnComplete
            // -> HandleException -> SignalProgress sets the flow's MRES), then re-reads IsCompleted and bails.
            // No direct wait-list head wake - there is no wait-list.
            TryClaim(runContinuationsAsynchronously: true);
        }

        // Claim the executor's parked wait and dispatch its continuation. Gated on ParkedAtSyncHead so an
        // async/Complete claim never STEALS a park the executor is holding at a sync head (reserved for
        // that head sync caller's takeover): stealing it makes the sync caller's own claim lose, and the
        // claim-loss Reset can eat the executor's re-park signal -> lost wake. The read and the claim are
        // one wake-lock hold, paired with OnExecutorSuspended's under-lock write. A claim dropped to a
        // sync-head park is intentional: the sync caller takes over, and its close-out re-signal re-drives
        // the executor for any async deferred during the window. Other parks (idle / async head) are not
        // reserved, so async drains freely - no FIFO misroute. EXCEPTION: once completing, the reservation
        // is lifted - there are no more takeovers, so Complete must be able to claim a sync-head park to
        // un-park an executor whose caller is bailing (else it strands and DrainSignal never fires). Only
        // Complete reaches here post-completion (the async Execute path throws at Enqueue first).
        public void TryClaim(bool runContinuationsAsynchronously)
        {
            var wakeSignal = WakeSignal;
            wakeSignal.AcquireWakeLock();
            var reserved = ParkedAtSyncHead && !Volatile.Read(ref IsCompleted);
            var claimed = !reserved && wakeSignal.TryClaimLocked();
            wakeSignal.ReleaseWakeLock();
            if (claimed)
                wakeSignal.DispatchClaimed(runContinuationsAsynchronously);
        }

        // Phase 1 of the sync handoff, under the protocol's _syncRoot: enqueue the sync flow at its real
        // FIFO position in the one queue (no priority slot, no wait-node - the flow IS its own waiter via
        // GetHandoffMres). Returns the flow for the out-of-lock rendezvous.
        public PgClientFlow EnqueueSyncWaiter(PgClientFlow flow)
        {
            // Capture the routing async-mode (sync here) as the flow's stable bind snapshot BEFORE it is
            // published to the executor's pull, so the pull's dequeue-then-check sees a value that agrees
            // with this routing and stays put even if the body later flips IsAsync. By routing, not the
            // mutable IsAsync: a flow on the sync path is a sync handoff regardless of its IsAsync state.
            flow.CaptureAsyncRoutingSnapshot(isAsync: false);
            _storage.Enqueue(flow);
            Volatile.Write(ref QueueNotEmpty, true);
            return flow;
        }

        // Phase 2, OUTSIDE the lock (blocks): drive the executor to this flow's FIFO turn and take it
        // over so the body runs on the caller's thread. Parks on the flow's OWN handoff MRES.
        public void WaitForExecutor(PgClientFlow flow)
        {
            var wakeSignal = WakeSignal;
            // Caller-handoff path only: the routing (TryQueueFlow / ExclusiveAccessFlow.Queue, gated on
            // NeedsSyncHandoff) sends autonomous sync flows (null MRES, no parked caller) down the async
            // dispatch path instead, so a flow that reaches here always carries its waiter MRES. Fail loud
            // rather than NRE if that invariant is ever bypassed.
            var mres = flow.GetHandoffMres()
                ?? throw new InvalidOperationException("WaitForExecutor reached with a null handoff MRES: an autonomous sync flow must route via async dispatch (NeedsSyncHandoff), not the caller-handoff park.");
            // Kick the executor so it pulls and drains earlier flows in FIFO order, dequeue-and-holding the
            // first sync head and parking - OnExecutorSuspended then signals THAT held flow's MRES. A no-op
            // if the executor is already running (it reaches our flow on its own).
            wakeSignal.Signal(runContinuationsAsynchronously: true);

            while (true)
            {
                mres.Wait();
                wakeSignal.AcquireWakeLock();
                // Reset under the lock, right after Wait: a successful claim hands the body a CLEAN MRES (the
                // body reuses it for its own WaitForContinuation rendezvous). Manual-reset + two signal
                // sources (OnExecutorSuspended here, the inert-drain SignalProgress on completion), so the
                // Reset must serialize with the Set under the wake lock.
                mres.Reset();
                if (ReferenceEquals(HeldSyncFlow, flow) && wakeSignal.TryClaimLocked())
                {
                    TakeoverPending = true;   // our inline pull dequeues our own (now held) flow
                    wakeSignal.ReleaseWakeLock();
                    // Inline: the pump continuation runs on this thread, dequeues our flow, runs its body,
                    // then one-shot fake-misses and re-parks (pump back to TP) so this returns.
                    wakeSignal.DispatchClaimed(runContinuationsAsynchronously: false);
                    break;
                }
                // Completion bail: the source completed and we did not claim a park to take our flow over.
                // It (and anything ahead of it) drains inert; its fault surfaces via the flow. Stop waiting
                // - the executor resolves Completed and will not signal us again.
                if (Volatile.Read(ref IsCompleted))
                {
                    wakeSignal.ReleaseWakeLock();
                    break;
                }
                wakeSignal.ReleaseWakeLock();
            }

            // Close-out: kick the executor to advance to the next FIFO flow. It dequeues-and-holds the next
            // sync head and OnExecutorSuspended signals THAT caller's MRES. On completion the executor
            // resolves Completed instead of advancing, but each parked caller wakes when ITS OWN flow drains
            // inert (its terminal SignalProgress sets its MRES), so no direct successor wake is needed here.
            wakeSignal.Signal(runContinuationsAsynchronously: true);
        }

        // Async enqueue (the EnqueueResult / Execute path). Capture the routing async-mode (async here)
        // before publishing - the executor dispatches it rather than holding it for a caller takeover.
        public void EnqueueItem(PgClientFlow flow) { flow.CaptureAsyncRoutingSnapshot(isAsync: true); _storage.Enqueue(flow); }
        public int Backlog => _storage.Count;

        // Consumer-side peek used by WaitCore's authoritative not-empty test.
        public bool HasItem() => _storage.TryPeek(out _);

        // Sole consumer once the executor has stopped (Shutdown's drain). A sync flow the executor
        // dequeued and HELD (HeldSyncFlow) is no longer in _storage but is the FIFO head, so drain it
        // first - else it is lost (its caller, if any, never took it over before the executor stopped).
        public void DrainInert(Action<PgClientFlow> onInert)
        {
            if (HeldSyncFlow is { } held)
            {
                HeldSyncFlow = null;
                onInert(held);
            }
            _storage.DrainInert(onInert);
        }

        // Dispatch the head on the executor ONLY if it is an async flow; a sync head is dequeued and HELD
        // for its caller's takeover. Dequeue-then-check (one SPSC op) rather than peek-then-dequeue (two
        // ops, a per-item cost on the hot async path): checking the head and dequeuing separately is a
        // TOCTOU race - the queue can be empty at the check and a producer can enqueue a SYNC flow before
        // the dequeue, normal-dispatching it (misroute) and stranding its caller. We dequeue once; if the
        // dequeued flow is sync it was a mis-take, so hold it in HeldSyncFlow and fake-miss so its caller
        // takes it over. Reads IsAsyncAtBind (the STABLE routing snapshot captured at enqueue), not the
        // mutable IsAsync a body flips mid-execution.
        public PgClientFlow? HeldSyncFlow;
        public bool TryDispatchAsyncOrHoldSync()
        {
            if (HeldSyncFlow is not null)
                return false;   // a sync flow is held, waiting for its caller: do not dispatch behind it
            if (!_storage.TryDequeue(out Current!))
                return false;   // empty
            if (!_storage.TryPeek(out _))
                Volatile.Write(ref QueueNotEmpty, false);
            if (Current.IsAsyncAtBind)
                return true;    // async: dispatch on the executor
            HeldSyncFlow = Current;   // sync: hold for its caller's takeover, fake-miss
            return false;
        }

        public bool TryDequeue()
        {
            if (_storage.TryDequeue(out Current!))
            {
                if (!_storage.TryPeek(out _))
                    Volatile.Write(ref QueueNotEmpty, false);
                return true;
            }
            return false;
        }
    }

    /// Result of <see cref="Enqueue"/>. Calling <see cref="Execute"/> wakes the executor (or
    /// no-ops if a sync handoff is currently in progress, in which case the sync caller will wake
    /// the executor itself after the handoff completes).
    public readonly struct EnqueueResult
    {
        readonly State? _state;
        internal EnqueueResult(State? state) => _state = state;

        public void Execute(bool runContinuationsAsynchronously)
        {
            if (_state is null) return;
            _state.TryClaim(runContinuationsAsynchronously);
        }
    }

    public struct Enumerator : IPipelineEnumerator<PgClientFlow>
    {
        readonly State _state;
        readonly CancellationTokenSource _cts;
        // Captured at construction so reads survive Dispose. See UnboundedQueueSource.Enumerator
        // for the rationale.
        readonly CancellationToken _completionToken;

        internal Enumerator(State state, CancellationToken externalCt)
        {
            _state = state;
            _cts = externalCt.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(externalCt)
                : new CancellationTokenSource();
            _completionToken = _cts.Token;
            _state.RegisterCompletion(_completionToken);
        }

        public CancellationToken CompletionToken => _completionToken;

        public void Complete() => _cts.Cancel();

        /// Synchronous pull. A sync flow's caller takes it over inline (the two takeover flags); a sync
        /// flow it is NOT taking over is fake-missed so the executor parks for that caller; otherwise the
        /// primary queue. Items route through State.Current.
        public bool TryGetNext([System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out PgClientFlow item)
        {
            // Sync takeover: the head sync caller's inline pull on its own thread. The first pull dequeues
            // its own flow - now the queue head, every earlier flow drained - then the next pull one-shot
            // fake-misses so the pump parks and hands back to TP rather than draining a later flow here.
            if (_state.TakeoverPending)
            {
                _state.TakeoverPending = false;
                _state.TakeoverActive = true;
                // Take the flow the executor dequeued and HELD for us. Its caller (this thread) runs its
                // body; the next pull one-shot fake-misses (TakeoverActive) so the pump re-parks.
                item = _state.HeldSyncFlow;
                _state.HeldSyncFlow = null;
                _state.Current = item!;
                return item is not null;
            }
            if (_state.TakeoverActive)
            {
                item = null;
                return false;
            }

            // Completion suppresses queue dispatch: once completed, NOTHING is dispatched or held for a
            // takeover - the whole queue drains inert (async flows rebind onto a new protocol; a sync
            // flow faults and its blocked caller bails). WaitCore resolves Completed (the !HasSyncWaiter
            // gate is gone) and Shutdown's DrainInert is the sole consumer, so taking a queued item here
            // would race that reclaim. (A flow already taken over runs via the TakeoverPending branch
            // above - that path predates completion.)
            if (Volatile.Read(ref _state.IsCompleted))
            {
                item = null;
                return false;
            }

            // Arm gate: under the periodic-flush threshold each TryGetNext-consume requires a fresh
            // WaitForNextAsync round to fire the flush seam. WaitCore re-arms on Retry; we consume it
            // here on take so the next pull is gated again. Outside that, the fast path runs.
            var gate = _state.FlushGate;
            var needsArm = gate.NeedsArm;
            if (needsArm && !gate.Armed)
            {
                item = null;
                return false;
            }
            // Dispatch an ASYNC head on the executor; a SYNC head is dequeued-and-held for its caller's
            // takeover (fake-miss here). One dequeue, check after - see TryDispatchAsyncOrHoldSync for why
            // a peek-then-dequeue would race a producer into a misroute.
            if (_state.TryDispatchAsyncOrHoldSync())
            {
                if (needsArm)
                    gate.ConsumeArm();
                item = _state.Current;
                return true;
            }
            item = null;
            return false;
        }

        /// Miss path. The common no-flush wait takes the thin signal shape. A flush is needed when
        /// in-flight flows have written queries the server hasn't seen (without it their read phase
        /// hangs), but the flush itself almost always completes inline (the socket send buffer has
        /// room), so we flush synchronously and fall through to the same thin signal. Only genuine
        /// write backpressure - flush not completing inline - rides the Task shape.
        public WaitForNextAwaitable WaitForNextAsync()
        {
            if (_state.FlushGate.FlushBeforePark() is { } flushTask)
                return WaitForNextAwaitable.FromTask(FlushThenWaitAsync(flushTask));
            return WaitCore();
        }

        WaitForNextAwaitable WaitCore()
        {
            var wakeSignal = _state.WakeSignal;
            wakeSignal.AcquireWakeLock();

            // One-shot takeover hand-back: the sync caller's pull just fake-missed (TakeoverActive).
            // Reset it and Arm so the pump parks here - the caller's inline DispatchClaimed returns and
            // the pump is back on TP. The caller's close-out re-signal resumes it for any trailing work.
            if (_state.TakeoverActive)
            {
                _state.TakeoverActive = false;
                return wakeSignal.Arm();
            }

            // Completion BEATS the primary queue, including any queued sync flow: once completing, the
            // whole queue drains inert and blocked sync callers bail (Complete wakes them; WaitForExecutor
            // sees IsCompleted and returns). No takeover during shutdown - an async flow ahead of a sync
            // waiter can't be skipped to reach it (it must drain inert / rebind, not run), so takeover
            // can't be done consistently; draining everything inert is the uniform model (previously this
            // was gated on a queued sync waiter to keep the executor alive for a takeover - that gate is
            // gone). Fire DrainSignal so Shutdown drains the residual as the sole consumer.
            if (Volatile.Read(ref _state.IsCompleted) || _completionToken.IsCancellationRequested)
            {
                wakeSignal.ReleaseWakeLock();
                _state.DrainSignal?.TrySetResult();
                return WaitForNextAwaitable.Completed();
            }

            // A dispatchable item is available - retry to consume it - UNLESS a sync flow is already held
            // for its caller's takeover, in which case we park here and let that caller take it (we must
            // not dispatch behind it). HasItem is the authoritative not-empty peek (QueueNotEmpty can go
            // stale-true). A sync head still in the queue gets dequeued-and-held on the retry's next pull.
            if (_state.HeldSyncFlow is null && _state.HasItem())
            {
                // An item is available - arm the next TryGetNext so it can consume one (only one
                // while the flush-threshold gate holds, so a flush round lands between items).
                _state.FlushGate.Rearm();
                wakeSignal.ReleaseWakeLock();
                return WaitForNextAwaitable.Retry();
            }

            return wakeSignal.Arm();
        }

        // Only reached on real write backpressure (the flush didn't complete inline), so a pooled
        // box is plenty - the promise-reuse builder would be overkill for how rarely this fires.
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        async ValueTask<bool> FlushThenWaitAsync(ValueTask flushTask)
        {
            await flushTask.ConfigureAwait(false);
            return await WaitCore();
        }

        public ValueTask DisposeAsync()
        {
            _cts.Cancel();   // idempotent; in case caller skipped Complete()
            _cts.Dispose();  // releases linked-CTS registration on the external token's source
            return default;
        }
    }

}
