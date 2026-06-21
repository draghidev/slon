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

    /// <summary>
    /// Enqueues an async-mode flow. The caller dispatches via the returned <see cref="EnqueueResult"/>.
    /// During a sync-flow handoff, the item is queued but the dispatch is a no-op. The executor will
    /// pick it up after the handoff window closes.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the source has been completed.</exception>
    public EnqueueResult Enqueue(PgClientFlow flow)
    {
        if (Volatile.Read(ref _state.IsCompleted))
            ThrowCompleted();

        _state.OnEnqueue?.Invoke();
        _state.EnqueueItem(flow);
        // Release store suffices: the flag's only cross-thread reader is the handoff close-out's
        // compensation, and EnqueueResult.Execute's under-lock gate is the full fence that publishes
        // it in time. A stale TRUE costs at most a spurious compensation wake (the wait re-check peeks
        // the queue, not this flag).
        Volatile.Write(ref _state.QueueNotEmpty, true);

        return new(_state);
    }

    /// Invoke the OnEnqueue (depth) hook on its own. The protocol calls this under _syncRoot for the
    /// sync path so the non-atomic single-producer IncrementDepth doesn't race a concurrent async
    /// Enqueue's increment (which also runs under _syncRoot). Then it calls EnqueueSyncWithHandoff with
    /// invokeOnEnqueue:false so the count isn't doubled.
    internal void RegisterEnqueue() => _state.OnEnqueue?.Invoke();

    /// Synchronously enqueues a sync-mode flow and blocks until the executor processes it on the
    /// caller's thread, which drives the rendezvous and the body. No TP work item at any point.
    /// 1: take the handoff slot (FIFO-serializes concurrent sync callers) and gate async signals via
    /// HandoffActive. 2: publish the flow and claim the executor's parked wait under the wake lock,
    /// acking the slot only on a winning claim so a racing async wake can't snipe it; re-wait and
    /// retry if the executor was busy. The claim dispatches the executor's continuation inline here
    /// to pull the flow and process it. If async producers deferred items during the window, wake
    /// the executor on TP afterward, else zero TP.
    // invokeOnEnqueue=false when the caller already invoked the OnEnqueue (depth) hook under its own
    // producer serialization (the protocol does this under _syncRoot, since DepthState.IncrementDepth
    // is non-atomic single-producer and a concurrent async Enqueue would otherwise race it). The
    // exclusive nested caller has no concurrent async producer, so it keeps the default.
    public void EnqueueSyncWithHandoff(PgClientFlow flow, bool invokeOnEnqueue = true)
    {
        if (Volatile.Read(ref _state.IsCompleted))
            ThrowCompleted();

        // FIFO baton-passing for concurrent sync producers. Each waiter is an intrusive linked-list
        // node blocking on its own MRES. The running head (always at SyncHead) signals the next
        // when it finishes. HandoffActive stays true across the entire chain so async producers
        // defer signaling for the whole run.
        var entry = new SyncHandoffEntry(flow);
        bool isHead;
        lock (_state.SyncWaiterLock)
        {
            isHead = _state.SyncHead is null && !_state.HandoffActive;
            if (_state.SyncTail is null)
                _state.SyncHead = entry;
            else
                _state.SyncTail.Next = entry;
            _state.SyncTail = entry;
            if (isHead)
                // Full-fence open (Interlocked, not Volatile.Write), symmetric with the close-out's
                // clear. A release-only open sits in this thread's store buffer past a concurrent
                // Complete/Execute's HandoffActive read: that reader sees the window stale-closed, skips
                // its defer, and claims the wait this handoff is rendezvousing on - the executor resolves
                // completed or wakes on the wrong thread, stranding the sync caller.
                Interlocked.Exchange(ref _state.HandoffActive, true);
        }

        if (!isHead)
            entry.WakeMres.Wait();

        if (invokeOnEnqueue)
            _state.OnEnqueue?.Invoke();

        // Publish the slot, then claim the executor's parked wait inline so its continuation runs
        // on this thread and pulls the flow. The claim and the HandoffAcked store are one wake-lock
        // hold: acking only once we own the parked wait (_pending observed true under the lock means
        // the executor is parked, not mid-pull) is what stops a racing async wake from sniping the
        // slot. HandoffActive is set under SyncWaiterLock, not the wake lock, so an async Execute can
        // read it stale-false (no StoreLoad ordering across the two locks), pass its gate, and claim
        // the parked wait we expected. That is no longer fatal: if our claim loses, the executor
        // re-arms under HandoffActive (skip-queue, so it parks without taking the unacked slot);
        // WaitForSuspended blocks until that re-arm and we retry. The common idle case claims on the
        // first try with no WaitForSuspended round.
        var wakeSignal = _state.WakeSignal;
        Volatile.Write(ref _state.HandoffSlot, entry.Flow);
        while (true)
        {
            wakeSignal.AcquireWakeLock();
            if (wakeSignal.TryClaimLocked())
            {
                Volatile.Write(ref _state.HandoffAcked, true);
                wakeSignal.ReleaseWakeLock();
                wakeSignal.DispatchClaimed(runContinuationsAsynchronously: false);
                break;
            }
            wakeSignal.ReleaseWakeLock();
            // Executor busy: it drains to its park point and re-arms (reset of the suspension
            // observation by whatever claim made _pending false means this blocks rather than
            // spinning on a stale TRUE), then we retry the claim.
            wakeSignal.WaitForSuspended();
        }

        // Baton hand-off: pop self from head, peek next. If a next waiter exists, signal it.
        // Otherwise close out the chain.
        SyncHandoffEntry? next;
        bool postHandoffWakeNeeded = false;
        lock (_state.SyncWaiterLock)
        {
            _state.SyncHead = entry.Next;
            if (_state.SyncHead is null)
            {
                _state.SyncTail = null;
                // Full-fence clear (not Volatile.Write): the close-out half of the Dekker pairs with
                // Enqueue's flag store and Complete's IsCompleted store. A release-only clear lets both
                // sides observe stale values at once (producer defers against a stale-open window while
                // this close-out reads the stale-false flag), losing the deferred wake - a queued item
                // never drains, or a deferred completion never re-delivers, hanging drain.
                Interlocked.Exchange(ref _state.HandoffActive, false);
                // Deliver wakes deferred during the window: async enqueues and a deferred completion.
                // The executor's inline run can re-arm before this close-out clears HandoffActive, so
                // without this a completion arriving mid-window strands it.
                postHandoffWakeNeeded = Volatile.Read(ref _state.QueueNotEmpty)
                    || Volatile.Read(ref _state.IsCompleted);
            }
            next = _state.SyncHead;
        }

        if (next is not null)
            next.WakeMres.Set();
        else if (postHandoffWakeNeeded)
            _state.WakeSignal.Signal(runContinuationsAsynchronously: true);
    }

    internal sealed class SyncHandoffEntry(PgClientFlow flow)
    {
        public readonly PgClientFlow Flow = flow;
        public readonly ManualResetEventSlim WakeMres = new(false);
        public SyncHandoffEntry? Next;
    }

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

    public Enumerator GetAsyncEnumerator(Action? onEnqueue = null, CancellationToken cancellationToken = default)
    {
        _state.OnEnqueue = onEnqueue;
        return new(_state, cancellationToken);
    }

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
        public Action? OnEnqueue;
        // Drain gate. Fired once from WaitCore's completed-resolution when the executor's pull
        // resolves completed (WaitForNextAsync delivers false). Shutdown awaits it before draining
        // the residual so the drain is the sole consumer of the SPSC queue (a concurrent executor
        // dequeue would tear the read). Set before Complete.
        public TaskCompletionSource? DrainSignal;

        public PgClientFlow? HandoffSlot;
        public bool HandoffActive;
        // Gates MoveNextAsync from taking the handoff slot before the sync caller has issued
        // SetResult. Without it the executor could loop back, observe HandoffSlot, and take the
        // flow on its own thread, defeating the inline-on-caller's-thread guarantee.
        public bool HandoffAcked;
        public bool IsCompleted;

        public readonly Lock SyncWaiterLock = new();
        public SyncHandoffEntry? SyncHead;
        public SyncHandoffEntry? SyncTail;

        public State(PgClientProtocol protocol, PipelineScheduler scheduler)
        {
            FlushGate = new(protocol);
            WakeSignal = new(runContinuationsAsynchronously: true, scheduler, enableWaitForSuspended: true);
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
            // Deferred during a sync-handoff window: an ungated completion wake steals the rendezvous,
            // running the sync flow on the wrong thread. No liveness lost: the handoff's inline claim
            // wakes the executor and its next wait observes IsCompleted (the close-out re-delivers if
            // it raced).
            TryClaim(runContinuationsAsynchronously: true);
        }

        // Claim the executor's parked wait and dispatch its continuation, but only when no sync-handoff
        // window is active. The HandoffActive read and the claim are one wake-lock hold: read-then-claim
        // would let a stale gate verdict (taken just before the window opened) pair with a claim landing
        // after the producer acked, stealing the rendezvous so the sync flow runs on the wrong thread.
        // The acquire is also the full fence that publishes the caller's prior store (QueueNotEmpty /
        // IsCompleted) to the close-out's compensation read, which is why those stores need no fence of
        // their own. A claim lost to an open window is dropped on purpose: the close-out re-delivers.
        public void TryClaim(bool runContinuationsAsynchronously)
        {
            var wakeSignal = WakeSignal;
            wakeSignal.AcquireWakeLock();
            var claimed = !Volatile.Read(ref HandoffActive) && wakeSignal.TryClaimLocked();
            wakeSignal.ReleaseWakeLock();
            if (claimed)
                wakeSignal.DispatchClaimed(runContinuationsAsynchronously);
        }

        // Two narrow take helpers. The HandoffActive skip-queue rule belongs at the call site
        // (MoveNextAsync applies it to avoid hijacking the sync caller's thread. GetResult does
        // not, because at wake time the executor must take whatever the producer signalled).
        public bool TryTakeHandoff()
        {
            if (!Volatile.Read(ref HandoffAcked))
                return false;
            if (Volatile.Read(ref HandoffSlot) is { } handoff)
            {
                HandoffSlot = null;
                Volatile.Write(ref HandoffAcked, false);
                Current = handoff;
                return true;
            }
            return false;
        }

        public void EnqueueItem(PgClientFlow flow) => _storage.Enqueue(flow);

        // Consumer-side peek used by WaitCore's authoritative not-empty test.
        public bool HasItem() => _storage.TryPeek(out _);

        // Sole consumer once the executor has stopped (Shutdown's drain).
        public void DrainInert(Action<PgClientFlow> onInert) => _storage.DrainInert(onInert);

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

    /// <summary>
    /// Result of <see cref="Enqueue"/>. Calling <see cref="Execute"/> wakes the executor (or
    /// no-ops if a sync handoff is currently in progress, in which case the sync caller will wake
    /// the executor itself after the handoff completes).
    /// </summary>
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

        /// Synchronous pull: handoff slot first (HandoffAcked-gated so a between-items pull can't
        /// snipe a slot before its sync producer's inline claim), then the primary queue when no
        /// handoff window is active. Items route through State.Current.
        public bool TryGetNext([System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out PgClientFlow item)
        {
            // Completion suppresses queue dispatch: once completed, the primary queue is reclaimed by
            // WaitCore's completed-resolution as the sole consumer, so taking a queued item here would
            // race that reclaim. An acked sync-handoff slot is exempt - its rendezvous must still
            // resolve on the producer's thread during shutdown.
            if (Volatile.Read(ref _state.IsCompleted))
            {
                if (_state.TryTakeHandoff())
                {
                    item = _state.Current;
                    return true;
                }
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
            if (_state.TryTakeHandoff() || (!Volatile.Read(ref _state.HandoffActive) && _state.TryDequeue()))
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

            // The acked sync-handoff slot always wins: the sync producer has observed the
            // suspension and issued its inline claim, so the executor's continuation must retry to
            // take it on the producer's thread. This beats completion so a shutdown racing a live
            // rendezvous can't strand the sync caller.
            if (Volatile.Read(ref _state.HandoffAcked))
            {
                _state.FlushGate.Rearm();
                wakeSignal.ReleaseWakeLock();
                return WaitForNextAwaitable.Retry();
            }

            // Completion BEATS the primary queue: resolve completed even with items still queued
            // (the source reclaims its residual instead of draining it through the executor). Fire
            // DrainSignal so Shutdown drains the residual as the sole consumer. Deferred during a
            // handoff window - resolving out from under a waiting sync producer would strand its
            // rendezvous, so the close-out re-delivers once the window clears.
            if ((Volatile.Read(ref _state.IsCompleted) || _completionToken.IsCancellationRequested)
                && !Volatile.Read(ref _state.HandoffActive))
            {
                wakeSignal.ReleaseWakeLock();
                _state.DrainSignal?.TrySetResult();
                return WaitForNextAwaitable.Completed();
            }

            // Not completed - dispatch a queued item. The queue test PEEKS (consumer-side, SPSC-legal)
            // instead of reading QueueNotEmpty: under publish-then-flag a TRUE flag implies the item
            // is already peekable, and the flag can go STALE-TRUE (the producer's flag store landing
            // after TryDequeue's dequeue-to-empty clear), so the peek is the authoritative test.
            // Skip-queue while a handoff window is active so we don't hijack the sync caller's thread.
            if (!Volatile.Read(ref _state.HandoffActive) && _state.HasItem())
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
