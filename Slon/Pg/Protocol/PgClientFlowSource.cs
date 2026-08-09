using System.Diagnostics;
using System.Runtime.CompilerServices;
using Draghi.Pipelining;
using Draghi.Pipelining.Internal;

namespace Slon.Pg.Protocol;

/// Pipeline source for <see cref="PgClientFlow"/>. Async flows run from the executor; a sync head is
/// held for its caller, which claims the executor wait and drives that flow inline. Both modes share
/// one FIFO and the same source-wait protocol.
readonly struct PgClientFlowSource : IPipelineSource<PgClientFlow, PgClientFlowSource.Enumerator>
{
    readonly State _state;
    internal State SourceState => _state;

    PgClientFlowSource(State state) => _state = state;

    // control: the one Control every flow in this source is bound to (the protocol's FlowControl for the
    // outer source, the scope's inner control for a nested source). Stored so the source can pull a flow's
    // handoff MRES through ExecutionControl rather than off a bare flow ref.
    public static PgClientFlowSource Create(PgClientProtocol protocol, PgClientProtocol.Control control,
        PipelineScheduler? executionScheduler = null, int maxInFlight = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxInFlight);
        return new(new State(protocol, control, executionScheduler ?? PipelineScheduler.ThreadPool, maxInFlight));
    }

    internal bool HasCapacity => _state.HasCapacity;
    internal bool TryAcquireCapacity(out bool capacityOwned) => _state.TryAcquireCapacity(out capacityOwned);
    internal void ReleaseUnboundCapacity() => _state.ReleaseCapacity();
    internal bool ReleaseCapacity() => _state.ReleaseCapacity();

    /// Enqueues an async-mode flow. The caller dispatches via the returned <see cref="EnqueueResult"/>.
    /// During a sync-flow handoff, the item is queued but the dispatch is a no-op. The executor will
    /// pick it up after the handoff window closes. Throws InvalidOperationException if the source has
    /// been completed.
    public EnqueueResult Enqueue(PgClientFlow flow, bool inlineEligible = false,
        TimeSpan activationTimeout = default)
    {
        if (Volatile.Read(ref _state.IsCompleted))
            ThrowCompleted();

        inlineEligible &= _state.EnqueueItem(flow, activationTimeout);
        return new(_state, inlineEligible);
    }

    /// Publishes a sync flow at its FIFO position. The caller separately chooses when to take the
    /// blocking handoff.
    public PgClientFlow EnqueueSyncWaiter(PgClientFlow flow, TimeSpan activationTimeout = default)
    {
        if (Volatile.Read(ref _state.IsCompleted))
            ThrowCompleted();
        return _state.EnqueueSyncWaiter(flow, activationTimeout);
    }

    public bool WaitForExecutor(PgClientFlow flow) => _state.WaitForExecutor(flow);

    // Wake the executor after publishing a sync flow. Its consumer claims the held head later.
    internal void SignalExecutor() => _state.WakeDriver.Drive(runContinuationsAsynchronously: true);

    // Drain the inert head of the source: items enqueued but never picked up by the executor.
    // CompleteAsync only sees dispatched flows, so anything still in the SPSC queue needs separate
    // disposition - the caller's handler faults each via ExecutionControl.Release (future migration
    // rebinds them onto a new protocol here instead). Call only after the executor has stopped
    // pulling (Shutdown awaits DrainSignal first) so this is the sole consumer.
    public void DrainInertItems(Action<PgClientFlow> onInert)
    {
        _state.DrainInert(onInert);
    }

    // Arm before source completion is published. The executor fires this when its pull resolves
    // completed and it will no longer dequeue source items.
    public void SetDrainSignal(TaskCompletionSource drainSignal) => _state.DrainSignal = drainSignal;

    public Enumerator CreateEnumerator(CancellationToken cancellationToken = default)
    {
        return new(_state, cancellationToken);
    }

    /// Backlog: flows enqueued but not yet dispatched. With Pipeline.Depth (in-flight = dispatched -
    /// completed), Depth + Backlog is the total outstanding. Lock-free read, may be stale.
    public int Backlog => _state.Backlog;
    internal bool IsInlineDrive => _state.WakeDriver.IsInlineOneShot;
    internal BacklogEnumerator GetBacklogEnumerator() => _state.GetBacklogEnumerator();

    internal void OnActivationHeartbeat(TimeSpan period) => _state.OnActivationHeartbeat(period);

    internal struct BacklogEnumerator
    {
        readonly PgClientFlow? _held;
        SlotEscalatingQueue<PgClientFlow>.Enumerator _storage;
        bool _yieldHeld;

        internal BacklogEnumerator(PgClientFlow? held, ref SlotEscalatingQueue<PgClientFlow> storage)
        {
            _held = held;
            _storage = storage.GetEnumerator();
            _yieldHeld = _held is not null;
        }

        public PgClientFlow Current { get; private set; } = null!;

        public bool MoveNext()
        {
            if (_yieldHeld)
            {
                _yieldHeld = false;
                Current = _held!;
                return true;
            }
            if (_storage.MoveNext())
            {
                Current = _storage.Current;
                return true;
            }
            return false;
        }
    }

    static void ThrowCompleted() => throw new InvalidOperationException("The source has been completed.");

    internal sealed class State
    {
        readonly PgClientProtocol _protocol;
        bool _flushArmed = true;
        // Primary storage: inline slot fast path + lazy one-way SPSC escalation (SlotEscalatingQueue).
        // The sequential common case - one in-flight flow, or a nested exclusive scope's serial
        // subflows - stays on the slot with no SPSC allocation; a pipelining connection escalates on
        // first overlap. Same SPSC contract as the bare queue it replaces (single producer = Enqueue,
        // single consumer = the executor's pull), so it is a drop-in. Non-readonly: mutated by ref this.
        SlotEscalatingQueue<PgClientFlow> _storage;
        // The source wait and its serialized driver. The driver also coordinates sync takeover at an
        // idle boundary, keeping executor ownership out of the queue implementation below.
        public readonly SourceWakeEvent WakeEvent;
        public readonly PgFlowSourceDriver WakeDriver;
        // Fired from the executor's completed pull. Shutdown then becomes the sole source consumer
        // and may migrate/fault inert items without waiting for dispatched flows to drain.
        public TaskCompletionSource? DrainSignal;
        // Sync-handoff FIFO is just _storage (sync+async in submission order); the current sync head the
        // executor is holding for its caller is HeldSyncFlow. No separate wait-list: each parked caller
        // parks on its OWN flow's MRES (PgClientFlow.HandoffEvent), which the executor signals when it
        // holds that flow. The old intrusive-list-of-wait-nodes (and its lagging-link spin) is gone.
        // One-shot takeover. The head caller's inline claim re-enters the pump on the caller's thread:
        // _takeoverPending makes that pull dequeue the head sync flow (its own), then _takeoverActive
        // makes the NEXT pull fake-miss so the pump parks (hands back to TP) instead of draining the
        // following flow on the caller's thread.
        public bool TakeoverPending;
        public bool TakeoverActive;
        public bool IsCompleted;

        // Published at the event handoff boundary because producers cannot inspect the SPSC head.
        // Ordinary notifications must not claim a wait reserved for the held sync flow's caller.
        public bool SyncHeadReserved;

        // Set after an inline async turn consumes its one-item budget. The next pull parks so the wake
        // driver can transfer any accumulated work to the scheduler.
        public bool InlineHandBack;

        // The Control every flow in this source is bound to. Used to mint a flow's ExecutionControl so the
        // source pulls the handoff MRES through it rather than off a bare flow ref (HandoffEvent is
        // protected on PgClientFlow).
        readonly PgClientProtocol.Control _control;
        readonly int _maxInFlight;
        int _inFlight;

        internal BacklogEnumerator GetBacklogEnumerator() => new(HeldSyncFlow, ref _storage);

        internal void OnActivationHeartbeat(TimeSpan period)
        {
            var backlog = GetBacklogEnumerator();
            while (backlog.MoveNext())
                backlog.Current.GetExecutionControl(_control).OnActivationHeartbeat(period);
        }

        public State(PgClientProtocol protocol, PgClientProtocol.Control control, PipelineScheduler scheduler,
            int maxInFlight)
        {
            _protocol = protocol;
            _control = control;
            WakeEvent = new(runContinuationsAsynchronously: true, scheduler);
            WakeDriver = new(this, WakeEvent);
            _maxInFlight = maxInFlight;
        }

        internal bool HasCapacity
            => _maxInFlight is 0 || Volatile.Read(ref _inFlight) < _maxInFlight;

        internal bool TryAcquireCapacity(out bool capacityOwned)
        {
            // Zero is deliberately the allocation- and atomic-free path. A finite source owns the
            // assignment boundary: accepted operations occupy capacity while queued, dispatched, and
            // recovering; work rejected here remains outside this wire and can be placed elsewhere.
            if (_maxInFlight is 0)
            {
                capacityOwned = false;
                return true;
            }

            var count = Volatile.Read(ref _inFlight);
            while (count < _maxInFlight)
            {
                var observed = Interlocked.CompareExchange(ref _inFlight, count + 1, count);
                if (observed == count)
                {
                    capacityOwned = true;
                    return true;
                }
                count = observed;
            }
            capacityOwned = false;
            return false;
        }

        internal bool ReleaseCapacity()
        {
            if (_maxInFlight is 0)
                return false;
            var count = Interlocked.Decrement(ref _inFlight);
            Debug.Assert(count >= 0);
            return count == _maxInFlight - 1;
        }

        internal bool NeedsFlushArm
            => _protocol.UnflushedBytes >= ProtocolDataWriter.UnflushedBytesFlushThreshold;

        internal bool FlushArmed => Volatile.Read(ref _flushArmed);

        internal void ConsumeFlushArm() => Volatile.Write(ref _flushArmed, false);

        internal void RearmFlush() => Volatile.Write(ref _flushArmed, true);

        // Flush before parking so in-flight writes reach the server. Socket writes normally complete
        // inline; only backpressure returns a task for the source wait to join.
        internal ValueTask? FlushBeforePark()
        {
            if (_protocol.UnflushedBytes is 0)
                return null;
            var task = _protocol.FlushAsync(CancellationToken.None);
            if (!task.IsCompletedSuccessfully)
                return task;
            task.GetAwaiter().GetResult();
            return null;
        }

        internal void SignalHeldSyncFlow()
            => HeldSyncFlow?.GetExecutionControl(_control).HandoffEvent?.Set();

        // Register Complete as the completion-token callback. Done here (not in Enumerator) so Complete
        // can stay private: its single-writer safety depends on the CTS firing it at most once, so the
        // sole entry point is this registration.
        public void RegisterCompletion(CancellationToken completionToken)
            => completionToken.UnsafeRegister(static state => ((State)state!).Complete(), this);

        // Private and single-writer by construction: the only caller is the CompletionToken registration
        // above, which the CTS fires at most once however many threads race _cts.Cancel (external
        // CompleteAsync + the executor's terminal DisposeAsync).
        void Complete()
        {
            Volatile.Write(ref IsCompleted, true);
            // Wake the executor so its next wait resolves Completed. During completion TryClaim is allowed
            // to claim a sync-head park too (see below): if the executor is parked at a sync head whose
            // caller is about to bail, that park must be un-parked here or it strands (DrainSignal never
            // fires). Complete only un-parks the executor for sync callers. Each parked caller wakes when
            // its flow is drained inert. FailUnstarted republishes the handoff event
            // after Release publishes IsCompleted, then the caller re-reads the terminal level and bails.
            // No direct wait-list head wake - there is no wait-list.
            WakeDriver.Drive(runContinuationsAsynchronously: true);
        }

        // Publish the flow at its real FIFO position. Its handoff event is also its wait node.
        public PgClientFlow EnqueueSyncWaiter(PgClientFlow flow, TimeSpan activationTimeout)
        {
            flow.GetExecutionControl(_control).Start(this, activationTimeout, dispatchAsync: false);
            _storage.Enqueue(flow);
            return flow;
        }

        // Drive the executor to this flow's turn and take it over on the caller's thread.
        public bool WaitForExecutor(PgClientFlow flow)
        {
            var wakeDriver = WakeDriver;
            // Caller-handoff path only: the routing (TryQueueFlow / ExclusiveAccessFlow.Queue, gated on
            // NeedsSyncHandoff) sends autonomous sync flows (null MRES, no parked caller) down the async
            // dispatch path instead, so a flow that reaches here always carries its waiter MRES. Fail loud
            // rather than NRE if that invariant is ever bypassed.
            var mres = flow.GetExecutionControl(_control).HandoffEvent
                ?? throw new InvalidOperationException("WaitForExecutor reached with a null handoff MRES: an autonomous sync flow must route via async dispatch (NeedsSyncHandoff), not the caller-handoff park.");
            // Kick the executor so it pulls and drains earlier flows in FIFO order, dequeue-and-holding the
            // first sync head and parking. OnHandoffReady then signals the held flow's MRES. A no-op
            // if the executor is already running (it reaches our flow on its own). Through the latch so the
            // kick can't spin up a second runner alongside one already live.
            wakeDriver.Drive(runContinuationsAsynchronously: true);

            while (true)
            {
                var handoff = wakeDriver.TryClaimHandoff(flow, mres, out var claim);
                if (handoff is PgFlowSourceDriver.HandoffStatus.Claimed)
                {
                    claim.DispatchInline();
                    break;
                }
                if (handoff is PgFlowSourceDriver.HandoffStatus.Completed)
                    return false;

                // The held head may still sit behind async work. Re-drive after the locked recheck,
                // then park; a signal between these operations remains set in the MRES.
                wakeDriver.Drive(runContinuationsAsynchronously: true);
                mres.Wait();
            }

            // Close-out: kick the executor to advance to the next FIFO flow. It dequeues-and-holds the next
            // sync head and OnHandoffReady signals that caller's MRES. On completion the executor
            // resolves Completed instead of advancing, but each parked caller wakes when its flow drains
            // inert (its terminal SignalProgress sets its MRES), so no direct successor wake is needed here.
            // Advance to the next FIFO flow, coalescing with a driver that is still unwinding.
            wakeDriver.Drive(runContinuationsAsynchronously: true);
            return true;
        }

        // Async enqueue (the EnqueueResult / Execute path).
        public bool EnqueueItem(PgClientFlow flow, TimeSpan activationTimeout)
        {
            flow.GetExecutionControl(_control).Start(this, activationTimeout, dispatchAsync: true);
            // If no earlier source item or held sync head exists, a successful inline claim is guaranteed
            // to dispatch this producer's own item. A failed claim means another executor strand is live.
            var inlineEligible = HeldSyncFlow is null && _storage.Count == 0;
            _storage.Enqueue(flow);
            return inlineEligible;
        }
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

        // Dispatch the head on the executor only if it is an async flow; a sync head is dequeued and held
        // for its caller's takeover. Dequeue-then-check (one SPSC op) rather than peek-then-dequeue (two
        // ops, a per-item cost on the hot async path): checking the head and dequeuing separately is a
        // TOCTOU race - the queue can be empty at the check and a producer can enqueue a SYNC flow before
        // the dequeue, normal-dispatching it (misroute) and stranding its caller. We dequeue once; if the
        // dequeued flow is sync it was a mis-take, so hold it in HeldSyncFlow and fake-miss so its caller
        // takes it over. Reads IsAsyncAtDispatch (the stable routing snapshot captured at enqueue), not the
        // mutable IsAsync a body flips mid-execution.
        public PgClientFlow? HeldSyncFlow;
        public bool TryDispatchAsyncOrHoldSync([System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out PgClientFlow item)
        {
            if (HeldSyncFlow is not null)
            {
                item = null;
                return false;   // a sync flow is held, waiting for its caller: do not dispatch behind it
            }
            if (!_storage.TryDequeue(out item!))
                return false;   // empty
            if (item.IsAsyncAtDispatch)
                return true;    // async: dispatch on the executor
            HeldSyncFlow = item;   // sync: hold for its caller's takeover, fake-miss
            item = null;
            return false;
        }

    }

    /// Result of <see cref="Enqueue"/>. Calling <see cref="Execute"/> wakes the executor (or
    /// no-ops if a sync handoff is currently in progress, in which case the sync caller will wake
    /// the executor itself after the handoff completes).
    public readonly struct EnqueueResult
    {
        readonly State? _state;
        readonly bool _inlineEligible;
        internal EnqueueResult(State? state, bool inlineEligible)
        {
            _state = state;
            _inlineEligible = inlineEligible;
        }

        public void Execute(bool runContinuationsAsynchronously)
        {
            if (_state is null) return;
            _state.WakeDriver.Drive(runContinuationsAsynchronously || !_inlineEligible);
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
        /// flow it is not taking over is fake-missed so the executor parks for that caller; otherwise the
        /// primary queue.
        public bool TryGetNext([System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out PgClientFlow item)
        {
            if (_state.InlineHandBack)
            {
                item = null;
                return false;
            }
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
                return item is not null;
            }
            if (_state.TakeoverActive)
            {
                item = null;
                return false;
            }

            // Completion suppresses queue dispatch: once completed, NOTHING is dispatched or held for a
            // takeover. The whole queue drains inert; its owner may migrate placement-independent flows
            // or fault wire-affine ones. WaitCore resolves Completed and Shutdown's DrainInert is the sole
            // consumer, so taking a queued item here would race that reclaim. A flow already taken over
            // runs via the TakeoverPending branch above; that path predates completion.
            if (Volatile.Read(ref _state.IsCompleted))
            {
                item = null;
                return false;
            }

            // Arm gate: under the periodic-flush threshold each TryGetNext-consume requires a fresh
            // WaitForNextAsync round to fire the flush seam. WaitCore re-arms on Retry; we consume it
            // here on take so the next pull is gated again. Outside that, the fast path runs.
            var needsArm = _state.NeedsFlushArm;
            if (needsArm && !_state.FlushArmed)
            {
                item = null;
                return false;
            }
            // Dispatch an ASYNC head on the executor; a SYNC head is dequeued-and-held for its caller's
            // takeover (fake-miss here). One dequeue, check after - see TryDispatchAsyncOrHoldSync for why
            // a peek-then-dequeue would race a producer into a misroute.
            if (_state.TryDispatchAsyncOrHoldSync(out item))
            {
                if (needsArm)
                    _state.ConsumeFlushArm();
                if (_state.WakeDriver.IsInlineOneShot)
                    _state.InlineHandBack = true;
                return true;
            }
            return false;
        }

        /// Miss path. The common no-flush wait takes the thin signal shape. A flush is needed when
        /// in-flight flows have written queries the server hasn't seen (without it their read phase
        /// hangs), but the flush itself almost always completes inline (the socket send buffer has
        /// room), so we flush synchronously and fall through to the same thin signal. Only genuine
        /// write backpressure - flush not completing inline - rides the Task shape.
        public WaitForNextAwaitable WaitForNextAsync()
        {
            if (_state.FlushBeforePark() is { } flushTask)
                return WaitForNextAwaitable.FromTask(FlushThenWaitAsync(flushTask));
            return WaitCore();
        }

        WaitForNextAwaitable WaitCore()
        {
            var wakeSignal = _state.WakeEvent;
            using var wait = wakeSignal.BeginWait();

            // One-shot takeover hand-back: the sync caller's pull just fake-missed (TakeoverActive).
            // Reset it; unless completing (checked below, before any arm), arm so the pump parks here.
            // The caller's inline DispatchClaimed returns and the pump is back on TP. The caller's
            // close-out re-signal resumes it for any trailing work.
            var takeoverHandBack = _state.TakeoverActive;
            if (takeoverHandBack)
                _state.TakeoverActive = false;
            var inlineHandBack = _state.InlineHandBack;
            if (inlineHandBack)
                _state.InlineHandBack = false;

            // Completion beats queued work: shutdown drains the residual as the sole consumer. This
            // check must remain inside every BeginWait boundary. Complete either precedes it and prevents
            // the wait, or follows registration and claims that wait through the driver. The rule also
            // covers pulls resumed from trailing work rather than from the source event.
            if (Volatile.Read(ref _state.IsCompleted) || _completionToken.IsCancellationRequested)
            {
                _state.DrainSignal?.TrySetResult();
                return WaitForNextAwaitable.Completed;
            }

            // Not completing: park the hand-back. Skips the item-retry branch below on purpose - the
            // one-shot must park even with items queued (hand the pump back to TP rather than draining
            // a later flow on the sync caller's thread).
            if (takeoverHandBack || inlineHandBack)
                return wait.WaitAsync();

            // A dispatchable item is available - retry to consume it - UNLESS a sync flow is already held
            // for its caller's takeover, in which case we park here and let that caller take it (we must
            // not dispatch behind it). A sync head still in the queue gets dequeued-and-held on the
            // retry's next pull.
            if (_state.HeldSyncFlow is null && _state.HasItem())
            {
                // An item is available - arm the next TryGetNext so it can consume one (only one
                // while the flush-threshold gate holds, so a flush round lands between items).
                _state.RearmFlush();
                return WaitForNextAwaitable.Retry;
            }

            return wait.WaitAsync();
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
