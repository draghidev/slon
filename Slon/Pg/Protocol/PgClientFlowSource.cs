using System.Diagnostics;
using Draghi.Pipelining;
using Draghi.Pipelining.Internal;

// Composes Draghi.Pipelining.Internal SPSC primitives. The Experimental tag is deliberate. This
// is the protocol's idle-flush + sync-flow handoff seam.
#pragma warning disable DRAGHI001

namespace Slon.Pg.Protocol;

/// <summary>
/// Pipeline source for <see cref="PgClientFlow"/>. SPSC primary queue on the thin
/// TryGetNext/WaitForNextAsync pull seam (a <see cref="WakeSignal"/> wait, no value-task source),
/// plus a sync-flow handoff slot that lets a synchronous producer's thread drive the executor for
/// its own item without any TP dispatch in the rendezvous path.
/// </summary>
/// <remarks>
/// <para>
/// <b>Idle handoff</b>. The sync producer publishes its flow into <c>HandoffSlot</c> and sets
/// <c>HandoffActive</c>, which gates async producers' signals AND completion wakes (see
/// <see cref="State.Complete"/>) from racing the rendezvous. The producer then blocks in
/// <see cref="WakeSignal.WaitForSuspended"/> until the executor's wait is armed and registered,
/// acks the slot, and claims the wait inline (<see cref="WakeSignal.Signal(bool)"/> with inline
/// dispatch): the executor's continuation runs on the producer's thread, its TryGetNext retry
/// takes the Acked slot, and the pipeline processes the flow synchronously before
/// <see cref="EnqueueSyncWithHandoff"/> returns. No TP work item is enqueued at any point in the
/// rendezvous. Protocol verified in Draghi.Pipelining/verification/WaitProtocol.tla (incl. the
/// completion-steal hazard its gate fixes and witness configs for each mechanism).
/// </para>
/// <para>
/// Async producers that arrive during <c>HandoffActive</c> enqueue to the SPSC queue without
/// signalling. After the sync handoff completes, the sync caller wakes the executor on TP only if
/// items (or a deferred completion) accrued during the window, zero TP otherwise.
/// </para>
/// </remarks>
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
        _state.Queue.Enqueue(flow);
        Volatile.Write(ref _state.QueueNotEmpty, true);

        return new(_state);
    }

    /// <summary>
    /// Synchronously enqueues a sync-mode flow and blocks until the executor processes it on the
    /// caller's thread. The caller's thread drives the rendezvous and the pipeline body for this
    /// flow. No TP work item is enqueued at any point.
    /// </summary>
    /// <remarks>
    /// Three-step rendezvous:
    /// <list type="number">
    /// <item>Claim the handoff slot (serializes concurrent sync callers) and gate async signals via
    /// <c>HandoffActive</c>.</item>
    /// <item>Wait on the VTS's parked-MRES. The MRES is set whenever the executor's awaiter calls
    /// <see cref="IValueTaskSource{TResult}.OnCompleted"/>, which happens once the executor has
    /// drained existing primary items and reached its park point.</item>
    /// <item>Set the VTS result inline on this thread. The executor's continuation resumes here,
    /// pulls the flow from <c>HandoffSlot</c>, and the pipeline processes it synchronously.</item>
    /// </list>
    /// If async producers deferred items during the handoff window, the sync caller wakes the
    /// executor on TP after the handoff to drain them. Otherwise zero TP work items.
    /// </remarks>
    public void EnqueueSyncWithHandoff(PgClientFlow flow)
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
                Volatile.Write(ref _state.HandoffActive, true);
        }

        if (!isHead)
            entry.WakeMres.Wait();

        _state.OnEnqueue?.Invoke();

        // Publish to the handoff slot. The previous head (if any) re-suspended the executor and
        // the suspension observation is already set. If we're the original head and the executor
        // was busy we still wait for the natural suspension.
        Volatile.Write(ref _state.HandoffSlot, entry.Flow);
        _state.WakeSignal.WaitForSuspended();
        // Ack only AFTER WaitForSuspended returns. TryGetNext gates on this so the executor
        // can't snipe HandoffSlot during a between-items pull that doesn't suspend.
        Volatile.Write(ref _state.HandoffAcked, true);
        // Inline claim: guaranteed to win - async signals defer during HandoffActive
        // (EnqueueResult.Execute) and Complete's wake defers too (State.Complete), so nothing
        // else can consume the suspended wait between our observation and this claim
        // (WaitProtocol.tla, HandoffInline). The executor's continuation runs on this thread:
        // it retries TryGetNext, takes the Acked handoff slot, and the pipeline processes the
        // flow inline before this call returns.
        var claimed = _state.WakeSignal.Signal(runContinuationsAsynchronously: false);
        Debug.Assert(claimed, "Sync handoff lost its rendezvous: the suspended wait was claimed elsewhere.");

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
                Volatile.Write(ref _state.HandoffActive, false);
                // Deliver wakes deferred during the window: async enqueues (Execute no-ops while
                // HandoffActive) AND a deferred completion (State.Complete's gate). The latter
                // matters because the executor's inline run can re-arm BEFORE this close-out
                // clears HandoffActive (its completed-resolution defers while the window is
                // active), so without this wake a completion arriving mid-window strands it.
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

    public Enumerator GetAsyncEnumerator(Action? onEnqueue = null, CancellationToken cancellationToken = default)
    {
        _state.OnEnqueue = onEnqueue;
        return new(_state, cancellationToken);
    }

    static void ThrowCompleted() => throw new InvalidOperationException("The source has been completed.");

    internal sealed class State
    {
        public readonly PgClientProtocol Protocol;
        public readonly SingleProducerSingleConsumerQueue<PgClientFlow> Queue = new();
        // The wait-protocol WakeSignal with suspension observation enabled: the executor's waits
        // arm against it (thin path, no value-task source), and EnqueueSyncWithHandoff
        // rendezvouses on WaitForSuspended. Scheduler-routed async wakes come with it.
        public readonly WakeSignal WakeSignal;
        public bool QueueNotEmpty;
        public PgClientFlow Current = null!;
        public Action? OnEnqueue;

        public PgClientFlow? HandoffSlot;
        public bool HandoffActive;
        // Acked is what gates MoveNextAsync from taking the handoff slot before the sync caller
        // has actually issued SetResult. Without this, the executor (busy processing a prior
        // item) can finish, loop back, observe HandoffSlot, and take the flow on its own
        // (non-caller) thread, defeating the inline-on-caller's-thread guarantee and leaving
        // the sync caller stranded waiting for a park that's been consumed by the wrong code
        // path.
        public bool HandoffAcked;
        public bool IsCompleted;

        public readonly Lock SyncWaiterLock = new();
        public SyncHandoffEntry? SyncHead;
        public SyncHandoffEntry? SyncTail;

        public State(PgClientProtocol protocol, PipelineScheduler scheduler)
        {
            Protocol = protocol;
            WakeSignal = new(runContinuationsAsynchronously: true, scheduler, enableWaitForSuspended: true);
        }

        public void Complete()
        {
            Volatile.Write(ref IsCompleted, true);
            // Deferred during a sync-handoff window: an ungated completion wake here steals the
            // rendezvous - the executor wakes on a TP thread, takes the Acked handoff slot, and
            // runs the sync flow on the WRONG thread while EnqueueSyncWithHandoff returns
            // mid-flight (WaitProtocol.tla exhibited the steal; this gate is its verified fix).
            // No liveness is lost: the handoff's inline claim wakes the executor, and its next
            // wait observes IsCompleted (completed-resolution also defers while HandoffActive,
            // see WaitCore, so a window opening right after this read still resolves correctly).
            if (!Volatile.Read(ref HandoffActive))
                WakeSignal.Signal(runContinuationsAsynchronously: true);
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

        public bool TryDequeue()
        {
            if (Queue.TryDequeue(out Current!))
            {
                if (!Queue.TryPeek(out _))
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
            if (Volatile.Read(ref _state.HandoffActive)) return;
            _state.WakeSignal.Signal(runContinuationsAsynchronously);
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
            _completionToken.UnsafeRegister(static state => ((State)state!).Complete(), _state);
        }

        public CancellationToken CompletionToken => _completionToken;

        public void Complete() => _cts.Cancel();

        /// <summary>Synchronous pull: handoff slot first (HandoffAcked-gated, so a between-items
        /// pull can't snipe a slot whose sync producer hasn't issued its inline claim yet), then
        /// the primary queue when no handoff window is active (don't hijack the sync caller's
        /// thread). Items route through <see cref="State.Current"/> because the take helpers
        /// deliver there.</summary>
        public bool TryGetNext([System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out PgClientFlow item)
        {
            if (_state.TryTakeHandoff() || (!Volatile.Read(ref _state.HandoffActive) && _state.TryDequeue()))
            {
                item = _state.Current;
                return true;
            }
            item = null;
            return false;
        }

        /// <summary>Miss path. The common no-flush wait takes the thin signal shape (lock,
        /// peek-style re-check, arm with the lock held through registration). Flush-before-wait
        /// is intrinsically async - in-flight flows have written queries the server hasn't
        /// received yet; without the flush their read phase waits forever and drain hangs - so
        /// that case rides the Task shape (it implies IO anyway).</summary>
        public WaitForNextAwaitable WaitForNextAsync()
        {
            if (_state.Protocol.UnflushedBytes is not 0)
                return WaitForNextAwaitable.FromTask(FlushThenWaitAsync());
            return WaitCore();
        }

        WaitForNextAwaitable WaitCore()
        {
            var wakeSignal = _state.WakeSignal;
            wakeSignal.AcquireWakeLock();

            // Availability re-check under the lock, peek-style: consumption stays in TryGetNext.
            if (Volatile.Read(ref _state.HandoffAcked)
                || (!Volatile.Read(ref _state.HandoffActive) && Volatile.Read(ref _state.QueueNotEmpty)))
            {
                wakeSignal.ReleaseWakeLock();
                return WaitForNextAwaitable.Retry();
            }

            // Completed-resolution DEFERS during a handoff window: resolving out from under a
            // waiting sync producer would strand its rendezvous (WaitProtocol.tla,
            // WaitRecheckCompleted's guard). Arm instead - the producer's inline claim drives
            // its flow, and the next wait observes completion (re-delivered by the handoff
            // chain close-out when it raced the window).
            if ((Volatile.Read(ref _state.IsCompleted) || _completionToken.IsCancellationRequested)
                && !Volatile.Read(ref _state.HandoffActive))
            {
                wakeSignal.ReleaseWakeLock();
                return WaitForNextAwaitable.Completed();
            }

            return wakeSignal.Arm();
        }

        async ValueTask<bool> FlushThenWaitAsync()
        {
            // CancellationToken.None on purpose: in-flight flows have written bytes that must
            // reach the wire so their read phase can drain. The writer's own _cts (linked to
            // AbortToken) is the correct cancellation gate for transport-level abort.
            await _state.Protocol.FlushAsync(CancellationToken.None).ConfigureAwait(false);
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
