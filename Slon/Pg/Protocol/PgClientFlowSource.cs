using System.Threading.Tasks.Sources;
using Draghi.Pipelining;
using Draghi.Pipelining.Internal;

// Composes Draghi.Pipelining.Internal SPSC primitives. The Experimental tag is deliberate. This
// is the protocol's idle-flush + sync-flow handoff seam.
#pragma warning disable DRAGHI001

namespace Slon.Pg.Protocol;

/// <summary>
/// Pipeline source for <see cref="PgClientFlow"/>. SPSC primary queue with a custom
/// <see cref="IValueTaskSource{TResult}"/> at the rendezvous point, plus a sync-flow handoff slot
/// that lets a synchronous producer's thread drive the executor for its own item without any TP
/// dispatch in the rendezvous path.
/// </summary>
/// <remarks>
/// <para>
/// <b>Idle handoff</b>. The sync producer publishes its flow into <c>HandoffSlot</c> and sets
/// <c>HandoffActive</c>, which gates async producers from racing the VTS. The producer then waits
/// on an MRES that the VTS's <see cref="IValueTaskSource{TResult}.OnCompleted"/> handler sets when
/// the executor parks. Once parked, the producer's <see cref="ResettableValueTaskSource.SetResult"/>
/// dispatches the executor's continuation inline on the producer's thread. The executor's resume
/// pulls <c>HandoffSlot</c> via <see cref="ResettableValueTaskSource.GetResult"/>, returns it, and
/// the pipeline processes the flow synchronously on the caller's thread. No TP work item is enqueued
/// at any point in the rendezvous.
/// </para>
/// <para>
/// Async producers that arrive during <c>HandoffActive</c> enqueue to the SPSC queue without
/// signalling. After the sync handoff completes, the sync caller wakes the executor on TP only if
/// items were deferred during the window, zero TP otherwise.
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

        // Publish to the handoff slot. The previous head (if any) re-parked the executor and the
        // ParkedMres is already set. If we're the original head and the executor was busy we still
        // wait for the natural park.
        Volatile.Write(ref _state.HandoffSlot, entry.Flow);
        _state.Vts.WaitForParked();
        // Ack only AFTER WaitForParked returns. MoveNextAsync gates on this so the executor
        // can't snipe HandoffSlot during a between-items MoveNextAsync call that doesn't park.
        Volatile.Write(ref _state.HandoffAcked, true);
        _state.Vts.SetResult(runContinuationsAsynchronously: false);
        // SetResult invoked the executor's continuation on this thread. The executor's GetResult
        // pulled HandoffSlot, returned the flow, the pipeline processed it inline, and called
        // MoveNextAsync again, which re-parked.

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
                postHandoffWakeNeeded = Volatile.Read(ref _state.QueueNotEmpty);
            }
            next = _state.SyncHead;
        }

        if (next is not null)
            next.WakeMres.Set();
        else if (postHandoffWakeNeeded)
            _state.Vts.SetResult(runContinuationsAsynchronously: true);
    }

    internal sealed class SyncHandoffEntry(PgClientFlow flow)
    {
        public readonly PgClientFlow Flow = flow;
        public readonly ManualResetEventSlim WakeMres = new(false);
        public SyncHandoffEntry? Next;
    }

    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken)
    {
        if (cancellationToken.CanBeCanceled)
            cancellationToken.UnsafeRegister(static state => ((State)state!).Complete(), _state);
        return new(_state, cancellationToken);
    }

    public void AttachDepthHook(Action onEnqueue) => _state.OnEnqueue = onEnqueue;

    static void ThrowCompleted() => throw new InvalidOperationException("The source has been completed.");

    internal sealed class State
    {
        public readonly PgClientProtocol Protocol;
        public readonly SingleProducerSingleConsumerQueue<PgClientFlow> Queue = new();
        public readonly ResettableValueTaskSource Vts;
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
            Vts = new(this);
        }

        public void Complete()
        {
            Volatile.Write(ref IsCompleted, true);
            Vts.SetResult(runContinuationsAsynchronously: true);
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
            _state.Vts.SetResult(runContinuationsAsynchronously);
        }
    }

    public struct Enumerator : IAsyncEnumerator<PgClientFlow>
    {
        readonly State _state;
        readonly CancellationToken _cancellationToken;

        internal Enumerator(State state, CancellationToken cancellationToken)
        {
            _state = state;
            _cancellationToken = cancellationToken;
        }

        public PgClientFlow Current => _state.Current;

        public ValueTask<bool> MoveNextAsync()
        {
            if (TryGetReady(out var result))
                return new(result);
            if (_state.Protocol.UnflushedBytes is not 0)
                return FlushThenPark();
            return Park();
        }

        bool TryGetReady(out bool result)
        {
            if (_state.TryTakeHandoff() || (!Volatile.Read(ref _state.HandoffActive) && _state.TryDequeue()))
            {
                result = true;
                return true;
            }
            if (Volatile.Read(ref _state.IsCompleted) || _cancellationToken.IsCancellationRequested)
            {
                result = false;
                return true;
            }
            result = default;
            return false;
        }

        ValueTask<bool> Park()
        {
            _state.Vts.Reset();
            // Race re-check after reset, before publishing the awaiter.
            if (TryGetReady(out var result))
                return new(result);
            return new(_state.Vts, _state.Vts.Version);
        }

        async ValueTask<bool> FlushThenPark()
        {
            await _state.Protocol.FlushAsync(_cancellationToken).ConfigureAwait(false);
            return await Park().ConfigureAwait(false);
        }

        public ValueTask DisposeAsync()
        {
            _state.Complete();
            return default;
        }
    }

    /// <summary>
    /// Resettable <see cref="IValueTaskSource{TResult}"/> that doubles as the parking primitive
    /// for the executor and the rendezvous primitive for sync handoff producers.
    /// </summary>
    /// <remarks>
    /// <see cref="OnCompleted"/> sets <see cref="_parkedMres"/>. Sync producers
    /// <see cref="WaitForParked"/> for it. <see cref="SetResult"/> clears the MRES before
    /// dispatching the continuation so the parked state stays accurate across resume.
    /// <see cref="GetResult"/> resolves <see cref="State.Current"/> by pulling
    /// <see cref="State.HandoffSlot"/> or dequeueing from the primary queue.
    /// </remarks>
    internal sealed class ResettableValueTaskSource : IValueTaskSource<bool>
    {
        readonly State _state;
        readonly ManualResetEventSlim _parkedMres = new(false);
        ManualResetValueTaskSourceCore<bool> _core;

        public ResettableValueTaskSource(State state) => _state = state;

        public short Version => _core.Version;

        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        public bool GetResult(short token)
        {
            _core.GetResult(token);
            // GetResult fires from a SetResult that promised either an item or completion. Don't
            // apply MoveNextAsync's HandoffActive skip-queue rule here. At wake time the executor
            // must take whatever the producer made visible, on whichever thread is dispatching.
            if (_state.TryTakeHandoff() || _state.TryDequeue())
                return true;
            return !Volatile.Read(ref _state.IsCompleted);
        }

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _core.OnCompleted(continuation, state, token, flags);
            _parkedMres.Set();
        }

        public void Reset()
        {
            _parkedMres.Reset();
            _core.Reset();
        }

        public void SetResult(bool runContinuationsAsynchronously)
        {
            // Continuation is about to run, executor is no longer parked.
            _parkedMres.Reset();
            _core.RunContinuationsAsynchronously = runContinuationsAsynchronously;
            // SetResult is idempotent at our use site: the parked executor may have raced an
            // earlier SetResult (e.g. completion). Guard via GetStatus.
            if (_core.GetStatus(_core.Version) == ValueTaskSourceStatus.Pending)
                _core.SetResult(true);
        }

        public void WaitForParked() => _parkedMres.Wait();
    }
}
