using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg.Protocol.Flows;

// Coordinates the execution body with its result consumer. Async consumers resume the body through a
// gate; sync consumers receive the body's continuation and run it on their own thread.
struct FlowCallerInteractionCore<TResult>
{
    ManualResetEventSlim? _waitEvent;
    Action? _handoffContinuation;
    // The handoff remains as a marker after consumption; the pending slot is claim-and-clear and is
    // the only continuation that may be scheduled or returned to a caller.
    Action? _pendingContinuation;
    int _progressSignaled;
    // One-shot flag set by WakeBody; consumed by the OnCompleted post-store re-check.
    bool _wakeRequested;
    // Cancellation may have to take ownership from a sync caller that will not return. Once set,
    // a claimed handoff runs on a dedicated driver instead of occupying a pool worker.
    bool _dedicatedWakeRequested;
    // First close wins for the current tenure. The consumer replays this durable state onto whichever
    // task-source generation it arms, so teardown never needs to target a generation directly.
    Exception? _closeException;
    public Exception? CloseException => Volatile.Read(ref _closeException);
    // Set the latch, monotone (first writer wins). Returns the latched exception (this call's or the
    // prior winner's), never null.
    public Exception SetCloseLatch(Exception exception)
        => Interlocked.CompareExchange(ref _closeException, exception, null) ?? exception;

    // Inline completion transfers the body to a synchronous caller; asynchronous completion preserves
    // autonomous execution without running the body on the signaller's stack.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<TResult> _gate;
    public void Initialize()
    {
        _gate.CanCompleteConcurrently = true;
    }

    public ValueTask<TResult> WaitForCaller(IValueTaskSource<TResult> source) => new(source, _gate.Version);

    // The IVTS facade forwards through this gate surface.
    public void ResumeBody(bool runContinuationsAsynchronously)
        => _gate.TrySetResult(default!, runContinuationsAsynchronously);
    public System.Threading.Tasks.Sources.ValueTaskSourceStatus GateStatus(short token) => _gate.GetStatus(token);
    public void OnGateCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _gate.OnCompleted(continuation, state, token, flags);
    // The consumer took the gate signal: read the result, then rearm for the next inter-result boundary.
    public TResult ConsumeGateResult(short token)
    {
        var result = _gate.GetResult(token);
        _gate.Reset();
        return result;
    }

    public void FaultBodyWait(Exception exception)
    {
        // Latch first (monotone), then fault the gate, so a consumer observing the gate fault always
        // reads the latch set.
        var latched = SetCloseLatch(exception);
        _gate.TrySetException(latched, runContinuationsAsynchronously: true);
        // The synchronous disposer may not have created its event yet. Publish a sticky progress
        // level so WaitForContinuation observes the close even when this Set would have been a no-op.
        SignalProgress();
    }

    // The source handoff completes before body/consumer rendezvous begins, so both may reuse this event.
    public ManualResetEventSlim GetWaitEvent()
    {
        if (_waitEvent is not { } waitEvent)
        {
            waitEvent = new();
            if (Interlocked.CompareExchange(ref _waitEvent, waitEvent, null) is { } existing)
                waitEvent = existing;
        }

        return waitEvent;
    }

    public bool IsWaiting { get; private set; }
    public bool HasHandoff => _handoffContinuation is not null;

    // Returns a continuation for the caller to run, or null when result/terminal progress produced the
    // wake. The durable handoff marker prevents a redundant first handoff.
    public Action? WaitForContinuation()
    {
        var waitEvent = GetWaitEvent();

        IsWaiting = true;
        try
        {
            // Check progress BEFORE blocking. A SignalProgress that ran before this GetWaitEvent (e.g. a body
            // that faulted while _waitEvent was still null) left the waitEvent unset but _progressSignaled true; without
            // this pre-check waitEvent.Wait would block forever waiting for a Set that already (no-op) happened.
            if (ConsumeProgress())
                return null;
            waitEvent.Wait();
            waitEvent.Reset();
            if (ConsumeProgress())
                return null;
            // Claim the continuation itself. _handoffContinuation is only a durable marker; returning
            // it after another driver claimed _pendingContinuation would execute the same pooled state
            // machine twice.
            return Interlocked.Exchange(ref _pendingContinuation, null);
        }
        finally
        {
            IsWaiting = false;
        }
    }

    // Dispose-pump continuation claim. A progress wake may win over a continuation published in the same
    // window; ordinary MoveNext must leave that continuation deferred because the result owns its turn,
    // while a disposing consumer has no result turn left and may claim it immediately.
    public Action? TryTakePendingContinuation()
        => Interlocked.Exchange(ref _pendingContinuation, null);

    // A rendezvous can publish a result and register the body's following continuation before the
    // caller wakes. The result owns this turn; put the continuation back so the next MoveNext drives it.
    public void DeferContinuation(Action continuation)
    {
        Interlocked.Exchange(ref _pendingContinuation, continuation);
        GetWaitEvent().Set();
    }

    // Wake a parked caller and claim any pending body continuation. Cancellation may use a dedicated
    // driver when the original synchronous caller will not return.
    public void WakeBody(bool useDedicatedDriver = false)
    {
        if (useDedicatedDriver)
            Volatile.Write(ref _dedicatedWakeRequested, true);
        Volatile.Write(ref _wakeRequested, true);
        SignalProgress();
        var stored = Interlocked.Exchange(ref _pendingContinuation, null);
        if (stored is not null)
            Dispatch(stored, Volatile.Read(ref _dedicatedWakeRequested));
    }

    static void Dispatch(Action continuation, bool useDedicatedDriver)
    {
        if (!useDedicatedDriver)
        {
            ThreadPool.UnsafeQueueUserWorkItem(static s => ((Action)s!)(), (object)continuation);
            return;
        }

        if (ExecutionContext.IsFlowSuppressed())
            StartDedicated(continuation);
        else
        {
            using (ExecutionContext.SuppressFlow())
                StartDedicated(continuation);
        }

        static void StartDedicated(Action continuation)
            => _ = Task.Factory.StartNew(static state => ((Action)state!)(), continuation,
                CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    // Wake any WaitForContinuation that's parked without registering a continuation. Used by
    // the body's result-delivery and completion paths so a sync caller can wake even when the
    // body's progress came from an async-I/O continuation that ran on a TP thread without
    // routing through YieldToCaller.
    public void SignalProgress()
    {
        // Atomic publication is load-bearing in both directions. Its full fence orders this level
        // before the following _waitEvent read: if that read misses a concurrently-installed event, the
        // waiter's fenced install and consume must observe the level instead. Atomic consume in
        // WaitForContinuation also cannot erase a newer publication with a trailing false store.
        Interlocked.Exchange(ref _progressSignaled, 1);
        _waitEvent?.Set();
    }

    bool ConsumeProgress() => Interlocked.Exchange(ref _progressSignaled, 0) != 0;

    public CallerHandoffAwaitable YieldToCaller(FieldRef<FlowCallerInteractionCore<TResult>> fieldRef)
        => new(fieldRef);

    public void Reset()
    {
        _waitEvent?.Reset();
        _handoffContinuation = null;
        _pendingContinuation = null;
        _progressSignaled = 0;
        _wakeRequested = false;
        _dedicatedWakeRequested = false;
        _closeException = null;
        _gate.Reset();
    }

    public readonly struct CallerHandoffAwaitable(FieldRef<FlowCallerInteractionCore<TResult>> fieldRef)
    {
        public Awaiter GetAwaiter() => new(fieldRef);

        public readonly struct Awaiter(FieldRef<FlowCallerInteractionCore<TResult>> fieldRef) : ICriticalNotifyCompletion
        {
            public bool IsCompleted => false;

            public void GetResult()
            {
                // Surface a pending cancellation set by FaultBodyWait. The gate source is
                // only completed when cancellation fires. In the normal sync-flow handoff path
                // it stays Pending and we just return.
                var gate = fieldRef.Invoke()._gate;
                if (gate.GetStatus(gate.Version) != System.Threading.Tasks.Sources.ValueTaskSourceStatus.Pending)
                    gate.GetResult(gate.Version);
            }

            public void OnCompleted(Action continuation)
            {
                ref var field = ref fieldRef.Invoke();
                if (!ReferenceEquals(field._handoffContinuation, continuation))
                    Volatile.Write(ref field._handoffContinuation, continuation);
                // _pendingContinuation is the wake-eligibility slot. Interlocked.Exchange acts
                // as a full memory barrier so the prior store to _handoffContinuation is ordered
                // before the _wakeRequested read below (StoreLoad).
                Interlocked.Exchange(ref field._pendingContinuation, continuation);
                field.GetWaitEvent().Set();
                // Race-safe re-check: if WakeBody ran before our store, it saw
                // _pendingContinuation = null and didn't queue. _wakeRequested stays set; self-
                // wake here. Exchange-claim ensures exactly-once delivery if WakeBody races
                // (one observes non-null, the other gets null).
                if (Volatile.Read(ref field._wakeRequested))
                {
                    var stored = Interlocked.Exchange(ref field._pendingContinuation, null);
                    if (stored is not null)
                        Dispatch(stored, Volatile.Read(ref field._dedicatedWakeRequested));
                }
            }

            public void UnsafeOnCompleted(Action continuation) => OnCompleted(continuation);
        }
    }
}
