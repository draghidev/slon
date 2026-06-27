using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Slon.Pg.Protocol.Flows;

// There are various cases where a flow should wait for caller action.
// In asynchronous flows we await the GateTask which will be triggered by the caller when progress is requested.
// We return a unique type so we won't clash with any other IValueTaskSource implementations, beyond that it is meaningless.
// For synchronous flows we surface the continuation of the flow, and where it left things, to make progress on the same thread.
// Each time the caller should process these new results we make the flow unwind the stack to get to the caller again.
// If it unwinds for any other reason the mres will not be set, so those bugs will be easily diagnosable.
struct FlowCallerInteractionCore<TResult>
{
    ManualResetEventSlim? _mres;
    Action? _continuation;
    // Wake-eligible continuation slot. Separate from _continuation because the latter
    // deliberately stays set after the caller takes it (HasContinuation acts as a "handoff
    // already done" marker). Re-queueing the stale _continuation on TP after the body has
    // already resumed crashes the process: that continuation points at a completed state
    // machine. _wakeContinuation has claim-and-clear semantics: set in OnCompleted,
    // Exchange-claimed by WaitForContinuation OR RequestWake. Verified: WakeProtocol.tla.
    Action? _wakeContinuation;
    bool _progressSignaled;
    // One-shot flag set by RequestWake; consumed by the OnCompleted post-store re-check.
    bool _wakeRequested;
    // The sticky close-latch: the canonical close exception for this flow's tenure, set out-of-band by
    // the protocol abort/stopping (CancelPendingWait) AND by the body's terminal close paths. The
    // consumer reads it on every Reset rearm to self-deliver the close to its just-armed generation,
    // so the live generation always has a completer regardless of which writer set the latch. Two
    // authorities (protocol abort, consumer Reset) agree without the abort ever targeting a version:
    // the abort only flips this monotone latch; the consumer delivers. First-writer-wins via CAS so a
    // concurrent abort + body-terminal cannot tear it. Cleared per tenure in Reset.
    Exception? _closeException;
    public Exception? CloseException => Volatile.Read(ref _closeException);
    // Set the latch, monotone (first writer wins). Returns the latched exception (this call's or the
    // prior winner's), never null.
    public Exception SetCloseLatch(Exception exception)
        => Interlocked.CompareExchange(ref _closeException, exception, null) ?? exception;

    // The async result/await channel. Encapsulated: callers drive it through the gate-intent methods below,
    // never the raw MRVTSC. The inline-vs-async continuation mode (runContinuationsAsynchronously) is the
    // load-bearing knob - inline = a sync disposer's park-before-open takeover resumes the body on its own
    // thread; async = an autonomous wake that must not run the body inline on the signaller's stack.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<TResult> _gate;

    public void Initialize()
    {
        _gate.CanCompleteConcurrently = true;
    }

    public ValueTask<TResult> GetGateTask(IValueTaskSource<TResult> source) => new(source, _gate.Version);

    // ---- Gate intent surface (the IVTS facade forwards its three interface methods to these) ----
    // Complete the gate. runContinuationsAsynchronously=false resumes a parked body inline on THIS thread
    // (the takeover); true schedules it (the autonomous wake).
    public void OpenGate(bool runContinuationsAsynchronously)
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

    public void CancelPendingWait(Exception exception)
    {
        // Latch first (monotone), then fault the gate, so a consumer observing the gate fault always
        // reads the latch set.
        var latched = SetCloseLatch(exception);
        _gate.TrySetException(latched, runContinuationsAsynchronously: true);
        _mres?.Set();
    }

    // Public: a sync flow exposes this as its handoff rendezvous primitive (GetHandoffMres) for the
    // wait-list-free source handoff. The turn-handshake is strictly sequential BEFORE any body<->caller
    // WaitForContinuation, so reusing the one MRES across both roles is safe.
    public ManualResetEventSlim GetMres()
    {
        if (_mres is not { } mres)
        {
            mres = new();
            if (Interlocked.CompareExchange(ref _mres, mres, null) is { } existing)
                mres = existing;
        }

        return mres;
    }

    public bool IsWaiting { get; private set; }
    public bool HasContinuation => _continuation is not null;

    // Park the caller until the body either registers a continuation for us to drive forward,
    // or signals progress via SignalProgress (because the body resumed on its own via async I/O
    // completion and produced a result or completion on the move-next task source). The two
    // wake conditions share one MRES. Returns the continuation to invoke, or null if the wake
    // came from a progress signal alone (the task source completed without a new continuation
    // being registered, e.g. body completion via SetResult(null)). Importantly, _continuation
    // is not cleared on wake. The body's SetContinuationAndUnblockWaiter overwrites it next
    // time, and HasContinuation acts as a "handoff already done" marker that ExecutePipelined
    // reads to skip a redundant handoff when it runs on TP while the caller is still inside
    // its first MoveNext.
    public Action? WaitForContinuation()
    {
        var mres = GetMres();

        IsWaiting = true;
        try
        {
            // Check progress BEFORE blocking. A SignalProgress that ran before this GetMres (e.g. a body
            // that faulted while _mres was still null) left the mres unset but _progressSignaled true; without
            // this pre-check mres.Wait would block forever waiting for a Set that already (no-op) happened.
            if (Volatile.Read(ref _progressSignaled))
            {
                _progressSignaled = false;
                return null;
            }
            mres.Wait();
            mres.Reset();
            if (Volatile.Read(ref _progressSignaled))
            {
                _progressSignaled = false;
                return null;
            }
            // Claim wake-eligibility so a concurrent RequestWake doesn't also queue the
            // continuation. _continuation itself stays for HasContinuation's marker role.
            Interlocked.Exchange(ref _wakeContinuation, null);
            return _continuation!;
        }
        finally
        {
            IsWaiting = false;
        }
    }

    // External wake request from consumer-side state changes (Enumerator.Dispose /
    // DisposeAsync). Spec: WakeProtocol.tla. Fires both wake mechanisms:
    //   - SignalProgress wakes a sync caller blocked in WaitForContinuation.
    //   - Interlocked.Exchange on _wakeContinuation queues the body's resume action to TP if
    //     present, covering async DisposeAsync on a sync-suspended body where the gate
    //     can't reach the wait.
    public void RequestWake()
    {
        Interlocked.Exchange(ref _wakeRequested, true);
        SignalProgress();
        var stored = Interlocked.Exchange(ref _wakeContinuation, null);
        if (stored is not null)
            ThreadPool.UnsafeQueueUserWorkItem(static s => ((Action)s!)(), (object)stored);
    }

    // Wake any WaitForContinuation that's parked without registering a continuation. Used by
    // the body's result-delivery and completion paths so a sync caller can wake even when the
    // body's progress came from an async-I/O continuation that ran on a TP thread without
    // routing through SetContinuationAndUnblockWaiter.
    public void SignalProgress()
    {
        // Volatile so WaitForContinuation's pre-block check sees it even when _mres is null (the Set no-ops).
        Volatile.Write(ref _progressSignaled, true);
        _mres?.Set();
    }

    public ContinuationCapturingAwaitable SetContinuationAndUnblockWaiter(FieldRef<FlowCallerInteractionCore<TResult>> fieldRef)
        => new(fieldRef);

    public void Reset()
    {
        _mres?.Reset();
        _continuation = null;
        _wakeContinuation = null;
        _progressSignaled = false;
        _wakeRequested = false;
        _closeException = null;
        _gate.Reset();
    }

    public readonly struct ContinuationCapturingAwaitable(FieldRef<FlowCallerInteractionCore<TResult>> fieldRef)
    {
        public Awaiter GetAwaiter() => new(fieldRef);

        public readonly struct Awaiter(FieldRef<FlowCallerInteractionCore<TResult>> fieldRef) : ICriticalNotifyCompletion
        {
            public bool IsCompleted => false;

            public void GetResult()
            {
                // Surface a pending cancellation set by CancelPendingWait. The gate source is
                // only completed when cancellation fires. In the normal sync-flow handoff path
                // it stays Pending and we just return.
                var gate = fieldRef.Invoke()._gate;
                if (gate.GetStatus(gate.Version) != System.Threading.Tasks.Sources.ValueTaskSourceStatus.Pending)
                    gate.GetResult(gate.Version);
            }

            public void OnCompleted(Action continuation)
            {
                ref var field = ref fieldRef.Invoke();
                if (!ReferenceEquals(field._continuation, continuation))
                    Volatile.Write(ref field._continuation, continuation);
                // _wakeContinuation is the wake-eligibility slot. Interlocked.Exchange acts
                // as a full memory barrier so the prior store to _continuation is ordered
                // before the _wakeRequested read below (StoreLoad).
                Interlocked.Exchange(ref field._wakeContinuation, continuation);
                field.GetMres().Set();
                // Race-safe re-check: if RequestWake ran before our store, it saw
                // _wakeContinuation = null and didn't queue. _wakeRequested stays set; self-
                // wake here. Exchange-claim ensures exactly-once delivery if RequestWake races
                // (one observes non-null, the other gets null).
                if (Volatile.Read(ref field._wakeRequested))
                {
                    var stored = Interlocked.Exchange(ref field._wakeContinuation, null);
                    if (stored is not null)
                        ThreadPool.UnsafeQueueUserWorkItem(static s => ((Action)s!)(), (object)stored);
                }
            }

            public void UnsafeOnCompleted(Action continuation) => OnCompleted(continuation);
        }
    }
}
