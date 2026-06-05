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

    public Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<TResult> GateTaskSource;

    public void Initialize()
    {
        GateTaskSource.CanCompleteConcurrently = true;
    }

    public ValueTask<TResult> GetGateTask(IValueTaskSource<TResult> source) => new(source, GateTaskSource.Version);

    public void CancelPendingWait(OperationCanceledException exception)
    {
        GateTaskSource.TrySetException(exception, runContinuationsAsynchronously: true);
        _mres?.Set();
    }

    ManualResetEventSlim GetMres()
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
    // wake conditions share one MRES. Returns the continuation to invoke, or null if we were
    // woken only by the progress signal.
    public Action? WaitForContinuation()
    {
        var mres = GetMres();

        IsWaiting = true;
        try
        {
            mres.Wait();
            mres.Reset();
            var continuation = _continuation;
            _continuation = null;
            return continuation;
        }
        finally
        {
            IsWaiting = false;
        }
    }

    // Wake any WaitForContinuation that's parked without registering a continuation. Used by
    // the body's result-delivery and completion paths so a sync caller can wake even when the
    // body's progress came from an async-I/O continuation that ran on a TP thread without
    // routing through SetContinuationAndUnblockWaiter.
    public void SignalProgress() => _mres?.Set();

    public ContinuationCapturingAwaitable SetContinuationAndUnblockWaiter(FieldRef<FlowCallerInteractionCore<TResult>> fieldRef)
        => new(fieldRef);

    public void Reset()
    {
        _mres?.Reset();
        _continuation = null;
        GateTaskSource.Reset();
    }

    public readonly struct ContinuationCapturingAwaitable(FieldRef<FlowCallerInteractionCore<TResult>> fieldRef)
    {
        public Awaiter GetAwaiter() => new(fieldRef);

        public readonly struct Awaiter(FieldRef<FlowCallerInteractionCore<TResult>> fieldRef) : ICriticalNotifyCompletion
        {
            public bool IsCompleted => false;

            public void GetResult()
            {
                // Make sure cancellations get picked up by synchronous flows as well.
                var gateTaskSource = fieldRef.Invoke().GateTaskSource;
                gateTaskSource.GetResult(gateTaskSource.Version);
            }

            public void OnCompleted(Action continuation)
            {
                ref var field = ref fieldRef.Invoke();
                if (!ReferenceEquals(field._continuation, continuation))
                    Volatile.Write(ref field._continuation, continuation);
                field.GetMres().Set();
            }

            public void UnsafeOnCompleted(Action continuation) => OnCompleted(continuation);
        }
    }
}
