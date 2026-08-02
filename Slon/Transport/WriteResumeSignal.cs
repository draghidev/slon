using System.Threading.Tasks.Sources;

namespace Slon.Transport;

// Reusable completion source for a non-blocking write. The transport returns Pending() on
// WouldBlock; the external driver completes it after waiting for writability, or with the wait
// failure. Consumption resets it for the next cycle.
//
// Completion runs the continuation inline so the resumed coroutine retries the write on the
// thread that observed writability. Dispatching here would add a hop to every backpressure cycle.
sealed class WriteResumeSignal : IValueTaskSource
{
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _core;
    bool _pending;

    internal bool IsPending => Volatile.Read(ref _pending);

    public ValueTask Pending()
    {
        Volatile.Write(ref _pending, true);
        return new(this, _core.Version);
    }

    public void Signal(Exception? exception = null)
    {
        Volatile.Write(ref _pending, false);
        if (exception is null)
            _core.SetResult(true, runContinuationsAsynchronously: false);
        else
            _core.SetException(exception, runContinuationsAsynchronously: false);
    }

    void IValueTaskSource.GetResult(short token)
    {
        try { _core.GetResult(token); }
        finally
        {
            Volatile.Write(ref _pending, false);
            _core.Reset();
        }
    }
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}
