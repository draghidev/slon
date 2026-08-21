using System.Threading.Tasks.Sources;
using Slon.Runtime;

namespace Slon.Transport;

// Reusable completion source for an operation that must pause until its external driver can
// resume it, or report the wait failure. Consumption resets it for the next cycle.
//
// Completion runs the continuation inline so the resumed coroutine retries the write on the
// thread that observed writability. Dispatching here would add a hop to every backpressure cycle.
sealed class ResumeSignal : IValueTaskSource
{
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _core;
    bool _pending;
    Deadline? _pendingDeadline;

    internal bool IsPending => Volatile.Read(ref _pending);
    internal static Deadline? CreateDeadline(TimeSpan timeout)
        => timeout == default || timeout == Timeout.InfiniteTimeSpan
            ? null
            : new Deadline(timeout);
    internal TimeSpan GetRemainingTimeout()
        => _pendingDeadline?.GetRemaining() ?? Timeout.InfiniteTimeSpan;

    public ValueTask Pending(Deadline? deadline = null)
    {
        _pendingDeadline = deadline;
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
            _pendingDeadline = null;
            _core.Reset();
        }
    }
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}
