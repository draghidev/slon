using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;

namespace Slon.Threading.Tasks.Sources;

readonly ref struct ContinuationDispatcher(ref Action<object?>? continuation, ref object? continuationState, ref object? capturedContext)
{
    static readonly Action<object?> CompletionSentinelAction = CompletionSentinel;

    [DoesNotReturn]
    static void ThrowInvalidOperationException() => throw new InvalidOperationException();

    static void CompletionSentinel(object? _) // named method to aid debugging
    {
        Debug.Fail("The sentinel delegate should never be invoked.");
        ThrowInvalidOperationException();
    }

    /// <summary>
    /// The callback to invoke when the operation completes if <see cref="OnCompleted"/> was called before the operation completed,
    /// or <see cref="ContinuationDispatcher.CompletionSentinelAction"/> if the operation completed before a callback was supplied,
    /// or null if a callback hasn't yet been provided and the operation hasn't yet completed.
    /// </summary>
    readonly ref Action<object?>? _continuation = ref continuation;

    /// <summary>State to pass to <see cref="_continuation"/>.</summary>
    readonly ref object? _continuationState = ref continuationState;

    /// <summary>
    /// Null if no special context was found.
    /// ExecutionContext if one was captured due to needing to be flowed.
    /// A scheduler (TaskScheduler or SynchronizationContext) if one was captured and needs to be used for callback scheduling.
    /// Or a CapturedContext if there's both an ExecutionContext and a scheduler.
    /// The most common and the fast path case to optimize for is null.
    /// </summary>
    readonly ref object? _capturedContext = ref capturedContext;

    /// <summary>Signals that the operation has completed. Invoked after the result or error has been set.</summary>
    public void SignalCompletion(bool runContinuationsAsynchronously)
    {
        // Always insert a read barrier for _continuation so the later read of _continuationState is ordered correctly.
        var continuation =
            Volatile.Read(ref _continuation) ??
            Interlocked.CompareExchange(ref _continuation, CompletionSentinelAction, null);

        // If null was returned we have transitioned to completed, we have nothing to invoke.
        if (continuation is null)
            return;

        if (_capturedContext is not { } context)
        {
            if (runContinuationsAsynchronously)
            {
                ThreadPool.UnsafeQueueUserWorkItem(continuation, _continuationState, preferLocal: true);
            }
            else
            {
                continuation(_continuationState);
            }
        }
        else if (context is ExecutionContext or CapturedSchedulerAndExecutionContext)
        {
            InvokeWithContext(context, continuation, _continuationState, !runContinuationsAsynchronously);
        }
        else
        {
            Debug.Assert(context is TaskScheduler or SynchronizationContext, $"group is {context}");
            ScheduleWithContext(context, continuation, _continuationState);
        }
    }

    /// <summary>Schedules the continuation action for this operation.</summary>
    /// <param name="continuation">The continuation to invoke when the operation has completed.</param>
    /// <param name="state">The state object to pass to <paramref name="continuation"/> when it's invoked.</param>
    /// <param name="flags">The flags describing the behavior of the continuation.</param>
    public void OnCompleted(Action<object?> continuation, object? state, ValueTaskSourceOnCompletedFlags flags)
    {
        ArgumentNullException.ThrowIfNull(continuation);

        if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0)
        {
            _capturedContext = ExecutionContext.Capture();
        }

        if ((flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) != 0)
        {
            if (SynchronizationContext.Current is { } sc && sc.GetType() != typeof(SynchronizationContext))
            {
                _capturedContext = _capturedContext is null ?
                    sc :
                    new CapturedSchedulerAndExecutionContext(sc, (ExecutionContext)_capturedContext);
            }
            else
            {
                var ts = TaskScheduler.Current;
                if (ts != TaskScheduler.Default)
                {
                    _capturedContext = _capturedContext is null ?
                        ts :
                        new CapturedSchedulerAndExecutionContext(ts, (ExecutionContext)_capturedContext);
                }
            }
        }

        // We need to set the continuation state before we swap in the delegate, so that
        // if there's a race between this and SetResult/Exception and SetResult/Exception
        // sees the _continuation as non-null, it'll be able to invoke it with the state
        // stored here.  However, this also means that if this is used incorrectly (e.g.
        // awaited twice concurrently), _continuationState might get erroneously overwritten.
        // To minimize the chances of that, we check preemptively whether _continuation
        // is already set to something other than the completion sentinel.
        object? storedContinuation = _continuation;
        if (storedContinuation is null)
        {
            _continuationState = state;
            storedContinuation = Interlocked.CompareExchange(ref _continuation, continuation, null);
            if (storedContinuation is null)
            {
                // Operation hadn't already completed, so we're done. The continuation will be
                // invoked when SetResult/Exception is called at some later point.
                return;
            }
        }

        // Operation already completed, so we need to queue the supplied callback.
        // At this point the storedContinuation should be the sentinal. If it's not, the instance was misused.
        Debug.Assert(storedContinuation is not null, $"{nameof(storedContinuation)} is null");
        if (!ReferenceEquals(storedContinuation, CompletionSentinelAction))
        {
            ThrowInvalidOperationException();
        }

        var capturedContext = _capturedContext;
        switch (capturedContext)
        {
            case null:
                ThreadPool.UnsafeQueueUserWorkItem(continuation, state, preferLocal: true);
                break;

            case ExecutionContext:
                ThreadPool.QueueUserWorkItem(continuation, state, preferLocal: true);
                break;

            default:
                ScheduleWithContext(capturedContext, continuation, state);
                break;
        }
    }

    static void ScheduleWithContext(object capturedContext, Action<object?> continuation, object? state)
    {
        Debug.Assert(
            capturedContext is SynchronizationContext or TaskScheduler or CapturedSchedulerAndExecutionContext,
            $"{nameof(capturedContext)} is {capturedContext}");

        switch (capturedContext)
        {
            case SynchronizationContext sc:
                ScheduleSynchronizationContext(sc, continuation, state);
                break;

            case TaskScheduler ts:
                ScheduleTaskScheduler(ts, continuation, state);
                break;

            default:
                var cc = (CapturedSchedulerAndExecutionContext)capturedContext;
                if (cc._scheduler is SynchronizationContext ccsc)
                {
                    ScheduleSynchronizationContext(ccsc, continuation, state);
                }
                else
                {
                    Debug.Assert(cc._scheduler is TaskScheduler, $"{nameof(cc._scheduler)} is {cc._scheduler}");
                    ScheduleTaskScheduler((TaskScheduler)cc._scheduler, continuation, state);
                }
                break;
        }

        static void ScheduleSynchronizationContext(SynchronizationContext sc, Action<object?> continuation, object? state) =>
            sc.Post(continuation.Invoke, state);

        static void ScheduleTaskScheduler(TaskScheduler scheduler, Action<object?> continuation, object? state) =>
            Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler);
    }

    static void InvokeWithContext(object capturedContext, Action<object?> continuation, object? continuationState, bool executeSynchronously)
    {
        // This is in a helper as the error handling causes the generated asm
        // for the surrounding code to become less efficient (stack spills etc)
        // and it is an uncommon path.
        Debug.Assert(continuation is not null, $"{nameof(continuation)} is null");
        Debug.Assert(capturedContext is ExecutionContext or CapturedSchedulerAndExecutionContext, $"{nameof(capturedContext)} is {capturedContext}");

        // Capture the current EC.  We'll switch over to the target EC and then restore back to this one.
        var currentContext = ExecutionContext.Capture();

        if (capturedContext is ExecutionContext ec)
        {
            ExecutionContext.Restore(ec); // Restore the captured ExecutionContext before executing anything.
            if (!executeSynchronously)
            {
                try
                {
                    ThreadPool.QueueUserWorkItem(continuation, continuationState, preferLocal: true);
                }
                finally
                {
                    if (currentContext is not null)
                        ExecutionContext.Restore(currentContext); // Restore the current ExecutionContext.
                }
            }
            else
            {
                // Running inline may throw. Capture the edi if it does as we changed the ExecutionContext,
                // so need to restore it back before propagating the throw.
                ExceptionDispatchInfo? edi = null;
                var syncContext = SynchronizationContext.Current;
                try
                {
                    continuation(continuationState);
                }
                catch (Exception ex)
                {
                    // Note: we have a "catch" rather than a "finally" because we want
                    // to stop the first pass of EH here.  That way we can restore the previous
                    // group before any of our callers' EH filters run.
                    edi = ExceptionDispatchInfo.Capture(ex);
                }
                finally
                {
                    // Set sync group back to what it was prior to coming in.
                    // Then restore the current ExecutionContext.
                    SynchronizationContext.SetSynchronizationContext(syncContext);
                    if (currentContext is not null)
                        ExecutionContext.Restore(currentContext);
                }

                // Now rethrow the exception, if there is one.
                edi?.Throw();
            }
        }
        else
        {
            var cc = (CapturedSchedulerAndExecutionContext)capturedContext;
            ExecutionContext.Restore(cc._executionContext); // Restore the captured ExecutionContext before executing anything.
            try
            {
                ScheduleWithContext(capturedContext, continuation, continuationState);
            }
            finally
            {
                if (currentContext is not null)
                    ExecutionContext.Restore(currentContext); // Restore the current ExecutionContext.
            }
        }
    }

    /// <summary>A tuple of both a non-null scheduler and a non-null ExecutionContext.</summary>
    sealed class CapturedSchedulerAndExecutionContext
    {
        internal readonly object _scheduler;
        internal readonly ExecutionContext _executionContext;

        public CapturedSchedulerAndExecutionContext(object scheduler, ExecutionContext executionContext)
        {
            Debug.Assert(scheduler is SynchronizationContext or TaskScheduler, $"{nameof(scheduler)} is {scheduler}");
            Debug.Assert(executionContext is not null, $"{nameof(executionContext)} is null");

            _scheduler = scheduler;
            _executionContext = executionContext;
        }
    }
}
