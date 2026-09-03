namespace Slon.Threading;

// Temporary host seam until the runtime provides the ambient scheduler lookup directly.
static class SchedulingContext
{
    static Action<Action<object?>, object?, bool>? _submit;
    static Action<Action<object?>, object?, bool>? _submitDetached;

    public static void SetDispatch(
        Action<Action<object?>, object?, bool>? submit,
        Action<Action<object?>, object?, bool>? submitDetached)
    {
        _submit = submit;
        _submitDetached = submitDetached;
    }

    internal static void Submit(Action<object?> action, object? state, bool preferLocal)
    {
        var submit = Volatile.Read(ref _submit);
        if (submit is null)
            ThreadPool.QueueUserWorkItem(action, state, preferLocal);
        else
            submit(action, state, preferLocal);
    }

    internal static void SubmitDetached(Action<object?> action, object? state, bool preferLocal)
        => _ = TrySubmitDetached(action, state, preferLocal);

    internal static bool TrySubmitDetached(Action<object?> action, object? state, bool preferLocal)
    {
        var submit = Volatile.Read(ref _submitDetached);
        if (submit is null)
            return ThreadPool.UnsafeQueueUserWorkItem(action, state, preferLocal);

        submit(action, state, preferLocal);
        return true;
    }
}
