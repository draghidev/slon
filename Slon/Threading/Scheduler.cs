namespace Slon.Threading;

abstract class Scheduler
{
    public abstract void SubmitDetached(Action<object?> action, object? state, bool preferLocal = true);
}

sealed class DelegatedScheduler : Scheduler
{
    readonly Action<Action<object?>, object?, bool> _submitDetached;

    public DelegatedScheduler(Action<Action<object?>, object?, bool> submitDetached)
    {
        ArgumentNullException.ThrowIfNull(submitDetached);
        _submitDetached = submitDetached;
    }

    public override void SubmitDetached(Action<object?> action, object? state, bool preferLocal = true)
        => _submitDetached(action, state, preferLocal);
}
