using Draghi.Pipelining;

namespace Slon.Tests;

sealed class PausableScheduler : PipelineScheduler
{
    readonly object _sync = new();
    readonly Queue<Work> _held = new();
    TaskCompletionSource? _idle;
    bool _paused;
    int _running;

    public Task PauseAsync()
    {
        lock (_sync)
        {
            _paused = true;
            return _running is 0
                ? Task.CompletedTask
                : (_idle ??= new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }
    }

    public void Resume()
    {
        Work[] held;
        lock (_sync)
        {
            if (!_paused)
                return;
            _paused = false;
            held = _held.ToArray();
            _held.Clear();
        }
        foreach (var work in held)
            SubmitDetached(work.Action, work.State, work.PreferLocal);
    }

    public override void SubmitDetached(Action<object?> action, object? state, bool preferLocal = true)
    {
        var work = new Work(action, state, preferLocal);
        lock (_sync)
        {
            if (_paused)
            {
                _held.Enqueue(work);
                return;
            }
            _running++;
        }
        PipelineScheduler.ThreadPool.SubmitDetached(
            static state => ((Dispatch)state!).Run(), new Dispatch(this, work), preferLocal);
    }

    void Finished()
    {
        TaskCompletionSource? idle = null;
        lock (_sync)
        {
            if (--_running is 0)
            {
                idle = _idle;
                _idle = null;
            }
        }
        idle?.TrySetResult();
    }

    readonly record struct Work(Action<object?> Action, object? State, bool PreferLocal);

    sealed class Dispatch(PausableScheduler scheduler, Work work)
    {
        public void Run()
        {
            try { work.Action(work.State); }
            finally { scheduler.Finished(); }
        }
    }
}
