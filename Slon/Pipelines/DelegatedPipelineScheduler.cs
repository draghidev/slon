using Draghi.Pipelining;
using Slon.Threading;

namespace Slon.Pipelines;

sealed class DelegatedPipelineScheduler(Scheduler scheduler) : PipelineScheduler
{
    public override void SubmitDetached(Action<object?> action, object? state, bool preferLocal = true)
        => scheduler.SubmitDetached(action, state, preferLocal);
}
