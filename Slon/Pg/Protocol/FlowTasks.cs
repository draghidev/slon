namespace Slon.Pg.Protocol;

public readonly struct FlowTasks(ValueTask trailingExecutionTask, ValueTask pipelineTask)
{
    public ValueTask TrailingExecutionTask { get; } = trailingExecutionTask;
    public ValueTask PipelineTask { get; } = pipelineTask;

    public FlowTasks(ValueTask pipelineTask)
        : this(default, pipelineTask) { }

    public static implicit operator FlowTasks(ValueTask pipelineTask) => new(pipelineTask);
}
