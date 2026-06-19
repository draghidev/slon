using Draghi.Pipelining;

namespace Slon.Pg.Protocol;

// Abstracts a pipeline's currently-executing / activated flow so a Control can read either pipeline's
// slots without knowing its concrete generic type. Slon-side on purpose: Draghi's Pipeline stays an
// unencumbered substrate; the adapter below bridges to its public getters. Lets the outer pool
// pipeline and an exclusive flow's inner pipeline (any nesting depth) be read through one handle.
interface IFlowSlots
{
    PgClientFlow? ExecutingItem { get; }
    PgClientFlow? ActivatedItem { get; }
}

// Thin adapter: forwards to a Pipeline's slot getters. Generic over the pipeline's type args so it
// stays decoupled from the concrete Policy/Source - one instance per pipeline, bound at creation.
sealed class PipelineFlowSlots<TPolicy, TSource, TEnumerator> : IFlowSlots
    where TPolicy : IPipelinePolicy<PgClientFlow>
    where TSource : IPipelineSource<PgClientFlow, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<PgClientFlow>
{
    readonly Pipeline<PgClientFlow, TPolicy, TSource, TEnumerator> _pipeline;

    public PipelineFlowSlots(Pipeline<PgClientFlow, TPolicy, TSource, TEnumerator> pipeline)
        => _pipeline = pipeline;

    public PgClientFlow? ExecutingItem => _pipeline.ExecutingItem;
    public PgClientFlow? ActivatedItem => _pipeline.ActivatedItem;
}
