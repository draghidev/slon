using BenchmarkDotNet.Attributes;

namespace Slon.Benchmark;

[IterationCount(15)]
[ThreadingDiagnoser]
[MemoryDiagnoser]
public class FlowBenchmarks
{
    [Benchmark]
    public void RawFlow()
    {
        try
        {
            var flow = _flow;
            flow.Reset();
            ((IProtocolFlow)flow).Start();
            flow.Execute(default, CancellationToken.None).Result.PipelineTask.GetAwaiter().GetResult();
            ((IProtocolFlow)flow).Complete();
        }
        catch (Exception e)
        {
            ((IProtocolFlow)_flow).Complete(e);
        }
    }

    [Benchmark]
    public async ValueTask RawFlowAsync()
    {
        try
        {
            var flow = _flow;
            flow.Reset();
            ((IProtocolFlow)flow).Start();
            await (await flow.Execute(default, CancellationToken.None)).PipelineTask;
            ((IProtocolFlow)flow).Complete();
        }
        catch (Exception e)
        {
            ((IProtocolFlow)_flow).Complete(e);
        }
    }

    [Benchmark]
    [ArgumentsSource(nameof(Sequential))]
    public void ProtocolFlowSequential(Arguments<Sequential> flows)
    {
        var flowsArray = flows.Flows;
        var protocol = flows.Protocol;

        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            flow.Reset();
            _ = protocol.TryQueue(flow);
        }

        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            if (!flow.IsCompleted)
                throw new InvalidOperationException("Should have completed synchronously");
        }
    }

    [Benchmark]
    [ArgumentsSource(nameof(Sequential))]
    public ValueTask ProtocolFlowSequentialAsync(Arguments<Sequential> flows)
    {
        var flowsArray = flows.Flows;
        var protocol = flows.Protocol;
        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            flow.Reset();
            _ = protocol.TryQueue(flow);
        }

        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            if (!flow.IsCompleted)
                throw new InvalidOperationException("Should have completed synchronously");
        }

        return new();
    }

    [Benchmark]
    [ArgumentsSource(nameof(Pipelined))]
    public void ProtocolFlowPipelined(Arguments<Pipelined> flows)
    {
        var flowsArray = flows.Flows;
        var protocol = flows.Protocol;
        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            flow.Reset();
            _ = protocol.TryQueue(flow);
        }

        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            if (flow.WaitForComplete() is var task && task.IsCompleted)
                task.GetAwaiter().GetResult();
            else
                task.AsTask().GetAwaiter().GetResult();
        }
    }

    [Benchmark]
    [ArgumentsSource(nameof(Pipelined))]
    public async ValueTask ProtocolFlowPipelinedAsync(Arguments<Pipelined> flows)
    {
        var flowsArray = flows.Flows;
        var protocol = flows.Protocol;
        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            flow.Reset();
            _ = protocol.TryQueue(flow);
        }

        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            await flow.WaitForComplete();
        }

        // flowsArray[^1].WaitForComplete().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark]
    [ArgumentsSource(nameof(PipelinedUserTriggered))]
    public void ProtocolFlowPipelinedUserTriggered(Arguments<PipelinedUserCompleted> flows)
    {
        var flowsArray = flows.Flows;
        var protocol = flows.Protocol;
        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            flow.Reset();
            _ = protocol.TryQueue(flow);
        }

        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            flow.UserCompleted();
            if (flow.WaitForComplete() is var task && task.IsCompleted)
                task.GetAwaiter().GetResult();
            else
                task.AsTask().GetAwaiter().GetResult();
        }

        // flowsArray[^1].WaitForComplete().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark]
    [ArgumentsSource(nameof(PipelinedUserTriggered))]
    public async ValueTask ProtocolFlowPipelinedUserTriggeredAsync(Arguments<PipelinedUserCompleted> flows)
    {
        var flowsArray = flows.Flows;
        var protocol = flows.Protocol;
        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            flow.Reset();
            _ = protocol.TryQueue(flow);
        }

        for (var i = 0; i < flowsArray.Length; i++)
        {
            var flow = flowsArray[i];
            flow.UserCompleted();
            await flow.WaitForComplete();
        }

        // await flowsArray[^1].WaitForComplete();
    }

    // Public wrapper struct to keep the internal types internal.
    public readonly struct Arguments<TMode> where TMode : struct
    {
        internal EmptyProtocol<TMode> Protocol { get; init; }
        internal EmptyFlow<TMode>[] Flows { get; init; }
        internal string Label { get; init; }
        public override string ToString() => $"{Flows.Length}, {Label}";
    }

    IEnumerable<object> CreateArgumentSet<TMode>() where TMode : struct
    {
        const string label = "tp";
        var protocol = new EmptyProtocol<TMode>(new EmptyProtocolOptions { RunEnqueueAsynchronously = false }, null);
        yield return new Arguments<TMode> { Protocol = protocol, Flows = [new EmptyFlow<TMode>(0)], Label = label };
        yield return new Arguments<TMode> { Protocol = protocol, Flows = Enumerable.Range(0, 100).Select(i => new EmptyFlow<TMode>(i)).ToArray(), Label = label };
        yield return new Arguments<TMode> { Protocol = protocol, Flows = Enumerable.Range(0, 100000).Select(i => new EmptyFlow<TMode>(i)).ToArray(), Label = label };
    }
    public IEnumerable<object> Sequential() => CreateArgumentSet<Sequential>();
    public IEnumerable<object> Pipelined() => CreateArgumentSet<Pipelined>();
    public IEnumerable<object> PipelinedUserTriggered() => CreateArgumentSet<PipelinedUserCompleted>();

    readonly EmptyFlow<Sequential> _flow = new(0);
}
