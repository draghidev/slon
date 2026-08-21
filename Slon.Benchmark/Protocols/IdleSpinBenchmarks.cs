using BenchmarkDotNet.Attributes;

namespace Slon.Benchmark;

/// <summary>
/// Measures sequential one-at-a-time throughput across execution modes.
/// SyncFirst/Sync modes use the enqueue spin (inline on caller's thread).
///
/// Key finding: Async mode is ~10x slower than SyncFirst/Sync because
/// flow completion uses runContinuationsAsynchronously: true, meaning 2 TP hops per item.
/// An idle spin in the execution loop was tested but provided no benefit (concurrent case
/// was ~7% worse with spin), so it was removed.
/// </summary>
[WarmupCount(3)]
[IterationCount(10)]
[ThreadingDiagnoser]
[MemoryDiagnoser]
public class IdleSpinBenchmarks
{
    const int ItemCount = 10_000;

    [ParamsAllValues]
    public bool RunAsync { get; set; }

    EmptyProtocol<Sequential> _protocol = null!;
    EmptyFlow<Sequential> _flow = null!;

    [GlobalSetup]
    public void Setup()
    {
        var options = new EmptyProtocolOptions
        {
            RunEnqueueAsynchronously = RunAsync
        };
        _protocol = new EmptyProtocol<Sequential>(options, null);
        _flow = new EmptyFlow<Sequential>(0);
    }

    /// <summary>
    /// Tight sequential loop: queue → wait for complete → repeat.
    /// Each iteration exercises the idle gap between items.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ItemCount)]
    public async ValueTask SequentialAsync()
    {
        var protocol = _protocol;
        var flow = _flow;

        for (var i = 0; i < ItemCount; i++)
        {
            flow.Reset();
            protocol.TryQueue(flow);

            if (!flow.IsCompleted)
                await flow.WaitForComplete();
        }
    }

    /// <summary>
    /// Concurrent producers: 4 callers sharing one pipeline, each doing sequential one-at-a-time.
    /// Tests whether idle spinning helps catch enqueues from other producers.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ItemCount)]
    public async ValueTask ConcurrentSequentialAsync()
    {
        var tasks = new ValueTask[4];
        for (var t = 0; t < 4; t++)
            tasks[t] = RunProducer();

        for (var t = 0; t < 4; t++)
            await tasks[t];

        async ValueTask RunProducer()
        {
            var protocol = _protocol;
            var flow = new EmptyFlow<Sequential>(0);

            for (var i = 0; i < ItemCount / 4; i++)
            {
                flow.Reset();
                protocol.TryQueue(flow);

                if (!flow.IsCompleted)
                    await flow.WaitForComplete();
            }
        }
    }
}






