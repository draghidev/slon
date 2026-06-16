using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Hammers PgClientFlowSource's shutdown coordination directly, with no Pipeline, flow bodies, or wire.
// A hand-rolled executor mirrors Pipeline.ExecuteSource's pull loop and the shutdown sequence mirrors
// PgClientProtocol.Shutdown, leaving the source as the only thing under test.
//
// Invariant: every enqueued flow is consumed exactly once, dispatched by the executor xor drained as
// inert. A torn SPSC dequeue surfaces as a null or double-consumed flow; a lost flow surfaces as a
// flow consumed zero times. Override count via SLON_STRESS_ITERATIONS (default 5000).
[TestClass]
[DoNotParallelize]
public class FlowSourceDirectStressTests
{
    static int Iterations
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("SLON_STRESS_ITERATIONS");
            return int.TryParse(raw, out var n) && n > 0 ? n : 5000;
        }
    }

    [TestMethod]
    public async Task Stress_SourceDispatchVsDrain_SingleConsumerHolds()
    {
        // Uninitialized protocol: the source only reads Protocol.UnflushedBytes on the pull path, which
        // is 0 before Initialize. Nothing else of the protocol is touched.
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));

        for (int i = 0; i < Iterations; i++)
        {
            const int N = 8;
            var source = PgClientFlowSource.Create(protocol);
            var enumerator = source.GetAsyncEnumerator();

            var flows = new PgClientFlow[N];
            var index = new Dictionary<PgClientFlow, int>(ReferenceEqualityComparer.Instance);
            for (int k = 0; k < N; k++)
            {
                flows[k] = CommandFlow.CreateUninitialized();
                index[flows[k]] = k;
            }
            var consume = new int[N];
            int nullSeen = 0, unknownSeen = 0;

            void Record(PgClientFlow? f)
            {
                if (f is not null && index.TryGetValue(f, out var k))
                    Interlocked.Increment(ref consume[k]);
                else if (f is null)
                    Interlocked.Increment(ref nullSeen);
                else
                    Interlocked.Increment(ref unknownSeen);
            }

            // Executor: the source interaction Pipeline.ExecuteSource performs.
            var executor = Task.Run(async () =>
            {
                while (true)
                {
                    if (enumerator.TryGetNext(out var item))
                    {
                        Record(item);
                        continue;
                    }
                    if (!await enumerator.WaitForNextAsync())
                        break;
                }
            });

            // Producer: enqueue all N, waking the executor each time.
            for (int k = 0; k < N; k++)
                source.Enqueue(flows[k]).Execute(runContinuationsAsynchronously: true);

            // Shutdown: the drain coordination PgClientProtocol.Shutdown performs.
            var executorStopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            source.SetDrainSignal(executorStopped);
            enumerator.Complete();
            await Task.WhenAny(executorStopped.Task, executor);
            source.DrainInertItems(Record);
            await executor.WaitAsync(TimeSpan.FromSeconds(10));
            await enumerator.DisposeAsync();

            if (nullSeen != 0)
                Assert.Fail($"iter {i}: DrainInertItems saw {nullSeen} null flow(s) - torn SPSC dequeue.");
            if (unknownSeen != 0)
                Assert.Fail($"iter {i}: saw {unknownSeen} unrecognized flow(s) - corrupted dequeue.");
            for (int k = 0; k < N; k++)
            {
                if (consume[k] == 0)
                    Assert.Fail($"iter {i}: flow {k} consumed 0 times - lost (would hang a real caller).");
                if (consume[k] > 1)
                    Assert.Fail($"iter {i}: flow {k} consumed {consume[k]} times - double dequeue (torn/corrupt).");
            }
        }
    }
}
