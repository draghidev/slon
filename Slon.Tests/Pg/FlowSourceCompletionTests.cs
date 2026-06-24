using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Shutdown racing a just-submitted sync flow: a sync caller appends its flow + wait-node, then the
// source COMPLETES before the executor ever pulls and holds that flow. Two things must hold:
//   - the executor's pull loop must not busy-spin. WaitCore gates completion off while a sync waiter is
//     queued (HasSyncWaiter) and otherwise Retries on HeldSyncFlow==null && HasItem - so an un-held
//     queued sync flow makes it spin: TryGetNext fake-misses on IsCompleted, WaitCore Retries, repeat.
//   - the sync caller must not strand in WaitForExecutor waiting for a takeover that never comes.
// The queued sync flow resolves exactly once (taken over xor drained inert) and the caller returns.
//
// Source-only, no Pipeline / flow bodies / wire. The hand-rolled executor mirrors Pipeline.ExecuteSource
// and the shutdown sequence mirrors PgClientProtocol.Shutdown.
[TestClass]
[DoNotParallelize]
public class FlowSourceCompletionTests
{
    static readonly TimeSpan Cap = TimeSpan.FromSeconds(10);

    [TestMethod]
    public async Task QueuedSyncFlow_CompletesBeforeHandoff_CallerReturnsAndFlowResolvedOnce()
    {
        // The source only reads Protocol.UnflushedBytes on the pull path (0 before Initialize); nothing
        // else of the protocol is touched.
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol);
        var enumerator = source.GetAsyncEnumerator();

        var flow = CommandFlow.CreateUninitialized();
        var consumed = 0;
        void Record(PgClientFlow? f)
        {
            if (ReferenceEquals(f, flow))
                Interlocked.Increment(ref consumed);
        }

        // Sync caller: append the flow + wait-node (this alone makes HasSyncWaiter true), then block in
        // WaitForExecutor. On the fixed source it is taken over or bailed; on the unfixed source it
        // strands waiting for a signal the spinning executor never sends.
        var node = source.EnqueueSyncWaiter(flow);
        var caller = Task.Run(() => source.WaitForExecutor(node));

        // Complete the source while the sync flow sits queued and un-held.
        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        source.SetDrainSignal(drained);
        enumerator.Complete();

        // Executor: Pipeline.ExecuteSource's pull loop, with a spin guard. On the unfixed source the loop
        // busy-spins on WaitCore.Retry; the guard trips and surfaces the hang as a failure rather than a
        // timeout. Each Retry resolves synchronously, so the cap is reached in well under a second.
        var executor = Task.Run(async () =>
        {
            long guard = 0;
            while (true)
            {
                if (enumerator.TryGetNext(out var item))
                {
                    Record(item);
                    continue;
                }
                if (++guard > 5_000_000)
                    throw new InvalidOperationException(
                        "executor busy-looped on WaitCore.Retry - the queued sync flow was never held or resolved.");
                if (!await enumerator.WaitForNextAsync())
                    break;
            }
        });

        await Task.WhenAny(drained.Task, executor);
        source.DrainInertItems(Record);
        await executor.WaitAsync(Cap);
        await caller.WaitAsync(Cap);
        await enumerator.DisposeAsync();

        Assert.AreEqual(1, consumed,
            "the queued sync flow must resolve exactly once (taken over by its caller xor drained inert).");
    }

    [TestMethod]
    public async Task AsyncAheadOfQueuedSyncFlow_CompletesBeforeHandoff_AllResolvedOnce()
    {
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol);
        var enumerator = source.GetAsyncEnumerator();

        // FIFO: an async flow queued AHEAD of the sync waiter. At completion the sync head can't be held
        // until the async ahead of it is disposed - the async head is left for DrainInert, which can't
        // run until the executor resolves Completed, which HasSyncWaiter gates off. Same spin family.
        var asyncFlow = CommandFlow.CreateUninitialized();
        var syncFlow = CommandFlow.CreateUninitialized();
        var consumed = new Dictionary<PgClientFlow, int>(ReferenceEqualityComparer.Instance)
        {
            [asyncFlow] = 0,
            [syncFlow] = 0,
        };
        void Record(PgClientFlow? f)
        {
            if (f is not null && consumed.ContainsKey(f))
                lock (consumed) consumed[f]++;
        }

        source.Enqueue(asyncFlow);
        var node = source.EnqueueSyncWaiter(syncFlow);
        var caller = Task.Run(() => source.WaitForExecutor(node));

        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        source.SetDrainSignal(drained);
        enumerator.Complete();

        var executor = Task.Run(async () =>
        {
            long guard = 0;
            while (true)
            {
                if (enumerator.TryGetNext(out var item))
                {
                    Record(item);
                    continue;
                }
                if (++guard > 5_000_000)
                    throw new InvalidOperationException(
                        "executor busy-looped on WaitCore.Retry - an async flow ahead of the sync waiter never advanced.");
                if (!await enumerator.WaitForNextAsync())
                    break;
            }
        });

        await Task.WhenAny(drained.Task, executor);
        source.DrainInertItems(Record);
        await executor.WaitAsync(Cap);
        await caller.WaitAsync(Cap);
        await enumerator.DisposeAsync();

        Assert.AreEqual(1, consumed[asyncFlow], "the queued async flow must resolve exactly once.");
        Assert.AreEqual(1, consumed[syncFlow], "the queued sync flow must resolve exactly once.");
    }
}
