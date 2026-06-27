using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// CommandFlow body-side drain mode: when the consumer abandons the result stream
// (Enumerator.Dispose / DisposeAsync) or the protocol fires StoppingToken (graceful
// shutdown), the body skips the user-handoff for remaining commands and drains the wire
// to RFQ on its own. Parameterized over (flowAsync, useAsyncDispose).
[TestClass]
public class CommandDrainTests
{
    static async ValueTask Dispose(CommandFlow.Enumerator e, bool useAsyncDispose)
    {
        if (useAsyncDispose)
            await e.DisposeAsync();
        else
            e.Dispose();
    }

    static CommandFlow.Enumerator GetEnumerator(CommandFlow flow, bool flowAsync)
        => flowAsync ? flow.GetAsyncEnumerator() : flow.GetEnumerator();

    static async ValueTask<bool> MoveNext(CommandFlow.Enumerator e, bool flowAsync)
        => flowAsync ? await e.MoveNextAsync() : e.MoveNext();

    [TestMethod]
    [DataRow(true, true, DisplayName = "async flow, async dispose")]
    [DataRow(true, false, DisplayName = "async flow, sync dispose")]
    [DataRow(false, true, DisplayName = "sync flow, async dispose")]
    [DataRow(false, false, DisplayName = "sync flow, sync dispose")]
    public async Task NextCommandResult_DrainsCurrentCommandBodySide(bool flowAsync, bool useAsyncDispose)
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        var flow = new CommandFlow(flowAsync,
            Command.Create("select generate_series(1, 100)"),
            Command.Create("select 'second'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = GetEnumerator(flow, flowAsync);

        Assert.IsTrue(await MoveNext(e, flowAsync), "first command result not delivered");
        Assert.IsTrue(await MoveNext(e, flowAsync), "second command result not delivered after drain of first");
        Assert.IsFalse(await MoveNext(e, flowAsync), "third call should return false (no more results)");
        await Dispose(e, useAsyncDispose);

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DataRow(true, true, DisplayName = "async flow, async dispose")]
    [DataRow(true, false, DisplayName = "async flow, sync dispose")]
    [DataRow(false, true, DisplayName = "sync flow, async dispose")]
    [DataRow(false, false, DisplayName = "sync flow, sync dispose")]
    public async Task ConsumerDispose_MidBatch_BodyDrainsRemaining_ConnectionUsable(bool flowAsync, bool useAsyncDispose)
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        var flow = new CommandFlow(flowAsync,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = GetEnumerator(flow, flowAsync);
        Assert.IsTrue(await MoveNext(e, flowAsync), "first command result not delivered");

        await Dispose(e, useAsyncDispose);

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    // Open-before-park lost-wake regression. Sync mid-batch dispose of an async flow opens the inter-result
    // gate (DisposeAsync sets draining + TrySetResult, OUTSIDE _rearmLock) while the body is still arming
    // that gate. Pre-fix this hung intermittently: the buffered gate edge was wiped by the body's gate
    // Reset and the parked body never re-read the draining level, so it never drained and pinned the
    // pipeline's activated slot - the next flow never activated. The fix re-checks the draining level at
    // the gate OnCompleted. Hammer the raced path on one reusable protocol; each iteration must leave the
    // wire usable (a hang shows as the "select 1" WaitAsync timing out, not a suite hang).
    [TestMethod]
    [DoNotParallelize]
    public async Task ConsumerDispose_MidBatch_SyncDispose_OpenBeforePark_Stress()
    {
        var iters = int.TryParse(Environment.GetEnvironmentVariable("SLON_STRESS_ITERATIONS"), out var n) && n > 0 ? n : 500;
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;
        for (var i = 0; i < iters; i++)
        {
            var flow = new CommandFlow(async: true,
                Command.Create("select generate_series(1, 50)"),
                Command.Create("select 'two'"),
                Command.Create("select 'three'"));
            Assert.IsTrue(protocol.TryQueue(flow));
            var e = flow.GetAsyncEnumerator();
            Assert.IsTrue(await e.MoveNextAsync(), $"iter {i}: first result not delivered");
            e.Dispose(); // SYNC dispose of an async flow, mid-batch - the raced path.
            await PgTestPool.RunAsync(protocol, "select 1").WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    // Deterministic open-before-park. A test hook holds the body right before it registers on the inter-result
    // rendezvous until a concurrent sync dispose has set draining + fired its wake - so the dispose's
    // draining+wake land BEFORE the park, every run (the interleaving the stress test can only hit by luck).
    // Under the two-way rendezvous the disposer is pumping WaitForContinuation, so when the body registers it
    // hands off and the disposer drives the drain INLINE: every drain read runs on the disposer's own thread.
    [TestMethod]
    public async Task ConsumerDispose_OpenBeforePark_Deterministic_ConnectionUsable()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;
        var flow = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));

        var bodyAtHook = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wakeFired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        flow.AfterDisposeWakeHook = () => wakeFired.TrySetResult();
        int disposeThread = 0;
        var drainThreads = new System.Collections.Concurrent.ConcurrentDictionary<int, byte>();
        flow.OnDrainReadHook = () => drainThreads.TryAdd(Environment.CurrentManagedThreadId, 0);
        var fired = false;
        flow.BeforeGateParkHook = async () =>
        {
            if (fired) return; // fire once: the inter-result park after command 0
            fired = true;
            bodyAtHook.SetResult();
            // Hold until the dispose's wake has fired, so we register AFTER it = open-before-park.
            await wakeFired.Task;
        };

        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        Assert.IsTrue(await e.MoveNextAsync(), "command 0 not delivered");

        await bodyAtHook.Task.WaitAsync(TimeSpan.FromSeconds(10));
        // Sync dispose on a separate thread; it pumps the rendezvous. Its wake releases the held body, which
        // registers and hands off to the disposer.
        var disposeDone = Task.Run(() => { disposeThread = Environment.CurrentManagedThreadId; e.Dispose(); });

        await disposeDone.WaitAsync(TimeSpan.FromSeconds(10)); // body handed off, dispose returned, no hang
        await PgTestPool.RunAsync(protocol, "select 1").WaitAsync(TimeSpan.FromSeconds(10)); // wire usable
        // Two-way rendezvous: every drain read ran on the disposer's own thread - one thread.
        Assert.AreEqual(1, drainThreads.Count,
            $"drain spanned threads [{string.Join(",", drainThreads.Keys)}], expected only disposer {disposeThread}");
        Assert.IsTrue(drainThreads.ContainsKey(disposeThread),
            $"drain ran on [{string.Join(",", drainThreads.Keys)}], not the disposer thread {disposeThread}");
    }

    [TestMethod]
    [DataRow(true, DisplayName = "async flow")]
    [DataRow(false, DisplayName = "sync flow")]
    public async Task StoppingToken_MidBatch_BodyDrainsRemaining_FlowCompletesCleanly(bool flowAsync)
    {
        var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(flowAsync,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = GetEnumerator(flow, flowAsync);
        Assert.IsTrue(await MoveNext(e, flowAsync), "first command result not delivered");

        var completeTask = protocol.CompleteAsync();

        // With the consumer still active, StoppingToken faults the move-next source with the
        // close exception rather than silently skipping remaining CommandResults.
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await MoveNext(e, flowAsync));
        await e.DisposeAsync();

        await completeTask.AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }

    // Graceful shutdown overlapping consumer-side dispose. The first MoveNext's outcome may
    // be a row delivery, a PgClientClosedException, or a quiet drain depending on timing, but
    // each (flowAsync, useAsyncDispose) pair must settle CompleteAsync cleanly.
    [TestMethod]
    [DataRow(true, true, DisplayName = "async flow, async dispose")]
    [DataRow(true, false, DisplayName = "async flow, sync dispose")]
    [DataRow(false, true, DisplayName = "sync flow, async dispose")]
    [DataRow(false, false, DisplayName = "sync flow, sync dispose")]
    public async Task StoppingToken_MidBatch_FollowedByDispose_FlowCompletesCleanly(bool flowAsync, bool useAsyncDispose)
    {
        var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(flowAsync,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = GetEnumerator(flow, flowAsync);
        // Fire the first MoveNext as a task so CompleteAsync can race the delivery.
        var firstTask = MoveNext(e, flowAsync).AsTask();
        var completeTask = protocol.CompleteAsync();

        // The first MoveNext may complete with a row or throw PgClientClosedException; both are
        // valid settle states and the dispose below converges regardless.
        try { await firstTask; }
        catch (PgClientClosedException) { }
        await Dispose(e, useAsyncDispose);

        await completeTask.AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }

    // Async flow: shutdown signalled before the consumer ever touches the flow. The parked
    // gate await is faulted by the heartbeat-driven OnStopping and the consumer's MoveNext
    // surfaces PgClientClosedException without any delivery.
    [TestMethod]
    public async Task StoppingToken_PreFireAsync_BodyFaultsWithoutDelivery()
    {
        var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var completeTask = protocol.CompleteAsync();

        var e = flow.GetAsyncEnumerator();
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await e.MoveNextAsync());
        await e.DisposeAsync();

        await completeTask.AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }

    // Pipelined multi-flow: both flows queued before any MoveNext, then CompleteAsync fires.
    // A delivers (its writes flush, postgres responds), B faults via OnStopping, and both
    // enumerators settle cleanly.
    [TestMethod]
    [DoNotParallelize]
    public async Task StoppingToken_PipelinedFlows_AllDrainCleanly()
    {
        var protocol = await PgTestPool.NewIsolatedAsync();

        var flowA = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 20)"),
            Command.Create("select 'a-two'"));
        var flowB = new CommandFlow(async: true, Command.Create("select 'b'"));
        Assert.IsTrue(protocol.TryQueue(flowA));
        Assert.IsTrue(protocol.TryQueue(flowB));

        var eA = flowA.GetAsyncEnumerator();
        var eB = flowB.GetAsyncEnumerator();

        // Fire the MoveNext as a task so CompleteAsync can run concurrently.
        var aFirstTask = eA.MoveNextAsync().AsTask();
        var completeTask = protocol.CompleteAsync();

        // Graceful shutdown overlapping the reads: each flow's first MoveNext may deliver a row (its writes
        // flushed and postgres responded before StoppingToken propagated) OR throw PgClientClosedException
        // (StoppingToken won the race). Both are valid settle states - under load A often delivers. What this
        // test pins is that BOTH enumerators dispose cleanly and CompleteAsync converges, regardless of which
        // side of the race each landed on.
        try { await aFirstTask; } catch (PgClientClosedException) { }
        try { await eB.MoveNextAsync(); } catch (PgClientClosedException) { }
        await eA.DisposeAsync();
        await eB.DisposeAsync();

        await completeTask.AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }

    // Graceful-then-abort escalation: a body parked on actual I/O (pg_sleep keeps the server
    // busy for 30s) can't observe StoppingToken via the per-CommandResult check because it
    // never gets there. CompleteAsync's graceful path schedules AbortToken via
    // CancelAfter(CompletionTimeout); when the timeout fires the decoder's CTS-linked CT
    // throws, the body's catch routes the closed exception out, and the consumer's pending
    // MoveNextAsync surfaces PgClientClosedException.
    [TestMethod]
    public async Task StoppingToken_GracefulEscalatesToAbort_AsyncFlowFaultsWithClosedException()
    {
        // Narrow timeout, but safe parallelized: the body is parked on pg_sleep the whole window,
        // so the graceful->abort escalation is deterministic - there is no timing race to lose.
        var protocol = await PgTestPool.NewIsolatedAsync(o => o.CompletionTimeout = TimeSpan.FromMilliseconds(100));

        var flow = new CommandFlow(async: true, Command.Create("select pg_sleep(30)"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = flow.GetAsyncEnumerator();
        var moveNextTask = e.MoveNextAsync().AsTask();

        var completeTask = protocol.CompleteAsync();

        await Assert.ThrowsExactlyAsync<PgClientClosedException>(
            async () => await moveNextTask.WaitAsync(TimeSpan.FromSeconds(10)));
        await e.DisposeAsync();

        await completeTask.AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }
}
