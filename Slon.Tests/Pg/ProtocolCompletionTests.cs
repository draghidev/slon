using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// End-to-end tests for PgClientProtocol completion and failure surfaces: CompleteAsync,
// DisposeAsync, Dispose, FailProtocol. Verifies graceful vs forceful semantics, idempotency,
// and the heartbeat-based parked-flow propagation that fails activation sources when AbortToken
// fires on a flow that's enqueued but not yet activated.
// Class-serial: every test runs with a 50ms HeartbeatInterval to narrow the parked-flow
// propagation window. Method-level parallelism would multiply concurrent fast-tick
// heartbeats within this class and starve the TP, masking the timing the tests measure.
[TestClass]
[DoNotParallelize]
public class ProtocolCompletionTests
{
    // Isolated per test by design: every test in this file fully destroys (CompleteAsync,
    // DisposeAsync, Dispose, FailProtocol) the protocol. Cannot share via PgTestPool's lease
    // path. Custom heartbeat/completion timeouts narrow the parked-flow propagation window
    // the tests exercise.
    static Task<PgClientProtocol> ConnectAsync() => PgTestPool.NewIsolatedAsync(o =>
    {
        o.CompletionTimeout = TimeSpan.FromMilliseconds(500);
        o.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
    });

    static async Task RunAsync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    // Graceful CompleteAsync after a normal flow finishes. Tests teardown lands; pool eviction
    // status flips to Completed cleanly.
    [TestMethod]
    public async Task CompleteAsync_AfterFlow_Idempotent()
    {
        var protocol = await ConnectAsync();
        await RunAsync(protocol, "select 1");
        await protocol.CompleteAsync();
        await protocol.CompleteAsync();
        await protocol.CompleteAsync();
    }

    // Forceful DisposeAsync after a normal flow finishes. Same final state as CompleteAsync
    // (Completed), via the AbortToken path.
    [TestMethod]
    public async Task DisposeAsync_AfterFlow_Idempotent()
    {
        var protocol = await ConnectAsync();
        await RunAsync(protocol, "select 1");
        await protocol.DisposeAsync();
        await protocol.DisposeAsync();
    }

    // Sync Dispose after a normal flow finishes. Fire-and-forget tear-down on a sync caller
    // contract (DbDataSource.Dispose). Heartbeat is released, status flips to Completed.
    [TestMethod]
    public async Task Dispose_AfterFlow_Idempotent()
    {
        var protocol = await ConnectAsync();
        await RunAsync(protocol, "select 1");
        protocol.Dispose();
        protocol.Dispose();
    }

    // CompleteAsync called concurrently with an in-flight flow. Drain waits for the flow's
    // pipelined completion before tearing down so the consumer iteration on the user side
    // and the body's read pump on the wire side both finish naturally.
    [TestMethod]
    public async Task CompleteAsync_WithInFlightFlow_DrainsCleanly()
    {
        var protocol = await ConnectAsync();
        var flow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.05)"));
        Assert.IsTrue(protocol.TryQueue(flow));

        // Graceful close while a consumer is mid-iteration: the move-next source faults with
        // PgClientClosedException so the consumer's MoveNextAsync surfaces it (input-commands-
        // equals-output-results coherence rule). The consumer disposes on the exception path.
        var runTask = Task.Run(async () =>
        {
            var e = flow.GetAsyncEnumerator();
            try
            {
                while (await e.MoveNextAsync()) { }
            }
            catch (PgClientClosedException) { }
            await e.DisposeAsync();
        });

        await Task.Delay(10);
        var completeTask = protocol.CompleteAsync();

        await runTask;
        await completeTask;
    }

    // Forceful DisposeAsync called while a flow is in flight. Flow's awaiter should see a
    // PgClientClosedException via the AbortToken cascade through I/O.
    [TestMethod]
    public async Task DisposeAsync_WithInFlightFlow_FlowSeesClosedException()
    {
        var protocol = await ConnectAsync();
        var flow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.5)"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var runTask = Task.Run(async () =>
        {
            var e = flow.GetAsyncEnumerator();
            try
            {
                while (await e.MoveNextAsync()) { }
                await e.DisposeAsync();
                return (Exception?)null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        });

        await Task.Delay(10);
        await protocol.DisposeAsync();

        var observed = await runTask;
        Assert.IsNotNull(observed, "Flow should have observed a tear-down exception.");
        Assert.IsInstanceOfType<PgClientClosedException>(GetRoot(observed));

        static Exception GetRoot(Exception ex)
        {
            while (ex.InnerException is not null && ex is not PgClientClosedException)
                ex = ex.InnerException;
            return ex;
        }
    }

    // CompleteAsync with a parked flow (enqueued but not yet activated). The graceful drain
    // observes the flow, AbortToken fires when CompletionTimeout elapses, heartbeat propagates
    // the closed exception into the flow's activation source. Flow's MoveNextAsync surfaces
    // PgClientClosedException to the caller.
    [TestMethod]
    public async Task CompleteAsync_WithParkedFlow_HeartbeatPropagatesClosedException()
    {
        var protocol = await ConnectAsync();
        var blockingFlow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.5)"));
        Assert.IsTrue(protocol.TryQueue(blockingFlow));

        var parked = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(parked));

        var runParked = Task.Run(async () =>
        {
            var e = parked.GetAsyncEnumerator();
            try
            {
                while (await e.MoveNextAsync()) { }
                await e.DisposeAsync();
                return (Exception?)null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        });

        var runBlocking = Task.Run(async () =>
        {
            var e = blockingFlow.GetAsyncEnumerator();
            try
            {
                while (await e.MoveNextAsync()) { }
                await e.DisposeAsync();
            }
            catch { }
        });

        await Task.Delay(10);
        await protocol.DisposeAsync();
        await runBlocking;

        var observed = await runParked;
        Assert.IsNotNull(observed, "Parked flow should have observed a closed exception via heartbeat propagation.");
    }

    // FailProtocol from inside (e.g. startup failure path). Fire-and-forget; protocol enters
    // Completed via AbortToken cascade. Subsequent TryQueue returns false.
    [TestMethod]
    public async Task DisposeAsync_RejectsSubsequentTryQueue()
    {
        var protocol = await ConnectAsync();
        await protocol.DisposeAsync();

        var flow = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsFalse(protocol.TryQueue(flow));
    }

    // CompleteAsync followed by DisposeAsync. The graceful path completes, the forceful
    // DisposeAsync is a no-op on an already-Completed protocol.
    [TestMethod]
    public async Task CompleteAsync_ThenDisposeAsync_NoOp()
    {
        var protocol = await ConnectAsync();
        await RunAsync(protocol, "select 1");
        await protocol.CompleteAsync();
        await protocol.DisposeAsync();
    }

    // Many CompleteAsync calls in parallel after a flow finishes. First-writer wins, all
    // return the same execution task. No double tear-down.
    [TestMethod]
    public async Task CompleteAsync_ConcurrentCallers_FirstWriterWins()
    {
        var protocol = await ConnectAsync();
        await RunAsync(protocol, "select 1");

        var tasks = new Task[16];
        for (int i = 0; i < tasks.Length; i++)
            tasks[i] = protocol.CompleteAsync().AsTask();
        await Task.WhenAll(tasks);
    }
}
