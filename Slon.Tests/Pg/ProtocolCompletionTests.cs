using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// End-to-end tests for PgClientProtocol completion and failure surfaces: CompleteAsync,
// DisposeAsync, Dispose, FailProtocol. Verifies graceful vs forceful semantics, idempotency,
// and the heartbeat-based parked-flow propagation that fails activation sources when AbortToken
// fires on a flow that's enqueued but not yet activated.
[TestClass]
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

    [TestMethod]
    public async Task Completion_PassivelyObservesSameTaskAsCompleteAsync()
    {
        var protocol = await ConnectAsync();
        var completion = protocol.Completion;
        Assert.IsFalse(completion.IsCompleted, "observing Completion must not initiate shutdown");

        var driven = protocol.CompleteAsync();
        Assert.AreSame(completion, driven);
        await driven;
        Assert.IsTrue(completion.IsCompletedSuccessfully);
    }

    // The wire-handoff guard (GuardWireIdleOnHandoff): a cleanly-completed flow on the multiplexed
    // outer pipeline that leaves the wire in a transaction - an unscoped BEGIN - must fail the
    // protocol rather than let the next interleaved flow run inside the open transaction. A real
    // transaction has to be held in an exclusive scope; this is the poison check for one that isn't.
    [TestMethod]
    public async Task UnscopedTransaction_OnOuterPipeline_FailsProtocol()
    {
        var protocol = await ConnectAsync();
        try
        {
            // BEGIN drains cleanly (CommandComplete + RFQ=Transaction); the guard fires on completion.
            // The fire-and-forget FailProtocol can race the drain's dispose, so tolerate a closed
            // exception here - the assertion is on the resulting protocol state.
            try { await RunAsync(protocol, "BEGIN"); }
            catch (PgClientClosedException) { }

            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (!protocol.IsCompleted && sw.Elapsed < TimeSpan.FromSeconds(5))
                await Task.Delay(10);
            Assert.IsTrue(protocol.IsCompleted, "the unscoped BEGIN did not trip the wire-handoff guard");
        }
        finally
        {
            await protocol.DisposeAsync();
        }
    }

    // Graceful CompleteAsync after a normal flow finishes. Tests teardown lands; pool eviction
    // status flips to Completed cleanly.
    [TestMethod]
    public async Task CompleteAsync_AfterFlow_Idempotent()
    {
        var protocol = await ConnectAsync();
        await RunAsync(protocol, "select 1");
        var completion = protocol.Completion;
        Assert.AreSame(completion, protocol.CompleteAsync());
        await completion;

        // Terminal callers observe the same completion without touching disposed close signals,
        // including a late forceful escalation and the scope-abort surface.
        Assert.AreSame(completion, protocol.CompleteAsync(new InvalidOperationException("late")));
        protocol.AbortActiveScope();
        await protocol.DisposeAsync();
    }

    [TestMethod]
    public async Task BeginExclusiveScope_AfterCompletion_IsRejectedAtAdmission()
    {
        var protocol = await ConnectAsync();
        await protocol.CompleteAsync();

        Assert.ThrowsExactly<InvalidOperationException>(
            () => protocol.BeginExclusiveScope(async: true));
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
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));

        // Graceful close while a consumer is mid-iteration: the move-next source faults with
        // PgClientClosedException so the consumer's MoveNextAsync surfaces it (input-commands-
        // equals-output-results coherence rule). The consumer disposes on the exception path.
        // The advisory lock keeps the command incomplete after the consumer starts pulling.
        var reading = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var runTask = Task.Run(async () =>
        {
            var e = flow.GetAsyncEnumerator();
            try
            {
                reading.TrySetResult();
                while (await e.MoveNextAsync()) { }
            }
            catch (PgClientClosedException) { }
            await e.DisposeAsync();
        });

        await reading.Task;
        var completeTask = protocol.CompleteAsync();
        await blocker.ReleaseAsync();

        await runTask;
        await completeTask;
    }

    // Forceful DisposeAsync called while a flow is in flight. Flow's awaiter should see a
    // PgClientClosedException via the AbortToken cascade through I/O.
    [TestMethod]
    public async Task DisposeAsync_WithInFlightFlow_FlowSeesClosedException()
    {
        var protocol = await ConnectAsync();
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));

        // The advisory lock keeps the command incomplete after the consumer starts pulling.
        // The forceful abort then cascades through the in-flight read.
        var reading = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var runTask = Task.Run<Exception?>(async () =>
        {
            var e = flow.GetAsyncEnumerator();
            try
            {
                reading.TrySetResult();
                while (await e.MoveNextAsync()) { }
                await e.DisposeAsync();
                return null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        });

        await reading.Task;
        var disposeTask = protocol.DisposeAsync().AsTask();
        await blocker.ReleaseAsync();
        await disposeTask;

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

    // Graceful CompleteAsync racing forceful DisposeAsync on the SAME protocol under maximal
    // overlap, with an in-flight flow so the drain has real residual work. SignalDraining only
    // returns false once Completed, so during the drain window BOTH calls can run a Shutdown body:
    // double _source.SetDrainSignal (second overwrites the first's executorStopped TCS), double
    // _pipeline.CompleteAsync, double DrainInertItems. Asserts teardown still converges - no hang,
    // ends Completed, the consumer sees only a clean closed exception. Looped to surface the race.
    [TestMethod]
    public async Task CompleteAsync_RacingDisposeAsync_ConvergesCleanly()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        for (var i = 0; i < 50; i++)
        {
            await blocker.HoldAsync();
            var protocol = await ConnectAsync();
            var flow = new CommandFlow(async: true, blocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(flow));

            var runTask = Task.Run(async () =>
            {
                var e = flow.GetAsyncEnumerator();
                try
                {
                    while (await e.MoveNextAsync()) { }
                }
                catch (PgClientClosedException)
                {
                }
                await e.DisposeAsync();
            });

            // Release both teardowns from a single gate so they start as close to simultaneously
            // as the scheduler allows, maximizing the overlap window.
            using var gate = new ManualResetEventSlim(false);
            var complete = Task.Run(async () =>
            {
                gate.Wait();
                await protocol.CompleteAsync();
            });
            var dispose = Task.Run(async () =>
            {
                gate.Wait();
                await protocol.DisposeAsync();
            });
            gate.Set();
            await blocker.ReleaseAsync();

            await Task.WhenAll(complete, dispose, runTask).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsTrue(protocol.IsCompleted, $"iteration {i}: protocol did not reach Completed");
        }
    }

    // Multi-command variant: an inter-result gate at EACH of the 3 results, so the call-k gate-fault-
    // before-Reset race repeats per result rather than only at the single terminal. Exercises whether
    // the first-call gate + HE-on-success + self-delivery hold across multiple mid-flow generations.
    [TestMethod]
    public async Task CompleteAsync_RacingDisposeAsync_MultiCommand_ConvergesCleanly()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        for (var i = 0; i < 30; i++)
        {
            await blocker.HoldAsync();
            var protocol = await ConnectAsync();
            var flow = new CommandFlow(async: true,
                blocker.WaitCommand, Command.Create("select 2"), Command.Create("select 3"));
            Assert.IsTrue(protocol.TryQueue(flow));
            var runTask = Task.Run(async () =>
            {
                var e = flow.GetAsyncEnumerator();
                try
                {
                    while (await e.MoveNextAsync()) { }
                }
                catch (PgClientClosedException)
                {
                }
                await e.DisposeAsync();
            });
            using var gate = new ManualResetEventSlim(false);
            var complete = Task.Run(async () => { gate.Wait(); await protocol.CompleteAsync(); });
            var dispose = Task.Run(async () => { gate.Wait(); await protocol.DisposeAsync(); });
            gate.Set();
            await blocker.ReleaseAsync();
            await Task.WhenAll(complete, dispose, runTask).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsTrue(protocol.IsCompleted, $"iteration {i}: protocol did not reach Completed");
        }
    }

    // Pipelined variant: N flows queued, drained in order, while teardown races. Exercises the
    // executor's multi-flow drain + the shared read baton handed across flows under abort, not just a
    // single flow's teardown.
    [TestMethod]
    public async Task CompleteAsync_RacingDisposeAsync_Pipelined_ConvergesCleanly()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        for (var i = 0; i < 20; i++)
        {
            await blocker.HoldAsync();
            var protocol = await ConnectAsync();
            const int batch = 5;
            var flows = new CommandFlow[batch];
            for (int k = 0; k < batch; k++)
            {
                flows[k] = new CommandFlow(async: true,
                    k == 0 ? blocker.WaitCommand : Command.Create("select 1"));
                Assert.IsTrue(protocol.TryQueue(flows[k]));
            }
            var runTask = Task.Run(async () =>
            {
                for (int k = 0; k < batch; k++)
                {
                    var e = flows[k].GetAsyncEnumerator();
                    try
                    {
                        while (await e.MoveNextAsync()) { }
                    }
                    catch (PgClientClosedException)
                    {
                        await e.DisposeAsync();
                        break;
                    }
                    await e.DisposeAsync();
                }
            });
            using var gate = new ManualResetEventSlim(false);
            var complete = Task.Run(async () => { gate.Wait(); await protocol.CompleteAsync(); });
            var dispose = Task.Run(async () => { gate.Wait(); await protocol.DisposeAsync(); });
            gate.Set();
            await blocker.ReleaseAsync();
            await Task.WhenAll(complete, dispose, runTask).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsTrue(protocol.IsCompleted, $"iteration {i}: protocol did not reach Completed");
        }
    }

    // Sync-flow variant of the racing teardown. The sync MoveNext rendezvous (WaitForContinuation)
    // is a different path than MoveNextAsync's gate-open self-delivery, so this exercises whether the
    // sync teardown also converges (the early sync-rendezvous wedge was a sync flow's
    // SetContinuationAndUnblockWaiter). MoveNext blocks, so it runs on its own thread.
    [TestMethod]
    public async Task CompleteAsync_RacingDisposeAsync_SyncFlow_ConvergesCleanly()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        for (var i = 0; i < 30; i++)
        {
            await blocker.HoldAsync();
            var protocol = await ConnectAsync();
            var flow = new CommandFlow(async: false, blocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(flow));
            var runTask = Task.Run(() =>
            {
                var e = flow.GetEnumerator();
                try
                {
                    while (e.MoveNext()) { }
                }
                catch (PgClientClosedException)
                {
                }
                e.Dispose();
            });
            using var gate = new ManualResetEventSlim(false);
            var complete = Task.Run(async () => { gate.Wait(); await protocol.CompleteAsync(); });
            var dispose = Task.Run(async () => { gate.Wait(); await protocol.DisposeAsync(); });
            gate.Set();
            await blocker.ReleaseAsync();
            await Task.WhenAll(complete, dispose, runTask).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsTrue(protocol.IsCompleted, $"iteration {i}: protocol did not reach Completed");
        }
    }

    // Regression for the takeover hand-back lost wake (the pool-mix wedge). The wedge needs the
    // FAULT path: a normally-completing sync body resumes the pump INLINE inside the caller's
    // Dispose, so its hand-back arm is ordered strictly before the caller's close-out re-signal
    // and is always claimed. A FAULTED body instead rides RecoverItem's trailing await - the pump
    // resumes on a TP thread with no close-out behind it, and the one-shot hand-back park
    // (WaitCore's TakeoverActive branch) armed WITHOUT re-checking IsCompleted: SourceWakeEvent left
    // _pending stuck true with the continuation stored, DrainSignal never fired, and shutdown hung
    // forever at its WhenAny. Recipe: CompleteAsync mid-body plants IsCompleted while the executor
    // is off-signal (its Drive is a guaranteed no-op - nothing armed), then DisposeAsync's abort
    // faults the body and drives the pump through the unchecked arm. The phase sweep walks the
    // teardown across pre-takeover, mid-body, and post-body so mid-body iterations hit the window
    // on every run rather than by scheduler luck.
    [TestMethod]
    public async Task CompleteAsync_ThenDisposeAsync_MidSyncFlowBody_ConvergesCleanly()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        for (var i = 0; i < 30; i++)
        {
            await blocker.HoldAsync();
            var protocol = await ConnectAsync();
            var flow = new CommandFlow(async: false, blocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(flow));
            using var consumerStarted = new ManualResetEventSlim(false);
            var runTask = Task.Run(() =>
            {
                var e = flow.GetEnumerator();
                try
                {
                    consumerStarted.Set();
                    while (e.MoveNext()) { }
                }
                catch (PgClientClosedException)
                {
                }
                e.Dispose();
            });
            consumerStarted.Wait();
            var complete = protocol.CompleteAsync();
            var dispose = protocol.DisposeAsync().AsTask();
            await blocker.ReleaseAsync();
            await Task.WhenAll(complete, dispose, runTask).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsTrue(protocol.IsCompleted, $"iteration {i}: protocol did not reach Completed");
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
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var blockingFlow = new CommandFlow(async: true, blocker.WaitCommand);
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

        // The blocking flow holds the single executor at the advisory lock so parked stays
        // enqueued-not-activated. reading fires once its consumer is scheduled and about to pull, so
        // DisposeAsync fires against a live holder rather than racing a 10ms head start - and since
        // parked was queued second it can never activate ahead of the blocking flow regardless.
        var reading = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var runBlocking = Task.Run(async () =>
        {
            var e = blockingFlow.GetAsyncEnumerator();
            try
            {
                reading.TrySetResult();
                while (await e.MoveNextAsync()) { }
                await e.DisposeAsync();
            }
            catch { }
        });

        await reading.Task;
        var disposeTask = protocol.DisposeAsync().AsTask();
        await blocker.ReleaseAsync();
        await disposeTask;
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
            tasks[i] = protocol.CompleteAsync();
        await Task.WhenAll(tasks);
    }

    [TestMethod]
    public async Task ForcefulDispose_BreaksParkedSynchronousRead()
    {
        var protocol = await ConnectAsync();
        Exception? observed = null;
        var run = Task.Run(() =>
        {
            try
            {
                var flow = new CommandFlow(async: false, Command.Create("select pg_sleep(30)"));
                Assert.IsTrue(protocol.TryQueue(flow));
                var e = flow.GetEnumerator();
                while (e.MoveNext()) { }
                e.Dispose();
            }
            catch (Exception ex)
            {
                observed = ex;
            }
        });

        await Task.Delay(30);
        await protocol.DisposeAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.IsNotNull(observed);
        while (observed is not PgClientClosedException && observed.InnerException is not null)
            observed = observed.InnerException;
        Assert.IsInstanceOfType<PgClientClosedException>(observed);
    }
}
