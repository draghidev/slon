using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Focused repro for the "PgDecoder reads ActivatedFlow past tenure" race surfaced by
// method-parallel Release runs against RecoveryTests.MultipleRfqsOutstanding_DrainsAllBeforeNext.
// Hammers two patterns inside a single test method so the loop runs in one process — much faster
// per attempt than restarting the test runner per iteration.
//
// Iterations override via SLON_STRESS_ITERATIONS (default 50). Default is a fast regression
// guard now the race is fixed; explicit stress runs override (e.g. SLON_STRESS_ITERATIONS=2000).
// Each test runs isolated against its own protocol so a failure inside the loop doesn't poison
// sibling tests.
[TestClass]
public class RecoveryStressTests
{
    static int Iterations
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("SLON_STRESS_ITERATIONS");
            return int.TryParse(raw, out var n) && n > 0 ? n : 50;
        }
    }

    static async Task RunAsync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    // Direct recursion of the original failure shape: recover from a multi-RFQ outstanding
    // failure, then run several sequential CommandFlows post-recovery. If the bug is in the
    // post-recovery activation/completion ordering, this iterates fast enough to hit it.
    [TestMethod]
    public async Task Stress_RecoveryThenSequentialReads()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        for (int i = 0; i < Iterations; i++)
        {
            var faulting = new RecoveryTests.FaultingFlow(async: true, RecoveryTests.FaultPhase.PreReturn, RecoveryTests.WriteShape.MultipleSyncsNoFlush);
            Assert.IsTrue(protocol.TryQueue(faulting));

            for (int j = 0; j < 5; j++)
                await RunAsync(protocol, "select 1");
        }
    }

    // Same stress without recovery in the loop - just rapid sequential pipelined reads. If this
    // ALSO fails, the bug isn't recovery-specific: it's the generic completion/activation
    // ordering of back-to-back CommandFlows. If this stays green while the recovery version
    // fails, the bug is gated by something recovery uniquely does.
    [TestMethod]
    public async Task Stress_SequentialReads_NoRecovery()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        for (int i = 0; i < Iterations * 5; i++)
            await RunAsync(protocol, "select 1");
    }

    // Recovery dispatch OVERLAPPING the pump's next dispatch - the execute-promise single-pump edge
    // case. A pipeline-task fault drives recovery; when it surfaces as a committed-waiter failure it
    // routes through RecoverWaiter on the advancer chain (a second dispatch stream, the path that
    // collided). For a waiter failure the failed flow's write window is already closed, so recovery
    // is a pure read-drain of the inherited RFQs - no Sync, we are not allowed to write anymore.
    // Queuing a follow-on flow back-to-back makes the pump dispatch it while recovery is in flight.
    // Before recovery got its own execute promise, the two ExecuteCore.Start calls raced the one
    // pooled promise => "already executing". This asserts the follow-on flow still completes (no
    // collision) and the protocol stays usable after each resync. The faulting flow completes with
    // its own injected fault; we observe-and-discard it.
    [TestMethod]
    public async Task Stress_RecoveryOverlapsNextDispatch()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        for (int i = 0; i < Iterations; i++)
        {
            var faulting = new RecoveryTests.FaultingFlow(async: true, RecoveryTests.FaultPhase.PipelineTask, RecoveryTests.WriteShape.QueryNoFlush);
            Assert.IsTrue(protocol.TryQueue(faulting));

            // Queue a normal flow immediately so the pump dispatches it while the faulting flow's
            // recovery runs on the advancer chain - the dispatch/recovery overlap on one promise.
            var follow = new CommandFlow(async: true, Command.Create("select 1"));
            Assert.IsTrue(protocol.TryQueue(follow));

            // The follow-on must complete cleanly (no tenure collision, clean wire after resync).
            var e = follow.GetAsyncEnumerator();
            try
            {
                while (await e.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10))) { }
            }
            catch (TimeoutException) { Assert.Fail($"iter {i}: follow-on flow hung (dispatch collided with recovery)."); }
            finally { await e.DisposeAsync(); }

            // The faulting flow completes with its injected fault; observe-and-discard.
            try { await faulting.WaitForComplete().AsTask().WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (TimeoutException) { Assert.Fail($"iter {i}: faulting flow never completed (recovery stranded)."); }
            catch { /* the injected fault - expected */ }

            // Protocol still at RFQ and reusable after the resync.
            await RunAsync(protocol, "select 1");
        }
    }
}
