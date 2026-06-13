using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Focused repro for the "PgDecoder reads ActivatedFlow past tenure" race surfaced by
// method-parallel Release runs against RecoveryTests.MultipleRfqsOutstanding_DrainsAllBeforeNext.
// Hammers two patterns inside a single test method so the loop runs in one process — much faster
// per attempt than restarting the test runner per iteration.
//
// Iterations override via DRAGHI_STRESS_ITERATIONS (default 50). Default is a fast regression
// guard now the race is fixed; explicit stress runs override (e.g. DRAGHI_STRESS_ITERATIONS=2000).
// Each test runs isolated against its own protocol so a failure inside the loop doesn't poison
// sibling tests.
[TestClass]
public class RecoveryStressTests
{
    static int Iterations
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS");
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
        var protocol = await PgTestPool.NewIsolatedAsync();
        try
        {
            for (int i = 0; i < Iterations; i++)
            {
                var faulting = new RecoveryTests.FaultingFlow(async: true, RecoveryTests.FaultPhase.PreReturn, RecoveryTests.WriteShape.MultipleSyncsNoFlush);
                Assert.IsTrue(protocol.TryQueue(faulting));

                for (int j = 0; j < 5; j++)
                    await RunAsync(protocol, "select 1");
            }
        }
        finally { await protocol.CompleteAsync(); }
    }

    // Same stress without recovery in the loop - just rapid sequential pipelined reads. If this
    // ALSO fails, the bug isn't recovery-specific: it's the generic completion/activation
    // ordering of back-to-back CommandFlows. If this stays green while the recovery version
    // fails, the bug is gated by something recovery uniquely does.
    [TestMethod]
    public async Task Stress_SequentialReads_NoRecovery()
    {
        var protocol = await PgTestPool.NewIsolatedAsync();
        try
        {
            for (int i = 0; i < Iterations * 5; i++)
                await RunAsync(protocol, "select 1");
        }
        finally { await protocol.CompleteAsync(); }
    }
}
