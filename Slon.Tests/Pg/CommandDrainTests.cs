using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// CommandFlow body-side drain mode: when the consumer abandons the result stream
// (Enumerator.Dispose) or the protocol fires StoppingToken (graceful shutdown), the body
// skips the user-handoff for remaining commands and drains the wire to RFQ on its own.
// Distinct from RecoveryDrainFlow (protocol-level recovery drain) and pipeline drain
// (framework-level CompleteAsync sweep) - "drain" is overloaded in this codebase, this one
// is the per-CommandFlow consumer-abandonment drain specifically.
[TestClass]
public class CommandDrainTests
{
    // Existing behavior verified: walking the outer enumerator to NextResult drives the
    // body to dispose the current ResultMessageEnumerator (draining DataRows + CC) before
    // yielding the next command's result. Single resultset drained body-side, next result
    // still flows to consumer.
    [TestMethod]
    public async Task NextCommandResult_DrainsCurrentCommandBodySide()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        var flow = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 100)"),
            Command.Create("select 'second'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = flow.GetAsyncEnumerator();

        Assert.IsTrue(await e.MoveNextAsync(), "first command result not delivered");
        // Don't read any rows from the first command - just advance the outer enumerator.
        // Body's dispose-side drain consumes the remaining DataRows + CommandComplete before
        // yielding the second command's result.
        Assert.IsTrue(await e.MoveNextAsync(), "second command result not delivered after drain of first");
        Assert.IsFalse(await e.MoveNextAsync(), "third call should return false (no more results)");
        await e.DisposeAsync();

        // Connection is at clean RFQ - prove via a follow-up query.
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    // Full drain via consumer-gone: Enumerator.DisposeAsync mid-batch transitions the body
    // into drain mode for ALL remaining commands. Body silently consumes their wire bytes
    // (Parse/Bind/Describe responses + DataRows + CommandComplete each) to the trailing
    // RFQ without yielding anything else to the consumer. Connection reaches clean RFQ.
    [TestMethod]
    public async Task ConsumerDispose_MidBatch_BodyDrainsRemaining_ConnectionUsable()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        var flow = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = flow.GetAsyncEnumerator();
        Assert.IsTrue(await e.MoveNextAsync(), "first command result not delivered");

        // Dispose mid-batch. Without drain mode, the body would wait forever for the consumer
        // to call MoveNext to drive forward through 2 more commands. With drain mode, the
        // body sees IsConsumerGone, drains commands 1-3 + RFQ, completes the move-next source,
        // and DisposeAsync returns once the wire is clean.
        await e.DisposeAsync();

        // Connection is at clean RFQ - prove via a follow-up query on the same protocol.
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    // Full drain via StoppingToken: protocol's graceful CompleteAsync fires StoppingToken,
    // the body sees it at the next per-command boundary and drains the same way. Verifies
    // the OR-converged drain transition: either input (consumer-gone or StoppingToken) is
    // sufficient to start the drain.
    [TestMethod]
    public async Task StoppingToken_MidBatch_BodyDrainsRemaining_FlowCompletesCleanly()
    {
        // NewIsolated because CompleteAsync makes the protocol unusable afterwards.
        var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = flow.GetAsyncEnumerator();
        Assert.IsTrue(await e.MoveNextAsync(), "first command result not delivered");

        // Fire graceful shutdown. StoppingToken cancels; the framework's drain awaits the
        // in-flight flow's completion. Body sees StoppingToken at the next boundary and
        // skips the user-handoff for the remaining commands.
        var completeTask = protocol.CompleteAsync();

        // Drive the consumer side forward. The body, parked on the gate from MoveNextAsync
        // returning true above, wakes here, sees StoppingToken, and skips to drain mode.
        // Body's SetResult(null) at loop exit makes this final call return false.
        Assert.IsFalse(await e.MoveNextAsync(), "drain should signal completion to consumer");
        await e.DisposeAsync();

        // Graceful shutdown completes cleanly - the body drained without faulting.
        await completeTask.AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }
}
