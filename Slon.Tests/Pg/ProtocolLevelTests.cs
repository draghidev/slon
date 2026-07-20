using System.Diagnostics;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Pg layer = PgClientProtocol direct. Everything above (PgConnection, ConnectionPool,
// AdoConnectionProxy) lives in its own test file at the Slon.Tests root. These tests use the
// shared assembly-scoped PgTestPool because they complete their flows cleanly; a poisoned
// protocol can't end up here without a test bug.
[TestClass]
public class ProtocolLevelTests
{
    [TestMethod]
    public async Task Sync_OnRawProtocol_Completes()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        await PgTestPool.RunSync(lease.Protocol, "select 1");
    }

    [TestMethod]
    public async Task Async_OnRawProtocol_Completes()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        await PgTestPool.RunAsync(lease.Protocol, "select 1");
    }

    // OnFlowRfq bookkeeping: the wire's transaction status is tracked on a SINGLE protocol field, routed
    // from every flow's terminating ReadyForQuery - from both the outer protocol Control (the autocommit
    // select) AND the inner-scope Control (the BEGIN/COMMIT subflows), proving no per-Control duplication.
    // A transaction MUST be scoped in an exclusive flow (holding the wire); running BEGIN/COMMIT as
    // separate flows on the multiplexed protocol would poison the pipeline.
    [TestMethod]
    [DoNotParallelize]
    public async Task TransactionStatus_TrackedAcrossOuterFlowAndExclusiveScope()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var p = lease.Protocol;

        // Outer Control: an autocommit command's RFQ is Idle.
        await PgTestPool.RunAsync(p, "select 1");
        Assert.AreEqual(TransactionStatus.Idle, p.TransactionStatus, "after autocommit select (outer Control)");

        // Inner Control: a transaction held exclusively, BEGIN/COMMIT as subflows on the inner pipeline.
        var scope = p.BeginExclusiveScope(async: true);
        await scope.HandoffReady;
        try
        {
            await Drain(scope.Queue(new CommandFlow(async: true, Command.Create("BEGIN"))));
            Assert.AreEqual(TransactionStatus.Transaction, p.TransactionStatus, "after BEGIN (inner Control)");

            await Drain(scope.Queue(new CommandFlow(async: true, Command.Create("COMMIT"))));
            Assert.AreEqual(TransactionStatus.Idle, p.TransactionStatus, "after COMMIT (inner Control)");
        }
        finally
        {
            await scope.CompleteScopeAsync();
        }

        static async Task Drain(CommandFlow cmd)
        {
            var e = cmd.GetAsyncEnumerator();
            while (await e.MoveNextAsync()) { }
            await e.DisposeAsync();
        }
    }

    // Many sync flows in a tight loop. Exercises the handoff state machine across repeated
    // cycles: HandoffSlot / HandoffActive / SyncHead / SyncTail / ParkedMres / VTS Reset all
    // need to return to rest between iterations. A leak in any would deadlock or skip results
    // within a few hundred runs.
    [TestMethod]
    public async Task RepeatedSync_OnRawProtocol_Completes()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        for (int i = 0; i < 200; i++)
            await PgTestPool.RunSync(lease.Protocol, "select 1");
    }

    // Many async flows in a tight loop. The post-handoff drain path doesn't apply here (no
    // sync producers), but the async-path VTS Reset / wake / GetResult cycle gets exercised.
    [TestMethod]
    public async Task RepeatedAsync_OnRawProtocol_Completes()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        for (int i = 0; i < 200; i++)
            await PgTestPool.RunAsync(lease.Protocol, "select 1");
    }

    // Alternating sync/async on the same protocol. Exercises the HandoffActive gate's
    // engage/disengage cycle and the executor's transition between the inline takeover path
    // and the normal async-wake path. Any state mishandled across the boundary would surface
    // here as a hang or incorrect ordering.
    [TestMethod]
    public async Task AlternatingSyncAsync_OnRawProtocol_Completes()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        for (int i = 0; i < 50; i++)
        {
            if ((i & 1) == 0)
                await PgTestPool.RunSync(lease.Protocol, "select 1");
            else
                await PgTestPool.RunAsync(lease.Protocol, "select 1");
        }
    }

    // Async flow followed by sync flow on the SAME protocol. Verifies the executor-busy arm
    // of EnqueueSyncWithHandoff: the sync caller's WaitForParked has to actually block (no
    // ParkedMres set yet because the executor is mid-flight), then wake when the executor
    // drains and reaches its park point. No TP enqueue is emitted by the handoff itself.
    [TestMethod]
    public async Task SyncAfterAsync_SameProtocol_Completes()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        await PgTestPool.RunAsync(lease.Protocol, "select 1");
        await PgTestPool.RunSync(lease.Protocol, "select 1");
    }

    // A blocked async command is active on the protocol while a sync command queues behind it.
    // Exposes (and previously triggered) a bug where the executor, busy processing the async
    // flow, would finish that flow, loop into MoveNextAsync, snipe HandoffSlot on its own
    // (non-caller) thread, process the sync flow on TP, then leave the sync caller stranded
    // waiting for the next park. The sync caller's eventual SetResult would dispatch the
    // executor's continuation onto a stale/empty state and RetireItem would NRE during
    // protocol shutdown.
    //
    // Fix: HandoffAcked gate on TryTakeHandoff. The executor cannot pick up HandoffSlot until
    // the sync caller has cleared WaitForParked and is about to SetResult inline.
    [TestMethod]
    public async Task SyncWhileAsyncInFlight_SameProtocol_BothComplete()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        await PgTestPool.RunAsync(protocol, "select 1"); // warm

        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var slow = new CommandFlow(async: true,
            Command.Create("select 1") with { WithSync = true }, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(slow));
        var slowEnum = slow.GetAsyncEnumerator();
        Assert.IsTrue(await slowEnum.MoveNextAsync());
        var slowTask = DrainAsync(slowEnum);

        var sync = new CommandFlow(async: false, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(sync));
        var syncTask = Task.Run(() =>
        {
            var e = sync.GetEnumerator();
            while (e.MoveNext()) { }
            e.Dispose();
        });

        await blocker.ReleaseAsync();
        await syncTask.WaitAsync(TimeSpan.FromSeconds(2));

        await slowTask;
        await slowEnum.DisposeAsync();
    }

    static async Task DrainAsync(CommandFlow.Enumerator enumerator)
    {
        while (await enumerator.MoveNextAsync()) { }
    }
}
