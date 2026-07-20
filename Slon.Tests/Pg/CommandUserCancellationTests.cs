using Slon.Pg;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// When the consumer's CancellationToken (passed to MoveNextAsync or GetAsyncEnumerator) fires, the
// awaiting MoveNextAsync surfaces an OperationCanceledException whose token matches the caller's. OCE
// is reserved for the caller's own token; PgClientClosedException is reserved for protocol shutdown.
// I/O is not cancelled: the body keeps reading and drains the wire to RFQ, leaving the protocol usable.
[TestClass]
public class CommandUserCancellationTests
{
    // Token is already cancelled before MoveNextAsync is called: the first MoveNextAsync surfaces OCE.
    [TestMethod]
    public async Task UserCt_PreFired_FirstMoveNextSurfacesOce()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        var flow = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var e = flow.GetAsyncEnumerator(cts.Token);
        var oce = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await e.MoveNextAsync(cts.Token));
        Assert.AreEqual(cts.Token, oce.CancellationToken);
        await e.DisposeAsync();
    }

    // CT fires after the first result has been delivered: the next MoveNextAsync surfaces OCE, and a
    // follow-up flow confirms recovery drained the wire and the protocol stays usable.
    [TestMethod]
    public async Task UserCt_FiresAfterFirstResult_NextMoveNextSurfacesOce_ProtocolUsable()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        var flow = new CommandFlow(async: true,
            Command.Create("select 'one'"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        using var cts = new CancellationTokenSource();
        var e = flow.GetAsyncEnumerator(cts.Token);

        Assert.IsTrue(await e.MoveNextAsync(cts.Token), "first command result not delivered");

        cts.Cancel();
        var oce = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await e.MoveNextAsync(cts.Token));
        Assert.AreEqual(cts.Token, oce.CancellationToken);
        await e.DisposeAsync();
    }

    // CT fires while an outstanding MoveNextAsync is parked on a slow read: the parked pull surfaces
    // OCE, the body finishes the in-flight read and drains the rest to RFQ, and the follow-up command
    // confirms the protocol stays usable.
    [TestMethod]
    [DoNotParallelize]
    public async Task UserCt_FiresMidRead_SurfacesOce_ProtocolUsable()
    {
        var advisoryLock = Random.Shared.NextInt64(1, long.MaxValue);
        await using var blocker = await PgTestPool.NewIsolatedAsync();
        await PgTestPool.RunAsync(blocker, $"select pg_advisory_lock({advisoryLock})");

        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        var flow = new CommandFlow(async: true,
            Command.Create("select 1") with { WithSync = true },
            Command.Create($"select pg_advisory_lock({advisoryLock})"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        using var cts = new CancellationTokenSource();
        var e = flow.GetAsyncEnumerator(cts.Token);

        // The first command's Sync makes its result observable before PostgreSQL enters the blocked
        // second command. The lock then prevents suite scheduling from letting result two win.
        Assert.IsTrue(await e.MoveNextAsync(cts.Token), "first command result not delivered");

        var moveNextTask = e.MoveNextAsync(cts.Token).AsTask();
        cts.Cancel();
        await PgTestPool.RunAsync(blocker, $"select pg_advisory_unlock({advisoryLock})");

        var oce = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNextTask.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.AreEqual(cts.Token, oce.CancellationToken);
        await e.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }
}
