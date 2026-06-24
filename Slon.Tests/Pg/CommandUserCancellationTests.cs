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
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        var flow = new CommandFlow(async: true,
            Command.Create("select 1"),
            Command.Create("select pg_sleep(0.1)"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        using var cts = new CancellationTokenSource();
        var e = flow.GetAsyncEnumerator(cts.Token);

        // Consuming the first result is the deterministic park signal. The body has finished command
        // one, and the next pull reads toward pg_sleep, which cannot deliver for ~100ms, so there is no
        // sleep-and-hope window racing the cancel.
        Assert.IsTrue(await e.MoveNextAsync(cts.Token), "first command result not delivered");

        // The pull that parks on the slow read. Cancel hits it while it is outstanding. Whether or not
        // it has reached the wire read, the cancel is never lost (see the *_NeverLosesWake stress
        // tests), so the outcome is OCE, and the ~100ms-away result cannot preempt the synchronous
        // Cancel below.
        var moveNextTask = e.MoveNextAsync(cts.Token).AsTask();
        cts.Cancel();

        var oce = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNextTask.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.AreEqual(cts.Token, oce.CancellationToken);
        await e.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }
}
