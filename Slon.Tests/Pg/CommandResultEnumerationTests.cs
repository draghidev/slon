using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

[TestClass]
public class CommandResultEnumerationTests
{
    [TestMethod]
    public async Task AsyncFlow_CanSwitchToSynchronousResultAdvancement()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var flow = lease.Protocol.Queue(new CommandFlow(async: true,
            Command.Create("select 1"), Command.Create("select 2")));
        var e = flow.GetAsyncEnumerator();

        Assert.IsTrue(await e.MoveNextAsync());
        await e.Current.DisposeAsync();
        Assert.IsTrue(e.MoveNext());
        e.Current.Dispose();
        Assert.IsFalse(e.MoveNext());
        e.Dispose();
    }

    [TestMethod]
    public async Task SuppressedResults_AreDrainedButNotPublished()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var flow = lease.Protocol.Queue(new CommandFlow(async: true,
            Command.Create("select 1") with { SuppressEnumeration = true },
            Command.Create("select 2"),
            Command.Create("select 3") with { SuppressEnumeration = true },
            Command.Create("select 4")));
        var e = flow.GetAsyncEnumerator();

        Assert.IsTrue(await e.MoveNextAsync());
        Assert.AreEqual(1, e.Current.GetMetadata().CommandIndex);
        await e.Current.DisposeAsync();
        Assert.IsTrue(await e.MoveNextAsync());
        Assert.AreEqual(3, e.Current.GetMetadata().CommandIndex);
        await e.Current.DisposeAsync();
        Assert.IsFalse(await e.MoveNextAsync());
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task SuppressedResult_ErrorStillFaultsTheFlow()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var flow = lease.Protocol.Queue(new CommandFlow(async: true,
            Command.Create("select 1") with { WithSync = true },
            Command.Create("SLECT 2") with { SuppressEnumeration = true, WithSync = true },
            Command.Create("select 3") with { WithSync = true }));
        var e = flow.GetAsyncEnumerator();

        Assert.IsTrue(await e.MoveNextAsync());
        await e.Current.DisposeAsync();
        await Assert.ThrowsExactlyAsync<PostgresException>(async () => await e.MoveNextAsync());
        await Assert.ThrowsExactlyAsync<PostgresException>(async () => await e.DisposeAsync());
    }
}
