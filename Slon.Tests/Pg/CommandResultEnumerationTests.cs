using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

[TestClass]
public class CommandResultEnumerationTests
{
    [ConnectionCreatingTestMethod]
    public async Task DescribeOnlyErrorSurfacesWhenInspectingTheResult()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(new CommandFlow(async: true,
            Command.Create("THIS IS NOT SQL") with { DescribeOnly = true }));
        var enumerator = flow.GetAsyncEnumerator();

        Assert.IsTrue(await enumerator.MoveNextAsync());
        Assert.ThrowsExactly<PgErrorException>(() => _ = enumerator.Current.HasRows);
        Assert.IsFalse(await enumerator.MoveNextAsync());
        await enumerator.DisposeAsync();
    }

    [ConnectionCreatingTestMethod]
    public async Task AsyncFlow_CanSwitchToSynchronousResultAdvancement()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(new CommandFlow(async: true,
            Command.Create("select 1"), Command.Create("select 2")));
        var e = flow.GetAsyncEnumerator();

        Assert.IsTrue(await e.MoveNextAsync());
        await e.Current.DisposeAsync();
        Assert.IsTrue(e.MoveNext());
        e.Current.Dispose();
        Assert.IsFalse(e.MoveNext());
        e.Dispose();
    }

    [ConnectionCreatingTestMethod]
    public async Task SuppressedResults_AreDrainedButNotPublished()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(new CommandFlow(async: true,
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

    [ConnectionCreatingTestMethod]
    public async Task SuppressedResult_ErrorStillFaultsTheFlow()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(new CommandFlow(async: true,
            Command.Create("select 1") with { WithSync = true },
            Command.Create("SLECT 2") with { SuppressEnumeration = true, WithSync = true },
            Command.Create("select 3") with { WithSync = true }));
        var e = flow.GetAsyncEnumerator();

        Assert.IsTrue(await e.MoveNextAsync());
        await e.Current.DisposeAsync();
        await Assert.ThrowsExactlyAsync<PgErrorException>(async () => await e.MoveNextAsync());
        await Assert.ThrowsExactlyAsync<PgErrorException>(async () => await e.DisposeAsync());
    }

    [ConnectionCreatingTestMethod]
    [DataRow(true, DisplayName = "async")]
    [DataRow(false, DisplayName = "sync")]
    public async Task ErrorWithoutSync_SkipsCommandsThroughRfq(bool async)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(new CommandFlow(async,
            Command.Create("SLECT 1"), Command.Create("select 2"), Command.Create("select 3")));
        var e = async ? flow.GetAsyncEnumerator() : flow.GetEnumerator();

        Assert.IsTrue(async ? await e.MoveNextAsync() : e.MoveNext());
        var result = e.Current;
        if (async)
            await result.DisposeAsync();
        else
            result.Dispose();
        Assert.ThrowsExactly<PgErrorException>(() => result.GetCommandComplete());

        Assert.IsFalse(async ? await e.MoveNextAsync() : e.MoveNext());
        await e.DisposeAsync();
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DataRow(true, DisplayName = "async")]
    [DataRow(false, DisplayName = "sync")]
    public async Task ErrorWithoutSync_ResumesAfterInternalSync(bool async)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(new CommandFlow(async,
            Command.Create("SLECT 1"),
            Command.Create("select 2") with { WithSync = true },
            Command.Create("select 3")));
        var e = async ? flow.GetAsyncEnumerator() : flow.GetEnumerator();

        Assert.IsTrue(async ? await e.MoveNextAsync() : e.MoveNext());
        var failed = e.Current;
        if (async)
            await failed.DisposeAsync();
        else
            failed.Dispose();
        Assert.ThrowsExactly<PgErrorException>(() => failed.GetCommandComplete());

        Assert.IsTrue(async ? await e.MoveNextAsync() : e.MoveNext());
        Assert.AreEqual(2, e.Current.GetMetadata().CommandIndex);
        if (async)
            await e.Current.DisposeAsync();
        else
            e.Current.Dispose();

        Assert.IsFalse(async ? await e.MoveNextAsync() : e.MoveNext());
        await e.DisposeAsync();
        await PgTestPool.RunAsync(protocol, "select 1");
    }
}
