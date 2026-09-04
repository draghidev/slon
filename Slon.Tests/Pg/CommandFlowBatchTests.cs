using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Text;

namespace Slon.Tests.Pg;

[TestClass]
public class CommandFlowBatchTests
{
    static async Task<CommandDescriptor> Prepare(PgClientProtocol protocol, string sql, EncodedCString name)
    {
        var flow = protocol.Queue(new CommandFlow(async: true,
            Command.Create(sql, commandName: name) with { DescribeOnly = true }));
        var results = flow.GetAsyncEnumerator();
        CommandDescriptor descriptor = default;
        while (await results.MoveNextAsync())
            descriptor = results.Current.GetMetadata().ToPreparedDescriptor();
        await results.DisposeAsync();
        return descriptor;
    }

    [ConnectionCreatingTestMethod]
    public async Task Batch_TwoPreparedCommands_EnumeratesResultsInOrder()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var first = await Prepare(protocol, "select 1::int4", "batch_1");
        var second = await Prepare(protocol, "select 2::int4", "batch_2");
        var results = protocol.Queue(new CommandFlow(async: true, new CommandList(
            Command.Create(first), Command.Create(second)))).GetAsyncEnumerator();
        var values = new List<int>();
        while (await results.MoveNextAsync())
        {
            var rows = results.Current.GetAsyncEnumerator();
            while (await rows.MoveNextAsync())
                values.Add(rows.Current.GetValue<int>(0));
            await rows.DisposeAsync();
        }
        await results.DisposeAsync();
        CollectionAssert.AreEqual(new[] { 1, 2 }, values);
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [ConnectionCreatingTestMethod]
    public async Task Batch_DisposeBeforeRead_DrainsEveryCommand()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var first = await Prepare(protocol, "select generate_series(1, 20)", "batch_dispose_1");
        var second = await Prepare(protocol, "select generate_series(1, 20)", "batch_dispose_2");
        var results = protocol.Queue(new CommandFlow(async: true, new CommandList(
            Command.Create(first), Command.Create(second)))).GetAsyncEnumerator();
        await results.DisposeAsync();
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [ConnectionCreatingTestMethod]
    public async Task Batch_ErrorWithoutBarrier_DiscardsSuccessorThroughFinalSync()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var results = protocol.Queue(new CommandFlow(async: true, new CommandList(
            Command.Create("select 1 / 0"), Command.Create("select 2::int4"))))
            .GetAsyncEnumerator();
        Assert.IsTrue(await results.MoveNextAsync());
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsFalse(await rows.MoveNextAsync());
        await rows.DisposeAsync();
        Assert.ThrowsExactly<PgErrorException>(() => results.Current.GetCommandComplete());
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [ConnectionCreatingTestMethod]
    public async Task Batch_ErrorBarrier_AllowsFollowingCommand()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var results = protocol.Queue(new CommandFlow(async: true, new CommandList(
            Command.Create("select 1 / 0") with { WithSync = true },
            Command.Create("select 2::int4"))))
            .GetAsyncEnumerator();
        Assert.IsTrue(await results.MoveNextAsync());
        var failedRows = results.Current.GetAsyncEnumerator();
        Assert.IsFalse(await failedRows.MoveNextAsync());
        await failedRows.DisposeAsync();
        Assert.ThrowsExactly<PgErrorException>(() => results.Current.GetCommandComplete());
        Assert.IsTrue(await results.MoveNextAsync());
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        Assert.AreEqual(2, rows.Current.GetValue<int>(0));
        Assert.IsFalse(await rows.MoveNextAsync());
        await rows.DisposeAsync();
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [ConnectionCreatingTestMethod]
    public async Task Execution_SyncTwoPreparedCommands_EnumeratesResultsInOrder()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var first = await Prepare(protocol, "select 1::int4", "execution_sync_1");
        var second = await Prepare(protocol, "select 2::int4", "execution_sync_2");
        var results = protocol.Queue(new CommandFlow(async: false, new CommandList(
            Command.Create(first), Command.Create(second)))).GetEnumerator();
        var values = new List<int>();
        while (results.MoveNext())
        {
            using var rows = results.Current.GetEnumerator();
            while (rows.MoveNext())
                values.Add(rows.Current.GetValue<int>(0));
        }
        results.Dispose();
        CollectionAssert.AreEqual(new[] { 1, 2 }, values);
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [ConnectionCreatingTestMethod]
    public async Task Execution_SyncDisposeBeforeRead_DrainsEveryCommand()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var first = await Prepare(protocol, "select generate_series(1, 20)", "execution_sync_dispose_1");
        var second = await Prepare(protocol, "select generate_series(1, 20)", "execution_sync_dispose_2");
        var results = protocol.Queue(new CommandFlow(async: false, new CommandList(
            Command.Create(first), Command.Create(second)))).GetEnumerator();
        results.Dispose();
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [ConnectionCreatingTestMethod]
    public async Task Execution_SyncErrorBarrier_AllowsFollowingCommand()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var results = protocol.Queue(new CommandFlow(async: false, new CommandList(
            Command.Create("select 1 / 0") with { WithSync = true },
            Command.Create("select 2::int4")))).GetEnumerator();
        Assert.IsTrue(results.MoveNext());
        using (var failedRows = results.Current.GetEnumerator())
            Assert.IsFalse(failedRows.MoveNext());
        Assert.ThrowsExactly<PgErrorException>(() => results.Current.GetCommandComplete());
        Assert.IsTrue(results.MoveNext());
        using (var rows = results.Current.GetEnumerator())
        {
            Assert.IsTrue(rows.MoveNext());
            Assert.AreEqual(2, rows.Current.GetValue<int>(0));
            Assert.IsFalse(rows.MoveNext());
        }
        Assert.IsFalse(results.MoveNext());
        results.Dispose();
        await PgTestPool.RunAsync(protocol, "select 3");
    }

}


