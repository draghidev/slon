using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

[TestClass]
public sealed class CommandResultCollectTests
{
    [ConnectionCreatingTestMethod]
    public async Task CollectsValueRowsAndLeavesWireReusable()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var values = new List<int>();
        var results = protocol.Queue(new CommandFlow(async: true,
            Command.Create("select generate_series(1, 100)"))).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync());
        await results.Current.CollectAsync(values,
            static (items, row) => items.Add(row.GetInt32(0)));
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();

        CollectionAssert.AreEqual(Enumerable.Range(1, 100).ToArray(), values);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task BuffersAStreamingRowBeforeCallingCollector()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync(
            options => options.DataRowStreamingThreshold = 1);
        var values = new List<(int Id, string Text)>();
        var results = protocol.Queue(new CommandFlow(async: true,
            Command.Create("select 42, repeat('x', 100000)"))).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync());
        await results.Current.CollectAsync(values,
            static (items, row) => items.Add((row.GetInt32(0), row.GetValue<string>(1))));
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();

        Assert.AreEqual(1, values.Count);
        Assert.AreEqual(42, values[0].Id);
        Assert.AreEqual(100000, values[0].Text.Length);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task ResultBufferingRetainsCollectedRowsPastTheBufferingBarrier()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync(
            options => options.DataRowStreamingThreshold = 1);
        var values = new List<(int Id, ReadOnlyMemory<byte> Message)>();
        var results = protocol.Queue(new CommandFlow(async: true, Command.Create(
            "select i, i::text || repeat('x', 20000) " +
            "from generate_series(1, 3) as i"))).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync());
        results.Current.EnableResultBuffering();
        await results.Current.CollectAsync(values,
            static (rows, row) => rows.Add((
                row.GetInt32(0), row.BorrowFieldMemory(1))));

        Assert.AreEqual(3, values.Count);
        for (var i = 0; i < values.Count; i++)
        {
            Assert.AreEqual(i + 1, values[i].Id);
            Assert.AreEqual((i + 1) + new string('x', 20000),
                System.Text.Encoding.UTF8.GetString(values[i].Message.Span));
        }

        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task CollectorFailureDrainsBeforeRethrowing()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var callbacks = 0;
        var results = protocol.Queue(new CommandFlow(async: true,
            Command.Create("select generate_series(1, 100)"))).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync());
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await results.Current.CollectAsync(0, (_, _) =>
            {
                callbacks++;
                throw new InvalidOperationException("collector failure");
            }));
        Assert.AreEqual("collector failure", exception.Message);
        Assert.AreEqual(1, callbacks);
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task CannotCollectAfterRowEnumerationStarted()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var results = protocol.Queue(new CommandFlow(async: true,
            Command.Create("select generate_series(1, 2)"))).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync());
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await results.Current.CollectAsync(0, static (_, _) => { }));
        await rows.DisposeAsync();
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();
    }
}
