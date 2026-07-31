using Slon.Pg;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

[TestClass]
public class QueuedFlowCompositionTests
{
    static async Task DrainAsync(CommandFlow flow)
    {
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    static async Task DrainExpecting(CommandFlow flow, bool async, int expectedResults)
    {
        var e = async ? flow.GetAsyncEnumerator() : flow.GetEnumerator();
        for (var i = 0; i < expectedResults; i++)
            Assert.IsTrue(async ? await e.MoveNextAsync() : e.MoveNext());
        Assert.IsFalse(async ? await e.MoveNextAsync() : e.MoveNext());
        await e.DisposeAsync();
    }

    [TestMethod]
    [DataRow(true, DisplayName = "async")]
    [DataRow(false, DisplayName = "sync")]
    public async Task OuterPipeline_QueueThenDrain(bool async)
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var flows = new CommandFlow[8];
        for (var i = 0; i < flows.Length; i++)
            flows[i] = lease.Protocol.Queue(new CommandFlow(async, Command.Create("select 1")));
        foreach (var flow in flows)
            await DrainExpecting(flow, async, 1);
    }

    [TestMethod]
    [DataRow(true, DisplayName = "async")]
    [DataRow(false, DisplayName = "sync")]
    public async Task MultiCommandFlows_QueueThenDrain(bool async)
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var flows = new CommandFlow[6];
        for (var i = 0; i < flows.Length; i++)
            flows[i] = lease.Protocol.Queue(new CommandFlow(async,
                Command.Create("select 1"), Command.Create("select 2"), Command.Create("select 3")));
        foreach (var flow in flows)
            await DrainExpecting(flow, async, 3);
    }

    [TestMethod]
    public async Task ExclusiveScope_QueueThenDrain()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var scope = lease.Protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;
        var flows = new CommandFlow[8];
        for (var i = 0; i < flows.Length; i++)
            flows[i] = scope.Queue(new CommandFlow(async: true, Command.Create("select 1")));
        foreach (var flow in flows)
            await DrainAsync(flow);
        await scope.CompleteScopeAsync();
    }

    [TestMethod]
    public async Task SyncFlows_AroundExclusiveScope_QueueBeforeDrain()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();

        var predecessor = protocol.Queue(new CommandFlow(async: false, Command.Create("select 1")));
        var scope = protocol.BeginExclusiveScope(async: true);
        var successor = protocol.Queue(new CommandFlow(async: false, Command.Create("select 2")));

        await DrainExpecting(predecessor, async: false, expectedResults: 1);
        await scope.HandoffReady;
        await scope.CompleteScopeAsync();
        await DrainExpecting(successor, async: false, expectedResults: 1);
    }
}
