using System.Diagnostics;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;
using Slon.Pools;
using Slon.Tests.Pg;
using Slon.Transport;

namespace Slon.Tests;

// Wraps a pooled PgConnection in the proxy, which adds: per-proxy CommandTracker,
// pipeline-depth counter, exclusive-scope flag, completion-action wiring. If cross-conn
// blocking appears at this layer, the proxy itself (or its tracker integration) is the
// coupling.
[TestClass]
public class AdoConnectionProxyTests
{
    static PgClientOptions NewOptions() => new()
    {
        EndPoint = TestEndPoint.Default,
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
    };

    static ConnectionPool<PgConnection> NewPool(int maxConnections = 4, CommandTracker? sharedTracker = null)
    {
        var options = NewOptions();
        var transportFactory = SocketStreamConnection.CreateFactory(options.EndPoint);
        var factory = new PgConnectionFactory(options, transportFactory, tracker: sharedTracker);
        return new ConnectionPool<PgConnection>(factory, new() { MaxConnections = maxConnections });
    }

    sealed class StubAdoConnection : IAdoConnection
    {
        public void Break(Exception exception) { }
    }

    static AdoConnectionProxy WrapInProxy(PgConnection pg, CommandTracker? sharedTracker = null) =>
        new(pg, new StubAdoConnection(), autoPrepare: false, tracker: sharedTracker);

    static async Task RunSyncOn(AdoConnectionProxy proxy, string sql)
    {
        var flow = new CommandFlow(async: false, Command.Create(sql));
        proxy.Enqueue(flow);
        var e = flow.GetEnumerator();
        while (e.MoveNext()) { }
        await e.DisposeAsync();
    }

    static async Task RunAsyncOn(AdoConnectionProxy proxy, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        proxy.Enqueue(flow);
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    static async Task DrainAsync(CommandFlow.Enumerator enumerator)
    {
        while (await enumerator.MoveNextAsync()) { }
    }

    [TestMethod]
    public async Task Sync_Completes()
    {
        await using var pool = NewPool();
        var pg = await pool.GetAsync(TimeSpan.FromSeconds(10));
        var proxy = WrapInProxy(pg);
        await RunSyncOn(proxy, "select 1");
    }

    [TestMethod]
    public async Task Async_Completes()
    {
        await using var pool = NewPool();
        var pg = await pool.GetAsync(TimeSpan.FromSeconds(10));
        var proxy = WrapInProxy(pg);
        await RunAsyncOn(proxy, "select 1");
    }

    [TestMethod]
    public async Task SyncWhileAsyncInFlight_SameProxy_BothComplete()
    {
        await using var pool = NewPool();
        var pg = await pool.GetAsync(TimeSpan.FromSeconds(10));
        var proxy = WrapInProxy(pg);

        await RunAsyncOn(proxy, "select 1"); // warm

        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var slow = new CommandFlow(async: true,
            Command.Create("select 1") with { WithSync = true }, blocker.WaitCommand);
        proxy.Enqueue(slow);
        var slowEnum = slow.GetAsyncEnumerator();
        Assert.IsTrue(await slowEnum.MoveNextAsync());
        var slowTask = DrainAsync(slowEnum);

        var sync = new CommandFlow(async: false, Command.Create("select 1"));
        proxy.Enqueue(sync);
        var syncTask = Task.Run(async () =>
        {
            var e = sync.GetEnumerator();
            while (e.MoveNext()) { }
            await e.DisposeAsync();
        });

        await blocker.ReleaseAsync();
        await syncTask.WaitAsync(TimeSpan.FromSeconds(2));

        await slowTask;
        await slowEnum.DisposeAsync();
    }
}
