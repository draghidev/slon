using System.Diagnostics;
using System.Net;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;
using Slon.Pools;
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
        EndPoint = new IPEndPoint(IPAddress.Loopback, 5432),
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
        var pg = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        var proxy = WrapInProxy(pg);
        await RunSyncOn(proxy, "select 1");
    }

    [TestMethod]
    public async Task Async_Completes()
    {
        await using var pool = NewPool();
        var pg = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        var proxy = WrapInProxy(pg);
        await RunAsyncOn(proxy, "select 1");
    }

    [TestMethod]
    public async Task SyncWhileAsyncInFlight_SameProxy_BothComplete()
    {
        await using var pool = NewPool();
        var pg = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        var proxy = WrapInProxy(pg);

        await RunAsyncOn(proxy, "select 1"); // warm

        var slow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.1)"));
        proxy.Enqueue(slow);
        var slowEnum = slow.GetAsyncEnumerator();
        var slowTask = DrainAsync(slowEnum);

        var sw = Stopwatch.StartNew();
        await RunSyncOn(proxy, "select 1");
        var syncElapsed = sw.Elapsed;

        await slowTask;
        await slowEnum.DisposeAsync();

        Assert.IsTrue(syncElapsed < TimeSpan.FromSeconds(2),
            $"sync took {syncElapsed.TotalMilliseconds:F1}ms — expected ≤2s");
    }
}
