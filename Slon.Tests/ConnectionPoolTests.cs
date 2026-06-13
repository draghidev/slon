using System.Diagnostics;
using System.Net;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;
using Slon.Pools;
using Slon.Transport;

namespace Slon.Tests;

// Adds connection lifecycle management (lease/release via the idle channel, pool-driven
// heartbeat). If cross-connection blocking surfaces here, the lease path or the pool's
// shared heartbeat thread is the coupling. Each test builds a fresh pool so lease/release
// semantics are tested in isolation.
[TestClass]
public class ConnectionPoolTests
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

    static async Task RunSyncOn(PgConnection conn, string sql)
    {
        var flow = new CommandFlow(async: false, Command.Create(sql));
        Assert.IsTrue(conn.TryQueue(flow));
        var e = flow.GetEnumerator();
        while (e.MoveNext()) { }
        await e.DisposeAsync();
    }

    static async Task RunAsyncOn(PgConnection conn, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(conn.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    static async Task DrainAsync(CommandFlow.Enumerator enumerator)
    {
        while (await enumerator.MoveNextAsync()) { }
    }

    [TestMethod]
    public async Task Sync_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        await RunSyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task Async_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        await RunAsyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task SyncWhileAsyncInFlight_SameLeasedConn_BothComplete()
    {
        await using var pool = NewPool();
        var conn = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));

        await RunAsyncOn(conn, "select 1"); // warm

        var slow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.1)"));
        Assert.IsTrue(conn.TryQueue(slow));
        var slowEnum = slow.GetAsyncEnumerator();
        var slowTask = DrainAsync(slowEnum);

        var sw = Stopwatch.StartNew();
        await RunSyncOn(conn, "select 1");
        var syncElapsed = sw.Elapsed;

        await slowTask;
        await slowEnum.DisposeAsync();

        Assert.IsTrue(syncElapsed < TimeSpan.FromSeconds(2),
            $"sync took {syncElapsed.TotalMilliseconds:F1}ms — expected ≤2s");
    }
}
