using System.Diagnostics;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Tests;

// One layer above PgClientProtocol: wraps it with a CommandTracker (null here, single-conn),
// heartbeat wiring, and maintenance plumbing. TryQueue is a thin pass-through. If cross-
// connection blocking surfaces here, the bug is in PgConnection's construction or wiring
// (e.g., shared heartbeat thread). Each test owns its connection end-to-end because
// PgConnection isn't pool-managed without an actual pool above it.
[TestClass]
public class PgConnectionTests
{
    static PgClientOptions NewOptions() => new()
    {
        EndPoint = new IPEndPoint(IPAddress.Loopback, 5432),
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
    };

    static async Task<PgConnection> ConnectAsync()
    {
        var options = NewOptions();
        var transport = await SocketStreamConnection.ConnectAsync((IPEndPoint)options.EndPoint);
        var conn = await PgConnection.CreateAsync(new PgClientProtocolOptions(options), options, transport);
        // No pool managing this conn. Mark in-service ourselves.
        conn.Start();
        return conn;
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
    public async Task Sync_Completes()
    {
        var conn = await ConnectAsync();
        try { await RunSyncOn(conn, "select 1"); }
        finally { await conn.Protocol.CompleteAsync(); }
    }

    [TestMethod]
    public async Task Async_Completes()
    {
        var conn = await ConnectAsync();
        try { await RunAsyncOn(conn, "select 1"); }
        finally { await conn.Protocol.CompleteAsync(); }
    }

    [TestMethod]
    public async Task SyncWhileAsyncInFlight_SameConn_BothComplete()
    {
        var conn = await ConnectAsync();
        try
        {
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
        finally { await conn.Protocol.CompleteAsync(); }
    }
}
