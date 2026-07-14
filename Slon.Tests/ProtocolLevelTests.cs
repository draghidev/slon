using System.Diagnostics;
using System.Net;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pools;
using Slon.Transport;

namespace Slon.Tests;

// Drops below SlonConnection, the pool, AdoConnectionProxy, SlonCommand etc. Constructs
// PgClientProtocols and TransportConnections directly so the sync handoff + mixing behavior
// can be exercised without the upper-layer noise that the ADO surface adds.
[TestClass]
public class ProtocolLevelTests
{
    static PgClientOptions NewOptions() => new()
    {
        EndPoint = new IPEndPoint(IPAddress.Loopback, 5432),
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
    };

    static async Task<PgClientProtocol> ConnectAsync()
    {
        var options = NewOptions();
        var transport = await SocketStreamConnection.ConnectAsync((IPEndPoint)options.EndPoint);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);
        return protocol;
    }

    static async Task RunSync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: false, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetEnumerator();
        while (e.MoveNext()) { }
        await e.DisposeAsync();
    }

    static async Task RunAsync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task Sync_OnRawProtocol_Completes()
    {
        var protocol = await ConnectAsync();
        try { await RunSync(protocol, "select 1"); }
        finally { await protocol.CompleteAsync(); }
    }

    [TestMethod]
    public async Task Async_OnRawProtocol_Completes()
    {
        var protocol = await ConnectAsync();
        try { await RunAsync(protocol, "select 1"); }
        finally { await protocol.CompleteAsync(); }
    }

    // Many sync flows in a tight loop. Exercises the handoff state machine across repeated
    // cycles: HandoffSlot / HandoffActive / SyncHead / SyncTail / ParkedMres / VTS Reset all
    // need to return to rest between iterations. A leak in any would deadlock or skip results
    // within a few hundred runs.
    [TestMethod]
    public async Task RepeatedSync_OnRawProtocol_Completes()
    {
        var protocol = await ConnectAsync();
        try
        {
            for (int i = 0; i < 200; i++)
                await RunSync(protocol, "select 1");
        }
        finally { await protocol.CompleteAsync(); }
    }

    // Many async flows in a tight loop. The post-handoff drain path doesn't apply here (no
    // sync producers), but the async-path VTS Reset / wake / GetResult cycle gets exercised.
    [TestMethod]
    public async Task RepeatedAsync_OnRawProtocol_Completes()
    {
        var protocol = await ConnectAsync();
        try
        {
            for (int i = 0; i < 200; i++)
                await RunAsync(protocol, "select 1");
        }
        finally { await protocol.CompleteAsync(); }
    }

    // Alternating sync/async on the same protocol. Exercises the HandoffActive gate's
    // engage/disengage cycle and the executor's transition between the inline takeover path
    // and the normal async-wake path. Any state mishandled across the boundary would surface
    // here as a hang or incorrect ordering.
    [TestMethod]
    public async Task AlternatingSyncAsync_OnRawProtocol_Completes()
    {
        var protocol = await ConnectAsync();
        try
        {
            for (int i = 0; i < 50; i++)
            {
                if ((i & 1) == 0)
                    await RunSync(protocol, "select 1");
                else
                    await RunAsync(protocol, "select 1");
            }
        }
        finally { await protocol.CompleteAsync(); }
    }

    // Async flow followed by sync flow on the SAME protocol. Verifies the executor-busy arm
    // of EnqueueSyncWithHandoff: the sync caller's WaitForParked has to actually block (no
    // ParkedMres set yet because the executor is mid-flight), then wake when the executor
    // drains and reaches its park point. No TP enqueue is emitted by the handoff itself.
    [TestMethod]
    public async Task SyncAfterAsync_SameProtocol_Completes()
    {
        var protocol = await ConnectAsync();
        try
        {
            await RunAsync(protocol, "select 1");
            await RunSync(protocol, "select 1");
        }
        finally { await protocol.CompleteAsync(); }
    }

    // Async pg_sleep started on protocol, sync issued before it returns. Sync gets queued and
    // processed once the executor parks. With pipelining the protocol queues both. The sync's
    // dispatch waits for the executor's natural park (after the async response comes back).
    //
    // Async pg_sleep started on protocol, sync issued WHILE the async drain is in flight.
    // Exposes (and previously triggered) a bug where the executor, busy processing the async
    // flow, would finish that flow, loop into MoveNextAsync, snipe HandoffSlot on its own
    // (non-caller) thread, process the sync flow on TP, then leave the sync caller stranded
    // waiting for the next park. The sync caller's eventual SetResult would dispatch the
    // executor's continuation onto a stale/empty state and CompleteWaiter would NRE during
    // protocol shutdown.
    //
    // Fix: HandoffAcked gate on TryTakeHandoff. The executor cannot pick up HandoffSlot until
    // the sync caller has cleared WaitForParked and is about to SetResult inline.
    [TestMethod]
    public async Task SyncWhileAsyncInFlight_SameProtocol_BothComplete()
    {
        var protocol = await ConnectAsync();
        try
        {
            await RunAsync(protocol, "select 1"); // warm

            var slow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.3)"));
            Assert.IsTrue(protocol.TryQueue(slow));
            var slowEnum = slow.GetAsyncEnumerator();
            var slowTask = DrainAsync(slowEnum);

            var sw = Stopwatch.StartNew();
            await RunSync(protocol, "select 1");
            var syncElapsed = sw.Elapsed;

            await slowTask;
            await slowEnum.DisposeAsync();

            Assert.IsTrue(syncElapsed < TimeSpan.FromSeconds(2),
                $"sync took {syncElapsed.TotalMilliseconds:F1}ms — expected ≤2s");
        }
        finally { await protocol.CompleteAsync(); }
    }

    static async Task DrainAsync(CommandFlow.Enumerator enumerator)
    {
        while (await enumerator.MoveNextAsync()) { }
    }

    // --- PgConnection layer ---
    // One layer up from the protocol: wraps it with a CommandTracker (null here, single-conn
    // test), heartbeat wiring, and maintenance plumbing. TryQueue is a thin pass-through.
    // If cross-connection blocking reappears at this layer, the bug is in PgConnection's
    // construction or wiring (e.g., shared heartbeat thread).

    static async Task<PgConnection> ConnectPgConnectionAsync()
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

    [TestMethod]
    public async Task PgConnection_Sync_Completes()
    {
        var conn = await ConnectPgConnectionAsync();
        try { await RunSyncOn(conn, "select 1"); }
        finally { await conn.Protocol.CompleteAsync(); }
    }

    [TestMethod]
    public async Task PgConnection_Async_Completes()
    {
        var conn = await ConnectPgConnectionAsync();
        try { await RunAsyncOn(conn, "select 1"); }
        finally { await conn.Protocol.CompleteAsync(); }
    }

    [TestMethod]
    public async Task PgConnection_SyncWhileAsyncInFlight_SameConn_BothComplete()
    {
        var conn = await ConnectPgConnectionAsync();
        try
        {
            await RunAsyncOn(conn, "select 1"); // warm

            var slow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.3)"));
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

    // --- ConnectionPool layer ---
    // Adds connection lifecycle management (lease/release via the idle channel, pool-driven
    // heartbeat). If cross-connection blocking surfaces here, the lease path or the pool's
    // shared heartbeat thread is the coupling.

    static ConnectionPool<PgConnection> NewPool(int maxConnections = 4, CommandTracker? sharedTracker = null)
    {
        var options = NewOptions();
        var transportFactory = SocketStreamConnection.CreateFactory(options.EndPoint);
        var factory = new PgConnectionFactory(options, transportFactory, tracker: sharedTracker);
        return new ConnectionPool<PgConnection>(factory, new() { MaxConnections = maxConnections });
    }

    [TestMethod]
    public async Task Pool_Sync_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        await RunSyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task Pool_Async_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        await RunAsyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task Pool_SyncWhileAsyncInFlight_SameLeasedConn_BothComplete()
    {
        await using var pool = NewPool();
        var conn = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));

        await RunAsyncOn(conn, "select 1"); // warm

        var slow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.3)"));
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

    [TestMethod]
    public async Task Pool_AsyncOnConnA_DoesNotBlock_SyncOnConnB()
    {
        await using var pool = NewPool();
        var connA = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        var connB = await pool.GetConnectionAsync(1L, TimeSpan.FromSeconds(10));

        await RunAsyncOn(connA, "select 1"); // warm
        await RunAsyncOn(connB, "select 1"); // warm

        var slow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.5)"));
        Assert.IsTrue(connA.TryQueue(slow));
        var slowEnum = slow.GetAsyncEnumerator();
        var slowTask = DrainAsync(slowEnum);
        await Task.Delay(50);

        var sw = Stopwatch.StartNew();
        await RunSyncOn(connB, "select 1");
        var syncElapsed = sw.Elapsed;

        await slowTask;
        await slowEnum.DisposeAsync();

        Assert.IsTrue(syncElapsed < TimeSpan.FromMilliseconds(100),
            $"sync on connB took {syncElapsed.TotalMilliseconds:F1}ms while async pg_sleep(0.5) was in flight on connA");
    }

    // --- AdoConnectionProxy layer ---
    // Wraps a pooled PgConnection in the proxy, which adds: per-proxy CommandTracker,
    // pipeline-depth counter, exclusive-scope flag, completion-action wiring. If cross-conn
    // blocking appears at this layer, the proxy itself (or its tracker integration) is the
    // coupling.

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

    [TestMethod]
    public async Task Proxy_Sync_Completes()
    {
        await using var pool = NewPool();
        var pg = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        var proxy = WrapInProxy(pg);
        await RunSyncOn(proxy, "select 1");
    }

    [TestMethod]
    public async Task Proxy_Async_Completes()
    {
        await using var pool = NewPool();
        var pg = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        var proxy = WrapInProxy(pg);
        await RunAsyncOn(proxy, "select 1");
    }

    [TestMethod]
    public async Task Proxy_SyncWhileAsyncInFlight_SameProxy_BothComplete()
    {
        await using var pool = NewPool();
        var pg = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        var proxy = WrapInProxy(pg);

        await RunAsyncOn(proxy, "select 1"); // warm

        var slow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.3)"));
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

    [TestMethod]
    public async Task Proxy_AsyncOnA_DoesNotBlock_SyncOnB_SharedTracker()
    {
        // Mirrors SlonDataSource's setup: both proxies share the same workload-scope
        // CommandTracker (SlonDataSource.GetCommandTracker is process-stable). If the tracker
        // serialises calls across connections, this will block.
        var sharedTracker = new CommandTracker(0, 5);
        await using var pool = NewPool();
        var pgA = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        var pgB = await pool.GetConnectionAsync(1L, TimeSpan.FromSeconds(10));
        var proxyA = WrapInProxy(pgA, sharedTracker);
        var proxyB = WrapInProxy(pgB, sharedTracker);

        await RunAsyncOn(proxyA, "select 1"); // warm
        await RunAsyncOn(proxyB, "select 1"); // warm

        var slow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.5)"));
        proxyA.Enqueue(slow);
        var slowEnum = slow.GetAsyncEnumerator();
        var slowTask = DrainAsync(slowEnum);
        await Task.Delay(50);

        var sw = Stopwatch.StartNew();
        await RunSyncOn(proxyB, "select 1");
        var syncElapsed = sw.Elapsed;

        await slowTask;
        await slowEnum.DisposeAsync();

        Assert.IsTrue(syncElapsed < TimeSpan.FromMilliseconds(100),
            $"sync on proxyB took {syncElapsed.TotalMilliseconds:F1}ms while async pg_sleep(0.5) was in flight on proxyA");
    }

    // --- SlonDataSource layer, raw proxy path ---
    // Constructs a real SlonDataSource (which builds everything: pool, SlonConnection with
    // AdoConnectionProxy attached via CreateProxy + the dataSource-shared tracker), opens two
    // SlonConnections, but BYPASSES SlonCommand and AdoBatchCore by reaching into the
    // SlonConnection's UnderlyingProxy and enqueuing raw CommandFlows directly. If this
    // passes, the bug is specifically in the SlonCommand/AdoBatchCore path. If it fails, the
    // bug is in SlonDataSource's connection setup (CreateProxy, SetProxy, etc.).
}
