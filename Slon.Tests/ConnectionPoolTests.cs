using System.Diagnostics;
using Slon.Pg;
using Slon.Pg.Protocol;
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

        var slow = new CommandFlow(async: true, Command.Create("select pg_sleep(0.05)"));
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

    static readonly TimeSpan Cap = TimeSpan.FromSeconds(10);

    static Exception Root(Exception ex)
    {
        while (ex is not PgClientClosedException && ex.InnerException is not null)
            ex = ex.InnerException;
        return ex;
    }

    static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, string because)
    {
        var sw = Stopwatch.StartNew();
        while (!condition() && sw.Elapsed < timeout)
            await Task.Delay(10);
        Assert.IsTrue(condition(), because);
    }

    // Terminal abort is the wire-dead path: forceful DisposeAsync RSTs the socket, fires AbortToken, and
    // drives Shutdown to Completed. The status surface (IsCompleted + CompletionException) is what the pool
    // evicts on; here we verify end-to-end that an aborted connection reaches Completed and the pool
    // reclaims its slot and opens a fresh, healthy connection in its place rather than handing the corpse
    // back. maxConnections:1 forces the same slot to be reused so the reclaim path is the one under test.
    [TestMethod]
    public async Task TerminalAbort_EvictsFromPool_ReacquireYieldsHealthy()
    {
        await using var pool = NewPool(maxConnections: 1);
        var conn1 = await pool.GetConnectionAsync(0L, Cap);
        await RunAsyncOn(conn1, "select 1"); // healthy before the abort

        await conn1.Protocol.DisposeAsync(); // forceful terminal abort (fire-and-forget; teardown runs async)
        // IsCompleted is set at the END of the background shutdown (SignalCompleted), so it is eventually-
        // consistent, not immediate. The pool's eviction gate keys off it, so it reclaims once it lands.
        await WaitUntilAsync(() => conn1.IsCompleted, Cap,
            "a terminally aborted connection must reach Completed — that is the pool's eviction gate.");

        var conn2 = await pool.GetConnectionAsync(0L, Cap);
        Assert.AreNotSame(conn1, conn2, "the pool must replace the aborted connection, not hand it back.");
        Assert.IsFalse(conn2.IsCompleted, "the replacement connection must be live.");
        await RunAsyncOn(conn2, "select 1"); // the replacement actually works
    }

    // Every flow queued behind the abort point (in-flight + backlog) must receive PgClientClosedException,
    // none may strand. The flows are queued but never driven, so they sit outstanding when the abort lands;
    // forceful DisposeAsync faults the in-flight ones via the pipeline completion and the backlog via the
    // inert drain. Draining each enumerator must surface the closed exception, not hang.
    [TestMethod]
    public async Task TerminalAbort_OutstandingPipelinedFlows_AllFaultClosed()
    {
        await using var pool = NewPool(maxConnections: 1);
        var conn = await pool.GetConnectionAsync(0L, Cap);

        const int N = 8;
        var enums = new CommandFlow.Enumerator[N];
        for (var i = 0; i < N; i++)
        {
            // pg_sleep keeps the head occupied so the rest genuinely stay queued behind it at abort time.
            var flow = new CommandFlow(async: true, Command.Create("select pg_sleep(10)"));
            Assert.IsTrue(conn.TryQueue(flow));
            enums[i] = flow.GetAsyncEnumerator();
        }

        await conn.Protocol.DisposeAsync(); // forceful terminal abort while all N are outstanding

        for (var i = 0; i < N; i++)
        {
            Exception? observed = null;
            try
            {
                await DrainAsync(enums[i]).WaitAsync(Cap);
            }
            catch (Exception ex)
            {
                observed = ex;
            }

            Assert.IsNotNull(observed, $"flow {i} behind the abort point should fault, not complete or hang.");
            Assert.IsInstanceOfType<PgClientClosedException>(Root(observed!),
                $"flow {i} surfaced {Root(observed!).GetType().Name}, expected PgClientClosedException.");
        }
    }
}
