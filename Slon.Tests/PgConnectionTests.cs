using System.Diagnostics;
using System.IO.Pipelines;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pools;
using Slon.Tests.Pg;
using Slon.Transport;

namespace Slon.Tests;

// One layer above PgClientProtocol: wraps it with a CommandTracker (null here, single-conn),
// heartbeat wiring, and maintenance plumbing. TryQueue is a thin pass-through. If cross-
// connection blocking surfaces here, the bug is in PgConnection's construction or wiring
// (e.g., shared heartbeat thread). Each test owns its connection end-to-end because
// PgConnection isn't pool-managed without an actual pool above it.
[TestClass]
public class PgConnectionTests : ConnectionCreatingTest
{
    sealed class AsyncOnlyTransport : TransportConnection
    {
        readonly Pipe _read = new();
        readonly Pipe _write = new();

        public override PipeReader Reader => _read.Reader;
        public override PipeWriter Writer => _write.Writer;
        public override void WaitWritable() { }
    }

    static PgClientOptions NewOptions() => new()
    {
        EndPoint = TestEndPoint.Default,
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
        Ssl = new() { Mode = PostgreSqlSslMode.Disable }
    };

    static async Task<PgConnection> ConnectAsync()
    {
        var options = NewOptions();
        var transport = await SocketStreamConnection.ConnectAsync(options.EndPoint);
        var conn = await PgConnection.CreateAsync(new PgClientProtocolOptions(options), options, transport);
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
    public void FailedPreStartupConnection_DoesNotJoinTracker()
    {
        var options = NewOptions();
        using var tracker = new CommandTracker(maxAuto: 1, autoMinimumUses: 1);

        Assert.ThrowsExactly<InvalidOperationException>(() =>
            PgConnection.Create(new PgClientProtocolOptions(options), options,
                new AsyncOnlyTransport(), tracker));

        Assert.AreEqual(0, tracker.RegisteredConnectionCount);
    }

    [TestMethod]
    public async Task FailedPostStartupWiring_CompletesSessionLifetime()
    {
        var options = NewOptions();
        var transport = await SocketStreamConnection.ConnectAsync(options.EndPoint);
        await using var tracker = new CommandTracker(maxAuto: 1, autoMinimumUses: 1);
        var context = new ConnectionPoolContext<PgConnection>(
            static (_, _) => throw new InvalidOperationException("heartbeat wiring failed"));

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await PgConnection.CreateAsync(new PgClientProtocolOptions(options), options,
                transport, tracker, context));

        Assert.AreEqual(0, tracker.RegisteredConnectionCount);
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

            await using var blocker = await PgAdvisoryLock.AcquireAsync();
            var slow = new CommandFlow(async: true,
                Command.Create("select 1") with { WithSync = true }, blocker.WaitCommand);
            Assert.IsTrue(conn.TryQueue(slow));
            var slowEnum = slow.GetAsyncEnumerator();
            Assert.IsTrue(await slowEnum.MoveNextAsync());
            var slowTask = DrainAsync(slowEnum);

            var sync = new CommandFlow(async: false, Command.Create("select 1"));
            Assert.IsTrue(conn.TryQueue(sync));
            var syncTask = Task.Run(async () =>
            {
                var e = sync.GetEnumerator();
                while (e.MoveNext()) { }
                await e.DisposeAsync();
            });

            await blocker.ReleaseAsync();
            await syncTask;

            await slowTask;
            await slowEnum.DisposeAsync();
        }
        finally { await conn.Protocol.CompleteAsync(); }
    }
}
