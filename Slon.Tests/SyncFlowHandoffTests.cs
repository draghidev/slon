using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon;

namespace Slon.Tests;

// End-to-end tests exercising PgClientFlowSource's sync handoff: zero-TP rendezvous, inline
// takeover on the caller's thread, the HandoffActive gate on async producers, the linked-list
// FIFO baton-pass between concurrent sync producers, and post-handoff queue drain.
[TestClass]
public class SyncFlowHandoffTests
{
    static SlonDataSource NewDataSource(int maxPoolSize = 2) =>
        new(new SlonDataSourceOptions
        {
            EndPoint = new IPEndPoint(IPAddress.Loopback, 5432),
            Username = "postgres",
            Password = "postgres123",
            Database = "postgres",
            MaxPoolSize = maxPoolSize,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            MaintenanceInterval = TimeSpan.FromSeconds(1),
        });

    [TestMethod]
    public async Task SimpleSelect_Completes()
    {
        await using var ds = NewDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        await using var cmd = new SlonCommand(conn, "select 1");
        // Just verify the sync flow runs end-to-end without hanging or faulting. The actual
        // RecordsAffected value isn't asserted yet because CommandResult.RecordsAffected is
        // not populated from CommandComplete (TODO in CommandResult.cs).
        cmd.ExecuteNonQuery();
    }

    // Verifies that a sync ExecuteNonQuery against an idle pipeline does not grow the thread
    // pool. Direct measurement of ThreadPool.ThreadCount delta against the architectural claim
    // "sync flow uses no extra TP capacity when the pipeline is idle."
    [TestMethod]
    public async Task IdlePipeline_DoesNotGrowThreadPool()
    {
        await using var ds = NewDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        await using (var warmup = new SlonCommand(conn, "select 1"))
            warmup.ExecuteNonQuery();
        await Task.Delay(100);

        var threadCountBefore = ThreadPool.ThreadCount;
        await using (var cmd = new SlonCommand(conn, "select 1"))
            cmd.ExecuteNonQuery();
        var threadCountAfter = ThreadPool.ThreadCount;

        Assert.IsTrue(threadCountAfter <= threadCountBefore,
            $"sync ExecuteNonQuery on idle pipeline grew TP from {threadCountBefore} to {threadCountAfter}; " +
            "expected no growth under the handoff design (caller's thread does all work)");
    }

    // Sanity check that nothing in the sync API surface secretly trampolines onto a TP thread
    // and returns on a different one. If this ever fires, something in the sync flow path is
    // doing implicit thread handoff against the contract.
    [TestMethod]
    public async Task ReturnsOnCallerThread()
    {
        await using var ds = NewDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        await using (var warmup = new SlonCommand(conn, "select 1"))
            warmup.ExecuteNonQuery();

        var beforeId = Environment.CurrentManagedThreadId;
        await using (var cmd = new SlonCommand(conn, "select 1"))
            cmd.ExecuteNonQuery();
        var afterId = Environment.CurrentManagedThreadId;

        Assert.AreEqual(beforeId, afterId,
            "sync ExecuteNonQuery returned on a different thread than it was called on; " +
            "sync semantics require the caller's thread stays put");
    }

    // Strong-negative test: measures TP completed-work-item delta during a sync query and
    // compares against the ambient drift baseline. Requires isolation from concurrent test
    // TP activity to be meaningful. Run individually:
    //   dotnet test --filter IdlePipeline_DoesNotChurnThreadPool
    //
    // Empirically (verified via dotnet-trace) the per-query TP work-item delta over ambient is at
    // most 1, and the absolute query TP work is 0-2:
    //  - 0-2: SocketAsyncEngine sync-emulation completions, once a socket has been used
    //         asynchronously (during connection setup) it stays in non-blocking mode for life.
    //         Subsequent sync Receive calls are emulated via kqueue/epoll and the completion
    //         fires on a TP thread. BCL behavior, not avoidable at our layer. These items don't
    //         block our progression so they're noise relative to the sync handoff guarantee.
    // Slon contributes zero TP work items to the sync flow path. The handoff rendezvous uses
    // a parked-MRES that the executor's IValueTaskSource.OnCompleted sets, so the sync caller's
    // thread blocks on what it would block on anyway, no TP enqueue.
    [TestMethod]
    [Ignore("Requires isolation from concurrent test TP activity. Run individually.")]
    public async Task IdlePipeline_DoesNotChurnThreadPool()
    {
        await using var ds = NewDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        await using (var warmup = new SlonCommand(conn, "select 1"))
            warmup.ExecuteNonQuery();

        await Task.Delay(200);

        var driftBefore = ThreadPool.CompletedWorkItemCount;
        await Task.Delay(100);
        var ambientDrift = ThreadPool.CompletedWorkItemCount - driftBefore;

        var workBefore = ThreadPool.CompletedWorkItemCount;
        await using (var cmd = new SlonCommand(conn, "select 1"))
            cmd.ExecuteNonQuery();
        var queryWork = ThreadPool.CompletedWorkItemCount - workBefore;

        var allowed = ambientDrift + 2;
        Assert.IsTrue(queryWork <= allowed,
            $"sync query consumed {queryWork} TP work items (ambient drift baseline {ambientDrift}, " +
            $"allowed {allowed}); expected 0-2 (SocketAsyncEngine BCL noise only)");
    }

    // Exercises the per-iteration handoff cycle repeatedly to verify there are no per-call leaks:
    // HandoffSlot/HandoffActive/QueueNotEmpty/SyncHead/SyncTail all return to their resting
    // state, the parked-MRES re-arms correctly across iterations, and the VTS reset cycles
    // cleanly. A leak in any of these would deadlock or skip results within a few hundred runs.
    [TestMethod]
    public async Task RepeatedQueries_OnSingleConnection_AllComplete()
    {
        await using var ds = NewDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        const int iterations = 200;
        var callerThread = Environment.CurrentManagedThreadId;
        for (int i = 0; i < iterations; i++)
        {
            await using var cmd = new SlonCommand(conn, "select 1");
            cmd.ExecuteNonQuery();
            Assert.AreEqual(callerThread, Environment.CurrentManagedThreadId,
                $"sync ExecuteNonQuery returned on a different thread at iteration {i}");
        }
    }

    // A sync ExecuteNonQuery issued AFTER an async query started on the SAME connection has to
    // wait for the async query to drain before the executor parks. Only then can the sync
    // caller's MRES fire and the inline takeover happen. This exercises the "executor busy"
    // arm of EnqueueSyncWithHandoff where WaitForParked actually blocks rather than returning
    // immediately. With the redesign there is no TP enqueue here either. The wait rides the
    // VTS's OnCompleted handler that the executor itself reaches at park time.
    [TestMethod]
    public async Task SyncAfterAsync_OnSameConnection_BothComplete()
    {
        await using var ds = NewDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        // Issue async first. ADO's contract doesn't allow truly-concurrent commands on the same
        // connection, so we await the async cmd. What we're really testing is that the post-
        // async pipeline state correctly admits a subsequent sync ExecuteNonQuery (HandoffSlot
        // cleared, VTS reset, MRES set after final park).
        await using (var asyncCmd = new SlonCommand(conn, "select 1"))
            await asyncCmd.ExecuteNonQueryAsync();

        await using var syncCmd = new SlonCommand(conn, "select 2");
        syncCmd.ExecuteNonQuery();
    }

    // Across separate connections from the same pool, each Slon connection has its own
    // PgClientProtocol and PgClientFlowSource. Concurrent sync callers don't contend on the same
    // source's waiter list (the per-connection design avoids it). This test verifies the pool
    // hands them out cleanly and each per-source handoff completes independently, N parallel
    // handoffs, no deadlock, no TP growth.
    [TestMethod]
    public async Task ConcurrentSync_AcrossConnections_AllComplete()
    {
        const int concurrency = 8;
        await using var ds = NewDataSource(maxPoolSize: concurrency);

        // Pre-open and warm each connection so the pool is steady before the parallel run.
        var conns = new SlonConnection[concurrency];
        for (int i = 0; i < concurrency; i++)
        {
            conns[i] = await ds.OpenConnectionAsync(CancellationToken.None);
            await using (var warmup = new SlonCommand(conns[i], "select 1"))
                warmup.ExecuteNonQuery();
        }
        await Task.Delay(100);

        var tpBefore = ThreadPool.ThreadCount;

        var threads = new Thread[concurrency];
        var exceptions = new Exception?[concurrency];
        for (int i = 0; i < concurrency; i++)
        {
            int idx = i;
            threads[i] = new Thread(() =>
            {
                try
                {
                    for (int j = 0; j < 20; j++)
                    {
                        using var cmd = new SlonCommand(conns[idx], "select 1");
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex) { exceptions[idx] = ex; }
            });
            threads[i].IsBackground = true;
        }

        foreach (var t in threads) t.Start();
        foreach (var t in threads) Assert.IsTrue(t.Join(TimeSpan.FromSeconds(30)), "thread timed out");

        for (int i = 0; i < concurrency; i++)
            Assert.IsNull(exceptions[i], $"thread {i} threw: {exceptions[i]}");

        var tpAfter = ThreadPool.ThreadCount;
        Assert.IsTrue(tpAfter <= tpBefore + 1,
            $"TP grew from {tpBefore} to {tpAfter} under {concurrency} concurrent sync callers; " +
            "handoff should keep work on callers' own threads");

        for (int i = 0; i < concurrency; i++)
            await conns[i].DisposeAsync();
    }

    // Sync ExecuteNonQuery in a loop, alternating with ExecuteNonQueryAsync, verifies the
    // post-handoff queue drain path (the conditional TP wake when async items piled up during
    // the handoff window). Even with mixing, ordering and completion are preserved.
    [TestMethod]
    public async Task AlternatingSyncAsync_OnSameConnection_AllComplete()
    {
        await using var ds = NewDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        const int iterations = 50;
        for (int i = 0; i < iterations; i++)
        {
            if ((i & 1) == 0)
            {
                await using var cmd = new SlonCommand(conn, "select 1");
                cmd.ExecuteNonQuery();
            }
            else
            {
                await using var cmd = new SlonCommand(conn, "select 1");
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    // Long-running async ExecuteNonQuery on one connection while another caller drives sync
    // ExecuteNonQuery on a different connection from the same pool. Each SlonConnection should
    // get its own PgConnection (and therefore its own wire) under the current pool ordering
    // (idle channel → empty slot → multiplex) plus the create-path Activate gate that suppresses
    // startup-time idle publishing. Without that gate, a freshly created PgConnection would
    // race itself into the idle channel before its first lease committed, and the second
    // OpenConnectionAsync would read it back out, ending up sharing one wire.
    [TestMethod]
    public async Task AsyncOnOneConn_DoesNotBlock_SyncOnAnother()
    {
        await using var ds = NewDataSource(maxPoolSize: 2);
        await using var asyncConn = await ds.OpenConnectionAsync(CancellationToken.None);
        await using var syncConn = await ds.OpenConnectionAsync(CancellationToken.None);

        await using (var warm1 = new SlonCommand(asyncConn, "select 1")) warm1.ExecuteNonQuery();
        await using (var warm2 = new SlonCommand(syncConn, "select 1")) warm2.ExecuteNonQuery();

        await using var slowCmd = new SlonCommand(asyncConn, "select pg_sleep(0.5)");
        var slowTask = slowCmd.ExecuteNonQueryAsync();
        await Task.Delay(50);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await using (var fastCmd = new SlonCommand(syncConn, "select 1"))
            fastCmd.ExecuteNonQuery();
        var contendedMs = sw.Elapsed.TotalMilliseconds;

        await slowTask;

        Assert.IsTrue(contendedMs < 100, $"contended sync took {contendedMs:F1}ms");
    }
}
