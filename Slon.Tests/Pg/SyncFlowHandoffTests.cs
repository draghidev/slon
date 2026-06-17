namespace Slon.Tests.Pg;

// Focus: the guarantees PgClientFlowSource's sync handoff promises that the basic completion
// tests in ProtocolLevelTests don't measure - the sync caller's thread stays put, the thread
// pool isn't grown, and concurrent sync producers across distinct protocols don't deadlock.
// Driven directly against PgClientProtocol so the assertions attribute to the handoff
// rendezvous, not to anything the ADO surface adds.
// Class-serial: IdlePipeline_DoesNotGrowThreadPool reads ThreadPool.ThreadCount, which would
// be perturbed by ConcurrentSync_AcrossProtocols_AllComplete's 8-thread burst running in
// parallel under method-level parallelism. Other classes are method-parallel by default.
[TestClass]
[DoNotParallelize]
public class SyncFlowHandoffTests
{
    // Sanity check that nothing in the sync path secretly trampolines onto a TP thread and
    // returns on a different one. If this ever fires, something in the sync flow path is
    // doing implicit thread handoff against the contract. Drives the raw protocol so the
    // assertion attributes to PgClientFlowSource's rendezvous, not to anything above it.
    [TestMethod]
    public async Task ReturnsOnCallerThread()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        await PgTestPool.RunSync(lease.Protocol, "select 1"); // warm

        var beforeId = Environment.CurrentManagedThreadId;
        await PgTestPool.RunSync(lease.Protocol, "select 1");
        var afterId = Environment.CurrentManagedThreadId;

        Assert.AreEqual(beforeId, afterId,
            "sync MoveNext returned on a different thread than it was called on; " +
            "sync semantics require the caller's thread stays put");
    }

    // Verifies that a sync flow against an idle pipeline does not grow the thread pool.
    // Direct measurement of ThreadPool.ThreadCount delta against the architectural claim:
    // sync flow uses no extra TP capacity when the pipeline is idle (caller's thread does
    // all the work).
    [TestMethod]
    public async Task IdlePipeline_DoesNotGrowThreadPool()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        await PgTestPool.RunSync(lease.Protocol, "select 1"); // warm
        await Task.Delay(100);

        var threadCountBefore = ThreadPool.ThreadCount;
        await PgTestPool.RunSync(lease.Protocol, "select 1");
        var threadCountAfter = ThreadPool.ThreadCount;

        Assert.IsTrue(threadCountAfter <= threadCountBefore,
            $"sync flow on idle pipeline grew TP from {threadCountBefore} to {threadCountAfter}; " +
            "expected no growth under the handoff design (caller's thread does all work)");
    }

    // Strong-negative test: measures TP completed-work-item delta during a sync flow and
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
        await using var lease = await PgTestPool.LeaseAsync();
        await PgTestPool.RunSync(lease.Protocol, "select 1"); // warm

        await Task.Delay(200);

        var driftBefore = ThreadPool.CompletedWorkItemCount;
        await Task.Delay(100);
        var ambientDrift = ThreadPool.CompletedWorkItemCount - driftBefore;

        var workBefore = ThreadPool.CompletedWorkItemCount;
        await PgTestPool.RunSync(lease.Protocol, "select 1");
        var queryWork = ThreadPool.CompletedWorkItemCount - workBefore;

        var allowed = ambientDrift + 2;
        Assert.IsTrue(queryWork <= allowed,
            $"sync flow consumed {queryWork} TP work items (ambient drift baseline {ambientDrift}, " +
            $"allowed {allowed}); expected 0-2 (SocketAsyncEngine BCL noise only)");
    }

    // Per-iteration thread-id check across many handoff cycles. Repeats the rendezvous tightly
    // so any per-call leak in HandoffSlot / HandoffActive / QueueNotEmpty / SyncHead / SyncTail
    // / the parked-MRES would either deadlock or break the caller-thread guarantee within a few
    // hundred runs.
    [TestMethod]
    public async Task RepeatedSync_StaysOnCallerThread()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        const int iterations = 200;
        var callerThread = Environment.CurrentManagedThreadId;
        for (int i = 0; i < iterations; i++)
        {
            await PgTestPool.RunSync(lease.Protocol, "select 1");
            Assert.AreEqual(callerThread, Environment.CurrentManagedThreadId,
                $"sync flow returned on a different thread at iteration {i}");
        }
    }

    // Each PgClientProtocol owns its own PgClientFlowSource; concurrent sync callers on
    // distinct protocols never contend on the same source's waiter list. N parallel threads,
    // each driving sync flows on its own protocol; verifies no per-source state leaks across
    // protocols and that each handoff completes independently.
    [TestMethod]
    public async Task ConcurrentSync_AcrossProtocols_AllComplete()
    {
        const int concurrency = 8;
        var leases = new PgTestPool.Lease[concurrency];
        var leased = 0;
        try
        {
            for (int i = 0; i < concurrency; i++)
            {
                leases[i] = await PgTestPool.LeaseAsync();
                leased++;
                await PgTestPool.RunSync(leases[i].Protocol, "select 1"); // warm
            }
            await Task.Delay(100);

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
                            PgTestPool.RunSync(leases[idx].Protocol, "select 1").GetAwaiter().GetResult();
                    }
                    catch (Exception ex) { exceptions[idx] = ex; }
                });
                threads[i].IsBackground = true;
            }

            foreach (var t in threads) t.Start();
            foreach (var t in threads) Assert.IsTrue(t.Join(TimeSpan.FromSeconds(30)), "thread timed out");

            for (int i = 0; i < concurrency; i++)
                Assert.IsNull(exceptions[i], $"thread {i} threw: {exceptions[i]}");
        }
        finally
        {
            for (int i = 0; i < leased; i++)
                await leases[i].DisposeAsync();
        }
    }
}
