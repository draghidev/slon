namespace Slon.Tests.Pg;

// Focus: the guarantees PgClientFlowSource's sync handoff promises that the basic completion
// tests in ProtocolLevelTests don't measure - the sync caller's thread stays put and concurrent
// sync producers across distinct protocols don't deadlock. Driven directly against
// PgClientProtocol so the assertions attribute to the handoff rendezvous, not to anything the
// ADO surface adds.
//
// The "uses no TP capacity" guarantee is the solo-only IdlePipeline_DoesNotChurnThreadPool
// spot-check (a process-global oracle, [Ignore]'d in-suite); the in-suite guard for the same
// contract is the DETERMINISTIC caller-thread check (ReturnsOnCallerThread /
// RepeatedSync_StaysOnCallerThread). A weaker in-suite ThreadCount variant was removed: it
// asserted the same thing with a stricter, untoleranced bound on a global oracle, so it flaked
// on the documented SocketAsyncEngine BCL noise (a TP thread injected during the window).
[TestClass]
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

    // Strong-negative smoke test: measures TP completed-work-item delta during a sync flow and
    // compares against the ambient drift baseline. Only worth running when working in the sync
    // flow / idle-handoff area, as a spot-check that the handoff still enqueues nothing.
    //
    // It stays [Ignore]'d, NOT for lack of a cleaner metric, but because the metric is the point.
    // ThreadPool.CompletedWorkItemCount is a process-global oracle: it counts EVERYTHING, including
    // a dispatch we never tracked (a stray continuation, a BCL path we didn't anticipate). That is
    // exactly what the test exists to catch. A pipeline-local counter we increment ourselves cannot
    // replace it: it only sees dispatches we already know about, so it would pass precisely when an
    // untracked enqueue is the bug. The same "counts everything" property makes it un-isolatable in
    // suite, any concurrent test's TP activity pollutes the delta. So isolation must come from a
    // quiet process (run solo), not a narrower metric. Do not "fix" this into a local counter.
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
    [Ignore("Global TP-counter oracle (counts untracked dispatches) needs a quiet process; run solo when touching sync flow / idle handoff. Not replaceable by a local counter.")]
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
