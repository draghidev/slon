using Slon.Pg;
using Slon.Pg.Protocol.Flows;
using static Slon.Tests.Pg.ProtocolDiag;

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
    sealed class WakeHolder
    {
        internal FlowCallerInteractionCore<ValueTuple> Core;
    }

    static ref FlowCallerInteractionCore<ValueTuple> GetWakeCore(WakeHolder holder) => ref holder.Core;

    [TestMethod]
    public async Task DedicatedWakeResumesOutsideTheThreadPool()
    {
        var holder = new WakeHolder();
        holder.Core.Initialize();
        var resumedOnThreadPool = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var suspended = Suspend(holder, resumedOnThreadPool);

        holder.Core.RequestWake(useDedicatedDriver: true);

        Assert.IsFalse(await resumedOnThreadPool.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        await suspended.WaitAsync(TimeSpan.FromSeconds(10));
        holder.Core.Reset();

        static async Task Suspend(WakeHolder holder, TaskCompletionSource<bool> resumedOnThreadPool)
        {
            FieldRef<FlowCallerInteractionCore<ValueTuple>> fieldRef;
            unsafe
            {
                fieldRef = FieldRef<FlowCallerInteractionCore<ValueTuple>>.Create(&GetWakeCore, holder);
            }
            await holder.Core.SetContinuationAndUnblockWaiter(fieldRef);
            resumedOnThreadPool.SetResult(Thread.CurrentThread.IsThreadPoolThread);
        }
    }

    [TestMethod]
    public void CloseBeforeSyncWait_IsRemembered()
    {
        var holder = new WakeHolder();
        holder.Core.Initialize();

        // Abort can precede creation of the synchronous disposer's event. The close must remain
        // observable as progress rather than disappear through a null event reference.
        holder.Core.CancelPendingWait(new InvalidOperationException("close"));
        Action? continuation = null;
        var waiter = new Thread(() => continuation = holder.Core.WaitForContinuation())
        {
            IsBackground = true
        };

        waiter.Start();
        Assert.IsTrue(waiter.Join(TimeSpan.FromSeconds(1)), "the close wake was lost before wait registration");
        Assert.IsNull(continuation);
    }

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
    // so any per-call leak in the handoff state
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
                await PgTestPool.RunSync(leases[i].Protocol, "select 1"); // warm (awaited => protocols quiescent)
            }

            var threads = new Thread[concurrency];
            var exceptions = new Exception?[concurrency];
            var progress = new int[concurrency];
            for (int i = 0; i < concurrency; i++)
            {
                int idx = i;
                threads[i] = new Thread(() =>
                {
                    try
                    {
                        for (int j = 0; j < 20; j++)
                        {
                            Volatile.Write(ref progress[idx], j);
                            PgTestPool.RunSync(leases[idx].Protocol, "select 1").GetAwaiter().GetResult();
                        }
                        Volatile.Write(ref progress[idx], 20);
                    }
                    catch (Exception ex) { exceptions[idx] = ex; }
                });
                threads[i].IsBackground = true;
            }

            foreach (var t in threads) t.Start();
            foreach (var t in threads)
            {
                if (t.Join(TimeSpan.FromSeconds(30)))
                    continue;
                // Self-classifying hang report, decode by gauges. A stuck activation turn reads as
                // activated={completed=True} (gate starvation). All-null slots split on the gauges:
                // backlog=1 means the flow was never pulled (dispatch wake lost), while backlog=0
                // outstanding=0 with a thread stuck mid-iteration means the flow fully completed and
                // the caller's handoff wake never fired (the rendezvous seam).
                var diag = string.Join("\n", leases.Take(leased).Select((l, i) =>
                    $"protocol {i}: progress={Volatile.Read(ref progress[i])}/20 backlog={l.Protocol.Backlog} outstanding={l.Protocol.Outstanding} " +
                    $"executor={Describe(l.Protocol.FlowControl.ExecutingFlow)} activated={Describe(l.Protocol.FlowControl.ActivatedFlow)}" +
                    (l.Protocol.Backlog > 0 ? $"\n  source: {SourceState(l.Protocol)}" : "")));
                Assert.Fail($"thread timed out\n{diag}");
            }

            for (int i = 0; i < concurrency; i++)
                Assert.IsNull(exceptions[i], $"thread {i} threw: {exceptions[i]}");
        }
        finally
        {
            for (int i = 0; i < leased; i++)
                await leases[i].DisposeAsync();
        }
    }


    // The core guarantee of the unified-queue handoff: N threads driving sync flows on the SAME
    // protocol concurrently each get woken back on the thread they submitted from. They contend on
    // the one source's wait-list (the executor pops its head and signals the matching caller's node
    // as it reaches each sync-tagged flow's turn in the single queue), so a misrouted wake - the
    // executor running a flow itself, or signalling the wrong caller's node - would surface as a
    // wrong-thread return or a deadlock. Distinct from ConcurrentSync_AcrossProtocols (which never
    // shares a source's wait-list).
    [TestMethod]
    public async Task ConcurrentSync_SameProtocol_EachReturnsOnItsOwnThread()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        await PgTestPool.RunSync(lease.Protocol, "select 1"); // warm

        const int concurrency = 8;
        const int iterations = 25;
        var threads = new Thread[concurrency];
        var mismatches = new int[concurrency];
        var exceptions = new Exception?[concurrency];
        // Release all threads together so their submits genuinely overlap on the one protocol.
        using var start = new Barrier(concurrency);

        for (int i = 0; i < concurrency; i++)
        {
            int idx = i;
            threads[i] = new Thread(() =>
            {
                try
                {
                    var ownThread = Environment.CurrentManagedThreadId;
                    start.SignalAndWait();
                    for (int j = 0; j < iterations; j++)
                    {
                        // Drive the synchronous handoff inline on THIS thread. RunSync's MoveNext
                        // loop is the takeover; it must return on the thread that called it.
                        PgTestPool.RunSync(lease.Protocol, "select 1").GetAwaiter().GetResult();
                        if (Environment.CurrentManagedThreadId != ownThread)
                            mismatches[idx]++;
                    }
                }
                catch (Exception ex) { exceptions[idx] = ex; }
            })
            { IsBackground = true };
        }

        foreach (var t in threads) t.Start();
        JoinAllOrDump(threads, lease.Protocol, "a sync caller thread timed out (possible misrouted wake / deadlock)");

        for (int i = 0; i < concurrency; i++)
        {
            Assert.IsNull(exceptions[i], $"thread {i} threw: {exceptions[i]}");
            Assert.AreEqual(0, mismatches[i],
                $"thread {i} returned from a sync handoff on a different thread {mismatches[i]} time(s); " +
                "the executor must wake each sync caller back on its own submitting thread");
        }
    }

    // Mixed sync + async enqueues on the SAME protocol, concurrently. This is the case the unified
    // queue exists for: sync flows take their FIFO position in the one queue interleaved with async
    // ones, and the executor must still route each sync-tagged flow's wake to its own caller's thread
    // while draining the async flows itself. A sync flow landing on the wrong side of the interleave
    // (the old priority-slot / skip-queue model) or a wake routed to the wrong caller surfaces here as
    // a wrong-thread return, a hang, or an incomplete async flow. A sync caller blocks until its own
    // flow completes, so a thread can't itself mix modes; half the threads drive sync, half async.
    //
    // Witnessed mixing via observable queue index: this test is the ONLY submitter to this protocol,
    // so a test-side gate around just the (grab-index + TryQueue) instant makes each caller's index
    // equal to its real queue position - without distorting the concurrency the redesign must handle
    // (the blocking sync handoff / async drive both run OUTSIDE the gate, on the caller's own thread).
    // Recording the mode per queue index lets the test PROVE the queue order genuinely interleaved
    // sync and async (adjacent indices of differing modes), so a run that was effectively sequential
    // fails as inconclusive rather than passing vacuously.
    [TestMethod]
    public async Task MixedSyncAsync_SameProtocol_SyncReturnsOnOwnThreadAndAllComplete()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;
        await PgTestPool.RunSync(protocol, "select 1"); // warm

        const int syncThreads = 4;
        const int asyncThreads = 4;
        const int iterations = 100;
        var threads = new Thread[syncThreads + asyncThreads];
        var mismatches = new int[syncThreads];
        var exceptions = new Exception?[syncThreads + asyncThreads];
        using var start = new Barrier(syncThreads + asyncThreads);

        // Submit gate: serializes ONLY the index-grab + enqueue, so index == queue position. 's'/'a'
        // per submit.
        var submitGate = new object();
        var nextIndex = 0;
        var order = new char[(syncThreads + asyncThreads) * iterations];

        // Enqueue under the gate (assigning the queue-position index), then return the queued flow for
        // the caller to drive OUTSIDE the gate. Mirrors PgTestPool.RunSync/RunAsync split into submit
        // (gated) + drive (ungated).
        CommandFlow Submit(bool async, char mode)
        {
            lock (submitGate)
            {
                var flow = new CommandFlow(async, Command.Create("select 1"));
                Assert.IsTrue(protocol.TryQueue(flow), "TryQueue failed");
                order[nextIndex++] = mode;
                return flow;
            }
        }

        for (int i = 0; i < syncThreads; i++)
        {
            int idx = i;
            threads[i] = new Thread(() =>
            {
                try
                {
                    var ownThread = Environment.CurrentManagedThreadId;
                    start.SignalAndWait();
                    for (int j = 0; j < iterations; j++)
                    {
                        var flow = Submit(async: false, 's');
                        var e = flow.GetEnumerator();
                        while (e.MoveNext()) { }          // sync handoff + body run inline on THIS thread
                        e.Dispose();
                        if (Environment.CurrentManagedThreadId != ownThread)
                            mismatches[idx]++;
                    }
                }
                catch (Exception ex) { exceptions[idx] = ex; }
            })
            { IsBackground = true };
        }

        for (int i = 0; i < asyncThreads; i++)
        {
            int slot = syncThreads + i;
            threads[slot] = new Thread(() =>
            {
                try
                {
                    start.SignalAndWait();
                    for (int j = 0; j < iterations; j++)
                    {
                        var flow = Submit(async: true, 'a');
                        var e = flow.GetAsyncEnumerator();
                        while (e.MoveNextAsync().AsTask().GetAwaiter().GetResult()) { }
                        e.DisposeAsync().AsTask().GetAwaiter().GetResult();
                    }
                }
                catch (Exception ex) { exceptions[slot] = ex; }
            })
            { IsBackground = true };
        }

        foreach (var t in threads) t.Start();
        JoinAllOrDump(threads, protocol, "a mixed-flow caller thread timed out (possible misrouted wake / deadlock)");

        for (int i = 0; i < threads.Length; i++)
            Assert.IsNull(exceptions[i], $"thread {i} threw: {exceptions[i]}");
        for (int i = 0; i < syncThreads; i++)
            Assert.AreEqual(0, mismatches[i],
                $"sync thread {i} returned on a different thread {mismatches[i]} time(s) under mixed load; " +
                "a sync flow interleaved with async work must still wake its own submitting thread");

        // Prove the queue order genuinely interleaved the modes: at least one adjacent pair of queued
        // flows has differing modes (a sync flow took a FIFO slot directly next to an async one). A
        // run that grouped all-sync-then-all-async would have no such adjacency and is inconclusive.
        var interleaved = false;
        for (int k = 1; k < order.Length; k++)
            if (order[k] != order[k - 1]) { interleaved = true; break; }
        Assert.IsTrue(interleaved,
            "the queue order never placed a sync flow adjacent to an async flow - the modes did not " +
            "actually interleave on the shared queue, so this run did not exercise mixed enqueuing");
    }

    // DIFFERENTIAL test for execution-order FIFO across sync and async. RED on the current design,
    // GREEN on the unified-queue design - it asserts the property the redesign exists to provide: a
    // sync flow takes its real FIFO position and does NOT jump ahead of earlier-submitted async work.
    //
    // The server records execution order directly: each flow runs `SELECT nextval(seq)::int` against a
    // shared sequence, so the value it reads back IS the rank the server assigned when it processed
    // that command (= the executor's wire/dispatch order). No client-side bookkeeping needed - the
    // rank rides in the result row. Submit a block (async, async, SYNC, async, async) with all flows
    // queued before any is driven, so the sync flow is genuinely submitted after a0/a1 while they are
    // still queued. Current design: the sync flow's priority HandoffSlot + skip-queue window run its
    // nextval BEFORE the earlier async ones, so its rank is lower than a0's/a1's - RED. Unified design:
    // FIFO, so its rank is higher - GREEN.
    [TestMethod]
    public async Task SyncFlow_DoesNotJumpAheadOfEarlierAsync_ExecutionOrderIsFifo()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;
        await PgTestPool.RunSync(protocol, "create temp sequence exec_rank");

        const int blocks = 50;
        var jumpAheadCount = 0;

        for (int b = 0; b < blocks; b++)
        {
            // Submit the whole block before driving any, so the flows co-reside in the queue.
            // Submission order: a0, a1, sync, a2, a3.
            var flows = new (CommandFlow flow, bool async)[5];
            for (int k = 0; k < 5; k++)
            {
                bool async = k != 2;             // position 2 is the sync flow
                flows[k] = (new CommandFlow(async, Command.Create("select nextval('exec_rank')::int")), async);
                Assert.IsTrue(protocol.TryQueue(flows[k].flow), "TryQueue failed");
            }

            // Drive each flow and read its assigned rank.
            var ranks = new int[5];
            var driveTasks = new Task<int>[5];
            for (int k = 0; k < 5; k++)
            {
                var (flow, async) = flows[k];
                driveTasks[k] = async ? Task.Run(() => DriveAsyncReadRank(flow)) : Task.Run(() => DriveSyncReadRank(flow));
            }
            try
            {
                ranks = await Task.WhenAll(driveTasks).WaitAsync(TimeSpan.FromSeconds(30));
            }
            catch (TimeoutException)
            {
                Assert.Fail($"block {b}: drive tasks timed out\nbacklog={protocol.Backlog} outstanding={protocol.Outstanding} " +
                    $"executor={Describe(protocol.FlowControl.ExecutingFlow)} activated={Describe(protocol.FlowControl.ActivatedFlow)}\n" +
                    $"source: {SourceState(protocol)}");
                return;
            }

            // The sync flow (index 2) must have a HIGHER rank than the earlier-submitted async flows
            // (index 0, 1): the server processed it after them. A lower rank means it jumped ahead.
            if (ranks[2] < ranks[0] || ranks[2] < ranks[1])
                jumpAheadCount++;
        }

        Assert.AreEqual(0, jumpAheadCount,
            $"the sync flow's execution rank was lower than an earlier-submitted async flow's in {jumpAheadCount}/{blocks} blocks; " +
            "under the unified queue a sync flow must take its FIFO position, not jump ahead via a priority slot");
    }

    // Misrouted-wake guard. When several sync callers contend on the one protocol's wait-list, the
    // executor reaching a queued sync flow's turn must wake THAT flow's submitting caller - not some
    // other parked caller. If node-order and flow-order ever desynchronize (the wait-list node and the
    // queued flow must be stamped together so node-at-head == flow-at-head), the executor could wake
    // caller X's thread to take over caller Y's flow at the head. That misroute is INVISIBLE when the
    // wire payloads are identical (everyone runs `select 1`, gets a correct result, returns on their
    // own thread). It is made visible here by giving each caller a DISTINCT payload: caller K submits
    // `select K`, and asserts it reads back K. A thread that drove a foreign flow reads the wrong value.
    //
    // Run on the current design this is a probe: green = the HandoffSlot / SyncHead coordination is
    // sound (a misroute is not a live bug); a red would have found one. On the unified design it guards
    // the atomic node+flow append against a future edit that splits them.
    [TestMethod]
    public async Task ConcurrentSync_SameProtocol_EachThreadDrivesItsOwnFlow()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;
        await PgTestPool.RunSync(protocol, "select 1"); // warm

        const int concurrency = 8;
        const int iterations = 100;
        var threads = new Thread[concurrency];
        var misroutes = new int[concurrency];   // times a caller read back a value it did not submit
        var exceptions = new Exception?[concurrency];
        using var start = new Barrier(concurrency);

        for (int i = 0; i < concurrency; i++)
        {
            int idx = i;
            threads[i] = new Thread(() =>
            {
                try
                {
                    start.SignalAndWait();
                    for (int j = 0; j < iterations; j++)
                    {
                        // A value unique to this caller-iteration. If the executor woke this thread to
                        // drive a different caller's flow, the read-back value won't match.
                        int payload = (idx + 1) * 100_000 + j;
                        var flow = new CommandFlow(async: false, Command.Create($"select {payload}"));
                        Assert.IsTrue(protocol.TryQueue(flow), "TryQueue failed");
                        var got = DriveSyncReadRank(flow);
                        if (got != payload)
                            misroutes[idx]++;
                    }
                }
                catch (Exception ex) { exceptions[idx] = ex; }
            })
            { IsBackground = true };
        }

        foreach (var t in threads) t.Start();
        JoinAllOrDump(threads, protocol, "a sync caller thread timed out (possible misrouted wake / deadlock)");

        for (int i = 0; i < concurrency; i++)
        {
            Assert.IsNull(exceptions[i], $"thread {i} threw: {exceptions[i]}");
            Assert.AreEqual(0, misroutes[i],
                $"thread {i} read back a value it did not submit {misroutes[i]} time(s) - the executor woke this " +
                "thread to drive a different caller's flow (a misrouted sync handoff: node-order vs flow-order desync)");
        }
    }

    static int DriveSyncReadRank(CommandFlow flow)
    {
        var rank = -1;
        var e = flow.GetEnumerator();
        while (e.MoveNext())
            foreach (var row in e.Current)
                rank = row.GetValue<int>(0);
        e.Dispose();
        return rank;
    }

    static async Task<int> DriveAsyncReadRank(CommandFlow flow)
    {
        var rank = -1;
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync())
            await foreach (var row in e.Current)
                rank = row.GetValue<int>(0);
        await e.DisposeAsync();
        return rank;
    }
}
