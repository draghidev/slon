using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Multiple-flows lifecycle: queue N flows up front, THEN drain them in order. The rest of the suite
// is consume-as-you-go (queue one, drain, repeat), so every flow is the pipeline head and flushes +
// activates inline - which is exactly why two regressions hid here:
//   - the pre-write GateTask batch deadlock (async, write side): the executor sat on a downstream
//     flow's consumer gate, stranding a prior flow's deferred flush. Fixed by the eager write.
//   - the GetDecoderAuto sync-shortcut dispatch collision (sync, read side): a sync flow's
//     DispatchPipelinedRead fast-path-Started at dispatch (GetDecoderAuto reports completed
//     unconditionally for sync), Starting the shared read promise before activation, into a
//     predecessor's still-held baton. Fixed by gating dispatch on real activation (GetDecoderAsync).
// The async/sync/multi-command queue-then-drain tests cover the lifecycle + the write-side deadlock.
// The sync read-side collision needs an async flow holding the baton while a sync flow is dispatched -
// which only happens across THREADS (a single caller uses sync OR async, never both); that's the
// concurrent ConcurrentSyncAndAsync_NoSharedPromiseCollision stress below.
[TestClass]
[DoNotParallelize]
public class PipelineBatchTests
{
    static async Task DrainAsync(CommandFlow flow)
    {
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    // Drives a single flow's enumerator in the given mode, asserting it delivers exactly
    // expectedResults (one per command) in order, then ends. A torn/lost/reordered batch surfaces as
    // a missing or extra result; a dispatch collision or stranded flush surfaces as a hang.
    static async Task DrainExpecting(CommandFlow flow, bool flowAsync, int expectedResults)
    {
        var e = flowAsync ? flow.GetAsyncEnumerator() : flow.GetEnumerator();
        for (int r = 0; r < expectedResults; r++)
            Assert.IsTrue(flowAsync ? await e.MoveNextAsync() : e.MoveNext(), $"result {r} of {expectedResults} not delivered");
        Assert.IsFalse(flowAsync ? await e.MoveNextAsync() : e.MoveNext(), "a result was delivered past the expected count");
        await e.DisposeAsync();
    }

    [TestMethod]
    [DataRow(true, DisplayName = "async")]
    [DataRow(false, DisplayName = "sync")]
    public async Task OuterPipeline_QueueThenDrain(bool flowAsync)
    {
        await using var lease = await PgTestPool.LeaseAsync();
        const int batch = 8;
        var cmds = new CommandFlow[batch];
        for (int k = 0; k < batch; k++)
            cmds[k] = lease.Protocol.Queue(new CommandFlow(flowAsync, Command.Create("select 1")));
        for (int k = 0; k < batch; k++)
            await DrainExpecting(cmds[k], flowAsync, 1);
    }

    // Backstop iteration count for the handoff/shared-promise race guards. The race is structurally
    // fixed (the Interlocked HandoffActive open), so the default is sized for a per-commit backstop, not
    // to reliably reproduce the original bug; SLON_STRESS_ITERATIONS scales it up for a soak.
    static int StressIters
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("SLON_STRESS_ITERATIONS");
            return int.TryParse(raw, out var n) && n > 0 ? n : 2000;
        }
    }

    // The sync read-side (GetDecoderAuto) collision guard. sync and async flows only coexist on one
    // protocol across THREADS - a single caller is wholly sync or wholly async, but TryQueueFlow takes
    // _syncRoot so distinct threads can interleave their flows, and the executor then pipelines them.
    // When an async flow parks holding the shared read promise and a sync flow is dispatched on the
    // pump behind it, the sync flow's DispatchPipelinedRead used to fast-path-Start (GetDecoderAuto
    // reports completed unconditionally for sync) straight into the held baton -> "The async method is
    // already executing", an UNHANDLED throw on the pump that crashes the process. Timing-dependent, so
    // both modes hammer concurrently on a shared (isolated) protocol; the fix (GetDecoderAsync gating
    // on real activation) makes the sync flow defer instead. A single-thread interleave is NOT a valid
    // repro: it deadlocks (the sync flow blocks the pump waiting for a caller already blocked draining
    // the async flow), and no single caller produces it.
    [TestMethod]
    public async Task ConcurrentSyncAndAsync_NoSharedPromiseCollision()
    {
        var iters = StressIters;
        var protocol = await PgTestPool.NewIsolatedAsync();
        var failure = new Exception?[1];
        void Capture(Exception ex) => Interlocked.CompareExchange(ref failure[0], ex, null);

        try
        {
            var asyncLoop = Task.Run(async () =>
            {
                try
                {
                    for (int i = 0; i < iters && Volatile.Read(ref failure[0]) is null; i++)
                        await PgTestPool.RunAsync(protocol, "select 1");
                }
                catch (Exception ex) { Capture(ex); }
            });

            // Sync flows want the caller's own thread for the handoff, so drive them off a dedicated
            // OS thread, not the pool (and never the async loop's thread - that is the deadlock above).
            var syncThread = new Thread(() =>
            {
                try
                {
                    for (int i = 0; i < iters && Volatile.Read(ref failure[0]) is null; i++)
                    {
                        var flow = new CommandFlow(async: false, Command.Create("select 1"));
                        if (!protocol.TryQueue(flow))
                            break;
                        var e = flow.GetEnumerator();
                        while (e.MoveNext()) { }
                        e.DisposeAsync().AsTask().GetAwaiter().GetResult();
                    }
                }
                catch (Exception ex) { Capture(ex); }
            })
            { IsBackground = true, Name = "batch-sync-loop" };
            syncThread.Start();

            await asyncLoop;
            syncThread.Join(TimeSpan.FromSeconds(120));

            if (failure[0] is { } ex)
                Assert.Fail($"concurrent sync/async raised {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            await protocol.CompleteAsync();
        }
    }

    // Each queued flow carries multiple commands, so the batch holds the shared read baton across a
    // flow's own inter-command consumer gates (not just a single read), under the queue-then-drain
    // lifecycle. Wrong identity/order would surface as a result-count mismatch per flow.
    [TestMethod]
    [DataRow(true, DisplayName = "async")]
    [DataRow(false, DisplayName = "sync")]
    public async Task OuterPipeline_MultiCommand_QueueThenDrain(bool flowAsync)
    {
        await using var lease = await PgTestPool.LeaseAsync();
        const int batch = 6;
        const int perFlow = 3;
        var cmds = new CommandFlow[batch];
        for (int k = 0; k < batch; k++)
            cmds[k] = lease.Protocol.Queue(new CommandFlow(flowAsync,
                Command.Create("select 1"), Command.Create("select 2"), Command.Create("select 3")));
        for (int k = 0; k < batch; k++)
            await DrainExpecting(cmds[k], flowAsync, perFlow);
    }

    [TestMethod]
    public async Task ExclusiveScope_QueueThenDrain()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var scope = lease.Protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;
        const int batch = 8;
        var cmds = new CommandFlow[batch];
        for (int k = 0; k < batch; k++)
            cmds[k] = scope.Queue(new CommandFlow(async: true, Command.Create("select 1")));
        for (int k = 0; k < batch; k++)
            await DrainAsync(cmds[k]);
        await scope.CompleteScopeAsync();
    }

    // Handoff-rendezvous race guard. An async and a sync caller run on ONE protocol; the sync caller's
    // EnqueueSyncWithHandoff opens a handoff window (HandoffActive, set under SyncWaiterLock) and claims
    // the executor's parked wait inline. HandoffActive is read by the async Execute under the WAKE lock,
    // so with no StoreLoad ordering across the two locks the async wake could read it stale-false, pass
    // its defer-gate, and snipe the parked wait the handoff expected to claim. The old code acked the
    // slot before claiming and asserted the claim always won (process-fatal when sniped, and a TP-stolen
    // executor could run the sync flow off-thread). The fix acks only on a winning claim and re-waits +
    // retries on a lost one. Bursty paired start/await maximizes the stale-read window. (Overlaps the
    // free-running ConcurrentSyncAndAsync_NoSharedPromiseCollision above, which exercises the same path
    // on a dedicated sync thread; kept as the targeted handoff guard.)
    [TestMethod]
    public async Task ConcurrentAsyncAndSync_SameProtocol_NoSharedPromiseCollision()
    {
        // Secondary guard overlapping ConcurrentSyncAndAsync's free-running coverage, so it runs a
        // quarter of the iterations; SLON_STRESS_ITERATIONS still scales it for a heavy soak.
        var iters = StressIters / 4;
        var protocol = await PgTestPool.NewIsolatedAsync();
        try
        {
            for (int i = 0; i < iters; i++)
            {
                var a = Task.Run(() => PgTestPool.RunAsync(protocol, "select 1"));
                var s = Task.Run(() => PgTestPool.RunSync(protocol, "select 1"));
                await Task.WhenAll(a, s);
            }
        }
        finally
        {
            await protocol.CompleteAsync();
        }
    }
}
