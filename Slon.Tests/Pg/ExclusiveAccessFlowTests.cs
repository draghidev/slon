using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Drives PgClientProtocol.BeginExclusiveScope directly (no ADO surface yet), so the assertions
// attribute to the wire-takeover + nested-pipeline composition itself. This is also the shell's first
// real execution - the acceptance test that the exclusive flow actually owns the wire and runs user
// subflows on its inner pipeline.
[TestClass]
[DoNotParallelize]
public class ExclusiveAccessFlowTests
{
    static async Task DrainAsync(CommandFlow flow)
    {
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task Scope_RoundTrip_RunsCommandOnInnerPipeline()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var scope = lease.Protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;   // acquired exclusive access; the flow owns the wire

        var cmd = scope.Queue(new CommandFlow(async: true, Command.Create("select 1")));
        await DrainAsync(cmd);

        await scope.CompleteScopeAsync();
    }

    [TestMethod]
    public async Task Scope_MultipleCommands_RunSequentially()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var scope = lease.Protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;

        for (int i = 0; i < 5; i++)
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));

        await scope.CompleteScopeAsync();
    }

    [TestMethod]
    public async Task Scope_FlyweightReuse_AcrossScopes()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        for (int i = 0; i < 3; i++)
        {
            var scope = lease.Protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
            await scope.CompleteScopeAsync();
        }
    }

    // The cascade (verification step 4): a protocol shutdown while a scope is OPEN must drive the inner
    // pipeline to teardown instead of stranding its idle inner executor. Open a scope, run a command to
    // completion (so the inner executor is parked idle, the scope still held), then gracefully
    // CompleteAsync the protocol WITHOUT calling CompleteScopeAsync. The protocol must reach Completed
    // promptly - the inner executor was woken via ExclusiveAccessFlow.OnStopping -> _completeInner - NOT
    // hang because the scope was never ended.
    //
    // Faults the protocol, so NewIsolatedAsync (not the shared pool). This exercises the protocol
    // graceful stop reaching the idle inner executor; it does NOT assert that a scope abort interrupts a
    // parked wire READ (that is ScopeAbortBreaksParkedSubflowTests), so the subflow is run to completion
    // first rather than left parked on a server-side wait.
    [TestMethod]
    public async Task ProtocolShutdown_WhileScopeOpen_CascadesToInnerTeardown()
    {
        // The cascade's OnStopping fires from the heartbeat tick, so CompleteAsync's latency is bounded
        // by the heartbeat interval. Use a 50ms interval (as ProtocolCompletionTests does) so the test
        // costs ~50ms instead of waiting out the 1s default.
        var protocol = await PgTestPool.NewIsolatedAsync(o => o.HeartbeatInterval = TimeSpan.FromMilliseconds(50));
        try
        {
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;

            // Run a command to completion so the inner executor is parked idle, but DO NOT end the scope.
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));

            // Shut down the protocol with the scope still open. Without the cascade, the inner executor
            // would never be woken and the protocol drain would hang.
            var cause = new InvalidOperationException("test shutdown");
            await protocol.CompleteAsync(cause).AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.IsTrue(protocol.IsCompleted, "Protocol must reach Completed; a hang means the inner executor was stranded.");
            Assert.AreSame(cause, protocol.CompletionException, "shutdown reason preserved");
        }
        finally
        {
            await protocol.DisposeAsync();
        }
    }

    // R1 allocation invariant (verification step 5): the per-connection scope machinery (inner pipeline
    // flyweight, ExclusiveAccessFlow, and the scope CloseSignal) is created once and reused, and the
    // normal completion path spares the scope CTS trip so its linked CTSes stay pristine and reusable.
    // After one warm-up cycle, repeated open/run/close cycles must not allocate per-cycle for the scope
    // signal. We don't assert exact zero (the subflow's CommandFlow + per-row materialization allocate);
    // we assert the steady-state per-cycle allocation does not GROW when the cascade is exercised vs a
    // baseline - i.e. the scope signal itself is amortized to zero. The proof that the CTS-sparing path
    // held is really ExclusiveAccessFlowStressTests.Stress_RepeatedScopes_Reuse at 20k (a tripped linked
    // CTS could not be reused); this is the direct steady-state check.
    // Homomorphism seam (NOT a re-test of flow behavior): the inner pipeline is the SAME machine as the
    // outer (same Control/source/executor), so command behavior - multi-result, errors, RFQ resync - is
    // inherited from the outer suite by construction. The ONE place inner deliberately diverges is the
    // inner Control's RecoversWireFailures => false. A backend SQL error is a NORMAL completion (RFQ
    // follows, recovery is NOT engaged - recovery is wire-fault-only), so it flows through the identical
    // homomorphic path at both levels. This asserts the inner Control routes a SQL error exactly as the
    // outer does: the error surfaces on result consumption, the inner pipeline resyncs to RFQ, and the
    // scope stays usable for a subsequent subflow. The subflow is the probe; the assertion is about the
    // inner Control's normal-completion routing being a faithful image of the outer's.
    //
    // SCOPE: this covers INPUT-CAUSED errors (the normal majority - a function of the caller's inputs;
    // backend sends ErrorResponse then ReadyForQuery, the session is fine). FATAL/PANIC/admin-shutdown/
    // protocol-violation errors (session-terminating, often no clean RFQ) are a SEPARATE not-yet-built
    // out-of-band path that must route like a wire fault (materialize the close reason, route to the
    // root/teardown, skip the in-band resync) rather than be treated as a normal result. A future test
    // for that class must NOT assume the scope stays usable.
    [TestMethod]
    public async Task SqlErrorSubflow_InScope_ResyncsAndScopeStaysUsable()
    {
        var protocol = await PgTestPool.NewIsolatedAsync();
        try
        {
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;

            // A subflow producing a backend SQL error (its own command, its own Sync/RFQ - a separate
            // batch from the next subflow, the realistic "a statement errored, run another" shape). The
            // error surfaces only on result consumption (MoveNextAsync itself does not throw), and the
            // inner pipeline resyncs to RFQ - recovery is NOT engaged (recovery is wire-fault-only).
            var errFlow = scope.Queue(new CommandFlow(async: true, Command.Create("select 1/0")));
            var e = errFlow.GetAsyncEnumerator();
            Assert.IsTrue(await e.MoveNextAsync(), "the error command's result should be delivered");
            PostgresException? thrown = null;
            try { e.Current.GetCommandComplete(); }
            catch (PostgresException ex) { thrown = ex; }
            Assert.IsNotNull(thrown, "division by zero should surface a PostgresException on consumption");
            StringAssert.StartsWith(thrown!.SqlState, "22", "numeric error class (division by zero = 22012)");
            while (await e.MoveNextAsync()) { }
            await e.DisposeAsync();

            // The scope is still usable: a fresh subflow runs cleanly on the same inner pipeline after the
            // error - the inner Control routed the error exactly as the outer would (resync, no recovery).
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));

            await scope.CompleteScopeAsync();
        }
        finally
        {
            await protocol.DisposeAsync();
        }
    }

    // Homomorphism seam: the inner SOURCE instance must escalate + pipeline exactly as the outer one
    // does. The one-at-a-time tests never overlap subflows, so the inner SlotEscalatingQueue stays on its
    // slot fast path and the inner executor never pipelines. Queue N async subflows BEFORE draining any,
    // forcing the inner source to escalate past the slot and the inner executor to carry multiple
    // in-flight - proving the nested source instance is a faithful image of the outer (single caller, so
    // this is queue-ahead async pipelining, the only in-scope pipelining a single connection can produce).
    [TestMethod]
    public async Task PipelinedSubflows_InScope_InnerSourceEscalatesAndPipelines()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var scope = lease.Protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;

        const int batch = 8;
        var flows = new CommandFlow[batch];
        // Queue all before draining any - this is what forces inner escalation + pipelining.
        for (int i = 0; i < batch; i++)
            flows[i] = scope.Queue(new CommandFlow(async: true, Command.Create("select " + i)));

        // Drain in order (FIFO = submission order = execution order on the inner single-pump executor).
        for (int i = 0; i < batch; i++)
            await DrainAsync(flows[i]);

        await scope.CompleteScopeAsync();
    }

    // Allocation oracle: GC.GetAllocatedBytesForCurrentThread() measures the WHOLE thread, so any
    // concurrent test's allocations (and JIT warmup, finalizers, async continuations landing here)
    // pollute the per-cycle delta in a parallel suite run - it false-fails under contention while
    // passing comfortably solo. Isolation must come from a quiet process, not a narrower metric (a
    // local counter could not see a per-cycle scope-signal alloc landing off-thread). Run solo:
    //   dotnet test --filter Scope_RepeatedCycles_NoPerCycleScopeSignalAlloc
    [TestMethod]
    [Ignore("Per-thread allocation oracle needs a quiet process; run solo when touching the exclusive-scope flyweight. A concurrent test's allocations pollute GC.GetAllocatedBytesForCurrentThread.")]
    public async Task Scope_RepeatedCycles_NoPerCycleScopeSignalAlloc()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        async Task Cycle()
        {
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
            await scope.CompleteScopeAsync();
        }

        // Warm up: first cycle builds the flyweight scope machinery (one-time alloc) + JITs the path.
        await Cycle();

        const int cycles = 50;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < cycles; i++)
            await Cycle();
        var perCycle = (GC.GetAllocatedBytesForCurrentThread() - before) / (double)cycles;

        // A fresh scope CloseSignal + its two linked CTSes per cycle would add well over a kilobyte each;
        // the flyweight reuse keeps the scope signal off the per-cycle budget. Generous bound (the cycle's
        // own CommandFlow/result allocations dominate) - it would be blown only by a per-cycle scope-signal
        // regression.
        Assert.IsTrue(perCycle < 8192,
            $"Per-cycle steady-state allocation {perCycle:F0} bytes suggests the scope signal is no longer a reused flyweight.");
    }
}
