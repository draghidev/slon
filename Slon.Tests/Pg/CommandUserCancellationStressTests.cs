using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Stress for the user-cancellation move-next rendezvous, exercising the two-token model:
//   - a token bound at SUBMIT (TryQueue) is flow-scoped and honored from the eager write, so a
//     pre-fired token cancels the WHOLE flow deterministically - the first result cannot race through;
//   - a token bound at GetAsyncEnumerator is flow-scoped too but bound AFTER the eager dispatch, so the
//     first result is ALLOWED to race (a delivered result or an OCE/close are all acceptable);
//   - a per-read MoveNextAsync(ct) token cancels just that read.
// The bug class is the lost wake / wrong-generation double-fire / pipeline-tenure reentry that the
// generation-bound registration + version-aware terminal closed. A hang (caught by HangCap) always
// fails; the submit-bound case additionally requires a strict OCE.
[TestClass]
[DoNotParallelize]
public class CommandUserCancellationStressTests
{
    static readonly TimeSpan HangCap = TimeSpan.FromSeconds(5);

    // I/O-bound (each iteration is a real pipelined command + cancel against a live PG backend over a
    // few pooled connections), so the default keeps each test ~100ms in the suite. The races surface
    // fast; a deliberate deep sweep raises the count via SLON_STRESS_ITERATIONS (the original 6000 was ~800ms).
    static int Iters => int.TryParse(Environment.GetEnvironmentVariable("SLON_STRESS_ITERATIONS"), out var n) && n > 0 ? n : 500;

    static CommandFlow TwoResultFlow() => new(async: true,
        Command.Create("select generate_series(1, 50)"),
        Command.Create("select 'two'"));

    // Drive one MoveNextAsync under the hang cap; returns the caught exception (or null on a delivered
    // result). A TimeoutException is the lost-wake failure and is asserted here so callers don't repeat it.
    static async Task<Exception?> MoveNextGuarded(CommandFlow.Enumerator e, CancellationToken ct, int i)
    {
        try { await e.MoveNextAsync(ct).AsTask().WaitAsync(HangCap); return null; }
        catch (TimeoutException) { Assert.Fail($"iter {i}: MoveNextAsync never completed (lost wake)."); throw; }
        catch (Exception ex) { return ex; }
    }

    static async Task DisposeGuarded(CommandFlow.Enumerator e, int i, string what)
    {
        try { await e.DisposeAsync().AsTask().WaitAsync(HangCap); }
        catch (TimeoutException) { Assert.Fail($"iter {i}: {what} never completed."); }
        catch (OperationCanceledException) { }
        catch (PgClientClosedException) { }
    }

    // The shared race loop: N iterations over a small connection pool, each running `body` on a fresh
    // protocol+iteration index, then disposing the enumerator it returns. Collapses the Pool / loop /
    // Cleanup boilerplate every race test repeated.
    //
    // LEASES from the shared pool (was NewIsolatedAsync, which both leaked - isolated protocols are
    // never reaped by the assembly DrainAsync sweep - and burned a fresh connection per protocol; 8 per
    // test x 7 tests = ~56 connections that piled up against max_connections under repeated runs). The
    // cancel-drain leaves the wire at RFQ each iteration, so the protocol is reusable - exactly the
    // lease contract. Leases return to the idle bag on DisposeAsync.
    static async Task RaceLoop(Func<PgClientProtocol, int, Task<CommandFlow.Enumerator>> body)
    {
        var leases = new PgTestPool.Lease[8];
        for (var p = 0; p < leases.Length; p++)
            leases[p] = await PgTestPool.LeaseAsync();
        try
        {
            for (var i = 0; i < Iters; i++)
            {
                CommandFlow.Enumerator e = default;
                try { e = await body(leases[i % leases.Length].Protocol, i); }
                finally { await DisposeGuarded(e, i, "DisposeAsync"); }
            }
        }
        finally
        {
            foreach (var lease in leases)
                await lease.DisposeAsync();
        }
    }

    static void AssertCancelOrClose(Exception? caught, int i)
    {
        if (caught is not null and not OperationCanceledException and not PgClientClosedException)
            Assert.Fail($"iter {i}: unexpected {caught.GetType().Name}: {caught}");
    }

    // Token bound at SUBMIT, pre-fired: the eager write honors it, so the first MoveNextAsync must
    // surface OCE deterministically (a result racing through is a failure), and never hang.
    [TestMethod]
    public Task SubmitBound_PreFired_SurfacesOce() => RaceLoop(async (protocol, i) =>
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();
        var flow = TwoResultFlow();
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var e = flow.GetAsyncEnumerator(cts.Token);
        Assert.IsInstanceOfType<OperationCanceledException>(
            await MoveNextGuarded(e, cts.Token, i),
            $"iter {i}: submit-bound pre-fired token must surface OCE, not a result");
        return e;
    });

    // Token bound at GetAsyncEnumerator, pre-fired: bound after the eager dispatch, so the first result
    // is ALLOWED to race. OCE, close, or a delivered result are all acceptable; only a hang fails.
    [TestMethod]
    public Task EnumeratorBound_PreFired_NeverLosesWake() => RaceLoop(async (protocol, i) =>
    {
        var flow = TwoResultFlow();
        Assert.IsTrue(protocol.TryQueue(flow));
        var cts = new CancellationTokenSource();
        cts.Cancel();
        var e = flow.GetAsyncEnumerator(cts.Token);
        AssertCancelOrClose(await MoveNextGuarded(e, cts.Token, i), i);
        return e;
    });

    // Timer-fired token (independent thread) bound at GetAsyncEnumerator: races the body and the
    // consumer. Either outcome is acceptable; only a hang fails.
    [TestMethod]
    public Task EnumeratorBound_TimerFired_NeverLosesWake() => RaceLoop(async (protocol, i) =>
    {
        var flow = TwoResultFlow();
        Assert.IsTrue(protocol.TryQueue(flow));
        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromTicks((i % 7) * 5000));
        var e = flow.GetAsyncEnumerator(cts.Token);
        AssertCancelOrClose(await MoveNextGuarded(e, cts.Token, i), i);
        return e;
    });

    // Pipelined overlap: queue TWO submit-bound pre-fired flows back-to-back on one protocol so their
    // executions overlap, maximizing the chance an off-stack cancel completion of one drives the
    // pipeline advance while the other's execution promise is tenured (the concurrent-tenure hazard).
    // Only a hang or an unexpected exception fails.
    [TestMethod]
    public async Task Pipelined_SubmitBound_PreFired_NoTenureCollision()
    {
        var leases = new PgTestPool.Lease[8];
        for (var p = 0; p < leases.Length; p++)
            leases[p] = await PgTestPool.LeaseAsync();
        try
        {
            for (var i = 0; i < Iters; i++)
            {
                var protocol = leases[i % leases.Length].Protocol;
                var ctsA = new CancellationTokenSource(); ctsA.Cancel();
                var ctsB = new CancellationTokenSource(); ctsB.Cancel();
                var a = TwoResultFlow();
                var b = TwoResultFlow();
                Assert.IsTrue(protocol.TryQueue(a, cancellationToken: ctsA.Token));
                Assert.IsTrue(protocol.TryQueue(b, cancellationToken: ctsB.Token));
                var ea = a.GetAsyncEnumerator(ctsA.Token);
                var eb = b.GetAsyncEnumerator(ctsB.Token);
                try
                {
                    await Task.WhenAll(
                        MoveNextGuarded(ea, ctsA.Token, i),
                        MoveNextGuarded(eb, ctsB.Token, i));
                }
                finally
                {
                    await DisposeGuarded(ea, i, "DisposeAsync (a)");
                    await DisposeGuarded(eb, i, "DisposeAsync (b)");
                }
            }
        }
        finally
        {
            foreach (var lease in leases)
                await lease.DisposeAsync();
        }
    }

    static CommandFlow ThreeResultFlow() => new(async: true,
        Command.Create("select generate_series(1, 50)"),
        Command.Create("select 'two'"),
        Command.Create("select 'three'"));

    // Dispose mid-batch, then reuse the same protocol; the dispose+reuse contract must hand the next op
    // a clean wire at RFQ. `waitForDrain` toggles the two dispose paths:
    //   true  (default): DisposeAsync PARKS on the body's drain (no poll/spin) -> wire at RFQ on return.
    //   false (opt-out): DisposeAsync faults+returns immediately; the body drains in the background and
    //                    item retirement still hands the next op a clean wire.
    static Task DisposeThenReuse(bool waitForDrain) => RaceLoop(async (protocol, i) =>
    {
        var flow = ThreeResultFlow();
        flow.WaitForDrainOnDispose = waitForDrain;
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = flow.GetAsyncEnumerator();
        Assert.IsNull(await MoveNextGuarded(e, default, i), $"iter {i}: result 1 should be delivered");

        // This test disposes mid-body (the dispose-then-reuse contract is the point), so do it here and
        // hand RaceLoop a default(Enumerator) - its trailing dispose then no-ops (flow == null).
        var label = waitForDrain ? "wait-for-drain" : "opt-out";
        await DisposeGuarded(e, i, $"{label} DisposeAsync");
        try { await PgTestPool.RunAsync(protocol, "select 1").WaitAsync(HangCap); }
        catch (TimeoutException) { Assert.Fail($"iter {i}: protocol unusable after {label} dispose (wire not at RFQ)."); }
        catch (Exception ex) { Assert.Fail($"iter {i}: reuse after {label} dispose threw {ex.GetType().Name}: {ex.Message}"); }
        return default;
    });

    // DEFAULT await-drain: DisposeAsync parks on the body's completion before returning, so the wire is
    // at RFQ and immediately reusable.
    [TestMethod]
    public Task WaitForDrainOnDispose_MidBatch_AwaitsDrain_ConnectionUsable() => DisposeThenReuse(waitForDrain: true);

    // Opt-OUT: DisposeAsync fast-returns; the body drains in the background and the next op still gets a
    // clean wire via item retirement. Guards the fast-return (MarkConsumerGone) path.
    [TestMethod]
    public Task DisposeFastReturn_OptOut_BodyDrainsInBackground_ConnectionUsable() => DisposeThenReuse(waitForDrain: false);

    // WaitForDrainOnDispose bounded by the flow token: a pre-fired token means the drain-wait must not
    // block on it - Dispose unwinds FAST and does not throw the OCE; the body finishes draining in the
    // background and the next op still succeeds.
    [TestMethod]
    public Task WaitForDrainOnDispose_TokenFired_UnwindsFast_NoThrow() => RaceLoop(async (protocol, i) =>
    {
        var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-fired: the drain-wait must not block on it
        var flow = TwoResultFlow();
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));

        var e = flow.GetAsyncEnumerator(cts.Token);
        AssertCancelOrClose(await MoveNextGuarded(e, cts.Token, i), i);  // OCE/close/result all fine

        await DisposeGuarded(e, i, "token-bounded DisposeAsync");  // disposed mid-body; RaceLoop no-ops on default
        try { await PgTestPool.RunAsync(protocol, "select 1").WaitAsync(HangCap); }
        catch (TimeoutException) { Assert.Fail($"iter {i}: protocol unusable after token-bounded dispose."); }
        catch (PgClientClosedException) { }
        return default;
    });

    // ErrorResponse surfacing on a consumer-gone drain (Npgsql parity). A command faults; the reader is
    // disposed mid-batch with WaitForDrainOnDispose, so DisposeAsync parks on the drain and rethrows the
    // Postgres error. Single faulting sync segment => a BARE PostgresException.
    [TestMethod]
    public async Task WaitForDrainOnDispose_DrainHitsError_ThrowsBarePostgresException()
    {
        // An input-caused ErrorResponse leaves the session fine (drains to RFQ), so this leases from the
        // shared pool rather than burning an isolated connection.
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;
        // First command succeeds; the SECOND faults (undefined table). One trailing sync => one segment.
        var flow = new CommandFlow(async: true,
            Command.Create("select 1"),
            Command.Create("select * from no_such_table_xyz"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = flow.GetAsyncEnumerator();
        Assert.IsTrue(await e.MoveNextAsync(), "first result not delivered");

        var ex = await Assert.ThrowsExactlyAsync<PostgresException>(async () => await e.DisposeAsync());
        Assert.AreEqual("42P01", ex.SqlState, "expected undefined_table");

        await PgTestPool.RunAsync(protocol, "select 1");  // connection still usable (drained to RFQ)
    }

    // Multi-sync: per-command-sync (PreferSimple+WithSync) commands that EACH fault => multiple fault
    // segments => DisposeAsync throws an AggregateException of PostgresExceptions (full fidelity).
    [TestMethod]
    public async Task WaitForDrainOnDispose_MultiSyncErrors_ThrowsAggregate()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;
        var bad1 = Command.Create("select * from no_such_table_a") with { PreferSimple = true, WithSync = true };
        var bad2 = Command.Create("select * from no_such_table_b") with { PreferSimple = true, WithSync = true };
        var flow = new CommandFlow(async: true, Command.Create("select 1") with { PreferSimple = true, WithSync = true }, bad1, bad2);
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = flow.GetAsyncEnumerator();
        Assert.IsTrue(await e.MoveNextAsync(), "first result not delivered");

        var agg = await Assert.ThrowsExactlyAsync<AggregateException>(async () => await e.DisposeAsync());
        Assert.IsTrue(agg.InnerExceptions.Count >= 2, $"expected >=2 errors, got {agg.InnerExceptions.Count}");
        Assert.IsTrue(agg.InnerExceptions[0] is PostgresException, "inner should be PostgresException");

        await PgTestPool.RunAsync(protocol, "select 1");
    }
}
