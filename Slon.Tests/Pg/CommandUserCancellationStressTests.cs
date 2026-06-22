using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// High-iteration stress for the user-cancellation move-next rendezvous, exercising the two-token model:
//   - a token bound at SUBMIT (TryQueue/BindCallerToken) is flow-scoped and honored from the eager write,
//     so a pre-fired token cancels the WHOLE flow deterministically - the first result cannot race through;
//   - a token bound at GetAsyncEnumerator is flow-scoped too but bound AFTER the eager dispatch, so the
//     first result is ALLOWED to race (either a delivered result or an OCE is acceptable);
//   - a per-read MoveNextAsync(ct) token cancels just that read.
// The bug class under test is the lost wake / wrong-generation double-fire / pipeline-tenure reentry that
// the generation-bound registration + version-aware terminal closed. A hang (caught by the per-iteration
// timeout) is always a failure; the submit-bound case additionally requires a strict OCE.
[TestClass]
[DoNotParallelize]
public class CommandUserCancellationStressTests
{
    static readonly TimeSpan HangCap = TimeSpan.FromSeconds(5);

    static int Iters => int.TryParse(Environment.GetEnvironmentVariable("SLON_STRESS_ITERS"), out var n) && n > 0 ? n : 6000;

    static async Task<PgClientProtocol[]> Pool(int n)
    {
        var arr = new PgClientProtocol[n];
        for (var i = 0; i < n; i++)
            arr[i] = await PgTestPool.NewIsolatedAsync();
        return arr;
    }

    static CommandFlow NewFlow() => new(async: true,
        Command.Create("select generate_series(1, 50)"),
        Command.Create("select 'two'"));

    static async Task Cleanup(CommandFlow.Enumerator e, int i)
    {
        try { await e.DisposeAsync().AsTask().WaitAsync(HangCap); }
        catch (TimeoutException) { Assert.Fail($"iter {i}: DisposeAsync never completed."); }
        catch (OperationCanceledException) { }
        catch (PgClientClosedException) { }
    }

    // Token bound at SUBMIT, pre-fired: the eager write honors it, so the first MoveNextAsync must surface
    // OCE deterministically and never hang.
    [TestMethod]
    public async Task SubmitBound_PreFired_SurfacesOce()
    {
        var protocols = await Pool(8);
        for (var i = 0; i < Iters; i++)
        {
            var protocol = protocols[i % protocols.Length];
            var flow = NewFlow();
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));

            var e = flow.GetAsyncEnumerator(cts.Token);
            try
            {
                OperationCanceledException? oce = null;
                try { await e.MoveNextAsync(cts.Token).AsTask().WaitAsync(HangCap); }
                catch (OperationCanceledException ex) { oce = ex; }
                catch (TimeoutException) { Assert.Fail($"iter {i}: MoveNextAsync never completed (lost wake)."); }
                Assert.IsNotNull(oce, $"iter {i}: submit-bound pre-fired token must surface OCE, not a result");
            }
            finally { await Cleanup(e, i); }
        }
    }

    // Token bound at GetAsyncEnumerator, pre-fired: bound after the eager dispatch, so the first result is
    // ALLOWED to race. Either an OCE or a delivered result is acceptable; only a hang fails.
    [TestMethod]
    public async Task EnumeratorBound_PreFired_NeverLosesWake()
    {
        var protocols = await Pool(8);
        for (var i = 0; i < Iters; i++)
        {
            var protocol = protocols[i % protocols.Length];
            var flow = NewFlow();
            Assert.IsTrue(protocol.TryQueue(flow));

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var e = flow.GetAsyncEnumerator(cts.Token);
            try
            {
                Exception? caught = null;
                try { await e.MoveNextAsync(cts.Token).AsTask().WaitAsync(HangCap); }
                catch (TimeoutException) { Assert.Fail($"iter {i}: MoveNextAsync never completed (lost wake)."); }
                catch (Exception ex) { caught = ex; }
                if (caught is not null and not OperationCanceledException and not PgClientClosedException)
                    Assert.Fail($"iter {i}: unexpected {caught.GetType().Name}: {caught}");
            }
            finally { await Cleanup(e, i); }
        }
    }

    // Timer-fired token (independent thread) bound at GetAsyncEnumerator: races the body and the consumer.
    // Either outcome is acceptable; only a hang fails.
    [TestMethod]
    public async Task EnumeratorBound_TimerFired_NeverLosesWake()
    {
        var protocols = await Pool(8);
        for (var i = 0; i < Iters; i++)
        {
            var protocol = protocols[i % protocols.Length];
            var flow = NewFlow();
            Assert.IsTrue(protocol.TryQueue(flow));

            using var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromTicks((i % 7) * 5000));

            var e = flow.GetAsyncEnumerator(cts.Token);
            try
            {
                Exception? caught = null;
                try { await e.MoveNextAsync(cts.Token).AsTask().WaitAsync(HangCap); }
                catch (TimeoutException) { Assert.Fail($"iter {i}: MoveNextAsync never completed (lost wake)."); }
                catch (Exception ex) { caught = ex; }
                if (caught is not null and not OperationCanceledException and not PgClientClosedException)
                    Assert.Fail($"iter {i}: unexpected {caught.GetType().Name}: {caught}");
            }
            finally { await Cleanup(e, i); }
        }
    }

    // Pipelined overlap: queue TWO cancellable flows back-to-back on one protocol so their executions
    // overlap, maximizing the chance an off-stack cancel completion of one flow drives the pipeline
    // advance while the other flow's execution promise is tenured (the concurrent-tenure hazard). Submit-
    // bound pre-fired token on each. Only a hang or an unexpected exception fails.
    [TestMethod]
    public async Task Pipelined_SubmitBound_PreFired_NoTenureCollision()
    {
        var protocols = await Pool(8);
        for (var i = 0; i < Iters; i++)
        {
            var protocol = protocols[i % protocols.Length];
            var a = NewFlow();
            var b = NewFlow();
            using var ctsA = new CancellationTokenSource();
            using var ctsB = new CancellationTokenSource();
            ctsA.Cancel();
            ctsB.Cancel();
            Assert.IsTrue(protocol.TryQueue(a, cancellationToken: ctsA.Token));
            Assert.IsTrue(protocol.TryQueue(b, cancellationToken: ctsB.Token));
            var ea = a.GetAsyncEnumerator(ctsA.Token);
            var eb = b.GetAsyncEnumerator(ctsB.Token);
            try
            {
                var ta = DriveOnce(ea, ctsA.Token, i);
                var tb = DriveOnce(eb, ctsB.Token, i);
                await Task.WhenAll(ta, tb);
            }
            finally
            {
                await Cleanup(ea, i);
                await Cleanup(eb, i);
            }
        }

        static async Task DriveOnce(CommandFlow.Enumerator e, CancellationToken ct, int i)
        {
            try { await e.MoveNextAsync(ct).AsTask().WaitAsync(HangCap); }
            catch (TimeoutException) { Assert.Fail($"iter {i}: MoveNextAsync never completed (lost wake)."); }
            catch (OperationCanceledException) { }
            catch (PgClientClosedException) { }
        }
    }

    // Opt-in await-drain: WaitForDrainOnDispose=true makes DisposeAsync PARK on the body's completion (no poll
    // loop, no spin) so the wire is drained to RFQ before it returns. Dispose mid-batch after result 1,
    // then immediately reuse the protocol - if the drain hadn't completed, the next op would desync.
    [TestMethod]
    public async Task WaitForDrainOnDispose_MidBatch_AwaitsDrain_ConnectionUsable()
    {
        var protocols = await Pool(8);
        var iters = Math.Min(Iters, 3000);
        for (var i = 0; i < iters; i++)
        {
            var protocol = protocols[i % protocols.Length];
            var flow = new CommandFlow(async: true,
                Command.Create("select generate_series(1, 50)"),
                Command.Create("select 'two'"),
                Command.Create("select 'three'"));
            flow.WaitForDrainOnDispose = true;
            Assert.IsTrue(protocol.TryQueue(flow));

            var e = flow.GetAsyncEnumerator();
            try { Assert.IsTrue(await e.MoveNextAsync().AsTask().WaitAsync(HangCap), $"iter {i}: result 1"); }
            catch (TimeoutException) { Assert.Fail($"iter {i}: first MoveNextAsync hung"); }

            // Dispose mid-batch; with WaitForDrainOnDispose it must not return until the body drained to RFQ.
            try { await e.DisposeAsync().AsTask().WaitAsync(HangCap); }
            catch (TimeoutException) { Assert.Fail($"iter {i}: WaitForDrainOnDispose DisposeAsync never completed (drain spin/hang)."); }

            // Immediately reuse - synchronously after the awaited drain, the wire must be at RFQ.
            try { await PgTestPool.RunAsync(protocol, "select 1").WaitAsync(HangCap); }
            catch (TimeoutException) { Assert.Fail($"iter {i}: protocol unusable after WaitForDrainOnDispose (wire not at RFQ)."); }
            catch (Exception ex) { Assert.Fail($"iter {i}: reuse after WaitForDrainOnDispose threw {ex.GetType().Name}: {ex.Message}"); }
        }
    }

    // WaitForDrainOnDispose bounded by the flow token: if the caller's token fires while Dispose is
    // waiting for the drain, Dispose must unwind FAST (not block the full drain) and not throw the OCE.
    // The body stays consumer-gone and finishes draining in the background. A pre-fired token here means
    // the wait returns immediately.
    [TestMethod]
    public async Task WaitForDrainOnDispose_TokenFired_UnwindsFast_NoThrow()
    {
        var protocols = await Pool(8);
        var iters = Math.Min(Iters, 3000);
        for (var i = 0; i < iters; i++)
        {
            var protocol = protocols[i % protocols.Length];
            using var cts = new CancellationTokenSource();
            cts.Cancel(); // pre-fired: the drain-wait must not block on it
            var flow = new CommandFlow(async: true,
                Command.Create("select generate_series(1, 50)"),
                Command.Create("select 'two'"));
            flow.WaitForDrainOnDispose = true;
            Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));

            var e = flow.GetAsyncEnumerator(cts.Token);
            // First read surfaces OCE (pre-fired) or a result; either is fine.
            try { await e.MoveNextAsync(cts.Token).AsTask().WaitAsync(HangCap); }
            catch (TimeoutException) { Assert.Fail($"iter {i}: first MoveNextAsync hung"); }
            catch (OperationCanceledException) { }
            catch (PgClientClosedException) { }

            // Dispose with WaitForDrainOnDispose + a fired token: must unwind fast and not throw.
            try { await e.DisposeAsync().AsTask().WaitAsync(HangCap); }
            catch (TimeoutException) { Assert.Fail($"iter {i}: token-bounded DisposeAsync did not unwind (blocked on drain past cancel)."); }
            catch (Exception ex) { Assert.Fail($"iter {i}: DisposeAsync threw {ex.GetType().Name} (should swallow the cancel): {ex.Message}"); }

            // The body still drains in the background; the next op may briefly wait on it but must succeed.
            try { await PgTestPool.RunAsync(protocol, "select 1").WaitAsync(HangCap); }
            catch (TimeoutException) { Assert.Fail($"iter {i}: protocol unusable after token-bounded dispose."); }
            catch (PgClientClosedException) { }
        }
    }

    // ErrorResponse surfacing on a consumer-gone drain (Npgsql parity). A command faults; the reader is
    // disposed mid-batch with WaitForDrainOnDispose, so DisposeAsync parks on the drain and must rethrow
    // the Postgres error. Single faulting sync segment => a BARE PostgresException.
    [TestMethod]
    public async Task WaitForDrainOnDispose_DrainHitsError_ThrowsBarePostgresException()
    {
        var protocol = await PgTestPool.NewIsolatedAsync();
        // First command succeeds and yields a result; the SECOND faults (undefined table). One trailing
        // sync (extended, WithSync=false) => one fault segment.
        var flow = new CommandFlow(async: true,
            Command.Create("select 1"),
            Command.Create("select * from no_such_table_xyz"));
        Assert.IsTrue(protocol.TryQueue(flow));
        flow.WaitForDrainOnDispose = true;

        var e = flow.GetAsyncEnumerator();
        Assert.IsTrue(await e.MoveNextAsync(), "first result not delivered");

        // Dispose mid-batch: the drain hits the second command's ErrorResponse; await must surface it.
        var ex = await Assert.ThrowsExactlyAsync<PostgresException>(async () => await e.DisposeAsync());
        Assert.AreEqual("42P01", ex.SqlState, "expected undefined_table");

        // Connection still usable (drained to RFQ).
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    // Multi-sync: per-command-sync (PreferSimple+WithSync) commands that EACH fault => multiple fault
    // segments => DisposeAsync throws an AggregateException of PostgresExceptions (full fidelity).
    [TestMethod]
    public async Task WaitForDrainOnDispose_MultiSyncErrors_ThrowsAggregate()
    {
        var protocol = await PgTestPool.NewIsolatedAsync();
        var bad1 = Command.Create("select * from no_such_table_a") with { PreferSimple = true, WithSync = true };
        var bad2 = Command.Create("select * from no_such_table_b") with { PreferSimple = true, WithSync = true };
        var flow = new CommandFlow(async: true, Command.Create("select 1") with { PreferSimple = true, WithSync = true }, bad1, bad2);
        Assert.IsTrue(protocol.TryQueue(flow));
        flow.WaitForDrainOnDispose = true;

        var e = flow.GetAsyncEnumerator();
        Assert.IsTrue(await e.MoveNextAsync(), "first result not delivered");

        var agg = await Assert.ThrowsExactlyAsync<AggregateException>(async () => await e.DisposeAsync());
        Assert.IsTrue(agg.InnerExceptions.Count >= 2, $"expected >=2 errors, got {agg.InnerExceptions.Count}");
        Assert.IsTrue(agg.InnerExceptions[0] is PostgresException, "inner should be PostgresException");

        await PgTestPool.RunAsync(protocol, "select 1");
    }
}

