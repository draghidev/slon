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
}

