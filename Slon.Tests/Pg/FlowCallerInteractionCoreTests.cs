using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

[TestClass]
public class FlowCallerInteractionCoreTests
{
    static readonly TimeSpan Cap = TimeSpan.FromSeconds(10);

    sealed class CoreBox
    {
        public FlowCallerInteractionCore<ValueTuple> Core;

        public CoreBox() => Core.Initialize();
    }

    [TestMethod]
    public void SignalProgress_BeforeWait_IsSticky()
    {
        var box = new CoreBox();

        box.Core.SignalProgress();

        Assert.IsNull(box.Core.WaitForContinuation());
    }

    [TestMethod]
    public async Task SignalProgress_AfterWait_WakesWithoutContinuation()
    {
        var box = new CoreBox();
        var waiter = Task.Factory.StartNew(
            () => box.Core.WaitForContinuation(),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        Assert.IsTrue(SpinWait.SpinUntil(() => box.Core.IsWaiting, Cap), "waiter did not park");
        box.Core.SignalProgress();

        Assert.IsNull(await waiter.WaitAsync(Cap));
    }

    [TestMethod]
    public async Task SignalProgress_RacingFirstWait_NeverStrands()
    {
        var iterations = StressEnv.Iterations(fallback: 1_000, cap: 100_000);
        using var phases = new Barrier(3);
        CoreBox? current = null;
        Exception? workerFailure = null;

        var waiter = Task.Factory.StartNew(() => RunWorker(wait: true), CancellationToken.None,
            TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var signaler = Task.Factory.StartNew(() => RunWorker(wait: false), CancellationToken.None,
            TaskCreationOptions.LongRunning, TaskScheduler.Default);

        for (var i = 0; i < iterations; i++)
        {
            Volatile.Write(ref current, new CoreBox());
            phases.SignalAndWait();
            if (!phases.SignalAndWait(Cap))
            {
                Volatile.Read(ref current)!.Core.SignalProgress();
                Assert.Fail($"iteration {i}: progress publication raced lazy event creation and stranded the waiter");
            }
            if (Volatile.Read(ref workerFailure) is { } failure)
                Assert.Fail($"iteration {i}: worker failed: {failure}");
        }

        await Task.WhenAll(waiter, signaler).WaitAsync(Cap);
        return;

        void RunWorker(bool wait)
        {
            try
            {
                for (var i = 0; i < iterations; i++)
                {
                    phases.SignalAndWait();
                    var box = Volatile.Read(ref current)!;
                    if (wait)
                    {
                        var continuation = box.Core.WaitForContinuation();
                        if (continuation is not null)
                            throw new InvalidOperationException("progress-only wake returned a continuation");
                    }
                    else
                        box.Core.SignalProgress();
                    phases.SignalAndWait();
                }
            }
            catch (Exception ex)
            {
                Interlocked.CompareExchange(ref workerFailure, ex, null);
                throw;
            }
        }
    }

    [TestMethod]
    public void Reset_RetainsRendezvousWithoutRetainingProgress()
    {
        var box = new CoreBox();
        box.Core.SignalProgress();
        Assert.IsNull(box.Core.WaitForContinuation());

        box.Core.Reset();
        var wait = Task.Factory.StartNew(
            () => box.Core.WaitForContinuation(),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        Assert.IsTrue(SpinWait.SpinUntil(() => box.Core.IsWaiting, Cap), "reused waiter did not park");

        box.Core.SignalProgress();
        Assert.IsNull(wait.WaitAsync(Cap).GetAwaiter().GetResult());
    }
}
