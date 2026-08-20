using System.Runtime.CompilerServices;
using Slon.Pools;
using static Slon.Pools.ConnectionPool;

namespace Slon.Tests;

[TestClass]
public class WaitQueueTests
{
    [TestMethod]
    public async Task NewWaiter_DrivesCapacityPublishedWhileDormant()
    {
        using var queue = new ConnectionPool.WaitQueue<int>();
        queue.NotifyAvailability();
        var available = new StrongBox<bool>(true);
        var attempts = new StrongBox<int>();
        var waiter = queue.CreateWaiter(
            static state =>
            {
                Interlocked.Increment(ref state.Attempts.Value);
                return Volatile.Read(ref state.Available.Value)
                    ? PlacementAttempt<int>.Placed(42)
                    : PlacementAttempt<int>.Unavailable;
            },
            (Available: available, Attempts: attempts));

        using var registration = queue.Enqueue(waiter);
        var completion = await waiter.AsValueTask();

        Assert.IsTrue(completion.HasResult);
        Assert.AreEqual(42, completion.Result);
        Assert.AreEqual(1, attempts.Value);
        Assert.IsFalse(queue.HasDemand);
    }

    [TestMethod]
    public async Task BellDuringPass_ForcesGenerationRecheck()
    {
        using var queue = new ConnectionPool.WaitQueue<int>();
        var attempts = new StrongBox<int>();
        var waiter = queue.CreateWaiter(
            static state =>
            {
                if (Interlocked.Increment(ref state.Attempts.Value) == 1)
                {
                    state.Coordinator.NotifyAvailability();
                    return PlacementAttempt<int>.Unavailable;
                }
                return PlacementAttempt<int>.Placed(42);
            },
            (Coordinator: queue, Attempts: attempts));

        using var registration = queue.Enqueue(waiter);
        var completion = await waiter.AsValueTask();

        Assert.IsTrue(completion.HasResult);
        Assert.AreEqual(42, completion.Result);
        Assert.AreEqual(2, attempts.Value);
        Assert.AreEqual(2, queue.Metrics.TotalExamined);
        Assert.AreEqual(1, queue.Metrics.TotalPlacements);
        Assert.AreEqual(1, queue.Metrics.TotalGenerationRestarts);
        Assert.IsTrue(queue.Metrics.MaxInlineDuration > TimeSpan.Zero);
    }

    [TestMethod]
    public async Task BellDuringAttempt_RestartsFifoBeforeTryingNewFollower()
    {
        using var queue = new ConnectionPool.WaitQueue<int>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var attempts = new StrongBox<int>();
        var winner = new StrongBox<int>();
        var first = queue.CreateWaiter(
            static state =>
            {
                var attempt = Interlocked.Increment(ref state.Attempts.Value);
                if (attempt == 2)
                {
                    state.Coordinator.NotifyAvailability();
                    state.Entered.TrySetResult();
                    state.Release.Wait();
                    return PlacementAttempt<int>.Unavailable;
                }
                if (attempt == 3 && Interlocked.CompareExchange(ref state.Winner.Value, 1, 0) == 0)
                    return PlacementAttempt<int>.Placed(1);
                return PlacementAttempt<int>.Unavailable;
            },
            (Coordinator: queue, Entered: entered, Release: release, Attempts: attempts, Winner: winner));
        using var firstRegistration = queue.Enqueue(first);

        var publication = Task.Factory.StartNew(queue.NotifyAvailability,
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await entered.Task;

        var second = queue.CreateWaiter(static winner =>
            Interlocked.CompareExchange(ref winner.Value, 2, 0) == 0
                ? PlacementAttempt<int>.Placed(2)
                : PlacementAttempt<int>.Unavailable, winner);
        using var secondRegistration = queue.Enqueue(second);
        release.Set();
        await publication;

        Assert.AreEqual(1, (await first.AsValueTask()).Result,
            "a new generation must restart selection at the FIFO head");
        Assert.AreEqual(1, winner.Value);
        Assert.IsFalse(second.AsValueTask().IsCompleted,
            "a follower must not consume capacity published after the head's prior attempt");
    }

    [TestMethod]
    public async Task JoinDuringPass_DoesNotRestartAheadOfExistingFollower()
    {
        using var queue = new ConnectionPool.WaitQueue<int>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var attempts = new StrongBox<int>();
        var first = queue.CreateWaiter(
            static state =>
            {
                var attempt = Interlocked.Increment(ref state.Attempts.Value);
                if (attempt == 2)
                {
                    state.Entered.TrySetResult();
                    state.Release.Wait();
                }
                else if (attempt == 3)
                {
                    var newcomer = state.Coordinator.CreateWaiter(
                        static _ => PlacementAttempt<int>.Unavailable, state: 0);
                    state.Coordinator.Enqueue(newcomer);
                }
                return PlacementAttempt<int>.Unavailable;
            },
            (Coordinator: queue, Entered: entered, Release: release, Attempts: attempts));
        using var firstRegistration = queue.Enqueue(first);

        var drive = Task.Factory.StartNew(queue.NotifyAvailability,
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await entered.Task;
        var attemptsSeenByFollower = new StrongBox<int>();
        var follower = queue.CreateWaiter(static state =>
        {
            state.Seen.Value = state.Attempts.Value;
            return PlacementAttempt<int>.Placed(2);
        }, (Seen: attemptsSeenByFollower, Attempts: attempts));
        using var followerRegistration = queue.Enqueue(follower);
        release.Set();

        Assert.AreEqual(2, (await follower.AsValueTask()).Result);
        await drive;
        Assert.AreEqual(3, attemptsSeenByFollower.Value,
            "a join during the pass must not restart at the head before the existing follower");
    }

    [TestMethod]
    public async Task GenerationChurn_DoesNotRestartFiniteSnapshot()
    {
        using var queue = new ConnectionPool.WaitQueue<int>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var attempts = new StrongBox<int>();
        var first = queue.CreateWaiter(static state =>
        {
            var attempt = Interlocked.Increment(ref state.Attempts.Value);
            if (attempt == 1)
            {
                state.Entered.TrySetResult();
                state.Release.Wait();
            }
            if (attempt is 2 or 3)
                state.Coordinator.NotifyAvailability();
            return PlacementAttempt<int>.Unavailable;
        }, (Coordinator: queue, Entered: entered, Release: release, Attempts: attempts));
        var firstEnqueue = Task.Factory.StartNew(() => queue.Enqueue(first),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await entered.Task;
        var attemptsSeenByFollower = new StrongBox<int>();
        var follower = queue.CreateWaiter(static state =>
        {
            state.Seen.Value = state.Attempts.Value;
            return PlacementAttempt<int>.Placed(2);
        }, (Seen: attemptsSeenByFollower, Attempts: attempts));
        using var followerRegistration = queue.Enqueue(follower);
        release.Set();

        Assert.AreEqual(2, (await follower.AsValueTask()).Result);
        using var firstRegistration = await firstEnqueue;
        Assert.AreEqual(2, attemptsSeenByFollower.Value,
            "generation changes become a subsequent-pass obligation and must not reset current progress");
    }

    [TestMethod]
    public async Task FirstCompatibleWaiterWinsWithoutRemovingRejectedHead()
    {
        using var queue = new ConnectionPool.WaitQueue<int>();
        var firstEnabled = new StrongBox<bool>();
        var first = queue.CreateWaiter(
            static enabled => enabled.Value
                ? PlacementAttempt<int>.Placed(1)
                : PlacementAttempt<int>.Unavailable,
            firstEnabled);
        using var firstRegistration = queue.Enqueue(first);

        var second = queue.CreateWaiter(static _ => PlacementAttempt<int>.Placed(2), state: 0);
        using var secondRegistration = queue.Enqueue(second);
        var secondCompletion = await second.AsValueTask();
        Assert.AreEqual(2, secondCompletion.Result);
        Assert.AreEqual(1, queue.Count);

        firstEnabled.Value = true;
        queue.NotifyAvailability();
        var firstCompletion = await first.AsValueTask();
        Assert.AreEqual(1, firstCompletion.Result);
    }

    [TestMethod]
    public async Task AlreadyCancelledWaiterNeverCallsPlacer()
    {
        using var queue = new ConnectionPool.WaitQueue<int>();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var waiter = queue.CreateWaiter(
            static _ => throw new InvalidOperationException("a cancelled waiter must not drive"), state: 0);

        using var registration = queue.Enqueue(waiter, cancellation.Token);
        var completion = await waiter.AsValueTask();

        Assert.IsFalse(completion.HasResult);
        Assert.IsInstanceOfType<OperationCanceledException>(completion.Exception);
        Assert.IsFalse(queue.HasDemand);
    }

    [TestMethod]
    public async Task QueuedCancellationCompletesWithoutDisturbingActiveDriver()
    {
        using var queue = new ConnectionPool.WaitQueue<int>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var first = queue.CreateWaiter(
            static state =>
            {
                state.Entered.TrySetResult();
                state.Release.Wait();
                return PlacementAttempt<int>.Unavailable;
            }, (Entered: entered, Release: release));
        var firstEnqueue = Task.Run(() => queue.Enqueue(first));
        await entered.Task;

        using var cancellation = new CancellationTokenSource();
        var second = queue.CreateWaiter(static _ => PlacementAttempt<int>.Placed(2), state: 0);
        using var secondRegistration = queue.Enqueue(second, cancellation.Token);
        cancellation.Cancel();
        var secondCompletion = await second.AsValueTask();
        Assert.IsInstanceOfType<OperationCanceledException>(secondCompletion.Exception);

        release.Set();
        using var firstRegistration = await firstEnqueue;
        queue.Dispose();
        var firstCompletion = await first.AsValueTask();
        Assert.IsInstanceOfType<ObjectDisposedException>(firstCompletion.Exception);
    }

    [TestMethod]
    public async Task CancellationDuringTrying_FailedPlacementCompletesCancellation()
        => await AssertTerminationDuringTrying(dispose: false, placementSucceeds: false);

    [TestMethod]
    public async Task CancellationDuringTrying_SuccessfulPlacementCarriesDeferredCancellation()
        => await AssertTerminationDuringTrying(dispose: false, placementSucceeds: true);

    [TestMethod]
    public async Task DisposalDuringTrying_FailedPlacementCompletesDisposal()
        => await AssertTerminationDuringTrying(dispose: true, placementSucceeds: false);

    [TestMethod]
    public async Task DisposalDuringTrying_SuccessfulPlacementCarriesDeferredDisposal()
        => await AssertTerminationDuringTrying(dispose: true, placementSucceeds: true);

    [TestMethod]
    public async Task DisposalSweep_CompletesQueuedWaiterAndDefersTryingWaiter()
    {
        var queue = new ConnectionPool.WaitQueue<int>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var first = queue.CreateWaiter(
            static state =>
            {
                state.Entered.TrySetResult();
                state.Release.Wait();
                return PlacementAttempt<int>.Unavailable;
            }, (Entered: entered, Release: release));
        var firstEnqueue = Task.Run(() => queue.Enqueue(first));
        await entered.Task;

        var second = queue.CreateWaiter(static _ => PlacementAttempt<int>.Placed(2), state: 0);
        using var secondRegistration = queue.Enqueue(second);
        queue.Dispose();

        var secondCompletion = await second.AsValueTask();
        Assert.IsFalse(secondCompletion.HasResult);
        Assert.IsInstanceOfType<ObjectDisposedException>(secondCompletion.Exception);

        release.Set();
        using var firstRegistration = await firstEnqueue;
        var firstCompletion = await first.AsValueTask();
        Assert.IsFalse(firstCompletion.HasResult);
        Assert.IsInstanceOfType<ObjectDisposedException>(firstCompletion.Exception);
    }

    [TestMethod]
    public void SynchronousWaiter_UsesSamePlacementResult()
    {
        using var queue = new ConnectionPool.WaitQueue<int>();
        var waiter = queue.CreateWaiter(
            static _ => PlacementAttempt<int>.Placed(42), state: 0, synchronous: true);

        using var registration = queue.Enqueue(waiter);
        var completion = waiter.Wait();

        Assert.IsTrue(completion.HasResult);
        Assert.AreEqual(42, completion.Result);
        Assert.IsNull(completion.Exception);
    }

    static async Task AssertTerminationDuringTrying(bool dispose, bool placementSucceeds)
    {
        var queue = new ConnectionPool.WaitQueue<int>();
        using var cancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var waiter = queue.CreateWaiter(
            static state =>
            {
                state.Entered.TrySetResult();
                state.Release.Wait();
                return state.Succeeds
                    ? PlacementAttempt<int>.Placed(42)
                    : PlacementAttempt<int>.Unavailable;
            }, (Entered: entered, Release: release, Succeeds: placementSucceeds));
        var enqueue = Task.Run(() => queue.Enqueue(waiter, cancellation.Token));
        await entered.Task;

        if (dispose)
            queue.Dispose();
        else
            cancellation.Cancel();
        release.Set();

        using var registration = await enqueue;
        var completion = await waiter.AsValueTask();
        Assert.AreEqual(placementSucceeds, completion.HasResult);
        if (placementSucceeds)
            Assert.AreEqual(42, completion.Result);
        if (dispose)
            Assert.IsInstanceOfType<ObjectDisposedException>(completion.Exception);
        else
            Assert.IsInstanceOfType<OperationCanceledException>(completion.Exception);
        queue.Dispose();
    }
}
