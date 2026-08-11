using System.Runtime.CompilerServices;
using Slon.Pools;

namespace Slon.Tests;

[TestClass]
public class AcquisitionCoordinatorTests
{
    [TestMethod]
    public async Task NewWaiter_DrivesCapacityPublishedWhileDormant()
    {
        using var coordinator = new AcquisitionCoordinator<int>();
        coordinator.NotifyAvailability();
        var available = new StrongBox<bool>(true);
        var attempts = new StrongBox<int>();
        var waiter = coordinator.CreateWaiter(
            static state =>
            {
                Interlocked.Increment(ref state.Attempts.Value);
                return Volatile.Read(ref state.Available.Value)
                    ? PlacementAttempt<int>.Placed(42)
                    : PlacementAttempt<int>.Unavailable;
            },
            (Available: available, Attempts: attempts));

        using var registration = coordinator.Enqueue(waiter);
        var completion = await waiter.AsValueTask();

        Assert.IsTrue(completion.HasResult);
        Assert.AreEqual(42, completion.Result);
        Assert.AreEqual(1, attempts.Value);
        Assert.IsFalse(coordinator.HasDemand);
    }

    [TestMethod]
    public async Task BellDuringPass_ForcesGenerationRecheck()
    {
        using var coordinator = new AcquisitionCoordinator<int>();
        var attempts = new StrongBox<int>();
        var waiter = coordinator.CreateWaiter(
            static state =>
            {
                if (Interlocked.Increment(ref state.Attempts.Value) == 1)
                {
                    state.Coordinator.NotifyAvailability();
                    return PlacementAttempt<int>.Unavailable;
                }
                return PlacementAttempt<int>.Placed(42);
            },
            (Coordinator: coordinator, Attempts: attempts));

        using var registration = coordinator.Enqueue(waiter);
        var completion = await waiter.AsValueTask();

        Assert.IsTrue(completion.HasResult);
        Assert.AreEqual(42, completion.Result);
        Assert.AreEqual(2, attempts.Value);
        Assert.AreEqual(2, coordinator.Metrics.TotalExamined);
        Assert.AreEqual(1, coordinator.Metrics.TotalPlacements);
        Assert.AreEqual(1, coordinator.Metrics.TotalGenerationRestarts);
        Assert.IsTrue(coordinator.Metrics.MaxInlineDuration > TimeSpan.Zero);
    }

    [TestMethod]
    public async Task FirstCompatibleWaiterWinsWithoutRemovingRejectedHead()
    {
        using var coordinator = new AcquisitionCoordinator<int>();
        var firstEnabled = new StrongBox<bool>();
        var first = coordinator.CreateWaiter(
            static enabled => enabled.Value
                ? PlacementAttempt<int>.Placed(1)
                : PlacementAttempt<int>.Unavailable,
            firstEnabled);
        using var firstRegistration = coordinator.Enqueue(first);

        var second = coordinator.CreateWaiter(static _ => PlacementAttempt<int>.Placed(2), state: 0);
        using var secondRegistration = coordinator.Enqueue(second);
        var secondCompletion = await second.AsValueTask();
        Assert.AreEqual(2, secondCompletion.Result);
        Assert.AreEqual(1, coordinator.Count);

        firstEnabled.Value = true;
        coordinator.NotifyAvailability();
        var firstCompletion = await first.AsValueTask();
        Assert.AreEqual(1, firstCompletion.Result);
    }

    [TestMethod]
    public async Task AlreadyCancelledWaiterNeverCallsPlacer()
    {
        using var coordinator = new AcquisitionCoordinator<int>();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var waiter = coordinator.CreateWaiter(
            static _ => throw new InvalidOperationException("a cancelled waiter must not drive"), state: 0);

        using var registration = coordinator.Enqueue(waiter, cancellation.Token);
        var completion = await waiter.AsValueTask();

        Assert.IsFalse(completion.HasResult);
        Assert.IsInstanceOfType<OperationCanceledException>(completion.Exception);
        Assert.IsFalse(coordinator.HasDemand);
    }

    [TestMethod]
    public async Task QueuedCancellationCompletesWithoutDisturbingActiveDriver()
    {
        using var coordinator = new AcquisitionCoordinator<int>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var first = coordinator.CreateWaiter(
            static state =>
            {
                state.Entered.TrySetResult();
                state.Release.Wait();
                return PlacementAttempt<int>.Unavailable;
            }, (Entered: entered, Release: release));
        var firstEnqueue = Task.Run(() => coordinator.Enqueue(first));
        await entered.Task;

        using var cancellation = new CancellationTokenSource();
        var second = coordinator.CreateWaiter(static _ => PlacementAttempt<int>.Placed(2), state: 0);
        using var secondRegistration = coordinator.Enqueue(second, cancellation.Token);
        cancellation.Cancel();
        var secondCompletion = await second.AsValueTask();
        Assert.IsInstanceOfType<OperationCanceledException>(secondCompletion.Exception);

        release.Set();
        using var firstRegistration = await firstEnqueue;
        coordinator.Dispose();
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
        var coordinator = new AcquisitionCoordinator<int>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var first = coordinator.CreateWaiter(
            static state =>
            {
                state.Entered.TrySetResult();
                state.Release.Wait();
                return PlacementAttempt<int>.Unavailable;
            }, (Entered: entered, Release: release));
        var firstEnqueue = Task.Run(() => coordinator.Enqueue(first));
        await entered.Task;

        var second = coordinator.CreateWaiter(static _ => PlacementAttempt<int>.Placed(2), state: 0);
        using var secondRegistration = coordinator.Enqueue(second);
        coordinator.Dispose();

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
        using var coordinator = new AcquisitionCoordinator<int>();
        using var waiter = coordinator.CreateWaiter(
            static _ => PlacementAttempt<int>.Placed(42), state: 0, synchronous: true);

        using var registration = coordinator.Enqueue(waiter);
        var completion = waiter.Wait();

        Assert.IsTrue(completion.HasResult);
        Assert.AreEqual(42, completion.Result);
        Assert.IsNull(completion.Exception);
    }

    static async Task AssertTerminationDuringTrying(bool dispose, bool placementSucceeds)
    {
        var coordinator = new AcquisitionCoordinator<int>();
        using var cancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var waiter = coordinator.CreateWaiter(
            static state =>
            {
                state.Entered.TrySetResult();
                state.Release.Wait();
                return state.Succeeds
                    ? PlacementAttempt<int>.Placed(42)
                    : PlacementAttempt<int>.Unavailable;
            }, (Entered: entered, Release: release, Succeeds: placementSucceeds));
        var enqueue = Task.Run(() => coordinator.Enqueue(waiter, cancellation.Token));
        await entered.Task;

        if (dispose)
            coordinator.Dispose();
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
        coordinator.Dispose();
    }
}
