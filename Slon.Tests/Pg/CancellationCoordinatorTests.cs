using Microsoft.Extensions.Time.Testing;
using Slon.Pg.Protocol;

namespace Slon.Tests.Pg;

// Direct tests for the production coordinator. The scenario exposes one wire's live activation and
// read frontier. No protocol, flow, decoder, transport, socket, or PostgreSQL process participates.
[TestClass]
public class CancellationCoordinatorTests
{
    static readonly TimeSpan Quantum = TimeSpan.FromMilliseconds(1);
    static readonly TimeSpan RetryInterval = TimeSpan.FromMilliseconds(5);
    static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromMilliseconds(20);

    sealed class Owner
    {
        public TimeSpan? GracePeriod { get; set; }
    }

    sealed class Wire
    {
        public Owner? ActivatedOwner { get; set; }
        public int ActivatedWindow { get; set; }
        public Owner? FrontierOwner { get; set; }
        public int FrontierWindow { get; set; } = -1;
        public Action? FrontierProbe { get; set; }

        public bool IsAtFrontier(Owner owner, int window)
        {
            FrontierProbe?.Invoke();
            return ReferenceEquals(FrontierOwner, owner) && FrontierWindow == window;
        }
    }

    sealed class Scenario : IDisposable
    {
        readonly CancellationTokenSource _abort = new();
        readonly TaskCompletionSource<Exception> _failure
            = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Scenario(Func<CancellationToken, ValueTask<CancelRequestState>>? cancelRequest,
            TimeSpan? cancelRequestDelay = null,
            Action<Exception, CancelRequestState>? requestFailed = null)
        {
            Time = new();
            Coordinator = new(Time, ConvergenceTimeout, RetryInterval,
                cancelRequestDelay ?? TimeSpan.Zero, _abort.Token, cancelRequest, Fail,
                _ => Wire.ActivatedOwner is { } activated
                    ? (activated, Wire.ActivatedWindow)
                    : (null, 0),
                (owner, window) => Wire.IsAtFrontier(owner, window),
                static owner => owner.GracePeriod, requestFailed);
            Wire.ActivatedOwner = Owner;
        }

        internal FakeTimeProvider Time { get; }
        internal Owner Owner { get; } = new();
        internal Wire Wire { get; } = new();
        internal object EpisodeKey { get; } = new();
        internal CancellationCoordinator<Owner> Coordinator { get; }
        internal Task<Exception> Failure => _failure.Task;

        internal Task Request(TaskCompletionSource? delivery = null,
            BackendCancellationTiming timing = BackendCancellationTiming.Immediate,
            bool remainingFlow = false, bool atReadFrontier = true, int window = 0)
            => Request(Owner, EpisodeKey, delivery, timing, remainingFlow, atReadFrontier, window);

        internal Task Request(Owner owner, object episodeKey,
            TaskCompletionSource? delivery = null,
            BackendCancellationTiming timing = BackendCancellationTiming.Immediate,
            bool remainingFlow = false, bool atReadFrontier = true, int window = 0)
        {
            delivery ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            Wire.ActivatedOwner = owner;
            Wire.ActivatedWindow = window;
            Wire.FrontierOwner = atReadFrontier ? owner : null;
            Wire.FrontierWindow = atReadFrontier ? window : -1;
            Coordinator.RequestCancellation(owner, window, timing, delivery, episodeKey,
                remainingFlow, BackendCancellationTiming.AtReadFrontier);
            return delivery.Task;
        }

        internal void EnterFrontier(Owner? owner = null, int window = 0)
        {
            owner ??= Owner;
            Wire.FrontierOwner = owner;
            Wire.FrontierWindow = window;
            Coordinator.OnReadFrontier();
        }

        internal void LeaveFrontier()
        {
            Wire.FrontierOwner = null;
            Wire.FrontierWindow = -1;
        }

        internal void Heartbeat(TimeSpan elapsed)
            => Coordinator.OnCancellationHeartbeat(elapsed);

        internal void CompleteWindow(int completedWindow, bool hasRemainingWindows)
            => Coordinator.OnWindowCompleted(Owner, completedWindow, hasRemainingWindows);

        internal void Advance(TimeSpan elapsed) => Time.Advance(elapsed);

        void Fail(Exception exception)
        {
            _failure.TrySetResult(exception);
            Coordinator.Terminate();
            _abort.Cancel();
        }

        public void Dispose()
        {
            Coordinator.Terminate();
            Coordinator.Dispose();
            _abort.Dispose();
        }
    }

    [TestMethod]
    public void RequestProbesFrontierOnlyAfterPublishingItsIntent()
    {
        using var scenario = new Scenario(_ => new(CancelRequestState.Sent));
        var observedPublishedIntent = false;
        scenario.Wire.FrontierProbe = () =>
            observedPublishedIntent = scenario.Coordinator.HasCancellationIntents;

        _ = scenario.Request(atReadFrontier: false,
            timing: BackendCancellationTiming.AtReadFrontier);

        Assert.IsTrue(observedPublishedIntent);
    }

    [TestMethod]
    public async Task SilentOwnerAndHungCancelRequest_DeadlineStillTerminates()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sender = new TaskCompletionSource<CancelRequestState>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var scenario = new Scenario(_ =>
        {
            started.TrySetResult();
            return new(sender.Task);
        });

        var delivery = scenario.Request();
        await started.Task;

        // No window completion, acknowledgement, frontier, release, substitution, or heartbeat is
        // supplied. Once accepted, convergence is owned by the coordinator's independent timer.
        scenario.Advance(ConvergenceTimeout);

        Assert.IsInstanceOfType<TimeoutException>(await scenario.Failure);
        await delivery;
        Assert.IsFalse(scenario.Coordinator.HasPendingCancellation);

        sender.SetResult(CancelRequestState.Sent);
        await Task.Yield();
        Assert.IsFalse(scenario.Coordinator.HasPendingCancellation);
    }

    [TestMethod]
    public async Task MissingSenderOwnsDeadlineWithoutMintingDispatchIntent()
    {
        using var scenario = new Scenario(cancelRequest: null);

        await scenario.Request();

        Assert.IsTrue(scenario.Coordinator.HasPendingCancellation);
        Assert.IsFalse(scenario.Coordinator.HasCancellationIntents);
        Assert.DoesNotContain("window=", scenario.Coordinator.DescribeState());
        scenario.Advance(ConvergenceTimeout);
        Assert.IsInstanceOfType<TimeoutException>(await scenario.Failure);
    }

    [TestMethod]
    public async Task AcknowledgedSyncWindowsReceiveIndependentCancellationDeadlines()
    {
        var calls = 0;
        using var scenario = new Scenario(_ =>
            new((Interlocked.Increment(ref calls), CancelRequestState.Sent).Item2));

        _ = scenario.Request(remainingFlow: true);
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1);
        await scenario.Coordinator.WaitForCancellationAttempt();
        Assert.IsTrue(scenario.Coordinator.OnCancellationObserved(scenario.Owner, window: 0));

        scenario.Advance(ConvergenceTimeout + ConvergenceTimeout);
        Assert.IsFalse(scenario.Failure.IsCompleted,
            "An acknowledged window no longer owns the cancellation deadline.");

        scenario.CompleteWindow(completedWindow: 0, hasRemainingWindows: true);
        scenario.Wire.ActivatedWindow = 1;
        scenario.EnterFrontier(window: 1);
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 2);
        await scenario.Coordinator.WaitForCancellationAttempt();

        scenario.Advance(ConvergenceTimeout - Quantum);
        Assert.IsFalse(scenario.Failure.IsCompleted,
            "The successor window receives a fresh deadline rather than inheriting elapsed time.");
        Assert.IsTrue(scenario.Coordinator.OnCancellationObserved(scenario.Owner, window: 1));

        scenario.Advance(ConvergenceTimeout + ConvergenceTimeout);
        Assert.IsFalse(scenario.Failure.IsCompleted);
        scenario.CompleteWindow(completedWindow: 1, hasRemainingWindows: false);
        scenario.Coordinator.OnOwnerReleased(scenario.Owner, wireIsIdle: true);
        Assert.IsFalse(scenario.Coordinator.HasPendingCancellation);
        Assert.IsFalse(scenario.Coordinator.HasPriorExposure(scenario.Owner, window: 1));
    }

    [TestMethod]
    public async Task AcknowledgedWindowStillBoundsItsPendingPhysicalSender()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sender = new TaskCompletionSource<CancelRequestState>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var scenario = new Scenario(_ =>
        {
            started.TrySetResult();
            return new(sender.Task);
        });

        var delivery = scenario.Request();
        await started.Task;
        Assert.IsTrue(scenario.Coordinator.OnCancellationObserved(scenario.Owner, window: 0));
        await delivery;

        scenario.Advance(ConvergenceTimeout);
        Assert.IsInstanceOfType<TimeoutException>(await scenario.Failure);
        sender.TrySetResult(CancelRequestState.Unknown);
    }

    [TestMethod]
    public async Task SenderBlockingBeforeReturningValueTaskCannotBlockDeadline()
    {
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        using var scenario = new Scenario(_ =>
        {
            entered.Set();
            release.Wait();
            return new(CancelRequestState.NotSent);
        });

        _ = scenario.Request();
        Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(1)));
        scenario.Advance(ConvergenceTimeout);
        Assert.IsInstanceOfType<TimeoutException>(await scenario.Failure);
        release.Set();
    }

    [TestMethod]
    public async Task NotSent_RetriesOnlyAfterPacingTick_AndDeliveryIsNotConvergence()
    {
        var calls = 0;
        using var scenario = new Scenario(_ =>
            new((Interlocked.Increment(ref calls), CancelRequestState.NotSent).Item2));

        var delivery = scenario.Request();
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1);
        await scenario.Coordinator.WaitForCancellationAttempt();
        Assert.IsFalse(delivery.IsCompleted);

        scenario.Heartbeat(RetryInterval - Quantum);
        Assert.AreEqual(1, Volatile.Read(ref calls));

        scenario.Heartbeat(Quantum);
        await delivery;
        Assert.AreEqual(2, Volatile.Read(ref calls));
        Assert.IsTrue(scenario.Coordinator.HasPendingCancellation);

        scenario.Advance(ConvergenceTimeout);
        Assert.IsInstanceOfType<TimeoutException>(await scenario.Failure);
    }

    [TestMethod]
    public async Task NaturalCompletionBeforeFirstDispatchCompletesDelivery()
    {
        var calls = 0;
        using var scenario = new Scenario(_ =>
            new((Interlocked.Increment(ref calls), CancelRequestState.Sent).Item2), RetryInterval);

        var delivery = scenario.Request(timing: BackendCancellationTiming.AfterGrace,
            atReadFrontier: false);
        Assert.IsFalse(delivery.IsCompleted);
        Assert.AreEqual(0, Volatile.Read(ref calls));

        scenario.CompleteWindow(completedWindow: 0, hasRemainingWindows: false);

        await delivery;
        Assert.AreEqual(0, Volatile.Read(ref calls));
        Assert.IsFalse(scenario.Coordinator.HasPendingCancellation);
    }

    [TestMethod]
    public async Task NaturalCompletionAfterNotSentCompletesDeliveryBeforeRetry()
    {
        var calls = 0;
        using var scenario = new Scenario(_ =>
            new((Interlocked.Increment(ref calls), CancelRequestState.NotSent).Item2));

        var delivery = scenario.Request();
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1);
        await scenario.Coordinator.WaitForCancellationAttempt();
        Assert.IsFalse(delivery.IsCompleted);

        scenario.CompleteWindow(completedWindow: 0, hasRemainingWindows: false);

        await delivery;
        scenario.Heartbeat(RetryInterval);
        Assert.AreEqual(1, Volatile.Read(ref calls));
        Assert.IsFalse(scenario.Coordinator.HasPendingCancellation);
    }

    [TestMethod]
    public async Task FinalWindowCompletionStopsPendingSenderAndCompletesDelivery()
    {
        var failures = 0;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sender = new TaskCompletionSource<CancelRequestState>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var scenario = new Scenario(
            token =>
            {
                started.TrySetResult();
                token.Register(static state =>
                {
                    var tuple = ((TaskCompletionSource Stopped,
                        TaskCompletionSource<CancelRequestState> Sender))state!;
                    tuple.Stopped.TrySetResult();
                    tuple.Sender.TrySetException(new OperationCanceledException());
                }, (stopped, sender));
                return new(sender.Task);
            },
            requestFailed: (_, _) => Interlocked.Increment(ref failures));

        var delivery = scenario.Request();
        await started.Task;
        scenario.CompleteWindow(completedWindow: 0, hasRemainingWindows: false);

        await delivery;
        await stopped.Task;
        await scenario.Coordinator.WaitForCancellationAttempt();
        Assert.AreEqual(0, Volatile.Read(ref failures));
        Assert.IsTrue(scenario.Coordinator.HasRetainedAttribution,
            "Unknown physical delivery must retain attribution until its reach is dead.");

        scenario.Coordinator.OnOwnerReleased(scenario.Owner, wireIsIdle: true);
        Assert.IsTrue(scenario.Coordinator.HasRetainedAttribution);
        Assert.IsFalse(scenario.Coordinator.HasPendingCancellation);
        scenario.Heartbeat(RetryInterval);
        Assert.IsFalse(scenario.Coordinator.HasRetainedAttribution);
    }

    [TestMethod]
    public async Task IdleAttributionRetentionStartsAfterPhysicalSenderSettlement()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sender = new TaskCompletionSource<CancelRequestState>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var scenario = new Scenario(_ =>
        {
            started.TrySetResult();
            return new(sender.Task);
        });

        _ = scenario.Request();
        await started.Task;
        scenario.CompleteWindow(completedWindow: 0, hasRemainingWindows: false);
        scenario.Coordinator.OnOwnerReleased(scenario.Owner, wireIsIdle: true);

        scenario.Heartbeat(RetryInterval + RetryInterval);
        Assert.IsTrue(scenario.Coordinator.HasRetainedAttribution,
            "Structural idle cannot consume the post-sender attribution interval while the sender is pending.");

        sender.SetResult(CancelRequestState.Unknown);
        await scenario.Coordinator.WaitForCancellationAttempt();
        scenario.Heartbeat(RetryInterval - Quantum);
        Assert.IsTrue(scenario.Coordinator.HasRetainedAttribution);
        scenario.Heartbeat(Quantum);
        Assert.IsFalse(scenario.Coordinator.HasRetainedAttribution);
    }

    [TestMethod]
    public async Task IdleAttributionTransfersToNewWorkAndStopsAging()
    {
        using var scenario = new Scenario(_ => new(CancelRequestState.Sent));
        var successor = new Owner();

        _ = scenario.Request();
        await scenario.Coordinator.WaitForCancellationAttempt();
        scenario.CompleteWindow(completedWindow: 0, hasRemainingWindows: false);
        scenario.Coordinator.OnOwnerReleased(scenario.Owner, wireIsIdle: true);

        scenario.Wire.ActivatedOwner = successor;
        scenario.Wire.ActivatedWindow = 0;
        scenario.Coordinator.AssignBoundary(successor, window: 0);
        Assert.IsTrue(scenario.Coordinator.HasPriorExposure(successor, window: 0));

        scenario.Heartbeat(RetryInterval + RetryInterval);
        Assert.IsTrue(scenario.Coordinator.HasPriorExposure(successor, window: 0),
            "An active successor owns event-based attribution; the old idle timeout no longer applies.");

        scenario.Coordinator.OnOwnerReleased(successor, wireIsIdle: true);
        scenario.Heartbeat(RetryInterval);
        Assert.IsFalse(scenario.Coordinator.HasRetainedAttribution);
    }

    [TestMethod]
    public async Task IdleAttributionRebasesAcrossOwnerLocalWindows()
    {
        using var scenario = new Scenario(_ => new(CancelRequestState.Sent));
        var successor = new Owner();

        _ = scenario.Request(window: 2);
        await scenario.Coordinator.WaitForCancellationAttempt();
        scenario.CompleteWindow(completedWindow: 2, hasRemainingWindows: false);
        scenario.Coordinator.OnOwnerReleased(scenario.Owner, wireIsIdle: true);

        scenario.Wire.ActivatedOwner = successor;
        scenario.Wire.ActivatedWindow = 0;
        scenario.Coordinator.AssignBoundary(successor, window: 0);

        Assert.IsTrue(scenario.Coordinator.HasPriorExposure(successor, window: 0));
        scenario.Heartbeat(RetryInterval + RetryInterval);
        Assert.IsTrue(scenario.Coordinator.HasPriorExposure(successor, window: 0));

        scenario.Coordinator.OnWindowCompleted(successor,
            completedWindow: 0, hasRemainingWindows: false);
        Assert.IsTrue(scenario.Coordinator.HasRetainedAttribution);
        scenario.Coordinator.OnOwnerReleased(successor, wireIsIdle: true);

        var finalSuccessor = new Owner();
        scenario.Wire.ActivatedOwner = finalSuccessor;
        scenario.Wire.ActivatedWindow = 0;
        scenario.Coordinator.AssignBoundary(finalSuccessor, window: 0);
        Assert.IsTrue(scenario.Coordinator.HasPriorExposure(finalSuccessor, window: 0));
        scenario.Coordinator.OnWindowCompleted(finalSuccessor,
            completedWindow: 0, hasRemainingWindows: false);
        Assert.IsFalse(scenario.Coordinator.HasRetainedAttribution);
    }

    [TestMethod]
    public async Task RequestUnspentAtItsTargetCanStrikeTwoSuccessorWindows()
    {
        var calls = 0;
        using var scenario = new Scenario(_ =>
            new((Interlocked.Increment(ref calls), CancelRequestState.Sent).Item2));

        _ = scenario.Request(remainingFlow: true);
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1);
        await scenario.Coordinator.WaitForCancellationAttempt();
        Assert.IsTrue(scenario.Coordinator.OnCancellationObserved(scenario.Owner, window: 0));

        scenario.CompleteWindow(completedWindow: 0, hasRemainingWindows: true);
        scenario.Wire.ActivatedWindow = 1;
        scenario.EnterFrontier(window: 1);
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 2);
        await scenario.Coordinator.WaitForCancellationAttempt();
        Assert.IsTrue(scenario.Coordinator.OnCancellationObserved(scenario.Owner, window: 1));

        // The predecessor exposure can satisfy window 1. The request sent for window 1 must therefore
        // retain both of its possible deliveries when the batch reaches idle.
        scenario.CompleteWindow(completedWindow: 1, hasRemainingWindows: false);
        scenario.Coordinator.OnOwnerReleased(scenario.Owner, wireIsIdle: true);

        var firstSuccessor = new Owner();
        scenario.Wire.ActivatedOwner = firstSuccessor;
        scenario.Wire.ActivatedWindow = 0;
        scenario.Coordinator.AssignBoundary(firstSuccessor, window: 0);
        Assert.IsTrue(scenario.Coordinator.HasPriorExposure(firstSuccessor, window: 0));
        scenario.Coordinator.OnCancellationObserved(firstSuccessor, window: 0);
        scenario.Coordinator.OnWindowCompleted(firstSuccessor,
            completedWindow: 0, hasRemainingWindows: false);
        scenario.Coordinator.OnOwnerReleased(firstSuccessor, wireIsIdle: true);

        var secondSuccessor = new Owner();
        scenario.Wire.ActivatedOwner = secondSuccessor;
        scenario.Wire.ActivatedWindow = 0;
        scenario.Coordinator.AssignBoundary(secondSuccessor, window: 0);
        Assert.IsTrue(scenario.Coordinator.HasPriorExposure(secondSuccessor, window: 0),
            "The first successor RFQ retired both requests after observing only one late strike.");
        scenario.Coordinator.OnCancellationObserved(secondSuccessor, window: 0);
        scenario.Coordinator.OnWindowCompleted(secondSuccessor,
            completedWindow: 0, hasRemainingWindows: false);
        Assert.IsFalse(scenario.Coordinator.HasRetainedAttribution,
            "The second successor RFQ must exhaust the request's bounded reach.");
    }

    [TestMethod]
    public async Task InnerCompletionRetainsExposureUntilTheWireIsIdle()
    {
        using var scenario = new Scenario(_ => new(CancelRequestState.Sent));
        var outerOwner = new Owner();

        await scenario.Request();
        await scenario.Coordinator.WaitForCancellationAttempt();
        scenario.CompleteWindow(completedWindow: 0, hasRemainingWindows: false);
        scenario.Coordinator.OnOwnerReleased(scenario.Owner, wireIsIdle: false);

        Assert.IsTrue(scenario.Coordinator.HasRetainedAttribution,
            "An inner pipeline becoming empty does not end a wire-level cancellation reach.");

        scenario.Wire.ActivatedOwner = outerOwner;
        scenario.Wire.ActivatedWindow = 0;
        scenario.Coordinator.AssignBoundary(outerOwner, window: 0);
        Assert.IsTrue(scenario.Coordinator.HasPriorExposure(outerOwner, window: 0),
            "The retained request must follow the wire into the outer scope cleanup.");

        scenario.Coordinator.OnCancellationObserved(outerOwner, window: 0);
        scenario.Coordinator.OnCancellationObserved(outerOwner, window: 0);
        scenario.Coordinator.OnCancellationObserved(outerOwner, window: 0);
        Assert.IsTrue(scenario.Coordinator.HasPriorExposure(outerOwner, window: 0),
            "Observed errors have no provenance and cannot prove the request's remaining reach dead.");
        scenario.Coordinator.OnWindowCompleted(outerOwner,
            completedWindow: 0, hasRemainingWindows: false);
        scenario.Coordinator.OnOwnerReleased(outerOwner, wireIsIdle: true);

        Assert.IsTrue(scenario.Coordinator.HasRetainedAttribution);
        Assert.IsFalse(scenario.Coordinator.HasPendingCancellation);
        scenario.Heartbeat(RetryInterval);
        Assert.IsFalse(scenario.Coordinator.HasRetainedAttribution);
    }

    [TestMethod]
    public async Task ConcurrentDeadlineAndSenderSettlement_CannotExtendDeadlineStress()
    {
        var iterations = StressEnv.Iterations(fallback: 512, cap: 1_000_000);
        for (var i = 0; i < iterations; i++)
        {
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var sender = new TaskCompletionSource<CancelRequestState>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            using var scenario = new Scenario(_ =>
            {
                started.TrySetResult();
                return new(sender.Task);
            });

            var delivery = scenario.Request();
            await started.Task;

            var advance = Task.Run(() => scenario.Advance(ConvergenceTimeout));
            var settle = Task.Run(() => sender.TrySetResult((i & 1) == 0
                ? CancelRequestState.Sent
                : CancelRequestState.NotSent));
            await Task.WhenAll(advance, settle);

            Assert.IsTrue(scenario.Failure.IsCompleted, $"deadline was extended, iteration {i}");
            await scenario.Failure;
            await delivery;
            Assert.IsFalse(scenario.Coordinator.HasPendingCancellation, $"iteration {i}");
        }
    }

    [TestMethod]
    public async Task ImmediateEscalation_SenderSettlementCannotRaceLeaseCancellationStress()
    {
        var iterations = StressEnv.Iterations(fallback: 512, cap: 1_000_000);
        for (var i = 0; i < iterations; i++)
        {
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var scenario = new Scenario(token =>
            {
                var sender = new TaskCompletionSource<CancelRequestState>();
                token.Register(static state =>
                    ((TaskCompletionSource<CancelRequestState>)state!).SetResult(CancelRequestState.Sent), sender);
                started.TrySetResult();
                return new(sender.Task);
            });

            var deliverySource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var delivery = scenario.Request(deliverySource);
            await started.Task;

            // Cancel() synchronously completes the sender task. Its continuation can therefore
            // settle the lease while the escalation call still owns the stop operation.
            await scenario.Request(deliverySource);
            await delivery;
            Assert.IsTrue(scenario.Coordinator.HasPendingCancellation, $"iteration {i}");
        }
    }

    [TestMethod]
    public async Task ImmediateRequestBehindAnotherEpisodeStopsSenderAndKeepsItsIntent()
    {
        var calls = 0;
        using var scenario = new Scenario(async token =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    return CancelRequestState.NotSent;
                }
            }
            return CancelRequestState.Sent;
        });
        var secondOwner = new Owner();

        _ = scenario.Request();
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1);
        var secondDelivery = scenario.Request(secondOwner, new object());

        await WaitUntilAsync(() => Volatile.Read(ref calls) == 2);
        await secondDelivery;
    }

    [TestMethod]
    public async Task LateNotSentFromPriorWindowDoesNotConsumeSuccessorRetryBudget()
        => await LatePriorWindowSettlementDoesNotConsumeSuccessorRetryBudget(CancelRequestState.NotSent);

    [TestMethod]
    public async Task PredecessorExposureDoesNotConsumeSuccessorWindowRetry()
        => await LatePriorWindowSettlementDoesNotConsumeSuccessorRetryBudget(CancelRequestState.Sent);

    async Task LatePriorWindowSettlementDoesNotConsumeSuccessorRetryBudget(CancelRequestState lateState)
    {
        var calls = 0;
        var first = new TaskCompletionSource<CancelRequestState>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var scenario = new Scenario(_ => Interlocked.Increment(ref calls) == 1
            ? new(first.Task)
            : new(CancelRequestState.Sent));

        _ = scenario.Request(remainingFlow: true);
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1);
        scenario.CompleteWindow(completedWindow: 0, hasRemainingWindows: true);
        scenario.EnterFrontier(window: 1);
        first.SetResult(lateState);

        await WaitUntilAsync(() => Volatile.Read(ref calls) == 2);
        await scenario.Coordinator.WaitForCancellationAttempt();
        Assert.DoesNotContain("window=0", scenario.Coordinator.DescribeState());
        Assert.AreEqual(2, Volatile.Read(ref calls));

        // The old request can still attribute a late strike to this window, but it was not
        // dispatched for this window and therefore cannot consume this window's retry.
        scenario.Heartbeat(RetryInterval);
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 3);
        await scenario.Coordinator.WaitForCancellationAttempt();
        Assert.DoesNotContain("window=1", scenario.Coordinator.DescribeState());

        scenario.Heartbeat(RetryInterval);
        Assert.AreEqual(3, Volatile.Read(ref calls));
    }

    [TestMethod]
    public async Task RetryRequiresFreshReadFrontierRegardlessOfInitialTiming()
    {
        var calls = 0;
        using var scenario = new Scenario(_ =>
            new((Interlocked.Increment(ref calls), CancelRequestState.Sent).Item2));

        var delivery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await scenario.Request(delivery);
        await scenario.Coordinator.WaitForCancellationAttempt();
        Assert.AreEqual(1, Volatile.Read(ref calls));

        scenario.LeaveFrontier();
        scenario.Heartbeat(RetryInterval);
        Assert.AreEqual(1, Volatile.Read(ref calls));

        // A later Immediate request may accelerate pacing, but cannot inherit attempt one's
        // timing policy and send while buffered input is still being consumed.
        await scenario.Request(delivery, atReadFrontier: false);
        Assert.AreEqual(1, Volatile.Read(ref calls));

        scenario.EnterFrontier();
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 2);
    }

    [TestMethod]
    public async Task DelayedFirstAttemptRequiresFrontierAtDispatchTime()
    {
        var calls = 0;
        using var scenario = new Scenario(_ =>
            new((Interlocked.Increment(ref calls), CancelRequestState.Sent).Item2), RetryInterval);

        _ = scenario.Request(timing: BackendCancellationTiming.AfterGrace);
        scenario.LeaveFrontier();

        scenario.Heartbeat(RetryInterval);
        Assert.AreEqual(0, Volatile.Read(ref calls));

        scenario.EnterFrontier();
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1);
    }

    [TestMethod]
    public async Task ImmediateRequestRemovesAnEqualDelayFrontierGateBeforeFirstAttempt()
    {
        var calls = 0;
        using var scenario = new Scenario(_ =>
            new((Interlocked.Increment(ref calls), CancelRequestState.Sent).Item2));
        var delivery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = scenario.Request(delivery, timing: BackendCancellationTiming.AfterGrace, atReadFrontier: false);
        Assert.AreEqual(0, Volatile.Read(ref calls));

        await scenario.Request(delivery, timing: BackendCancellationTiming.Immediate, atReadFrontier: false);
        Assert.AreEqual(1, Volatile.Read(ref calls));
    }

    [TestMethod]
    public async Task HigherWindowRequestUsesCurrentReadFrontier()
    {
        var calls = 0;
        using var scenario = new Scenario(_ =>
            new((Interlocked.Increment(ref calls), CancelRequestState.Sent).Item2), RetryInterval);
        var delivery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = scenario.Request(delivery, timing: BackendCancellationTiming.AfterGrace,
            atReadFrontier: false, window: 0);
        _ = scenario.Request(delivery, timing: BackendCancellationTiming.AfterGrace,
            atReadFrontier: true, window: 1);

        scenario.Heartbeat(RetryInterval);
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1);
    }

    [TestMethod]
    public async Task CompletedDeadlineOwner_TransfersArmWithoutResettingNextDeadline()
    {
        using var scenario = new Scenario(cancelRequest: null);
        var firstOwner = new Owner();
        var secondOwner = new Owner();

        await scenario.Request(firstOwner, new object());
        scenario.Advance(TimeSpan.FromMilliseconds(5));
        await scenario.Request(secondOwner, new object());

        scenario.Coordinator.OnOwnerReleased(firstOwner, wireIsIdle: true);
        scenario.Advance(TimeSpan.FromMilliseconds(19));
        Assert.IsFalse(scenario.Failure.IsCompleted);

        scenario.Advance(TimeSpan.FromMilliseconds(1));
        Assert.IsInstanceOfType<TimeoutException>(await scenario.Failure);
    }

    static async Task WaitUntilAsync(Func<bool> condition)
    {
        while (!condition())
            await Task.Yield();
    }
}
