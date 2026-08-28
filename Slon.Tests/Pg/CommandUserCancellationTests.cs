using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// When the consumer's CancellationToken (passed to MoveNextAsync or GetAsyncEnumerator) fires, the
// awaiting MoveNextAsync surfaces an OperationCanceledException whose token matches the caller's. OCE
// is reserved for the caller's own token; PgClientClosedException is reserved for protocol shutdown.
// I/O is not cancelled: the body keeps reading and drains the wire to RFQ, leaving the protocol usable.
[TestClass]
public class CommandUserCancellationTests : ConnectionCreatingTest
{
    static void AssertCancellationAttempts(int attempts, string diagnostics)
    {
        // PostgreSQL's signal_child (src/backend/postmaster/postmaster.c) calls kill(pid, SIGINT)
        // and then kill(-pid, SIGINT), assuming that signaling the backend twice is harmless. They
        // normally coalesce, but the backend can consume the first between the calls; the second then
        // re-arms QueryCancelPending and can ride into an already-pipelined successor. The related
        // process-group behavior was discussed on pgsql-hackers in the 2023 thread
        // "We shouldn't signal process groups with SIGQUIT", but that discussion did not audit SIGINT.
        Assert.IsTrue(attempts is 1 or 2, diagnostics);
    }

    sealed class AdmissionProbeFlow : PgClientFlow
    {
        readonly Action _onExecute;

        public AdmissionProbeFlow(Action onExecute) : base(supportsDeferredFlush: true)
        {
            _onExecute = onExecute;
            IsAsync = true;
        }

        protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
        {
            _onExecute();
            return new(new FlowTasks());
        }
    }

    sealed class DrainProbe : CommandFlowObserver
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected internal override void OnDrainStarted(CommandFlow flow, object? state)
            => Started.TrySetResult();
    }

    // Token is already cancelled before MoveNextAsync is called: the first MoveNextAsync surfaces OCE.
    [TestMethod]
    public async Task UserCt_PreFired_FirstMoveNextSurfacesOce()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var e = flow.GetAsyncEnumerator(cts.Token);
        var oce = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await e.MoveNextAsync(cts.Token));
        Assert.AreEqual(cts.Token, oce.CancellationToken);
        await DisposeBoundedAsync(e);
    }

    // CT fires after the first result has been delivered: the next MoveNextAsync surfaces OCE, and a
    // follow-up flow confirms recovery drained the wire and the protocol stays usable.
    [TestMethod]
    public async Task UserCt_FiresAfterFirstResult_NextMoveNextSurfacesOce_ProtocolUsable()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(async: true,
            Command.Create("select 'one'"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        using var cts = new CancellationTokenSource();
        var e = flow.GetAsyncEnumerator(cts.Token);

        Assert.IsTrue(await e.MoveNextAsync(cts.Token), "first command result not delivered");

        cts.Cancel();
        var oce = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await e.MoveNextAsync(cts.Token));
        Assert.AreEqual(cts.Token, oce.CancellationToken);
        await DisposeBoundedAsync(e);
    }

    // CT fires while an outstanding MoveNextAsync is parked on a slow read: the parked pull surfaces
    // OCE, the body finishes the in-flight read and drains the rest to RFQ, and the follow-up command
    // confirms the protocol stays usable.
    [TestMethod]
    public async Task UserCt_FiresMidRead_SurfacesOce_ProtocolUsable()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();

        await using var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(async: true,
            Command.Create("select 1") with { WithSync = true },
            blocker.WaitCommand,
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        using var cts = new CancellationTokenSource();
        var e = flow.GetAsyncEnumerator(cts.Token);

        // The first command's Sync makes its result observable before PostgreSQL enters the blocked
        // second command. The lock then prevents suite scheduling from letting result two win.
        Assert.IsTrue(await e.MoveNextAsync(cts.Token), "first command result not delivered");

        var moveNextTask = e.MoveNextAsync(cts.Token).AsTask();
        cts.Cancel();
        await blocker.ReleaseAsync();

        var oce = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNextTask);
        Assert.AreEqual(cts.Token, oce.CancellationToken);
        await DisposeBoundedAsync(e);

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    // Autonomous execution may enter the command read before the consumer supplies its per-read
    // token. The late token must still arm backend cancellation for that active read.
    [TestMethod]
    public async Task UserCt_SuppliedAfterReadStarted_RequestsCancellation_ProtocolUsable()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var cancelRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(
            o => o.CancelSender = (processId, secretKey, token) =>
            {
                cancelRequested.TrySetResult();
                return new(CancelRequestState.NotSent);
            });

        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));
        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);

        using var cts = new CancellationTokenSource();
        var enumerator = flow.GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();
        cts.Cancel();

        await cancelRequested.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await blocker.ReleaseAsync();
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.AreEqual(cts.Token, exception.CancellationToken);
        await enumerator.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task UserCt_ThenProtocolClose_DuringDrain_CompletesPendingMoveNext()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
            o.CancelSender = (_, _, _) => new(CancelRequestState.NotSent));

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true,
            firstBlocker.WaitCommand with { WithSync = true },
            secondBlocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await firstBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        await firstBlocker.ReleaseAsync();

        // Cancellation has now been observed at the first command boundary and the body is draining
        // autonomously. Closing the wire while the second command is in flight must still publish a
        // terminal result to the already-armed MoveNext generation.
        await secondBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        await protocol.DisposeAsync();

        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await moveNext);
        await enumerator.DisposeAsync();
    }

    [TestMethod]
    public async Task ServerCancel_InterruptsBlockedCommand_AndProtocolRemainsUsable()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var cancelDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(
            o => o.CancelSender = async (processId, secretKey, token) =>
            {
                // This witness needs the physical request's completion, even when observing its
                // strike asks the sender tenure to stop before the side connection has returned.
                await sender(processId, secretKey, CancellationToken.None);
                cancelDelivered.TrySetResult();
                return CancelRequestState.Sent;
            });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));

        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();
        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        Assert.AreSame(flow, protocol.FlowControl.ActivatedFlow);
        cts.Cancel();
        await cancelDelivered.Task;

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        Assert.AreEqual(cts.Token, exception.CancellationToken);
        await enumerator.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
        Assert.IsFalse(protocol.HasPendingCancellation);
    }

    [TestMethod]
    public async Task ServerCancel_CancelAsyncWaitsForDeliveryAttempt()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var senderEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivery = new TaskCompletionSource<CancelRequestState>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = (_, _, _) =>
        {
            senderEntered.TrySetResult();
            return new(delivery.Task);
        });

        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync().AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        var cancellation = flow.CancelAsync();
        await senderEntered.Task;
        Assert.IsFalse(cancellation.IsCompleted);

        delivery.SetResult(CancelRequestState.Sent);
        await cancellation;
        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
    }

    [TestMethod]
    public async Task ServerCancel_AbandonedSyncFlowGraduatesAndDrains()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
            o.CancelSender = (_, _, _) => new(CancelRequestState.Sent));
        var flow = new CommandFlow(async: false,
            Command.Create("select 1") with { WithSync = true },
            blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetEnumerator();

        Assert.IsTrue(await Task.Run(enumerator.MoveNext));
        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);

        await flow.CancelAsync();
        await blocker.ReleaseAsync();
        await flow.WaitForComplete().AsTask();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_NotSent_DoesNotCondemnProtocol()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempts = 0;
        var firstAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attemptsExhausted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.CancellationRetryInterval = TimeSpan.FromMilliseconds(1);
            o.CancelSender = (_, _, _) =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                if (attempt == 1)
                    firstAttempt.TrySetResult();
                else if (attempt == 2)
                    attemptsExhausted.TrySetResult();
                return new(CancelRequestState.NotSent);
            };
        });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        await firstAttempt.Task;
        while (!attemptsExhausted.Task.IsCompleted)
        {
            // Retry pacing is heartbeat-driven. Advance the logical clock directly so this contract
            // test does not inherit the production one-second interval as suite wall time.
            await protocol.Heartbeat(TimeSpan.FromMilliseconds(1));
            await Task.Yield();
        }
        await attemptsExhausted.Task;
        Assert.IsFalse(protocol.Completion.IsCompleted);
        Assert.IsTrue(protocol.HasPendingCancellation);

        await blocker.ReleaseAsync();
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        Assert.AreEqual(cts.Token, exception.CancellationToken);
        await enumerator.DisposeAsync();

        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_GraceExpiresOnHeartbeatWithoutPerIntentTimer()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.HeartbeatInterval = TimeSpan.FromHours(1);
            o.CancelRequestDelay = TimeSpan.FromSeconds(100);
            o.CancelSender = (_, _, _) =>
            {
                Interlocked.Increment(ref attempts);
                attempted.TrySetResult();
                return new(CancelRequestState.Sent);
            };
        });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        Assert.IsTrue(protocol.HasPendingCancellation);
        Assert.AreEqual(0, Volatile.Read(ref attempts));

        await protocol.Heartbeat(TimeSpan.FromSeconds(40));
        Assert.AreEqual(0, Volatile.Read(ref attempts));
        await protocol.Heartbeat(TimeSpan.FromSeconds(60));
        await attempted.Task;
        Assert.AreEqual(1, Volatile.Read(ref attempts));

        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_WaitsForCancellationReadFrontierBeforeDispatch()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = (_, _, _) =>
        {
            Interlocked.Increment(ref attempts);
            attempted.TrySetResult();
            return new(CancelRequestState.Sent);
        });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        // Model cancellation arriving while the decoder is consuming an available batch: the
        // pending physical read is temporarily no longer an eligible cancellation frontier.
        protocol.FlowControl.LeaveCancellationReadFrontier(flow);
        cts.Cancel();
        await Task.Yield();
        Assert.AreEqual(0, Volatile.Read(ref attempts));

        protocol.FlowControl.EnterCancellationReadFrontier(flow, flow.CancellationWindow);
        await attempted.Task;
        Assert.AreEqual(1, Volatile.Read(ref attempts));

        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_RetryWaitsForFreshCancellationReadFrontier()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var firstAttempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondAttempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.CancellationRetryInterval = TimeSpan.FromMilliseconds(1);
            o.CancelSender = (_, _, _) =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                (attempt == 1 ? firstAttempted : secondAttempted).TrySetResult();
                return new(CancelRequestState.Sent);
            };
        });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        await firstAttempted.Task;

        // Buffered input ends the current cancellation-safe read wait. Retry pacing may expire,
        // but that must not inherit the first attempt's eligibility.
        protocol.FlowControl.LeaveCancellationReadFrontier(flow);
        await protocol.Heartbeat(TimeSpan.FromMilliseconds(1));
        Assert.AreEqual(1, Volatile.Read(ref attempts));

        protocol.FlowControl.EnterCancellationReadFrontier(flow, flow.CancellationWindow);
        await secondAttempted.Task;
        Assert.AreEqual(2, Volatile.Read(ref attempts));

        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () => await moveNext);
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_AlreadyPublishedReadFrontierDispatchesWithoutHeartbeat()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.HeartbeatInterval = TimeSpan.FromHours(1);
            o.CancelRequestDelay = TimeSpan.Zero;
            o.CancelSender = (_, _, _) =>
            {
                Interlocked.Increment(ref attempts);
                attempted.TrySetResult();
                return new(CancelRequestState.Sent);
            };
        });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        Assert.IsTrue(protocol.FlowControl.IsAtCancellationReadFrontier(flow, flow.CancellationWindow));

        cts.Cancel();
        await attempted.Task;
        Assert.AreEqual(1, Volatile.Read(ref attempts));

        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_FlowFinishesDuringGrace_SuppressesSideChannelAttempt()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.HeartbeatInterval = TimeSpan.FromHours(1);
            o.CancelRequestDelay = TimeSpan.FromSeconds(100);
            o.CancelSender = (_, _, _) =>
            {
                Interlocked.Increment(ref attempts);
                return new(CancelRequestState.Sent);
            };
        });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);

        await protocol.Heartbeat(TimeSpan.FromSeconds(100));
        Assert.AreEqual(0, Volatile.Read(ref attempts));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ConsumerDispose_UsesItsOwnGraceBeforeSideChannelAttempt()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.HeartbeatInterval = TimeSpan.FromHours(1);
            o.CancelRequestDelay = TimeSpan.Zero;
            o.CancelSender = (_, _, _) =>
            {
                Interlocked.Increment(ref attempts);
                attempted.TrySetResult();
                return new(CancelRequestState.Sent);
            };
        });

        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync().AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        var dispose = enumerator.DisposeAsync().AsTask();
        await protocol.Heartbeat(TimeSpan.FromMilliseconds(999));
        Assert.AreEqual(0, Volatile.Read(ref attempts));
        await protocol.Heartbeat(TimeSpan.FromMilliseconds(1));
        await attempted.Task;
        Assert.AreEqual(1, Volatile.Read(ref attempts));

        await blocker.ReleaseAsync();
        await dispose;
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_ReadTimeoutAfterAmbiguousRetryAbortsWire()
    {
        var iterations = StressEnv.Iterations(fallback: 1, cap: 5_000);
        for (var iteration = 0; iteration < iterations; iteration++)
        {
            try
            {
                await RunReadTimeoutAfterAmbiguousRetryAsync();
            }
            catch (Exception ex)
            {
                ex.Data["stressIteration"] = iteration;
                throw;
            }
        }
    }

    static async Task RunReadTimeoutAfterAmbiguousRetryAsync()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var firstAttempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondAttempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstReadArmed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var drainReadArmed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var drainProbe = new DrainProbe();
        var attempts = 0;
        var observeReadArms = false;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.HeartbeatInterval = TimeSpan.FromHours(1);
            o.CancellationTimeout = TimeSpan.FromMinutes(10);
            o.CancellationRetryInterval = TimeSpan.FromMinutes(2);
            o.ReadTimeoutArmed = () =>
            {
                if (!Volatile.Read(ref observeReadArms))
                    return;
                (drainProbe.Started.Task.IsCompleted ? drainReadArmed : firstReadArmed).TrySetResult();
            };
            // Report possible delivery without sending: the statement remains blocked while the
            // coordinator retains the same ambiguity as a real CancelRequest that missed its window.
            o.CancelSender = (_, _, _) =>
            {
                if (Interlocked.Increment(ref attempts) == 1)
                    firstAttempted.TrySetResult();
                else
                    secondAttempted.TrySetResult();
                return new(CancelRequestState.Sent);
            };
        });

        var flow = new CommandFlow(async: true, new CommandFlowOptions
        {
            Commands = new(blocker.WaitCommand with { Timeout = TimeSpan.FromSeconds(100) }),
            Observer = drainProbe
        });
        Volatile.Write(ref observeReadArms, true);
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        _ = enumerator.MoveNextAsync().AsTask();

        await firstReadArmed.Task;
        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        var dispose = enumerator.DisposeAsync().AsTask();
        await protocol.Heartbeat(TimeSpan.FromSeconds(1));
        await firstAttempted.Task;
        await protocol.FlowControl.WaitForCancellationAttempt();

        // Expiring the original read completes the consumer with its own timeout and starts a second
        // ambiguous request. The body retains the wire obligation and enters semantic drain.
        await protocol.Heartbeat(TimeSpan.FromSeconds(100));
        await secondAttempted.Task;
        await protocol.FlowControl.WaitForCancellationAttempt();
        await drainProbe.Started.Task;
        await drainReadArmed.Task;

        // Enumeration is already complete, but this body-owned timeout must still escalate the same
        // episode. Wait for the read's own arm, not merely drain entry: the heartbeat must follow the
        // timeout tenure it is meant to expire. Two ambiguous requests leave no safe action but abort.
        await protocol.Heartbeat(TimeSpan.FromSeconds(100));
        await protocol.Completion;
        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<TimeoutException>(async () => await dispose);
        Assert.AreEqual(2, Volatile.Read(ref attempts));
    }

    [TestMethod]
    public async Task ServerCancel_ReadTimeoutBypassesCallerCancellationGrace()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.HeartbeatInterval = TimeSpan.FromHours(1);
            o.CancelRequestDelay = TimeSpan.FromHours(1);
            o.CancelSender = (_, _, _) =>
            {
                attempted.TrySetResult();
                return new(CancelRequestState.Sent);
            };
        });

        var flow = new CommandFlow(async: true,
            blocker.WaitCommand with { Timeout = TimeSpan.FromSeconds(100) });
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync().AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        await protocol.Heartbeat(TimeSpan.FromSeconds(100));
        await attempted.Task;
        await blocker.ReleaseAsync();

        await Assert.ThrowsExactlyAsync<TimeoutException>(
            async () => await moveNext);
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_ReadTimeoutCancelsEveryRemainingCommandWindow()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        var sendSecondRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.HeartbeatInterval = TimeSpan.FromHours(1);
            o.CancelRequestDelay = TimeSpan.FromHours(1);
            o.CancelSender = async (processId, secretKey, token) =>
            {
                if (Interlocked.Increment(ref attempts) == 2)
                    await sendSecondRequest.Task;
                return await sender(processId, secretKey, token);
            };
        });

        try
        {
            var flow = new CommandFlow(async: true,
                firstBlocker.WaitCommand with { WithSync = true, Timeout = TimeSpan.FromSeconds(100) },
                secondBlocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(flow));
            var enumerator = flow.GetAsyncEnumerator();
            var moveNext = enumerator.MoveNextAsync().AsTask();
            var processId = protocol.FlowControl.BackendProcessId;

            await firstBlocker.WaitUntilContendedAsync(processId);
            await protocol.Heartbeat(TimeSpan.FromSeconds(100));
            await Assert.ThrowsExactlyAsync<TimeoutException>(async () => await moveNext);
            await secondBlocker.WaitUntilContendedOrCompletedAsync(processId);
            sendSecondRequest.TrySetResult();
            await enumerator.DisposeAsync();
            await WaitUntilAsync(() => !protocol.HasPendingCancellation);
            var attemptCount = Volatile.Read(ref attempts);
            if (attemptCount is not (1 or 2))
            {
                Assert.Fail($"attempts={attemptCount}, state={ProtocolDiag.CancellationState(protocol)}, " +
                            $"flow={ProtocolDiag.CancellationFlowState(flow)}\n" +
                            $"first: {await firstBlocker.DescribeAsync(processId)}\n" +
                            $"second: {await secondBlocker.DescribeAsync(processId)}");
            }
            Assert.IsFalse(protocol.HasPendingCancellation);
            Assert.IsFalse(await firstBlocker.IsContendedAsync(processId));
            Assert.IsFalse(await secondBlocker.IsContendedAsync(processId));
            await PgTestPool.RunAsync(protocol, "select 1");
        }
        finally
        {
            sendSecondRequest.TrySetResult();
        }
    }

    [TestMethod]
    public async Task ServerCancel_AcknowledgedWindowConvergesWhileSenderSettlementIsPending()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        var settleFirstAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.HeartbeatInterval = TimeSpan.FromHours(1);
            o.CancelRequestDelay = TimeSpan.FromHours(1);
            o.CancelSender = async (processId, secretKey, token) =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                // Once this send begins, the test models an irrevocable request whose settlement
                // remains held after PostgreSQL has already acted on it.
                var result = await sender(processId, secretKey, CancellationToken.None);
                if (attempt == 1)
                    await settleFirstAttempt.Task;
                return result;
            };
        });

        var flow = new CommandFlow(async: true,
            firstBlocker.WaitCommand with { WithSync = true, Timeout = TimeSpan.FromSeconds(100) },
            secondBlocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync().AsTask();
        var processId = protocol.FlowControl.BackendProcessId;

        try
        {
            await firstBlocker.WaitUntilContendedAsync(processId);
            await protocol.Heartbeat(TimeSpan.FromSeconds(100));
            await Assert.ThrowsExactlyAsync<TimeoutException>(async () => await moveNext);

            // The first request has already advanced the flow to its second command while the sender
            // task still owns settlement. Its exposure may already satisfy that successor, or settling
            // it may dispatch the successor's retained intent; either outcome must converge.
            await secondBlocker.WaitUntilContendedOrCompletedAsync(processId);
            settleFirstAttempt.TrySetResult();
            await enumerator.DisposeAsync();
            await WaitUntilAsync(() => !protocol.HasPendingCancellation);
            var attemptCount = Volatile.Read(ref attempts);
            if (attemptCount is not (1 or 2))
            {
                Assert.Fail($"attempts={attemptCount}, state={ProtocolDiag.CancellationState(protocol)}, " +
                            $"flow={ProtocolDiag.CancellationFlowState(flow)}\n" +
                            $"first: {await firstBlocker.DescribeAsync(processId)}\n" +
                            $"second: {await secondBlocker.DescribeAsync(processId)}");
            }
            await PgTestPool.RunAsync(protocol, "select 1");
        }
        finally
        {
            settleFirstAttempt.TrySetResult();
        }
    }

    [TestMethod]
    public async Task ServerCancel_LatePredecessorRequestSatisfiesSuccessorIntent()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        var senderEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sendRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.HeartbeatInterval = TimeSpan.FromHours(1);
            o.CancelRequestDelay = TimeSpan.FromHours(1);
            o.CancelSender = async (processId, secretKey, token) =>
            {
                Interlocked.Increment(ref attempts);
                senderEntered.TrySetResult();
                await sendRequest.Task;
                // Releasing the gate deliberately sends the predecessor request after its window
                // completed; a later stop cannot recall those bytes.
                return await sender(processId, secretKey, CancellationToken.None);
            };
        });

        var flow = new CommandFlow(async: true,
            firstBlocker.WaitCommand with { WithSync = true, Timeout = TimeSpan.FromSeconds(100) },
            secondBlocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync().AsTask();

        try
        {
            await firstBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
            await protocol.Heartbeat(TimeSpan.FromSeconds(100));
            await senderEntered.Task;
            await Assert.ThrowsExactlyAsync<TimeoutException>(async () => await moveNext);

            // Let the requested window finish before its delayed CancelRequest reaches PostgreSQL.
            // The request then cancels the successor and must satisfy, rather than dispatch, the
            // successor intent retained while its reach was ambiguous.
            await firstBlocker.ReleaseAsync();
            await secondBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
            sendRequest.TrySetResult();
            await enumerator.DisposeAsync();

            AssertCancellationAttempts(Volatile.Read(ref attempts),
                "The predecessor exposure may absorb the successor strike before its own request dispatches.");
            await WaitUntilAsync(() => !protocol.HasPendingCancellation);
            await PgTestPool.RunAsync(protocol, "select 1");
        }
        finally
        {
            sendRequest.TrySetResult();
        }
    }

    [TestMethod]
    public async Task ServerCancel_DisposeCancelsEveryRemainingCommandWindow()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        var sendSecondRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.CancelSender = async (processId, secretKey, token) =>
            {
                if (Interlocked.Increment(ref attempts) == 2)
                    await sendSecondRequest.Task;
                return await sender(processId, secretKey, token);
            };
        });

        try
        {
            var flow = new CommandFlow(async: true,
                Command.Create("select 1") with { WithSync = true },
                firstBlocker.WaitCommand with { WithSync = true },
                secondBlocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(flow));
            var enumerator = flow.GetAsyncEnumerator();
            Assert.IsTrue(await enumerator.MoveNextAsync());
            var processId = protocol.FlowControl.BackendProcessId;
            await firstBlocker.WaitUntilContendedAsync(processId);

            var disposeTask = enumerator.DisposeAsync().AsTask();
            await secondBlocker.WaitUntilContendedOrCompletedAsync(processId);
            sendSecondRequest.TrySetResult();
            await disposeTask;

            await WaitUntilAsync(() => !protocol.HasPendingCancellation);
            var attemptCount = Volatile.Read(ref attempts);
            if (attemptCount is not (1 or 2))
            {
                Assert.Fail($"attempts={attemptCount}, state={ProtocolDiag.CancellationState(protocol)}, " +
                            $"flow={ProtocolDiag.CancellationFlowState(flow)}\n" +
                            $"first: {await firstBlocker.DescribeAsync(processId)}\n" +
                            $"second: {await secondBlocker.DescribeAsync(processId)}");
            }
            Assert.IsFalse(await firstBlocker.IsContendedAsync(processId));
            Assert.IsFalse(await secondBlocker.IsContendedAsync(processId));
            await PgTestPool.RunAsync(protocol, "select 1");
        }
        finally
        {
            sendSecondRequest.TrySetResult();
        }
    }

    [TestMethod]
    public async Task ServerCancel_NotSentAfterInstigatorCompletes_RetiresIntent()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var senderEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivery = new TaskCompletionSource<CancelRequestState>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = async (_, _, _) =>
        {
            senderEntered.TrySetResult();
            return await delivery.Task;
        });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        await senderEntered.Task;
        await blocker.ReleaseAsync();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        await enumerator.DisposeAsync();
        Assert.IsTrue(protocol.HasPendingCancellation);

        delivery.TrySetResult(CancelRequestState.NotSent);
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        Assert.IsFalse(protocol.Completion.IsCompleted);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_AttemptHoldsLaterFlowExecutionUntilDeliverySettles()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var senderEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivery = new TaskCompletionSource<CancelRequestState>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = async (_, _, _) =>
        {
            senderEntered.TrySetResult();
            return await delivery.Task;
        });

        using var cts = new CancellationTokenSource();
        var canceledFlow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(canceledFlow, cancellationToken: cts.Token));
        var canceled = canceledFlow.GetAsyncEnumerator(cts.Token);
        var canceledMove = canceled.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        await senderEntered.Task;

        var successorExecuted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var successor = new AdmissionProbeFlow(successorExecuted.SetResult);
        Assert.IsTrue(protocol.TryQueue(successor));
        await WaitUntilAsync(() => ReferenceEquals(protocol.FlowControl.ExecutingFlow, successor));
        Assert.IsFalse(successorExecuted.Task.IsCompleted,
            "A later flow executed while cancellation delivery was still unknown.");

        delivery.SetResult(CancelRequestState.Sent);
        await successorExecuted.Task;
        await blocker.ReleaseAsync();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await canceledMove);
        await canceled.DisposeAsync();
        await successor.WaitForComplete().AsTask();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_UnknownDelivery_UsesBoundaryWithoutCondemningProtocol()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = (_, _, _) =>
        {
            attempted.TrySetResult();
            throw new IOException("synthetic failure after delivery became uncertain");
        });

        using var cts = new CancellationTokenSource();
        var canceledFlow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(canceledFlow, cancellationToken: cts.Token));
        var canceled = canceledFlow.GetAsyncEnumerator(cts.Token);
        var canceledMove = canceled.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        await attempted.Task;
        Assert.IsTrue(protocol.HasPendingCancellation);
        Assert.IsFalse(protocol.Completion.IsCompleted);

        var boundaryFlow = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(boundaryFlow));
        var boundary = boundaryFlow.GetAsyncEnumerator();
        var boundaryMove = boundary.MoveNextAsync().AsTask();

        await blocker.ReleaseAsync();
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await canceledMove);
        Assert.AreEqual(cts.Token, exception.CancellationToken);
        await canceled.DisposeAsync();

        Assert.IsTrue(await boundaryMove);
        Assert.IsFalse(await boundary.MoveNextAsync());
        await boundary.DisposeAsync();
        Assert.IsFalse(protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_LoadedPipeline_RetiresAtFirstPostAckRfq()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var senderEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(
            o => o.CancelSender = async (processId, secretKey, token) =>
            {
                senderEntered.TrySetResult();
                await deliver.Task.WaitAsync(token);
                // Opening the gate models the request becoming physically irrevocable. A later
                // RFQ may ask the sender tenure to stop, but cannot recall these bytes.
                await sender(processId, secretKey, CancellationToken.None);
                cancelDelivered.TrySetResult();
                return CancelRequestState.Sent;
            });

        try
        {
            using var cts = new CancellationTokenSource();
            var canceledFlow = new CommandFlow(async: true, blocker.WaitCommand);
            var priorSuccessor = new CommandFlow(async: true, blocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(canceledFlow, cancellationToken: cts.Token));
            Assert.IsTrue(protocol.TryQueue(priorSuccessor));

            var canceled = canceledFlow.GetAsyncEnumerator(cts.Token);
            var canceledMove = canceled.MoveNextAsync(cts.Token).AsTask();
            var successor = priorSuccessor.GetAsyncEnumerator();
            var successorMove = successor.MoveNextAsync().AsTask();

            await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
            cts.Cancel();
            // Observe pending while the sender is provably in flight: deterministic (the intent is
            // alive and the reach is pre-registered). Post-delivery pending is a legal transient,
            // the strike can complete the instigator's window and retire the reach at any time.
            await senderEntered.Task;
            Assert.IsTrue(protocol.HasPendingCancellation);
            deliver.TrySetResult();
            await cancelDelivered.Task;

            var boundaryFlow = new CommandFlow(async: true, Command.Create("select 1"));
            Assert.IsTrue(protocol.TryQueue(boundaryFlow));
            var boundary = boundaryFlow.GetAsyncEnumerator();
            var boundaryMove = boundary.MoveNextAsync().AsTask();

            var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
                async () => await canceledMove);
            Assert.AreEqual(cts.Token, exception.CancellationToken);
            await canceled.DisposeAsync();

            await blocker.ReleaseAsync();
            Assert.IsTrue(await successorMove);
            Assert.IsFalse(await successor.MoveNextAsync());
            await successor.DisposeAsync();

            Assert.IsTrue(await boundaryMove);
            Assert.IsFalse(await boundary.MoveNextAsync());
            await boundary.DisposeAsync();
            Assert.IsFalse(protocol.HasPendingCancellation);
        }
        finally
        {
            // A failure before the natural set must not strand the sender on the gate: a parked
            // sender outlives the test and wedges teardown behind the in-flight dispatch.
            deliver.TrySetResult();
        }
    }

    [TestMethod]
    public async Task ServerCancel_LateDeliveryStrikesSuccessorWithoutMisattribution()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var successorBlocker = await PgAdvisoryLock.AcquireAsync();
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var senderEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = async (
            processId, secretKey, token) =>
        {
            senderEntered.TrySetResult();
            // Model a request whose bytes are already irrevocable: the coordinator may request
            // that this physical tenure stop, but it cannot recall the packet.
            await deliver.Task;
            var state = await sender(processId, secretKey, CancellationToken.None);
            delivered.TrySetResult();
            return state;
        });

        try
        {
            using var cts = new CancellationTokenSource();
            var canceledFlow = new CommandFlow(async: true, firstBlocker.WaitCommand);
            var successorFlow = new CommandFlow(async: true, successorBlocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(canceledFlow, cancellationToken: cts.Token));
            Assert.IsTrue(protocol.TryQueue(successorFlow));

            var canceled = canceledFlow.GetAsyncEnumerator(cts.Token);
            var canceledMove = canceled.MoveNextAsync(cts.Token).AsTask();
            var successor = successorFlow.GetAsyncEnumerator();
            var successorMove = successor.MoveNextAsync().AsTask();

            await firstBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
            await WaitUntilAsync(() => successorFlow.IsStarted);
            cts.Cancel();
            await senderEntered.Task;

            // Let the intended command finish before delivering the already-started side request.
            // PostgreSQL then applies it to the pipelined successor that is now running.
            await firstBlocker.ReleaseAsync();
            var cancellation = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
                async () => await canceledMove);
            Assert.AreEqual(cts.Token, cancellation.CancellationToken);
            await canceled.DisposeAsync();

            await successorBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
            deliver.TrySetResult();
            await delivered.Task;

            Assert.IsTrue(await successorMove);
            var result = successor.Current;
            var rows = result.GetAsyncEnumerator();
            while (await rows.MoveNextAsync()) { }
            await rows.DisposeAsync();
            var collateral = Assert.ThrowsExactly<PgCollateralException>(
                () => result.GetCommandComplete());
            Assert.AreEqual(PgCollateralSource.Cancellation, collateral.CollateralSource);
            var backendError = Assert.IsInstanceOfType<PgErrorException>(collateral.InnerException);
            Assert.AreEqual(PgErrorCodes.QueryCanceled, backendError.SqlState);
            Assert.IsTrue(backendError.IsCollateralCancellation);
            StringAssert.Contains(collateral.Message, "drivers cannot eliminate this race");
            Assert.IsFalse(backendError.Message.Contains("drivers cannot eliminate this race"),
                "the collateral wrapper owns attribution prose; its PostgreSQL cause stays canonical");
            var projected = Assert.IsInstanceOfType<SlonException>(AdoException.Project(collateral));
            Assert.IsTrue(projected.IsCollateral);
            Assert.IsTrue(projected.IsTransient);
            StringAssert.Contains(projected.Message, "drivers cannot eliminate this race");
            var projectedError = Assert.IsInstanceOfType<PostgreSqlException>(projected.InnerException);
            Assert.AreSame(projectedError, projected.PostgreSqlError);
            Assert.IsTrue(projectedError.IsCollateralCancellation);
            Assert.IsFalse(projectedError.Message.Contains("drivers cannot eliminate this race"));
            Assert.IsFalse(await successor.MoveNextAsync());
            await successor.DisposeAsync();

            await WaitUntilAsync(() => !protocol.HasPendingCancellation);
            await PgTestPool.RunAsync(protocol, "select 1");
        }
        finally
        {
            // A failure before the natural set must not strand the sender on the gate: a parked
            // sender outlives the test and wedges teardown behind the in-flight dispatch.
            deliver.TrySetResult();
        }
    }

    [TestMethod]
    public async Task ServerCancel_RemainingFlow_RedrivesAfterPostAckRfq()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        var attempts = 0;
        var secondAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.CancelSender = (_, _, _) =>
            {
                if (Interlocked.Increment(ref attempts) == 2)
                    secondAttempt.TrySetResult();
                return new(CancelRequestState.Sent);
            };
        });

        var flow = new CommandFlow(async: true,
            firstBlocker.WaitCommand with { WithSync = true },
            secondBlocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync().AsTask();

        await firstBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        var cancellation = flow.CancelAsync();
        await WaitUntilAsync(() => Volatile.Read(ref attempts) == 1);
        await cancellation;
        await firstBlocker.ReleaseAsync();

        await secondBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        await secondAttempt.Task;
        await secondBlocker.ReleaseAsync();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_PerReadTokenTargetsOnlyCurrentWindow()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = (_, _, _) =>
        {
            Interlocked.Increment(ref attempts);
            return new(CancelRequestState.Sent);
        });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true,
            Command.Create("select 1") with { WithSync = true },
            firstBlocker.WaitCommand with { WithSync = true },
            secondBlocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        Assert.IsTrue(await enumerator.MoveNextAsync());
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await firstBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        await WaitUntilAsync(() => Volatile.Read(ref attempts) == 1);
        await firstBlocker.ReleaseAsync();
        await secondBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        await secondBlocker.ReleaseAsync();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        await enumerator.DisposeAsync();
        Assert.AreEqual(1, Volatile.Read(ref attempts));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task ServerCancel_ExclusiveScope_UsesInnerPipelineBoundary()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = sender);
        var scope = protocol.QueueExclusiveScope(async: true);
        await scope.HandoffReady;

        using var cts = new CancellationTokenSource();
        var flow = scope.Queue(new CommandFlow(async: true, blocker.WaitCommand), cts.Token);
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext);
        Assert.AreEqual(cts.Token, exception.CancellationToken);
        await enumerator.DisposeAsync();

        // The request can still strike the immediate successor outside the inner pipeline. Complete
        // the scope so that bounded reach reaches its outer wire boundary before expecting retirement.
        await scope.CompleteScopeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    // Synthesizes the intermittent field failure deterministically: a cancellation that Slon did not
    // send strikes the second lock window while its own second dispatch is held pre-send. The retained
    // intent must be satisfied by the foreign strike rather than dispatched, both windows must die with
    // user-request cancels from ProcessInterrupts, and the flow must settle clean. This pins the
    // designed behavior under a foreign signal and gives real failures a line-by-line reference run.
    [TestMethod]
    public async Task ServerCancel_ForeignPacketStrikesSecondWindow_SatisfiesRetainedIntent()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var canceller = await PgTestPool.NewIsolatedAsync();
        var secondAttemptEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var holdSecondAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var abandonSecondAttempt = false;
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.CancelSender = async (processId, secretKey, token) =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                if (attempt == 1)
                {
                    await PgTestPool.RunAsync(canceller, $"select pg_cancel_backend({processId})");
                    return CancelRequestState.Sent;
                }
                if (attempt == 2)
                {
                    secondAttemptEntered.TrySetResult();
                    await holdSecondAttempt.Task;
                    if (Volatile.Read(ref abandonSecondAttempt))
                        return CancelRequestState.NotSent;
                }
                return await sender(processId, secretKey, token);
            };
        });

        try
        {
            var flow = new CommandFlow(async: true,
                Command.Create("select 1") with { WithSync = true },
                firstBlocker.WaitCommand with { WithSync = true },
                secondBlocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(flow));
            var enumerator = flow.GetAsyncEnumerator();
            Assert.IsTrue(await enumerator.MoveNextAsync());
            var processId = protocol.FlowControl.BackendProcessId;
            var secretKey = protocol.FlowControl.BackendSecretKey;
            await firstBlocker.WaitUntilContendedAsync(processId);

            var disposeTask = enumerator.DisposeAsync().AsTask();
            // The first attempt strikes the first lock window unheld. The second dispatch then enters
            // the sender and parks pre-send, so the foreign packet below races nothing.
            await secondAttemptEntered.Task;
            await secondBlocker.WaitUntilContendedAsync(processId);
            await sender(processId, secretKey, CancellationToken.None);
            await disposeTask;

            Volatile.Write(ref abandonSecondAttempt, true);
            holdSecondAttempt.TrySetResult();
            await WaitUntilAsync(() => !protocol.HasPendingCancellation);

            var attemptCount = Volatile.Read(ref attempts);
            if (attemptCount != 2)
                Assert.Fail($"attempts={attemptCount}, state={ProtocolDiag.CancellationState(protocol)}, " +
                            $"flow={ProtocolDiag.CancellationFlowState(flow)}");
            Assert.IsFalse(await firstBlocker.IsContendedAsync(processId));
            Assert.IsFalse(await secondBlocker.IsContendedAsync(processId));
            await PgTestPool.RunAsync(protocol, "select 1");
        }
        finally
        {
            Volatile.Write(ref abandonSecondAttempt, true);
            holdSecondAttempt.TrySetResult();
        }
    }

    // Same scenario, but the foreign signal is pg_cancel_backend from another session: no packet or
    // cancel connection, and an identical user-request error.
    [TestMethod]
    public async Task ServerCancel_PgCancelBackendStrikesSecondWindow_IndistinguishableFromOwn()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var canceller = await PgTestPool.NewIsolatedAsync();
        var secondAttemptEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var holdSecondAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var abandonSecondAttempt = false;
        var attempts = 0;
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.CancelSender = async (processId, secretKey, token) =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                if (attempt == 1)
                {
                    await PgTestPool.RunAsync(canceller, $"select pg_cancel_backend({processId})");
                    return CancelRequestState.Sent;
                }
                if (attempt == 2)
                {
                    secondAttemptEntered.TrySetResult();
                    await holdSecondAttempt.Task;
                    if (Volatile.Read(ref abandonSecondAttempt))
                        return CancelRequestState.NotSent;
                }
                throw new InvalidOperationException("Unexpected cancellation attempt.");
            };
        });

        try
        {
            var flow = new CommandFlow(async: true,
                Command.Create("select 1") with { WithSync = true },
                firstBlocker.WaitCommand with { WithSync = true },
                secondBlocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(flow));
            var enumerator = flow.GetAsyncEnumerator();
            Assert.IsTrue(await enumerator.MoveNextAsync());
            var processId = protocol.FlowControl.BackendProcessId;
            await firstBlocker.WaitUntilContendedAsync(processId);

            var disposeTask = enumerator.DisposeAsync().AsTask();
            await secondAttemptEntered.Task;
            await secondBlocker.WaitUntilContendedAsync(processId);
            await PgTestPool.RunAsync(canceller, $"select pg_cancel_backend({processId})");
            await disposeTask;

            Volatile.Write(ref abandonSecondAttempt, true);
            holdSecondAttempt.TrySetResult();
            await WaitUntilAsync(() => !protocol.HasPendingCancellation);

            var attemptCount = Volatile.Read(ref attempts);
            if (attemptCount != 2)
                Assert.Fail($"attempts={attemptCount}, state={ProtocolDiag.CancellationState(protocol)}, " +
                            $"flow={ProtocolDiag.CancellationFlowState(flow)}");
            Assert.IsFalse(await firstBlocker.IsContendedAsync(processId));
            Assert.IsFalse(await secondBlocker.IsContendedAsync(processId));
            await PgTestPool.RunAsync(protocol, "select 1");
        }
        finally
        {
            Volatile.Write(ref abandonSecondAttempt, true);
            holdSecondAttempt.TrySetResult();
        }
    }

    // Recurrence amplifier for the intermittent one-attempt failure: repeats the exact
    // dispose-cancels-every-window dance against a fresh protocol per iteration while the blockers and
    // their advisory keys persist. On a hit the assert carries the full evidence pack. Scale with
    // SLON_STRESS_ITERATIONS, uncap with SLON_UNCAPPED for a deliberate soak.
    [TestMethod]
    public async Task ServerCancel_DisposeCancelsEveryRemainingCommandWindowStress()
    {
        var iterations = StressEnv.Iterations(fallback: 1, cap: 5_000);
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);

        for (var iteration = 0; iteration < iterations; iteration++)
        {
            var sendSecondRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var attempts = 0;
            await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
            {
                o.CancelSender = async (processId, secretKey, token) =>
                {
                    if (Interlocked.Increment(ref attempts) == 2)
                        await sendSecondRequest.Task;
                    return await sender(processId, secretKey, token);
                };
            });

            try
            {
                var flow = new CommandFlow(async: true,
                    Command.Create("select 1") with { WithSync = true },
                    firstBlocker.WaitCommand with { WithSync = true },
                    secondBlocker.WaitCommand);
                Assert.IsTrue(protocol.TryQueue(flow));
                var enumerator = flow.GetAsyncEnumerator();
                Assert.IsTrue(await enumerator.MoveNextAsync());
                var processId = protocol.FlowControl.BackendProcessId;
                await firstBlocker.WaitUntilContendedAsync(processId);

                var disposeTask = enumerator.DisposeAsync().AsTask();
                await secondBlocker.WaitUntilContendedOrCompletedAsync(processId);
                sendSecondRequest.TrySetResult();
                await disposeTask;

                await WaitUntilAsync(() => !protocol.HasPendingCancellation);
                var attemptCount = Volatile.Read(ref attempts);
                if (attemptCount is not (1 or 2))
                {
                    Assert.Fail($"iteration={iteration}, attempts={attemptCount}, " +
                                $"state={ProtocolDiag.CancellationState(protocol)}, " +
                                $"flow={ProtocolDiag.CancellationFlowState(flow)}\n" +
                                $"first: {await firstBlocker.DescribeAsync(processId)}\n" +
                                $"second: {await secondBlocker.DescribeAsync(processId)}");
                }
                await PgTestPool.RunAsync(protocol, "select 1");
            }
            finally
            {
                sendSecondRequest.TrySetResult();
            }
        }
    }

    static async Task WaitUntilAsync(Func<bool> predicate)
    {
        while (!predicate())
            await Task.Yield();
    }

    static Task DisposeBoundedAsync(IAsyncDisposable enumerator)
        => enumerator.DisposeAsync().AsTask();

}
