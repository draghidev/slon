using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// When the consumer's CancellationToken (passed to MoveNextAsync or GetAsyncEnumerator) fires, the
// awaiting MoveNextAsync surfaces an OperationCanceledException whose token matches the caller's. OCE
// is reserved for the caller's own token; PgClientClosedException is reserved for protocol shutdown.
// I/O is not cancelled: the body keeps reading and drains the wire to RFQ, leaving the protocol usable.
[TestClass]
public class CommandUserCancellationTests
{
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

    // Token is already cancelled before MoveNextAsync is called: the first MoveNextAsync surfaces OCE.
    [TestMethod]
    public async Task UserCt_PreFired_FirstMoveNextSurfacesOce()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

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
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

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
    [DoNotParallelize]
    public async Task UserCt_FiresMidRead_SurfacesOce_ProtocolUsable()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();

        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

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
            async () => await moveNextTask.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.AreEqual(cts.Token, oce.CancellationToken);
        await DisposeBoundedAsync(e);

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task ServerCancel_InterruptsBlockedCommand_AndProtocolRemainsUsable()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var cancelDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(
            o => o.CancelSender = async (processId, secretKey, token) =>
            {
                await sender(processId, secretKey, token);
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
        await cancelDelivered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.AreEqual(cts.Token, exception.CancellationToken);
        await enumerator.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
        Assert.IsFalse(protocol.HasPendingCancellation);
    }

    [TestMethod]
    [DoNotParallelize]
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
        await senderEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.IsFalse(cancellation.IsCompleted);

        delivery.SetResult(CancelRequestState.Sent);
        await cancellation.WaitAsync(TimeSpan.FromSeconds(10));
        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
    }

    [TestMethod]
    [DoNotParallelize]
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

        Assert.IsTrue(await Task.Run(enumerator.MoveNext).WaitAsync(TimeSpan.FromSeconds(10)));
        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);

        await flow.CancelAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await blocker.ReleaseAsync();
        await flow.WaitForComplete().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task ServerCancel_NotSent_DoesNotCondemnProtocol()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempts = 0;
        var attemptsExhausted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = (_, _, _) =>
        {
            if (Interlocked.Increment(ref attempts) == 2)
                attemptsExhausted.TrySetResult();
            return new(CancelRequestState.NotSent);
        });

        using var cts = new CancellationTokenSource();
        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow, cancellationToken: cts.Token));
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        await attemptsExhausted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.IsFalse(protocol.Completion.IsCompleted);
        Assert.IsTrue(protocol.HasPendingCancellation);

        await blocker.ReleaseAsync();
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.AreEqual(cts.Token, exception.CancellationToken);
        await enumerator.DisposeAsync();

        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
        await attempted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.AreEqual(1, Volatile.Read(ref attempts));

        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
        protocol.FlowControl.LeaveCancellationReadFrontier();
        cts.Cancel();
        await Task.Yield();
        Assert.AreEqual(0, Volatile.Read(ref attempts));

        protocol.FlowControl.EnterCancellationReadFrontier(flow, flow.CancellationWindow);
        await attempted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.AreEqual(1, Volatile.Read(ref attempts));

        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
        await attempted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.AreEqual(1, Volatile.Read(ref attempts));

        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);

        await protocol.Heartbeat(TimeSpan.FromSeconds(100));
        Assert.AreEqual(0, Volatile.Read(ref attempts));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
        await attempted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await blocker.ReleaseAsync();

        await Assert.ThrowsExactlyAsync<TimeoutException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        await Assert.ThrowsExactlyAsync<TimeoutException>(async () => await enumerator.DisposeAsync());
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
        await senderEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await blocker.ReleaseAsync();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        await enumerator.DisposeAsync();
        Assert.IsTrue(protocol.HasPendingCancellation);

        delivery.TrySetResult(CancelRequestState.NotSent);
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        Assert.IsFalse(protocol.Completion.IsCompleted);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
        await senderEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var successorExecuted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var successor = new AdmissionProbeFlow(successorExecuted.SetResult);
        Assert.IsTrue(protocol.TryQueue(successor));
        await WaitUntilAsync(() => ReferenceEquals(protocol.FlowControl.ExecutingFlow, successor));
        Assert.IsFalse(successorExecuted.Task.IsCompleted,
            "A later flow executed while cancellation delivery was still unknown.");

        delivery.SetResult(CancelRequestState.Sent);
        await successorExecuted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await blocker.ReleaseAsync();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await canceledMove.WaitAsync(TimeSpan.FromSeconds(10)));
        await canceled.DisposeAsync();
        await successor.WaitForComplete().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
        await attempted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.IsTrue(protocol.HasPendingCancellation);
        Assert.IsFalse(protocol.Completion.IsCompleted);

        var boundaryFlow = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(boundaryFlow));
        var boundary = boundaryFlow.GetAsyncEnumerator();
        var boundaryMove = boundary.MoveNextAsync().AsTask();

        await blocker.ReleaseAsync();
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await canceledMove.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.AreEqual(cts.Token, exception.CancellationToken);
        await canceled.DisposeAsync();

        Assert.IsTrue(await boundaryMove.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.IsFalse(await boundary.MoveNextAsync());
        await boundary.DisposeAsync();
        Assert.IsFalse(protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
                await sender(processId, secretKey, token);
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
            await senderEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsTrue(protocol.HasPendingCancellation);
            deliver.TrySetResult();
            await cancelDelivered.Task.WaitAsync(TimeSpan.FromSeconds(10));

            var boundaryFlow = new CommandFlow(async: true, Command.Create("select 1"));
            Assert.IsTrue(protocol.TryQueue(boundaryFlow));
            var boundary = boundaryFlow.GetAsyncEnumerator();
            var boundaryMove = boundary.MoveNextAsync().AsTask();

            var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
                async () => await canceledMove.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.AreEqual(cts.Token, exception.CancellationToken);
            await canceled.DisposeAsync();

            await blocker.ReleaseAsync();
            Assert.IsTrue(await successorMove.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.IsFalse(await successor.MoveNextAsync());
            await successor.DisposeAsync();

            Assert.IsTrue(await boundaryMove.WaitAsync(TimeSpan.FromSeconds(10)));
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
    [DoNotParallelize]
    public async Task ServerCancel_LateDeliveryStrikesSuccessorWithoutMisattribution()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var successorBlocker = await PgAdvisoryLock.AcquireAsync();
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        var senderEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = async (processId, secretKey, token) =>
        {
            senderEntered.TrySetResult();
            await deliver.Task.WaitAsync(token);
            var state = await sender(processId, secretKey, token);
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
            await senderEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));

            // Let the intended command finish before delivering the already-started side request.
            // PostgreSQL then applies it to the pipelined successor that is now running.
            await firstBlocker.ReleaseAsync();
            var cancellation = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
                async () => await canceledMove.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.AreEqual(cts.Token, cancellation.CancellationToken);
            await canceled.DisposeAsync();

            await successorBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
            deliver.TrySetResult();
            await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.IsTrue(await successorMove.WaitAsync(TimeSpan.FromSeconds(10)));
            var result = successor.Current;
            var rows = result.GetAsyncEnumerator();
            while (await rows.MoveNextAsync()) { }
            await rows.DisposeAsync();
            var collateral = Assert.ThrowsExactly<PostgresException>(() => result.GetCommandComplete());
            Assert.AreEqual(PgErrorCodes.QueryCanceled, collateral.SqlState);
            Assert.IsTrue(collateral.IsCollateralCancellation);
            StringAssert.Contains(collateral.Message, "clients cannot eliminate this race");
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
    [DoNotParallelize]
    public async Task ServerCancel_RemainingFlow_RedrivesAfterPostAckRfq()
    {
        await using var firstBlocker = await PgAdvisoryLock.AcquireAsync();
        await using var secondBlocker = await PgAdvisoryLock.AcquireAsync();
        var attempts = 0;
        var secondAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = (_, _, _) =>
        {
            if (Interlocked.Increment(ref attempts) == 2)
                secondAttempt.TrySetResult();
            return new(CancelRequestState.Sent);
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
        await cancellation.WaitAsync(TimeSpan.FromSeconds(10));
        await firstBlocker.ReleaseAsync();

        await secondBlocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        await secondAttempt.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await secondBlocker.ReleaseAsync();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        await enumerator.DisposeAsync();
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
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
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        await enumerator.DisposeAsync();
        Assert.AreEqual(1, Volatile.Read(ref attempts));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task AdoCancel_TargetsItsActiveCommandFlow()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        await using var command = AdoTestPool.CreateCommand($"select pg_advisory_xact_lock({blocker.Key})");
        var execution = command.ExecuteScalarAsync();

        await blocker.WaitUntilContendedAsync();
        command.Cancel();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await execution.WaitAsync(TimeSpan.FromSeconds(10)));
        await blocker.ReleaseAsync();
        await AdoTestPool.ExecuteNonQueryAsync("select 1");
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task ServerCancel_ExclusiveScope_UsesInnerPipelineBoundary()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var options = PgTestPool.NewOptions();
        var sender = PgTestPool.CreateCancelSender(options);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = sender);
        var scope = protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;

        using var cts = new CancellationTokenSource();
        var flow = scope.Queue(new CommandFlow(async: true, blocker.WaitCommand), cts.Token);
        var enumerator = flow.GetAsyncEnumerator(cts.Token);
        var moveNext = enumerator.MoveNextAsync(cts.Token).AsTask();

        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cts.Cancel();
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.AreEqual(cts.Token, exception.CancellationToken);
        await enumerator.DisposeAsync();

        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        await scope.CompleteScopeAsync();
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    static async Task WaitUntilAsync(Func<bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!predicate())
            await Task.Delay(1, timeout.Token);
    }

    static Task DisposeBoundedAsync(IAsyncDisposable enumerator)
        => enumerator.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
}
