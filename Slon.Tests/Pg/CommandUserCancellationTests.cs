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
        await DisposeOrDumpAsync(e, protocol, nameof(UserCt_PreFired_FirstMoveNextSurfacesOce));
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
        await DisposeOrDumpAsync(e, protocol, nameof(UserCt_FiresAfterFirstResult_NextMoveNextSurfacesOce_ProtocolUsable));
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
        await DisposeOrDumpAsync(e, protocol, nameof(UserCt_FiresMidRead_SurfacesOce_ProtocolUsable));

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
                return CancelRequestDelivery.Sent;
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
    public async Task ServerCancel_NotSent_DoesNotCondemnProtocol()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var attempts = 0;
        var attemptsExhausted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = (_, _, _) =>
        {
            if (Interlocked.Increment(ref attempts) == 2)
                attemptsExhausted.TrySetResult();
            return new(CancelRequestDelivery.NotSent);
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
    public async Task ServerCancel_NotSentAfterInstigatorCompletes_RetiresIntent()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var senderEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivery = new TaskCompletionSource<CancelRequestDelivery>(TaskCreationOptions.RunContinuationsAsynchronously);
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

        delivery.TrySetResult(CancelRequestDelivery.NotSent);
        await WaitUntilAsync(() => !protocol.HasPendingCancellation);
        Assert.IsFalse(protocol.Completion.IsCompleted);
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
        await WaitUntilAsync(() => protocol.HasUnassignedCancellationBoundary);
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
    public async Task ServerCancel_LoadedPipeline_RetiresAtFirstPostAckFlow()
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
                return CancelRequestDelivery.Sent;
            });

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
        await cancelDelivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await WaitUntilAsync(() => protocol.HasUnassignedCancellationBoundary);

        var boundaryFlow = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(boundaryFlow));
        Assert.IsFalse(protocol.HasUnassignedCancellationBoundary);
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

    static Task DisposeOrDumpAsync(IAsyncDisposable enumerator, PgClientProtocol protocol, string test)
        => ProtocolDiag.WhenAllOrDump(protocol, $"{test}: enumerator disposal did not retire the flow",
            TimeSpan.FromSeconds(10), ("DisposeAsync", enumerator.DisposeAsync().AsTask()));
}
