using Microsoft.Extensions.Time.Testing;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// CommandFlow body-side drain mode: when the consumer abandons the result stream
// (Enumerator.Dispose / DisposeAsync) or the protocol fires StoppingToken (graceful
// shutdown), the body skips the user-handoff for remaining commands and drains the wire
// to RFQ on its own. Parameterized over (flowAsync, useAsyncDispose).
[TestClass]
public class CommandDrainTests : ConnectionCreatingTest
{
    sealed class ResultObserver : CommandFlowObserver
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal override void OnCommandResult(CommandFlow flow, CommandResult result, object? state)
            => Entered.TrySetResult();
    }

    sealed class ThrowingCompletionObserver(Exception exception) : CommandFlowObserver
    {
        internal override void OnCommandResult(CommandFlow flow, CommandResult result, object? state)
            => result.OnCompleted(static (_, state) => throw (Exception)state!, exception);
    }

    sealed class ReleaseOrderingFlow : PgClientFlow
    {
        readonly ManualResetEventSlim _continueRelease = new();

        public ReleaseOrderingFlow() => IsAsync = true;

        public TaskCompletionSource ReleaseEntered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void ContinueRelease() => _continueRelease.Set();

        protected override ValueTask<FlowTasks> ExecuteAuto(Context context) => new(new FlowTasks());

        protected override void OnReleasing(Exception? exception)
        {
            ReleaseEntered.TrySetResult();
            _continueRelease.Wait();
        }
    }

    sealed class CompletionFlow : PgClientFlow
    {
        public CompletionFlow() => IsAsync = true;
        protected override ValueTask<FlowTasks> ExecuteAuto(Context context) => new(new FlowTasks());
    }

    [TestMethod]
    public async Task Release_DoesNotPublishReuseGateBeforeTenureResourcesAreReleased()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = new ReleaseOrderingFlow();
        var completion = flow.WaitForComplete();
        var release = Task.Factory.StartNew(
            () => flow.GetExecutionControl(protocol.FlowControl).Release(),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        await flow.ReleaseEntered.Task;
        try
        {
            Assert.IsFalse(completion.IsCompleted,
                "terminal publication would let the flow be reset while its prior tenure is still releasing resources");
        }
        finally
        {
            flow.ContinueRelease();
        }
        await release;
        await completion;
    }

    [TestMethod]
    public void TerminalProgress_IsStickyBeforeSyncDisposerArms()
    {
        var core = new FlowCallerInteractionCore<ValueTuple>();
        core.Initialize();
        core.SignalProgress();
        Assert.IsNull(core.WaitForContinuation(),
            "progress published before the waiter arms must be consumed without parking");
    }

    [TestMethod]
    public async Task SyncCompletionWait_DoesNotDependOnAsyncContinuationDispatch()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = new CompletionFlow();

        var waiter = Task.Factory.StartNew(
            () => flow.WaitForCompleteSynchronously(),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        SpinWait.SpinUntil(() => flow.CompletionWaiterPending);

        await Task.Factory.StartNew(
            () => flow.GetExecutionControl(protocol.FlowControl).Release(),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        await waiter;
    }

    [TestMethod]
    public async Task SyncCompletionBeforeEventRegistration_DoesNotPark()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = new CompletionFlow();

        // Complete before synchronous disposal has created its lazy event. Completion installs the
        // core's sentinel, so GetStatus observes the terminal level and disposal must not park.
        flow.GetExecutionControl(protocol.FlowControl).Release();

        var wait = Task.Factory.StartNew(
            () => flow.WaitForCompleteSynchronously(),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        await wait;
    }

    static async ValueTask Dispose(CommandFlow.Enumerator e, bool useAsyncDispose)
    {
        if (useAsyncDispose)
            await e.DisposeAsync();
        else
            e.Dispose();
    }

    static CommandFlow.Enumerator GetEnumerator(CommandFlow flow, bool flowAsync)
        => flowAsync ? flow.GetAsyncEnumerator() : flow.GetEnumerator();

    static async ValueTask<bool> MoveNext(CommandFlow.Enumerator e, bool flowAsync)
        => flowAsync ? await e.MoveNextAsync() : e.MoveNext();

    [TestMethod]
    [DataRow(true, true, DisplayName = "async flow, async dispose")]
    [DataRow(true, false, DisplayName = "async flow, sync dispose")]
    [DataRow(false, true, DisplayName = "sync flow, async dispose")]
    [DataRow(false, false, DisplayName = "sync flow, sync dispose")]
    public async Task NextCommandResult_DrainsCurrentCommandBodySide(bool flowAsync, bool useAsyncDispose)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(flowAsync,
            Command.Create("select generate_series(1, 100)"),
            Command.Create("select 'second'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = GetEnumerator(flow, flowAsync);

        Assert.IsTrue(await MoveNext(e, flowAsync), "first command result not delivered");
        Assert.IsTrue(await MoveNext(e, flowAsync), "second command result not delivered after drain of first");
        Assert.IsFalse(await MoveNext(e, flowAsync), "third call should return false (no more results)");
        await Dispose(e, useAsyncDispose);

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task CommandResultCompletionCallback_FaultsOnlyOwningFlow()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var violation = new InvalidOperationException("synthetic command-result completion failure");
        var flow = new CommandFlow(async: true, new CommandFlowOptions
        {
            Commands = new(Command.Create("select 1")),
            Observer = new ThrowingCompletionObserver(violation)
        });
        var successor = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(flow));
        Assert.IsTrue(protocol.TryQueue(successor));

        var enumerator = flow.GetAsyncEnumerator();
        Assert.IsTrue(await enumerator.MoveNextAsync());
        var rows = enumerator.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        Assert.IsFalse(await rows.MoveNextAsync());
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await enumerator.MoveNextAsync());
        Assert.AreSame(violation, failure);

        var successorEnumerator = successor.GetAsyncEnumerator();
        while (await successorEnumerator.MoveNextAsync()) { }
        await successorEnumerator.DisposeAsync();
    }

    [TestMethod]
    [DataRow(true, true, DisplayName = "async flow, async dispose")]
    [DataRow(true, false, DisplayName = "async flow, sync dispose")]
    [DataRow(false, true, DisplayName = "sync flow, async dispose")]
    [DataRow(false, false, DisplayName = "sync flow, sync dispose")]
    public async Task ConsumerDispose_MidBatch_BodyDrainsRemaining_ConnectionUsable(bool flowAsync, bool useAsyncDispose)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(flowAsync,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = GetEnumerator(flow, flowAsync);
        Assert.IsTrue(await MoveNext(e, flowAsync), "first command result not delivered");

        await Dispose(e, useAsyncDispose);

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    // Open-before-park lost-wake regression. Sync mid-batch dispose of an async flow opens the inter-result
    // gate (DisposeAsync sets draining + TrySetResult, OUTSIDE _rearmLock) while the body is still arming
    // that gate. Pre-fix this hung intermittently: the buffered gate edge was wiped by the body's gate
    // Reset and the parked body never re-read the draining level, so it never drained and pinned the
    // pipeline's activated slot - the next flow never activated. The fix re-checks the draining level at
    // the gate OnCompleted. Hammer the raced path on one reusable protocol; each iteration must leave the
    // wire usable (a hang shows as the "select 1" WaitAsync timing out, not a suite hang).
    [TestMethod]
    [DoNotParallelize]
    public async Task ConsumerDispose_MidBatch_SyncDispose_OpenBeforePark_Stress()
    {
        var iters = StressEnv.Iterations(fallback: 8, cap: 8_000);
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        for (var i = 0; i < iters; i++)
        {
            var flow = new CommandFlow(async: true,
                Command.Create("select generate_series(1, 50)"),
                Command.Create("select 'two'"),
                Command.Create("select 'three'"));
            Assert.IsTrue(protocol.TryQueue(flow));
            var e = flow.GetAsyncEnumerator();
            Assert.IsTrue(await e.MoveNextAsync(), $"iter {i}: first result not delivered");
            e.Dispose(); // SYNC dispose of an async flow, mid-batch - the raced path.
            await PgTestPool.RunAsync(protocol, "select 1");
        }
    }

    // Path coverage for the sync-dispose pump's cross-thread completion. The pump parks in
    // WaitForContinuation on an in-flight body; here a
    // forceful abort closes the socket so the in-flight read faults ON ITS POOL THREAD (SetResult(null)->
    // CompleteEnumeration, or CompleteEnumerationWithException), completing the body cross-thread while the pump is parked - the
    // sticky terminal publication must wake it. No other live-server test reaches this interleaving.
    [TestMethod]
    public async Task InFlightCompletion_RacesSyncDispose_PumpNeverStrands_Stress()
    {
        // Each iteration is a full connect + force-abort cycle. Cap it because this is path coverage,
        // not a throughput soak.
        var stress = StressEnv.Iterations(fallback: 0, cap: int.MaxValue);
        var iters = Math.Min(Math.Max(stress, 5), 300);
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        for (var i = 0; i < iters; i++)
        {
            await blocker.HoldAsync();
            var protocol = await PgTestPool.NewIsolatedAsync();
            var flow = new CommandFlow(async: true, blocker.WaitCommand);
            Assert.IsTrue(protocol.TryQueue(flow));
            var e = flow.GetAsyncEnumerator();
            var moveNext = e.MoveNextAsync().AsTask();

            // Race the sync-dispose pump against a forceful abort (closes the socket -> in-flight read faults
            // cross-thread, completing the body off the disposer's thread while it is parked in the pump).
            var disposeTask = Task.Run(() => { try { e.Dispose(); } catch { /* abort surfaces; we assert no-hang */ } });
            var abortTask = Task.Run(async () => { try { await protocol.DisposeAsync(); } catch { } });
            await blocker.ReleaseAsync();

            await Task.WhenAll(disposeTask, abortTask);
            try { await moveNext; } catch { }
        }
    }

    [TestMethod]
    [DataRow(true, DisplayName = "async flow")]
    [DataRow(false, DisplayName = "sync flow")]
    public async Task StoppingToken_MidBatch_BodyDrainsRemaining_FlowCompletesCleanly(bool flowAsync)
    {
        var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(flowAsync,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = GetEnumerator(flow, flowAsync);
        Assert.IsTrue(await MoveNext(e, flowAsync), "first command result not delivered");

        var completeTask = protocol.CompleteAsync();

        // With the consumer still active, StoppingToken faults the move-next source with the
        // close exception rather than silently skipping remaining CommandResults.
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await MoveNext(e, flowAsync));
        await e.DisposeAsync();

        await completeTask;
    }

    // Graceful shutdown overlapping consumer-side dispose. The first MoveNext's outcome may
    // be a row delivery, a PgClientClosedException, or a quiet drain depending on timing, but
    // each (flowAsync, useAsyncDispose) pair must settle CompleteAsync cleanly.
    [TestMethod]
    [DataRow(true, true, DisplayName = "async flow, async dispose")]
    [DataRow(true, false, DisplayName = "async flow, sync dispose")]
    [DataRow(false, true, DisplayName = "sync flow, async dispose")]
    [DataRow(false, false, DisplayName = "sync flow, sync dispose")]
    public async Task StoppingToken_MidBatch_FollowedByDispose_FlowCompletesCleanly(bool flowAsync, bool useAsyncDispose)
    {
        var protocol = await PgTestPool.NewIsolatedAsync();

        var flow = new CommandFlow(flowAsync,
            Command.Create("select generate_series(1, 50)"),
            Command.Create("select 'two'"),
            Command.Create("select 'three'"));
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = GetEnumerator(flow, flowAsync);
        // Fire the first MoveNext as a task so CompleteAsync can race the delivery.
        var firstTask = MoveNext(e, flowAsync).AsTask();
        var completeTask = protocol.CompleteAsync();

        // The first MoveNext may complete with a row or throw PgClientClosedException; both are
        // valid settle states and the dispose below converges regardless.
        try { await firstTask; }
        catch (PgClientClosedException) { }
        await Dispose(e, useAsyncDispose);

        await completeTask;
    }

    // Async flow: shutdown signalled before the consumer ever touches the flow. The parked
    // gate await is faulted by the heartbeat-driven OnStopping and the consumer's MoveNext
    // surfaces PgClientClosedException without any delivery.
    [TestMethod]
    public async Task StoppingToken_PreFireAsync_BodyFaultsWithoutDelivery()
    {
        var protocol = await PgTestPool.NewIsolatedAsync();
        var observer = new ResultObserver();

        var flow = new CommandFlow(async: true, new CommandFlowOptions
        {
            Commands = new(Command.Create("select generate_series(1, 50)"), Command.Create("select 'two'")),
            Observer = observer
        });
        Assert.IsTrue(protocol.TryQueue(flow));

        await observer.Entered.Task;
        var completeTask = protocol.CompleteAsync();
        Assert.IsTrue(protocol.IsDraining);
        while (!protocol.FlowControl.StoppingToken.IsCancellationRequested)
            await Task.Yield();
        flow.GetExecutionControl(protocol.FlowControl).OnHeartbeat(TimeSpan.Zero);
        await protocol.Heartbeat(TimeSpan.Zero);

        var e = flow.GetAsyncEnumerator();
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await e.MoveNextAsync());
        await e.DisposeAsync();

        while (!completeTask.IsCompleted)
        {
            await protocol.Heartbeat(TimeSpan.Zero);
            await Task.Yield();
        }
        await completeTask;
    }

    // Pipelined multi-flow: both flows queued before any MoveNext, then CompleteAsync fires.
    // A delivers (its writes flush, postgres responds), B faults via OnStopping, and both
    // enumerators settle cleanly.
    [TestMethod]
    [DoNotParallelize]
    public async Task StoppingToken_PipelinedFlows_AllDrainCleanly()
    {
        var protocol = await PgTestPool.NewIsolatedAsync();

        var flowA = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 20)"),
            Command.Create("select 'a-two'"));
        var flowB = new CommandFlow(async: true, Command.Create("select 'b'"));
        Assert.IsTrue(protocol.TryQueue(flowA));
        Assert.IsTrue(protocol.TryQueue(flowB));

        var eA = flowA.GetAsyncEnumerator();
        var eB = flowB.GetAsyncEnumerator();

        // Fire the MoveNext as a task so CompleteAsync can run concurrently.
        var aFirstTask = eA.MoveNextAsync().AsTask();
        var completeTask = protocol.CompleteAsync();

        // Graceful shutdown overlapping the reads: each flow's first MoveNext may deliver a row (its writes
        // flushed and postgres responded before StoppingToken propagated) OR throw PgClientClosedException
        // (StoppingToken won the race). Both are valid settle states - under load A often delivers. What this
        // test pins is that BOTH enumerators dispose cleanly and CompleteAsync converges, regardless of which
        // side of the race each landed on.
        try { await aFirstTask; } catch (PgClientClosedException) { }
        try { await eB.MoveNextAsync(); } catch (PgClientClosedException) { }
        await eA.DisposeAsync();
        await eB.DisposeAsync();

        await completeTask;
    }

    // Graceful-then-abort escalation: a body parked on an advisory lock can't observe StoppingToken
    // through the per-result check. CompleteAsync's graceful path schedules forceful protocol
    // escalation after CompletionTimeout; the abort token cancels the decoder before the transport
    // is aborted, the body's catch routes the closed exception out, and the consumer's pending
    // MoveNextAsync surfaces PgClientClosedException.
    [TestMethod]
    public async Task StoppingToken_GracefulEscalatesToAbort_AsyncFlowFaultsWithClosedException()
    {
        // Narrow timeout, but safe parallelized: the body is parked on the advisory lock for the whole window,
        // so the graceful->abort escalation is deterministic - there is no timing race to lose.
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var time = new FakeTimeProvider();
        var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.CompletionTimeout = TimeSpan.FromSeconds(1);
            o.TimeProvider = time;
        });

        var flow = new CommandFlow(async: true, blocker.WaitCommand);
        Assert.IsTrue(protocol.TryQueue(flow));

        var e = flow.GetAsyncEnumerator();
        var moveNextTask = e.MoveNextAsync().AsTask();
        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);

        var completeTask = protocol.CompleteAsync();
        time.Advance(TimeSpan.FromSeconds(1));

        await Assert.ThrowsExactlyAsync<PgClientClosedException>(
            async () => await moveNextTask);
        await blocker.ReleaseAsync();
        await blocker.WaitUntilBackendGoneAsync(protocol.FlowControl.BackendProcessId);
        await e.DisposeAsync();

        await completeTask;
    }
}
