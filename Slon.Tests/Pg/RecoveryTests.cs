using System.Buffers.Binary;
using System.Threading.Tasks.Sources;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Runtime.CompilerServices;
using Slon.Transport;

namespace Slon.Tests.Pg;

// End-to-end tests for PgClientProtocol.Policy.TryRecoverItemFailure and the substitute
// ResyncRecoveryFlow it returns. Each test wires a real PG connection, queues a deliberately
// throwing flow at a chosen failure point, then verifies that subsequent flows on the same
// protocol still succeed (the wire was cleaned by the recovery item).
//
// The throwing flow lives in this file (FaultingFlow) so the failure phase is controllable
// per test without leaking into the production flow library.
//
// Verification contract note. The pipeline framework deliberately does NOT complete a failed
// item when TryRecoverItemFailure returns true - the recovery item substitutes for it, and the
// POLICY completes the failed flow when the recovery completes (ResyncRecoveryFlow.BindFailedFlow
// -> CompleteItem's binding discharge; the failed item's lifetime extends as far as the
// recovery does). Most tests still verify via the next flow succeeding (the wire was cleaned);
// the failed flow's own completion carries its original exception, plus the recovery's fault
// when the recovery itself died (see RecoveryItselfFails_FailedFlowCompletes_WithBothFaults).
[TestClass]
public class RecoveryTests
{
    // Isolated per test by design: every recovery test faults a flow or kills the transport.
    // Cannot share via PgTestPool. The fault-injection tests further down
    // construct their own transport inline because they need to call transport.Writer/Reader
    // .Complete to synthesize wire death; this helper only covers the recoverable cases that
    // own a clean protocol up to the point of injecting the fault inside a FaultingFlow.
    static Task<PgClientProtocol> ConnectAsync() => PgTestPool.NewIsolatedAsync();

    static PgClientOptions NewOptions() => PgTestPool.NewOptions();

    static async Task RunAsync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    static async Task RunSync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: false, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetEnumerator();
        while (e.MoveNext()) { }
        await e.DisposeAsync();
    }

    // Row count over the raw protocol. The flow enumerator yields CommandResults, each of which is
    // itself enumerable over its Rows - so count the inner rows.
    static async Task<int> CountRows(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        var rows = 0;
        while (await e.MoveNextAsync())
        {
            var r = e.Current.GetAsyncEnumerator();
            while (await r.MoveNextAsync()) rows++;
            await r.DisposeAsync();
        }
        await e.DisposeAsync();
        return rows;
    }

    // Where the FaultingFlow's body throws. PreReturn = before returning FlowTasks (surfaces
    // as PipelineItemFailureKind.ExecuteItemTask). PipelineTask = the returned pipeline task
    // faults (PipelineItemFailureKind.ExecutionPipelineTask or PipelineTask). TrailingTask = the
    // returned trailing execution task faults (PipelineItemFailureKind.TrailingExecutionTask).
    internal enum FaultPhase
    {
        PreReturn,
        PipelineTask,
        TrailingTask
    }

    // What to write to the wire before the body throws. Determines the RFQ obligation and
    // pending-byte state the recovery sees.
    internal enum WriteShape
    {
        None,
        QueryNoFlush,
        ParseBindExecuteNoSync,
        MultipleSyncsNoFlush,
        // Two simple queries: two inherited RFQs, each with a real result (so a held read has a
        // non-auto message to return and terminate on, unlike MultipleSyncsNoFlush's bare Syncs).
        TwoQueriesNoFlush,
        // Each PipelinedStatements entry as its own Parse/Bind/Execute, no Sync - one implicit block.
        PipelinedParseBindExecuteNoSync
    }

    sealed class AwaitObservedValueTaskSource : IValueTaskSource
    {
        ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };
        readonly TaskCompletionSource _awaitObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask Task => new(this, _core.Version);
        public Task AwaitObserved => _awaitObserved.Task;
        public void SetResult() => _core.SetResult(true);

        void IValueTaskSource.GetResult(short token) => _core.GetResult(token);
        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);
        void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags)
        {
            _awaitObserved.TrySetResult();
            _core.OnCompleted(continuation, state, token, flags);
        }
    }

    // Test flow that lets each test pick its failure phase and write shape. Recovery's input
    // signal is the failed flow's recorded write state (rfqCount, lastMessageInducesRfq,
    // protocol's unflushed bytes), so faithful reproduction of those wire states is the
    // primary job here.
    internal sealed class FaultingFlow : PgClientFlow
    {
        readonly FaultPhase _phase;
        readonly WriteShape _shape;
        readonly ValueTaskSourcePromise<bool> _readPromise = new();
        // The wait-list-free sync handoff parks the caller on the flow's OWN MRES (HandoffEvent). A
        // standalone one is enough for a test flow that is taken over (OnExecutorSuspended Sets it, the
        // caller Waits); it does not reuse a caller-core like CommandFlow.
        protected override ManualResetEventSlim? HandoffEvent { get; } = new(false);

        /// Runs after the write shape lands but before the fault fires. Lets a test kill the
        /// transport at the exact point where the flow's own writes succeeded but the
        /// recovery's wire I/O will fail (see RecoveryItselfFails_*).
        public Action? AfterWrites { get; init; }

        /// When set, the flow's trailing task is backed by this TCS instead of a sync
        /// default/fault. Lets a test control when (and whether) the still-in-flight
        /// trailing completes, exercising the recovery's substitution-substrate contract
        /// of capturing-and-awaiting `OutstandingPhaseTask` (move-to-trailing path).
        public TaskCompletionSource? ControllableTrailing { get; init; }
        public ValueTask? ControllableTrailingTask { get; init; }

        /// When set, the flow's PIPELINE task acquires the decoder (the read turn) and holds it
        /// until this completes - a real read still in flight on the single-consumer wire. Pair
        /// with FaultPhase.TrailingTask so the trailing faults while the read is parked: the
        /// framework then hands the pending READ as OutstandingPhaseTask, and the recovery must
        /// await it before its own DrainPhase read (the read-side mirror of ControllableTrailing).
        public TaskCompletionSource? ControllablePipelineRead { get; init; }

        /// Keeps the pipeline task pending until the test completes or faults it. Unlike
        /// ControllablePipelineRead this does not acquire the decoder.
        public TaskCompletionSource? ControllablePipeline { get; init; }

        /// Completed by the pipeline read once it has actually acquired the decoder (the read turn).
        /// Lets a test fault the trailing only AFTER the read is genuinely holding the turn, so the
        /// recovery runs while the outstanding read is live (the definitive read-outstanding case).
        public TaskCompletionSource? PipelineReadAcquired { get; init; }

        /// Number of messages the held read consumes before completing. >1 lets the read cross an
        /// inherited RFQ boundary, so a test can park it before the boundary, fault, then let it
        /// cross post-snapshot (the drain-count reconciliation probe).
        public int HeldReadConsumeCount { get; init; } = 1;

        /// Completed by the held read after it consumes its first message: with a count of 1 it has
        /// consumed everything it will, with a higher count it parks on the next. Orders a test's
        /// trailing fault after the consume without a fixed delay.
        public TaskCompletionSource? HeldReadConsumedFirst { get; init; }

        public bool RequestCancellation { get; init; }

        public Exception Failure { get; init; } = new InvalidOperationException("FaultingFlow synthetic failure.");

        /// SQL for the QueryNoFlush / ParseBindExecuteNoSync shapes (defaults to a trivial select).
        /// Lets a test run a side-effecting statement (e.g. BEGIN + CREATE TEMP TABLE) to observe
        /// whether recovery closed the transaction the failed flow left open.
        public string QueryText { get; init; } = "select 1";

        /// For WriteShape.PipelinedParseBindExecuteNoSync: each statement is written as its own
        /// Parse/Bind/Execute with NO Sync between them - one extended-protocol implicit block. Lets a
        /// test observe whether recovery commits or rolls back uncommitted spanning work.
        public string[]? PipelinedStatements { get; init; }

        public FaultingFlow(bool async, FaultPhase phase, WriteShape shape)
            : base(supportsDeferredFlush: true)
        {
            _phase = phase;
            _shape = shape;
            IsAsync = async;
        }

        protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
        {
            var encoder = context.GetEncoder();
            switch (_shape)
            {
                case WriteShape.None:
                    break;
                case WriteShape.QueryNoFlush:
                    encoder.WriteQuery(QueryText);
                    break;
                case WriteShape.ParseBindExecuteNoSync:
                    encoder.WriteParse(QueryText);
                    encoder.WriteBind();
                    encoder.WriteExecute();
                    break;
                case WriteShape.MultipleSyncsNoFlush:
                    encoder.WriteSync();
                    encoder.WriteSync();
                    encoder.WriteSync();
                    break;
                case WriteShape.TwoQueriesNoFlush:
                    encoder.WriteQuery("select 1");
                    encoder.WriteQuery("select 2");
                    break;
                case WriteShape.PipelinedParseBindExecuteNoSync:
                    foreach (var stmt in PipelinedStatements!)
                    {
                        encoder.WriteParse(stmt);
                        encoder.WriteBind();
                        encoder.WriteExecute();
                    }
                    break;
            }

            AfterWrites?.Invoke();

            if (RequestCancellation)
                RequestBackendCancellation();

            if (_phase is FaultPhase.PreReturn)
                throw Failure;

            ValueTask pipelineTask;
            if (ControllablePipeline is { } controllablePipeline)
                pipelineTask = new ValueTask(controllablePipeline.Task);
            else if (ControllablePipelineRead is { } readGate)
                pipelineTask = HoldReadTurn(context, readGate, PipelineReadAcquired, HeldReadConsumedFirst, HeldReadConsumeCount);
            else
                pipelineTask = _phase is FaultPhase.PipelineTask ? FailedTask(Failure) : ValueTask.CompletedTask;
            ValueTask trailingTask;
            if (ControllableTrailingTask is { } controllableTrailingTask)
            {
                trailingTask = controllableTrailingTask;
            }
            else if (ControllableTrailing is { } controllable)
            {
                // Controllable trailing: the framework captures this as OutstandingPhaseTask
                // when PipelineTask sync-faults. The recovery's TrailingPhase awaits it before
                // its WriteSync, observe-and-discard regardless of outcome.
                trailingTask = new ValueTask(controllable.Task);
            }
            else if (_phase is FaultPhase.TrailingTask)
            {
                trailingTask = FailedTask(Failure);
            }
            else
            {
                trailingTask = default;
            }

            return new(new FlowTasks(trailingExecutionTask: trailingTask, pipelineTask: pipelineTask));

            static ValueTask FailedTask(Exception exception)
                => new(Task.FromException(exception));

            // Acquire the decoder (the single-consumer read turn), signal it, wait on the test's
            // release gate, then do a real read. This is the unfinished pipelineTask: when recovery activates and robs the
            // turn, this read's next decoder USE fails the per-use validity check and faults - and
            // that late fault on an already-recovering flow is the recovery-of-recovery trigger.
            static async ValueTask HoldReadTurn(Context context, TaskCompletionSource gate, TaskCompletionSource? acquired, TaskCompletionSource? consumedFirst, int consumeCount)
            {
                PgDecoder decoder = await context.GetDecoderAsync().ConfigureAwait(false);
                acquired?.TrySetResult();
                await gate.Task.ConfigureAwait(false);
                for (var i = 0; i < consumeCount; i++)
                {
                    await decoder.GetNextAsync().ConfigureAwait(false);
                    if (i == 0)
                        consumedFirst?.TrySetResult();
                }
            }
        }
    }

    // Pipeline-task failure: the read phase faults. Recovery sees the failed flow's recorded
    // state and constructs a no-op drain (nothing written, nothing pending) so the wire stays
    // intact and the next flow runs cleanly.
    //
    // Verification: queue the faulting flow (do NOT await its completion, the pipeline
    // contract abandons it when recovery succeeds), then queue and await the next flow. If
    // the next flow completes successfully recovery worked.
    [TestMethod]
    public async Task PipelineTask_FailureRecovers_NextFlowSucceeds()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.None);
        Assert.IsTrue(protocol.TryQueue(faulting));

        await RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task FramingViolation_DoesNotRecover_QueuedSuccessorIsCollateral()
    {
        await using var protocol = await ConnectAsync();
        var failureGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var violation = new PgFramingException("synthetic framing violation");
        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.None)
        {
            ControllablePipeline = failureGate,
        };
        var successor = new CommandFlow(async: true, Command.Create("select 1"));

        Assert.IsTrue(protocol.TryQueue(faulting));
        Assert.IsTrue(protocol.TryQueue(successor));
        failureGate.SetException(violation);

        var failed = await Assert.ThrowsExactlyAsync<PgClientException>(
            () => faulting.WaitForComplete().AsTask().WaitAsync(TestTimeout.Hang));
        Assert.AreSame(violation, failed.InnerException);

        var e = successor.GetAsyncEnumerator();
        var collateral = await Assert.ThrowsExactlyAsync<PgCollateralException>(
            () => e.MoveNextAsync().AsTask().WaitAsync(TestTimeout.Hang));
        Assert.AreEqual(PgCollateralKind.ProtocolFailure, collateral.Kind);
        Assert.AreSame(violation, collateral.InnerException);
    }

    [TestMethod]
    public async Task ProtocolExpectationViolation_Recovers_QueuedSuccessorSucceeds()
    {
        await using var protocol = await ConnectAsync();
        var failureGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var violation = new PgProtocolException("synthetic expectation violation");
        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.None)
        {
            ControllablePipeline = failureGate,
        };
        var successor = new CommandFlow(async: true, Command.Create("select 1"));

        Assert.IsTrue(protocol.TryQueue(faulting));
        Assert.IsTrue(protocol.TryQueue(successor));
        failureGate.SetException(violation);

        var failed = await Assert.ThrowsExactlyAsync<PgClientException>(
            () => faulting.WaitForComplete().AsTask());
        Assert.AreSame(violation, failed.InnerException);

        var e = successor.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task InFlightReadOnlyRecovery_BlocksNewAdmissionUntilRecovered()
    {
        await using var protocol = await ConnectAsync();
        // The query blocks on the fixture-held advisory lock, so it is in flight by construction
        // and recovery's drain parks on an RFQ that cannot arrive until the release below - the
        // admission-suspended window provably spans the asserts, no timing margin involved.
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var pipeline = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.QueryNoFlush)
        {
            QueryText = $"select pg_advisory_xact_lock({blocker.Key})",
            ControllablePipeline = pipeline,
        };
        Assert.IsTrue(protocol.TryQueue(faulting));

        // Let the pending pipeline task enter the in-flight sequence, then fault it after the
        // executor has released the write window. Recovery can only drain the flow's existing RFQ.
        while (protocol.PipelineDepth == 0)
            await Task.Yield();
        pipeline.TrySetException(new InvalidOperationException("synthetic in-flight pipeline fault"));
        while (protocol.IsSchedulable)
            await Task.Yield();

        var duringRecovery = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsFalse(protocol.TryQueue(duringRecovery),
            "read-only recovery must not enlarge the set condemned if its inherited boundary is invalid");

        // Unblock the in-flight query; its RFQ arrives and recovery's drain completes.
        await blocker.ReleaseAsync();
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => faulting.WaitForComplete().AsTask());
        Assert.IsTrue(protocol.IsSchedulable);
        await RunAsync(protocol, "select 1");
    }

    [TestMethod]
    public async Task PipelineFailure_DuringGracefulShutdown_StillRecoversWire()
    {
        var protocol = await ConnectAsync();
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var writesLanded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new InvalidOperationException("synthetic pipeline failure during graceful shutdown");
        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.QueryNoFlush)
        {
            QueryText = $"select pg_advisory_xact_lock({blocker.Key})",
            ControllablePipeline = pipeline,
            AfterWrites = writesLanded.SetResult,
            Failure = failure,
        };
        Assert.IsTrue(protocol.TryQueue(faulting));
        await writesLanded.Task;

        // CompleteAsync changes Ready to Draining before the flow fails. Recovery eligibility is
        // based on the sticky fact that startup established the query protocol, not on whether the
        // protocol still admits new work. Recovery must therefore flush the inherited Query and
        // park on its advisory lock before graceful shutdown can complete.
        var complete = protocol.CompleteAsync();
        var failedCompletion = faulting.WaitForComplete().AsTask();
        pipeline.SetException(failure);

        var contended = false;
        while (!failedCompletion.IsCompleted && !contended)
            contended = await blocker.IsContendedAsync(protocol.FlowControl.BackendProcessId);
        Assert.IsFalse(failedCompletion.IsCompleted,
            "the failed flow completed before recovery drained its inherited RFQ");
        Assert.IsFalse(complete.IsCompleted,
            "graceful shutdown completed without recovering the failed flow's pending query");
        Assert.IsTrue(contended);

        await blocker.ReleaseAsync();
        var failed = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => failedCompletion);
        Assert.AreSame(failure, failed);
        await complete;
    }

    [TestMethod]
    public async Task RecoverySubstitution_RetiresUndeliveredCancellationIntent()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync(o => o.CancelSender = (_, _, _) =>
            throw new AssertFailedException("an unactivated failed flow must not dispatch cancellation"));
        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.None)
        {
            RequestCancellation = true,
        };
        Assert.IsTrue(protocol.TryQueue(faulting));

        await RunAsync(protocol, "select 1");

        Assert.IsFalse(protocol.HasPendingCancellation);
        Assert.IsFalse(protocol.Completion.IsCompleted);
    }

    // Execute-item-task failure (PreReturn throw) after a Query was written but not flushed.
    // _rfqCount is 1, _lastMessageInducesRfq is true, UnflushedBytes > 0. Recovery doesn't
    // inject a Sync (lastMessageInducesRfq is true) and flushes the Query so the server
    // produces its RFQ, which the drain consumes.
    [TestMethod]
    public async Task ExecuteItemTask_FailureRecovers_NextFlowSucceeds()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.QueryNoFlush);
        Assert.IsTrue(protocol.TryQueue(faulting));

        await RunAsync(protocol, "select 1");
    }

    // Trailing-execution-task failure. The policy treats this uniformly with ExecuteItemTask:
    // trailing failures don't revoke the flow's write privileges on the protocol, so the
    // recovery flow can flush pending bytes and inject a Sync normally. If the underlying wire
    // is actually dead the recovery flow's own flush/drain will surface that exception. If the
    // wire is alive (cancellation, timeout, encoder hiccup), recovery cleans up and the next
    // flow proceeds.
    [TestMethod]
    public async Task TrailingExecutionTask_FailureRecovers_NextFlowSucceeds()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: true, FaultPhase.TrailingTask, WriteShape.None);
        Assert.IsTrue(protocol.TryQueue(faulting));

        await RunAsync(protocol, "select 1");
    }

    // Parse+Bind+Execute then throw pre-return. _rfqCount is 0, _lastMessageInducesRfq is
    // false, UnflushedBytes > 0. Recovery's injectSync gate fires (drainCount becomes 1) and
    // the recovery flushes the pending bytes so the server actually processes them.
    [TestMethod]
    public async Task WriterStateRemediation_NoSyncSent_RecoveryInjectsSync()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.ParseBindExecuteNoSync);
        Assert.IsTrue(protocol.TryQueue(faulting));

        // Multiple subsequent flows succeed: confirms the recovery's injected Sync drained
        // the extended-protocol sequence cleanly and no stray RFQ was left on the wire.
        for (int i = 0; i < 3; i++)
            await RunAsync(protocol, "select 1");
    }

    // Recovery closes a transaction the failed flow left OPEN, landing the wire Idle. The faulting flow
    // opens a transaction (BEGIN) and faults with it still open ('T'). Its last write is a Query (it
    // induces its own RFQ, canWriteSync=false), so the realign path injects no Sync - the always-written
    // ROLLBACK is what closes the block. Without it the next flow inherits the open transaction, its own
    // RFQ comes back non-Idle, and the wire-handoff guard fails the protocol; so a clean subsequent flow
    // is the proof recovery rolled the transaction back. (This is also the exclusive-scope-abort shape:
    // an open transaction propagated to the root that recovery must close.)
    [TestMethod]
    public async Task Recovery_ClosesOpenTransaction_NextFlowIdle()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.QueryNoFlush)
        {
            QueryText = "BEGIN"
        };
        Assert.IsTrue(protocol.TryQueue(faulting));

        // Throws PgClientClosedException (the wire-handoff guard) if recovery left the transaction open;
        // a clean completion means recovery's ROLLBACK landed the wire Idle.
        await RunAsync(protocol, "select 1");
        await RunAsync(protocol, "select 1");
    }

    // VERIFICATION: does PG hold pipelined extended-protocol Executes (no Sync, no BEGIN) in one
    // implicit block that a Sync commits - so recovery's realigning Sync would COMMIT a faulted flow's
    // partial work? Two INSERTs are pipelined into a REAL table (visible cross-connection) with no Sync,
    // then the flow faults and recovery runs. A separate connection counts survivors: 2 = the Sync
    // committed the uncommitted block (the hazard - abort-before-Sync is needed), 0 = rolled back.
    // Two INSERTs are pipelined into a real table with no Sync between them - one implicit block - then
    // the flow faults and recovery runs. All on one connection: a real (non-temp) table is visible to a
    // later flow on the same connection once committed, so the survivor count tells the tale. 2 = the
    // realigning Sync committed the faulted flow's uncommitted block (the hazard); 0 = rolled back.
    [TestMethod]
    public async Task Recovery_PipelinedImplicitBlock_SurvivorCount()
    {
        await Recovery_PipelinedSurvivors(["INSERT INTO {0} VALUES (1)", "INSERT INTO {0} VALUES (2)"]);
    }

    // Same, but the failed flow opened an EXPLICIT transaction itself (BEGIN via extended), so recovery's
    // BEGIN-upgrade lands BEGIN-inside-BEGIN - a harmless WARNING (not an error), a no-op that leaves the
    // open transaction for the ROLLBACK to unwind. Both INSERTs must still roll back.
    [TestMethod]
    public async Task Recovery_PipelinedExplicitBegin_RollsBackAll()
    {
        await Recovery_PipelinedSurvivors(["BEGIN", "INSERT INTO {0} VALUES (1)", "INSERT INTO {0} VALUES (2)"]);
    }

    static async Task Recovery_PipelinedSurvivors(string[] statementTemplates)
    {
        await using var protocol = await ConnectAsync();
        var table = "recovery_span_" + Guid.NewGuid().ToString("N");
        await RunAsync(protocol, $"CREATE TABLE {table} (x int)");
        try
        {
            var faulting = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.PipelinedParseBindExecuteNoSync)
            {
                PipelinedStatements = [.. statementTemplates.Select(t => string.Format(t, table))]
            };
            Assert.IsTrue(protocol.TryQueue(faulting));
            await RunAsync(protocol, "select 1"); // drive recovery to completion (and prove the wire is Idle)

            var survived = await CountRows(protocol, $"SELECT x FROM {table}");
            Assert.AreEqual(0, survived,
                $"{survived} of 2 pipelined INSERTs survived recovery - the resync committed the faulted flow's uncommitted work instead of rolling it back.");
        }
        finally
        {
            await RunAsync(protocol, $"DROP TABLE {table}");
        }
    }

    // Three Syncs written without flush, then pre-return throw. _rfqCount is 3,
    // _lastMessageInducesRfq is true, UnflushedBytes > 0. Recovery doesn't inject a Sync
    // (drainCount stays at 3) and flushes so all three Syncs hit the server. Drain consumes
    // exactly three RFQs.
    //
    // Verification: run several subsequent flows. Any miscount would leave an extra RFQ on
    // the wire that the next CommandFlow would consume out of order and either fail or
    // mis-decode. Running multiple successful flows after recovery is the cleanest plumb of
    // "all RFQs were drained, exactly zero leftovers" since the protocol doesn't expose the
    // drain counter directly.
    [TestMethod]
    public async Task MultipleRfqsOutstanding_DrainsAllBeforeNext()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.MultipleSyncsNoFlush);
        Assert.IsTrue(protocol.TryQueue(faulting));

        for (int i = 0; i < 5; i++)
            await RunAsync(protocol, "select 1");
    }

    // The recovery flow's async mode is inherited from the failed flow's IsAsyncAtBind. An
    // async-failed flow yields an async recovery, so the drain decode does not block the
    // executor on a sync path. Verified end-to-end: async failure followed by an async flow
    // succeeds without deadlock.
    [TestMethod]
    public async Task AsyncFlowFailure_RecoveryUsesAsyncMode()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.QueryNoFlush);
        Assert.IsTrue(protocol.TryQueue(faulting));

        await RunAsync(protocol, "select 1");
    }

    // Sync-failed flow: the recovery is constructed with async=false. The substitute is run
    // through the executor's normal path (recovery items aren't enqueued via the sync
    // handoff), but the inherited IsAsync flag guards the recovery's own dispatch shape.
    // End-to-end: a sync flow fails, a sync flow runs after, both complete. Confirms the
    // recovery handed off cleanly between the two sync caller threads without stranding the
    // executor.
    [TestMethod]
    public async Task SyncFlowFailure_RecoveryHandoffWorks()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: false, FaultPhase.PreReturn, WriteShape.QueryNoFlush);
        Assert.IsTrue(protocol.TryQueue(faulting));

        await RunSync(protocol, "select 1");
    }

    // Pipelined sibling behind a faulting flow. Both are queued back to back, the first
    // faults pre-return with bytes already buffered. Recovery has to clean the wire AND
    // preserve the sibling's queue position so the sibling can still run. End-to-end: queue
    // both, the sibling completes successfully on the same protocol.
    [TestMethod]
    public async Task FaultingFlow_WithPipelinedSibling_SiblingCompletes()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.QueryNoFlush);
        var sibling = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(faulting));
        Assert.IsTrue(protocol.TryQueue(sibling));

        var e = sibling.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    // Two faulting flows back to back. The first fails, the second is enqueued behind, also
    // fails. Recovery must handle the second recovery on top of the first, leaving the wire
    // clean for a third (healthy) flow. Confirms recovery composes with itself.
    [TestMethod]
    public async Task BackToBackFailures_RecoveryComposes()
    {
        await using var protocol = await ConnectAsync();
        var f1 = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.QueryNoFlush);
        var f2 = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.QueryNoFlush);
        Assert.IsTrue(protocol.TryQueue(f1));
        Assert.IsTrue(protocol.TryQueue(f2));

        await RunAsync(protocol, "select 1");
    }

    // Multi-command CommandFlow that succeeds after recovery clears a prior failed flow.
    // Confirms recovery's wire-cleanup doesn't disturb the multi-command read state machine
    // that a subsequent flow's iteration depends on.
    [TestMethod]
    public async Task MultiCommandFlow_AfterRecovery_Completes()
    {
        await using var protocol = await ConnectAsync();
        var faulting = new FaultingFlow(async: true, FaultPhase.PreReturn, WriteShape.ParseBindExecuteNoSync);
        Assert.IsTrue(protocol.TryQueue(faulting));

        var multi = new CommandFlow(async: true, Command.Create("select 1"), Command.Create("select 2"), Command.Create("select 3"));
        Assert.IsTrue(protocol.TryQueue(multi));
        var e = multi.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    // The recovery itself failing: the transport is dead by the time the recovery drain tries
    // to flush, so the ResyncRecoveryFlow faults instead of cleaning the wire. Two contracts
    // under test: (1) recovery-of-recovery does not exist - the recovery's own fault completes
    // it directly (in a Debug run, a policy re-consult would fire TryRecoverItemFailure's
    // assert and crash the test); (2) the binding discharge still completes the FAILED flow on
    // every recovery exit - with its original exception as primary and the recovery's fault
    // attached (a flow's completion exception carries every failure that terminated its
    // position).
    //
    // Deterministic by construction: the flow kills the writer (Writer.Complete - the
    // PipeWriter's own lifecycle API, no disposal-ownership violation) AFTER its writes land
    // but BEFORE its synthetic fault fires, so the original failure stays the synthetic one
    // and the recovery's first wire I/O (the injected-Sync flush) hits a completed writer.
    [TestMethod]
    public async Task RecoveryItselfFails_FailedFlowCompletes_WithBothFaults()
    {
        var options = NewOptions();
        var transport = await SocketStreamConnection.ConnectAsync(options.EndPoint);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);

        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.ParseBindExecuteNoSync)
        {
            AfterWrites = () => transport.Writer.Complete(new IOException("synthetic transport death")),
        };
        Assert.IsTrue(protocol.TryQueue(faulting));

        Exception? completion = null;
        try
        {
            await faulting.WaitForComplete().AsTask();
        }
        catch (Exception ex)
        {
            completion = ex;
        }

        Assert.IsInstanceOfType<PgClientException>(completion,
            $"Failed flow must complete with both terminal facts when the recovery also died. Got: {completion}");
        var aggregate = (AggregateException)completion.InnerException!;
        Assert.AreEqual(2, aggregate.InnerExceptions.Count);
        Assert.IsFalse(aggregate.InnerExceptions.Any(e => e is PgClientException));
        Assert.IsInstanceOfType<InvalidOperationException>(aggregate.InnerExceptions[0],
            "The original failure is the primary.");
        StringAssert.Contains(aggregate.InnerExceptions[0].Message, "synthetic");
        // The recovery's flush over the completed writer surfaces as ObjectDisposedException
        // (the PipeWriter completed-writer convention) - the recovery's own terminal fact,
        // riding behind the original.
        Assert.IsInstanceOfType<ObjectDisposedException>(aggregate.InnerExceptions[1]);
    }

    // Read-side death: the recovery's flush SUCCEEDS (writer alive), and its DrainPhase read
    // faults instead - the recovery's PIPELINE task rather than its trailing task. This is the
    // guarded-task path end to end in the real protocol: the recovery commits as an ordinary
    // tail, its late fault travels as the framework's marker, and the framework completes it
    // directly (a policy re-consult would fire TryRecoverItemFailure's assert in this Debug
    // run). The failed flow still completes with both terminal facts.
    [TestMethod]
    public async Task RecoveryReadFails_FailedFlowCompletes_WithBothFaults()
    {
        var options = NewOptions();
        var transport = await SocketStreamConnection.ConnectAsync(options.EndPoint);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);

        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.ParseBindExecuteNoSync)
        {
            AfterWrites = () => transport.Reader.Complete(new IOException("synthetic read death")),
        };
        Assert.IsTrue(protocol.TryQueue(faulting));

        Exception? completion = null;
        try
        {
            await faulting.WaitForComplete().AsTask();
        }
        catch (Exception ex)
        {
            completion = ex;
        }

        Assert.IsInstanceOfType<PgClientException>(completion,
            $"Failed flow must complete with both terminal facts when the recovery also died. Got: {completion}");
        var aggregate = (AggregateException)completion.InnerException!;
        Assert.AreEqual(2, aggregate.InnerExceptions.Count);
        Assert.IsFalse(aggregate.InnerExceptions.Any(e => e is PgClientException));
        Assert.IsInstanceOfType<InvalidOperationException>(aggregate.InnerExceptions[0],
            "The original failure is the primary.");
        StringAssert.Contains(aggregate.InnerExceptions[0].Message, "synthetic");
        Assert.AreNotSame(aggregate.InnerExceptions[0], aggregate.InnerExceptions[1],
            "The recovery's own read fault rides behind the original.");
    }

    // Substitution-substrate contract: PipelineTask kind with a still-in-flight trailing.
    // The framework captures the failed flow's TrailingExecutionTask into the context's
    // OutstandingPhaseTask and the policy hands it to ResyncRecoveryFlow.BindFailedFlow.
    // ResyncRecoveryFlow's ExecuteAuto sees the trailing as not-completed-successfully and
    // takes the move-to-trailing path: returns FlowTasks fast (no inline-await wedge of the
    // executor pump), and the actual await of outstanding + WriteSync happens in the
    // recovery's trailing phase running concurrently with its DrainPhase. Pending outstanding
    // that EVENTUALLY succeeds: recovery awaits it cleanly, then writes its Sync, then drains.
    [TestMethod]
    public async Task PipelineTask_PendingTrailingThatEventuallySucceeds_RecoveryCompletesAndSiblingRuns()
    {
        var protocol = await ConnectAsync();
        var trailingTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.ParseBindExecuteNoSync)
        {
            ControllableTrailing = trailingTcs,
        };
        Assert.IsTrue(protocol.TryQueue(faulting));

        // Start the sibling, then release the trailing, then await: recovery's retirement awaits
        // the moved-to-trailing outstanding and the sibling is queued behind recovery, so the
        // trailing must resolve for the sibling to complete. Releasing after the sibling is
        // in flight keeps the outstanding pending across recovery's move-to-trailing check
        // without a timed release.
        var sibling = RunAsync(protocol, "select 1");
        trailingTcs.TrySetResult();
        await sibling;

        await protocol.CompleteAsync();
    }

    [TestMethod]
    public async Task PipelineTask_QueryPendingTrailing_WaitsBeforeRecoveryWrites()
    {
        await using var protocol = await ConnectAsync();
        var trailing = new AwaitObservedValueTaskSource();
        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.QueryNoFlush)
        {
            ControllableTrailingTask = trailing.Task,
        };
        Assert.IsTrue(protocol.TryQueue(faulting));

        var sibling = RunAsync(protocol, "select 1");
        var first = await Task.WhenAny(trailing.AwaitObserved, sibling)
            ;
        Assert.AreSame(trailing.AwaitObserved, first,
            "recovery wrote ROLLBACK before the failed flow's pending trailing write settled");

        trailing.SetResult();
        await sibling;
    }

    // Sync-flow variant of the pending-trailing move-to-trailing path (the "sync in trailing"
    // punch-list check). The recovery inherits IsAsync=false, so its Sync flush runs on the sync
    // path. The move-to-trailing deadlock-avoidance relies on DrainPhase reading the wire
    // CONCURRENTLY with the wait on outstanding; if a sync recovery serializes
    // trailing-await-then-drain, the TCP-window cycle re-forms. The sync flow runs off-thread with
    // a timeout so a recovery deadlock surfaces as a loud TimeoutException, not a hung runner.
    [TestMethod]
    public async Task SyncFlowFailure_PendingTrailing_DrainsConcurrentlyWithoutDeadlock()
    {
        var protocol = await ConnectAsync();
        try
        {
            var trailingTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var faulting = new FaultingFlow(async: false, FaultPhase.PipelineTask, WriteShape.ParseBindExecuteNoSync)
            {
                ControllableTrailing = trailingTcs,
            };
            Assert.IsTrue(protocol.TryQueue(faulting));

            // Off-thread: RunSync's blocking MoveNext drives the wire on a TP thread, so a recovery
            // deadlock surfaces as this timeout rather than wedging the test thread. Start the
            // sibling, then release the trailing, then await: recovery's retirement awaits the
            // moved trailing and the sibling queues behind recovery.
            var sibling = Task.Run(() => RunSync(protocol, "select 1"));
            trailingTcs.TrySetResult();
            await sibling;
        }
        finally
        {
            // A wedged protocol's completion may also hang; guard it so a deadlock result stays a
            // clean TimeoutException from the body, not a hung cleanup.
            try { await protocol.CompleteAsync(); }
            catch { }
        }
    }

    // Read-outstanding direction (the inverse of ThrowIfCannotWrite's failed-flow WRITE permit):
    // the trailing faults only AFTER the pipeline read has genuinely acquired the decoder
    // (PipelineReadAcquired), so recovery is installed while the failed read still holds the turn.
    // This is NOT benign on its own - if recovery's ActivateHeadItem robbed the read-turn, the
    // failed read would decode the wrong message and its late fault would re-enter nonexistent
    // recovery-of-recovery. The fix is what this test pins: the policy forwards OutstandingPhaseTask
    // for the TrailingExecutionTask kind (outstandingIsRead), the decoder permit
    // (ResyncRecoveryFlow.FailedReadOutstanding -> PgDecoder.CurrentExecutionControl) resolves to the
    // FailedFlow so the in-flight read finishes on its OWN control, and DrainPhase awaits that read
    // before the recovery takes the read turn. A timeout/desync here means that sequencing
    // regressed.
    [TestMethod]
    public async Task TrailingTask_Failure_WaitsForOutstandingReadBeforeRecovery()
    {
        var protocol = await ConnectAsync();
        try
        {
            var releaseGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var acquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var trailing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var faulting = new FaultingFlow(async: true, FaultPhase.TrailingTask, WriteShape.ParseBindExecuteNoSync)
            {
                ControllablePipelineRead = releaseGate,
                PipelineReadAcquired = acquired,
                ControllableTrailing = trailing,
            };
            Assert.IsTrue(protocol.TryQueue(faulting));

            // The read genuinely owns the decoder before the trailing failure is published.
            await acquired.Task;
            trailing.TrySetException(new InvalidOperationException("synthetic trailing fault while read held"));

            var sibling = RunAsync(protocol, "select 1");
            for (var i = 0; i < 8; i++)
                await Task.Yield();
            Assert.IsFalse(sibling.IsCompleted,
                "recovery must not revoke a still-running pipeline phase's decoder ownership");

            releaseGate.TrySetResult();
            await sibling;
        }
        finally
        {
            try { await protocol.CompleteAsync(); }
            catch { }
        }
    }

    // Substitution-substrate contract: PipelineTask kind with a trailing that ALSO faults.
    // The recovery's TrailingPhase observes-and-discards the outstanding's exception (the
    // failed flow's primary fault is already in FailureException; the trailing's outcome is
    // subordinate). Recovery then proceeds with WriteSync + drain. This is the critical
    // anti-recovery-on-recovery property: the framework does NOT re-enter TryRecoverItemFailure
    // for the trailing's fault - if we naively re-awaited the trailing without catching, the
    // recovery would fault and Slon's recovery-on-recovery assert would fire.
    [TestMethod]
    public async Task PipelineTask_PendingTrailingThatAlsoFaults_RecoveryObservesAndDiscardsAndProceeds()
    {
        var protocol = await ConnectAsync();
        var trailingTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.ParseBindExecuteNoSync)
        {
            ControllableTrailing = trailingTcs,
        };
        Assert.IsTrue(protocol.TryQueue(faulting));

        // Start the sibling, then fault the trailing, then await: the recovery must catch and
        // discard the trailing fault, not propagate it into a second policy consultation.
        // Recovery's retirement awaits the moved trailing, so the fault must land for the
        // sibling (queued behind recovery) to complete. The wire is still intact (the WriteSync
        // went out, the drain consumed the failed flow's inherited RFQs).
        var sibling = RunAsync(protocol, "select 1");
        trailingTcs.TrySetException(new InvalidOperationException("synthetic trailing fault"));
        await sibling;

        await protocol.CompleteAsync();
    }

    // Substitution-substrate contract: the slot inheritance. While recovery is the
    // ExecutingItem (its tenure), the FAILED flow's identity is extended through the
    // substitute via the gate permissivity in ThrowIfCannotWrite. We verify the recovery
    // mechanism end-to-end by confirming that the failed flow's binding discharge completes
    // it with its original exception, AND a subsequent flow runs successfully (proves wire
    // was cleaned by the substitute).
    [TestMethod]
    public async Task PipelineTask_RecoverySubstitute_DischargesFailedFlowBindingAndCleansWire()
    {
        var protocol = await ConnectAsync();

        var faulting = new FaultingFlow(async: true, FaultPhase.PipelineTask, WriteShape.ParseBindExecuteNoSync);
        Assert.IsTrue(protocol.TryQueue(faulting));

        // Failed flow completes via the binding discharge (ResyncRecoveryFlow.FailedFlow
        // captured at TryRecoverItemFailure time; CompleteItem fires on the failed flow when
        // recovery completes). Its exception is the original synthetic fault, NOT the
        // recovery's behavior.
        Exception? failedCompletion = null;
        try { await faulting.WaitForComplete().AsTask(); }
        catch (Exception ex) { failedCompletion = ex; }
        Assert.IsInstanceOfType<InvalidOperationException>(failedCompletion);
        StringAssert.Contains(failedCompletion!.Message, "synthetic");

        // Wire was cleaned by the substitute (recovery's WriteSync + drain).
        await RunAsync(protocol, "select 1");

        await protocol.CompleteAsync();
    }

    // Regression guard for the drain-count over-drain fix. Scripted no-PG harness: two RFQ-inducing
    // writes (snapshot RfqCount = 2); the held read consumes one message before the trailing faults
    // (parked before RFQ#1) and the rest after. With count 2 the second read crosses RFQ#1
    // post-snapshot. Returns how the failed flow completed: its synthetic fault on success, a
    // TimeoutException if recovery parked.
    async Task<Exception?> RunDrainReconciliationScenario(int heldReadConsumeCount)
    {
        var options = PgTestPool.NewOptions();
        var transport = new BackpressureWriteTransport(Handshake(), sendWindow: 1 << 20);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);

        var releaseGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var acquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var trailing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var consumedFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var faulting = new FaultingFlow(async: true, FaultPhase.TrailingTask, WriteShape.TwoQueriesNoFlush)
        {
            ControllablePipelineRead = releaseGate,
            PipelineReadAcquired = acquired,
            ControllableTrailing = trailing,
            HeldReadConsumeCount = heldReadConsumeCount,
            HeldReadConsumedFirst = consumedFirst,
        };
        Assert.IsTrue(protocol.TryQueue(faulting));
        var completed = faulting.WaitForComplete().AsTask();

        try
        {
            await acquired.Task;
            releaseGate.TrySetResult();

            transport.ReleaseSegment(CommandComplete());
            await consumedFirst.Task;

            trailing.TrySetException(new InvalidOperationException("synthetic trailing fault while read held"));
            // Recovery's resync ROLLBACK is written inline in its ExecuteAuto (read-outstanding
            // shape), strictly after Create snapshots RfqCount - observing it proves the snapshot
            // exists before RFQ#1 is released below, so the crossing stays post-snapshot.
            await transport.WaitForWrittenAsync("ROLLBACK");

            transport.ReleaseSegment(ReadyForQuery());
            transport.ReleaseSegment(CommandComplete());
            transport.ReleaseSegment(ReadyForQuery());
            // Recovery appends a ROLLBACK (closes any open transaction) whose RFQ it also drains, so the
            // scripted server answers it - otherwise recovery parks waiting on an RFQ that never comes.
            transport.ReleaseSegment(CommandComplete());
            transport.ReleaseSegment(ReadyForQuery());

            try { await completed; return null; }
            catch (Exception ex) { return ex; }
        }
        finally
        {
            try { await protocol.DisposeAsync().AsTask(); }
            catch { }
        }
    }

    // Control: the held read consumes only its one message and finishes BEFORE the fault, so it
    // never crosses an RFQ. Same recovery path and bytes as the repro below - the only difference
    // is the crossing - so this passing isolates the crossing as the cause.
    [TestMethod]
    public async Task ReadStopsBeforeRfq_RecoveryDrainsClean()
    {
        var completion = await RunDrainReconciliationScenario(heldReadConsumeCount: 1);
        Assert.IsNotInstanceOfType<TimeoutException>(completion, "control must not park: the read never crosses an RFQ");
        Assert.IsInstanceOfType<InvalidOperationException>(completion, "failed flow should complete with its synthetic fault");
    }

    // The held read crosses RFQ#1 after recovery snapshots RfqCount, so DrainPhase must reconcile
    // against the failed flow's live count and drain only the one remaining RFQ. Before the fix
    // this timed out - recovery drained the snapshot and parked for an RFQ the read had consumed.
    [TestMethod]
    public async Task ReadCrossesRfqAfterSnapshot_RecoveryReconciles()
    {
        var completion = await RunDrainReconciliationScenario(heldReadConsumeCount: 2);
        Assert.IsNotInstanceOfType<TimeoutException>(completion, "recovery over-drained: drained snapshotted RfqCount ignoring the RFQ the read crossed post-snapshot");
        Assert.IsInstanceOfType<InvalidOperationException>(completion, "failed flow should complete with its synthetic fault once recovery reconciles");
    }

    static byte[] Handshake()
    {
        var b = new byte[64];
        var o = 0;
        b[o++] = (byte)'R'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 0); o += 4;
        b[o++] = (byte)'K'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 12); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 4321); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8765); o += 4;
        b[o++] = (byte)'Z'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 5); o += 4; b[o++] = (byte)'I';
        return b.AsSpan(0, o).ToArray();
    }

    static byte[] CommandComplete()
    {
        ReadOnlySpan<byte> body = "SELECT 1 "u8;
        var msg = new byte[1 + 4 + body.Length];
        msg[0] = (byte)'C';
        BinaryPrimitives.WriteInt32BigEndian(msg.AsSpan(1), 4 + body.Length);
        body.CopyTo(msg.AsSpan(5));
        return msg;
    }

    static byte[] ReadyForQuery()
    {
        var msg = new byte[6];
        msg[0] = (byte)'Z';
        BinaryPrimitives.WriteInt32BigEndian(msg.AsSpan(1), 5);
        msg[5] = (byte)'I';
        return msg;
    }
}
