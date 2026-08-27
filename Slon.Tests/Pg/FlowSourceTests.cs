using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// PgClientFlowSource dispatch, inline-drive bounds and shutdown coordination without the pipeline,
// flow bodies or wire. A hand-written consumer pins exactly-once dispatch-versus-drain ownership.
[TestClass]
public class FlowSourceTests
{
    sealed class ReleasingSyncFlow : PgClientFlow
    {
        public FlowHandoffEvent Handoff { get; } = new();

        public ReleasingSyncFlow() => IsAsync = false;

        private protected override FlowHandoffEvent? HandoffEvent => Handoff;
        protected override ValueTask<FlowTasks> ExecuteAuto(Context context) => new(new FlowTasks());
        protected override void OnStopping(Exception exception) => Handoff.Set();
        // Reproduce a waiter consuming the early OnStopping edge before terminal publication.
        protected override void OnReleasing(Exception? exception) => Handoff.Reset();
    }

    // In-memory, but exercises the source's spin/Mres wait points (PgClientFlowSource), which escalate to
    // Sleep(1) once the threadpool is saturated - so a blanket high count goes super-linear. Cap; the raw
    // value still flows under SLON_UNCAPPED=1 for a deliberate soak.
    static int Iterations => StressEnv.Iterations(fallback: 512, cap: 20_000);

    // True only within the dynamic extent of the test's inline-driving Execute call. The budget
    // invariant is stack-bounded, not thread-bounded: preferLocal dispatch may legally resume
    // transferred work on the freed caller thread, so thread identity is not a valid oracle.
    [ThreadStatic]
    static bool _onInlineDriveStack;

    [TestMethod]
    public void FailUnstarted_SignalsSyncHandoffAfterTerminalPublication()
    {
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol, protocol.FlowControl);
        var flow = new ReleasingSyncFlow();
        source.EnqueueSyncWaiter(flow);

        flow.GetExecutionControl(protocol.FlowControl).FailUnstarted(new IOException("source completed"));

        Assert.IsTrue(flow.IsCompleted);
        Assert.IsTrue(flow.Handoff.IsSet);
    }

    [TestMethod]
    public async Task IdleInlineDrive_IsBoundedToTheEnqueuingItem()
    {
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol, protocol.FlowControl);
        var enumerator = source.CreateEnumerator();
        var first = new CommandFlow(async: true);
        var second = new CommandFlow(async: true);
        var callerThread = Environment.CurrentManagedThreadId;
        var firstThread = 0;
        var secondOnInlineDriveStack = false;
        var secondSeen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task Consume()
        {
            while (true)
            {
                if (enumerator.TryGetNext(out var item))
                {
                    if (ReferenceEquals(item, first))
                    {
                        firstThread = Environment.CurrentManagedThreadId;
                        // Arrives while the caller owns the one-item runner. It must be transferred to
                        // the scheduler after the inline hand-back, not consumed on this stack.
                        source.Enqueue(second).Execute(runContinuationsAsynchronously: false);
                    }
                    else if (ReferenceEquals(item, second))
                    {
                        secondOnInlineDriveStack = _onInlineDriveStack;
                        secondSeen.TrySetResult();
                    }
                    continue;
                }
                if (!await enumerator.WaitForNextAsync())
                    return;
            }
        }

        var consumer = Consume();
        _onInlineDriveStack = true;
        try
        {
            source.Enqueue(first, inlineEligible: true).Execute(runContinuationsAsynchronously: false);
        }
        finally
        {
            _onInlineDriveStack = false;
        }

        Assert.AreEqual(callerThread, firstThread, "The idle claimant did not drive its own item inline.");
        await secondSeen.Task;
        Assert.IsFalse(secondOnInlineDriveStack, "A successor escaped the one-item inline budget.");

        enumerator.Complete();
        await consumer;
        await enumerator.DisposeAsync();
    }

    [TestMethod]
    public async Task HeldSyncFlow_WithConsumedNotification_ClaimsPublishedWaitBeforeParking()
    {
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol, protocol.FlowControl);
        var enumerator = source.CreateEnumerator();
        var flow = new CommandFlow(async: false);
        source.EnqueueSyncWaiter(flow);

        Assert.IsFalse(enumerator.TryGetNext(out _));
        var wait = enumerator.WaitForNextAsync();
        var resumed = 0;
        wait.GetAwaiter().UnsafeOnCompleted(() => Interlocked.Increment(ref resumed));

        // The notification is an edge; the held head and published source wait remain claimable.
        flow.GetExecutionControl(protocol.FlowControl).HandoffEvent!.Reset();
        source.WaitForExecutor(flow);

        Assert.AreEqual(1, resumed);
        Assert.IsTrue(enumerator.TryGetNext(out var taken));
        Assert.AreSame(flow, taken);
        enumerator.Complete();
        await enumerator.DisposeAsync();
    }

    [TestMethod]
    public async Task Stress_SourceDispatchVsDrain_SingleConsumerHolds()
    {
        // Uninitialized protocol: the source only reads Protocol.UnflushedBytes on the pull path, which
        // is 0 before Initialize. Nothing else of the protocol is touched.
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));

        for (int i = 0; i < Iterations; i++)
        {
            const int N = 8;
            var source = PgClientFlowSource.Create(protocol, protocol.FlowControl);
            var enumerator = source.CreateEnumerator();

            var flows = new PgClientFlow[N];
            var index = new Dictionary<PgClientFlow, int>(ReferenceEqualityComparer.Instance);
            for (int k = 0; k < N; k++)
            {
                flows[k] = new CommandFlow(async: true);
                index[flows[k]] = k;
            }
            var consume = new int[N];
            var executorConsume = new int[N];
            var drainConsume = new int[N];
            int nullSeen = 0, unknownSeen = 0;

            void Record(PgClientFlow? f, bool drain)
            {
                if (f is not null && index.TryGetValue(f, out var k))
                {
                    Interlocked.Increment(ref consume[k]);
                    Interlocked.Increment(ref (drain ? ref drainConsume[k] : ref executorConsume[k]));
                }
                else if (f is null)
                    Interlocked.Increment(ref nullSeen);
                else
                    Interlocked.Increment(ref unknownSeen);
            }

            // Executor: the source interaction Pipeline.ExecuteSource performs.
            var executor = Task.Run(async () =>
            {
                while (true)
                {
                    if (enumerator.TryGetNext(out var item))
                    {
                        Record(item, drain: false);
                        continue;
                    }
                    if (!await enumerator.WaitForNextAsync())
                        break;
                }
            });

            // Producer: enqueue all N, waking the executor each time.
            for (int k = 0; k < N; k++)
                source.Enqueue(flows[k]).Execute(runContinuationsAsynchronously: true);

            // Shutdown: the drain coordination PgClientProtocol.Shutdown performs.
            var executorStopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            source.SetDrainSignal(executorStopped);
            enumerator.Complete();
            await Task.WhenAny(executorStopped.Task, executor);
            source.DrainInertItems(flow => Record(flow, drain: true));
            await executor;
            await enumerator.DisposeAsync();

            List<string>? anomalies = null;
            if (nullSeen != 0)
                (anomalies ??= []).Add($"DrainInertItems saw {nullSeen} null flow(s)");
            if (unknownSeen != 0)
                (anomalies ??= []).Add($"saw {unknownSeen} unrecognized flow(s)");
            for (int k = 0; k < N; k++)
            {
                if (consume[k] == 0)
                    (anomalies ??= []).Add($"flow {k} consumed 0 times");
                else if (consume[k] > 1)
                    (anomalies ??= []).Add($"flow {k} consumed {consume[k]} times " +
                        $"(executor={executorConsume[k]}, drain={drainConsume[k]})");
            }
            if (anomalies is not null)
                Assert.Fail($"iter {i}: {string.Join("; ", anomalies)}");
        }
    }


    [TestMethod]
    public async Task QueuedSyncFlow_CompletesBeforeHandoff_CallerReturnsAndFlowResolvedOnce()
    {
        // The source only reads Protocol.UnflushedBytes on the pull path (0 before Initialize); nothing
        // else of the protocol is touched.
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol, protocol.FlowControl);
        var enumerator = source.CreateEnumerator();

        var flow = new CommandFlow(async: false);
        var consumed = 0;
        void Record(PgClientFlow? f)
        {
            if (ReferenceEquals(f, flow))
            {
                Interlocked.Increment(ref consumed);
                // Mirror the protocol's drain onInert (flow.Complete -> OnComplete -> SignalProgress -> MRES
                // .Set): a never-held sync flow drained inert wakes its handoff caller, which bails on
                // IsCompleted. With the wait-list-free source, this drain wake IS the completion-bail.
                f.GetExecutionControl(protocol.FlowControl).HandoffEvent?.Set();
            }
        }

        // Sync caller: append the flow (this alone makes HasSyncWaiter true), then block in WaitForExecutor.
        // On the fixed source it is taken over or bailed; on the unfixed source it strands waiting for a
        // signal the spinning executor never sends.
        var node = source.EnqueueSyncWaiter(flow);
        var caller = Task.Run(() => source.WaitForExecutor(node));

        // Complete the source while the sync flow sits queued and un-held.
        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        source.SetDrainSignal(drained);
        enumerator.Complete();

        // Executor: Pipeline.ExecuteSource's pull loop, with a spin guard. On the unfixed source the loop
        // busy-spins on WaitCore.Retry; the guard trips and surfaces the hang as a failure rather than a
        // timeout. Each Retry resolves synchronously, so the cap is reached in well under a second.
        var executor = Task.Run(async () =>
        {
            long guard = 0;
            while (true)
            {
                if (enumerator.TryGetNext(out var item))
                {
                    Record(item);
                    continue;
                }
                if (++guard > 5_000_000)
                    throw new InvalidOperationException(
                        "executor busy-looped on WaitCore.Retry - the queued sync flow was never held or resolved.");
                if (!await enumerator.WaitForNextAsync())
                    break;
            }
        });

        await Task.WhenAny(drained.Task, executor);
        source.DrainInertItems(Record);
        await executor;
        await caller;
        await enumerator.DisposeAsync();

        Assert.AreEqual(1, consumed,
            "the queued sync flow must resolve exactly once (taken over by its caller xor drained inert).");
    }

    [TestMethod]
    public async Task AsyncAheadOfQueuedSyncFlow_CompletesBeforeHandoff_AllResolvedOnce()
    {
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol, protocol.FlowControl);
        var enumerator = source.CreateEnumerator();

        // FIFO: an async flow queued AHEAD of the sync waiter. At completion the sync head can't be held
        // until the async ahead of it is disposed - the async head is left for DrainInert, which can't
        // run until the executor resolves Completed, which HasSyncWaiter gates off. Same spin family.
        var asyncFlow = new CommandFlow(async: true);
        var syncFlow = new CommandFlow(async: false);
        var consumed = new Dictionary<PgClientFlow, int>(ReferenceEqualityComparer.Instance)
        {
            [asyncFlow] = 0,
            [syncFlow] = 0,
        };
        void Record(PgClientFlow? f)
        {
            if (f is not null && consumed.ContainsKey(f))
            {
                lock (consumed) consumed[f]++;
                // Mirror the protocol's drain onInert wake (see QueuedSyncFlow): waking the sync flow's
                // handoff caller is the completion-bail under the wait-list-free source. Harmless on the
                // async head (no caller parks on its MRES).
                f.GetExecutionControl(protocol.FlowControl).HandoffEvent?.Set();
            }
        }

        source.Enqueue(asyncFlow);
        var node = source.EnqueueSyncWaiter(syncFlow);
        var caller = Task.Run(() => source.WaitForExecutor(node));

        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        source.SetDrainSignal(drained);
        enumerator.Complete();

        var executor = Task.Run(async () =>
        {
            long guard = 0;
            while (true)
            {
                if (enumerator.TryGetNext(out var item))
                {
                    Record(item);
                    continue;
                }
                if (++guard > 5_000_000)
                    throw new InvalidOperationException(
                        "executor busy-looped on WaitCore.Retry - an async flow ahead of the sync waiter never advanced.");
                if (!await enumerator.WaitForNextAsync())
                    break;
            }
        });

        await Task.WhenAny(drained.Task, executor);
        source.DrainInertItems(Record);
        await executor;
        await caller;
        await enumerator.DisposeAsync();

        Assert.AreEqual(1, consumed[asyncFlow], "the queued async flow must resolve exactly once.");
        Assert.AreEqual(1, consumed[syncFlow], "the queued sync flow must resolve exactly once.");
    }
}
