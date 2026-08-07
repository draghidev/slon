using System.Buffers;
using System.Net.Sockets;
using System.IO.Pipelines;
using Microsoft.Extensions.Time.Testing;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Deterministic reproduction of the racing-teardown bug behind
// ProtocolCompletionTests.CompleteAsync_RacingDisposeAsync_ConvergesCleanly.
//
// A consumer drives an async CommandFlow with the failing test's pattern (read loop + catch +
// DisposeAsync) while graceful CompleteAsync (StoppingToken) and forceful DisposeAsync (AbortToken)
// race the teardown. Two teardown signals are UNSYNCHRONIZED:
//   read-fault  - AbortToken faults the in-flight decoder read (PgDecoder.TranslateReadCancellation),
//                 the body throws PgClientClosedException out of its read.
//   gate-fault  - the heartbeat's OnAbort/OnStopping calls CancelPendingWait, faulting the gate the
//                 body parks on between results and setting the sticky CancelException.
//
// The existing in-memory harness (StoppingTokenInMemoryTests) already gives us:
//   - capture-once-replay-on-demand of real server bytes (CaptureAsync + GatedReplayTransport),
//   - ArmReadPark to detect a flow parked mid-read,
//   - FakeTimeProvider to fire heartbeat/abort at chosen instants.
// The NEW lever this class needs is control of read-fault vs gate-fault ORDERING relative to the
// consumer's MoveNextAsync/Reset. The body delivers result 1 inline (its SetResult runs the consumer
// continuation on the body thread), so once the first MoveNextAsync returns the body has executed past
// delivery to its inter-result gate await; a short settle lands it on that park before the test
// perturbs the teardown. The gate sources are set-before-await safe, so the settle only narrows the
// schedule - no production test hook is needed.
//
// Three orderings, each a regression test for the racing-teardown fix:
//   1. Body-driven throw  - pre-fix: uncaught close out of DisposeAsync; now converges.
//   2. Gate-first throw    - pre-fix: uncaught close out of DisposeAsync; now converges.
//   3. Mirror lost-completion - never reachable (self-deliver / live-generation delivery cover it);
//      asserts convergence so a future regression of that protection trips here.
[TestClass]
public class RacingDisposeInMemoryTests
{

    // One real connection, captured once: handshake + the flow's RFQ-delimited response split into
    // wire messages so the test can release RowDescription/DataRow while holding CommandComplete+RFQ,
    // parking the body's drain on its second read.
    static byte[]? _handshake;
    static IReadOnlyList<byte[]>? _messages;
    // Three-command flow capture: command boundaries (CommandComplete) inside one Sync, so the body
    // parks on the INTER-RESULT gate between commands - the multi-command lost-completion window.
    static IReadOnlyList<byte[]>? _multiMessages;
    static IReadOnlyList<byte[]>? _largeRowMessages;
    static PgClientOptions? _options;

    sealed class WriteThenWaitFlow(TaskCompletionSource written, TaskCompletionSource release) : PgClientFlow(supportsDeferredFlush: true)
    {
        public WriteThenWaitFlow(TaskCompletionSource written, TaskCompletionSource release, bool async)
            : this(written, release)
            => IsAsync = async;

        protected override async ValueTask<FlowTasks> ExecuteAuto(Context context)
        {
            context.GetEncoder().WriteQuery("select 1");
            written.SetResult();
            await release.Task.ConfigureAwait(false);
            return new FlowTasks();
        }
    }

    [TestMethod]
    public async Task GracefulCloseWriteCompletionSurfacesClosedException()
    {
        var clock = new FakeTimeProvider();
        var transport = new GatedReplayTransport(_handshake!);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(_options!)
        {
            TimeProvider = clock,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
        });
        await protocol.StartAsync(_options!, transport);

        var written = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var flow = new WriteThenWaitFlow(written, release, async: true);
        Assert.IsTrue(protocol.TryQueue(flow));
        await written.Task;

        var completion = protocol.CompleteAsync();
        transport.CompleteWriteCleanly();
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await protocol.FlushAsync(CancellationToken.None));
        release.SetResult();

        await completion;
    }

    [TestMethod]
    public async Task GracefulCloseReadEofSurfacesClosedException()
    {
        var clock = new FakeTimeProvider();
        var transport = new GatedReplayTransport(_handshake!);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(_options!)
        {
            TimeProvider = clock,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
        });
        await protocol.StartAsync(_options!, transport);

        var readParked = transport.ArmReadPark();
        var flow = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync().AsTask();
        await readParked;

        var completion = protocol.CompleteAsync();
        transport.CompleteReadCleanly();
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await moveNext);
        await enumerator.DisposeAsync();
        await completion;
    }

    [TestMethod]
    public async Task GracefulCloseMidRowEofSurfacesClosedException()
    {
        var clock = new FakeTimeProvider();
        var transport = new GatedReplayTransport(_handshake!);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(_options!)
        {
            TimeProvider = clock,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
        });
        await protocol.StartAsync(_options!, transport);

        var initialReadParked = transport.ArmReadPark();
        var flow = new CommandFlow(async: true, Command.Create("select repeat('x', 131072)"));
        Assert.IsTrue(protocol.TryQueue(flow));
        var flowEnumerator = flow.GetAsyncEnumerator();
        var resultPending = flowEnumerator.MoveNextAsync().AsTask();
        await initialReadParked;

        var messages = _largeRowMessages!;
        var dataRowIndex = messages.ToList().FindIndex(static message => message[0] == (byte)'D');
        Assert.IsGreaterThanOrEqualTo(0, dataRowIndex);
        for (var i = 0; i < dataRowIndex; i++)
            transport.ReleaseSegment(messages[i]);
        transport.ReleaseSegment(messages[dataRowIndex]
            .AsSpan(0, BackendMessageBatch.Segmenter.DefaultDataRowStreamingThreshold).ToArray());

        Assert.IsTrue(await resultPending);
        var rows = flowEnumerator.Current.GetAsyncEnumerator(CommandResult.RowBuffering.Streaming);
        Assert.IsTrue(await rows.MoveNextAsync().AsTask());

        var continuationReadParked = transport.ArmReadPark();
        var valuePending = rows.Current.GetValueAsync<string>(0).AsTask();
        await continuationReadParked;

        var completion = protocol.CompleteAsync();
        transport.CompleteReadCleanly();
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await valuePending);
        try { await rows.DisposeAsync().AsTask(); } catch (PgClientClosedException) { }
        try { await flowEnumerator.DisposeAsync().AsTask(); } catch (PgClientClosedException) { }
        await completion;
    }

    [ClassInitialize]
    public static async Task ClassInit(TestContext _)
    {
        _options = PgTestPool.NewOptions();
        var (handshake, response) = await CaptureAsync(_options, Command.Create("select generate_series(1, 3)"));
        _handshake = handshake;
        _messages = SplitMessages(response);

        var (_, multiResponse) = await CaptureAsync(_options,
            Command.Create("select 1"), Command.Create("select 2"), Command.Create("select 3"));
        _multiMessages = SplitMessages(multiResponse);

        var (_, largeRowResponse) = await CaptureAsync(_options, Command.Create("select repeat('x', 131072)"));
        _largeRowMessages = SplitMessages(largeRowResponse);
    }

    // ---------------------------------------------------------------------------------------------
    // The scenario builder. Drives one flow to the post-result-1 park, then hands control back to the
    // caller to pin the teardown. Returns a live Scenario the test steps.
    // ---------------------------------------------------------------------------------------------
    sealed class Scenario : IAsyncDisposable
    {
        public required PgClientProtocol Protocol { get; init; }
        public required CommandFlow Flow { get; init; }
        public required GatedReplayTransport Transport { get; init; }
        public required FakeTimeProvider Clock { get; init; }
        public required CommandFlow.Enumerator Enumerator { get; set; }
        public required IReadOnlyList<byte[]> Messages { get; init; }

        // Fire the heartbeat tick (drives OnAbort/OnStopping -> CancelPendingWait -> gate-fault).
        public void Heartbeat() => Clock.Advance(TimeSpan.FromSeconds(1));

        // Release one captured wire message to the reader.
        public void Release(int index) => Transport.ReleaseSegment(Messages[index]);

        public async ValueTask DisposeAsync()
        {
            try { await Protocol.DisposeAsync(); } catch { }
            Clock.Advance(TimeSpan.FromSeconds(120));
            try { await Protocol.CompleteAsync(); } catch { }
        }
    }

    // Builds a protocol+flow, fires the consumer's first MoveNextAsync, releases RowDescription+first
    // DataRow so the consumer receives result 1, then waits for the body to park on the inter-result
    // gate (post-result-1). CommandComplete+RFQ stay HELD: the body's drain (DisposeAsync's
    // MoveNextAsync) will park on the second read once the gate opens.
    static async Task<Scenario> BuildToFirstResultParked()
    {
        var clock = new FakeTimeProvider();
        var transport = new GatedReplayTransport(_handshake!);
        var protocolOptions = new PgClientProtocolOptions(_options!)
        {
            TimeProvider = clock,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            CompletionTimeout = TimeSpan.FromSeconds(30),
        };
        var protocol = PgClientProtocol.Create(protocolOptions);
        await protocol.StartAsync(_options!, transport);

        var aParked = transport.ArmReadPark();

        var flow = new CommandFlow(async: true, Command.Create("select generate_series(1, 3)"));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();

        var first = e.MoveNextAsync().AsTask();

        // The body writes its command and parks on the response read.
        await aParked;

        // Release RowDescription + first DataRow: enough for ReadUntilExecute to return and the body
        // to deliver result 1, then park on the inter-result gate. CommandComplete + RFQ stay held.
        // The split puts each message in its own segment; release up to (but not including) the
        // CommandComplete.
        var msgs = _messages!;
        var releaseCount = MessagesBeforeCommandComplete(msgs);
        for (var i = 0; i < releaseCount; i++)
            transport.ReleaseSegment(msgs[i]);

        // Consumer's first MoveNextAsync opens the first-result gate; result 1 is delivered. The body's
        // SetResult runs the consumer continuation inline, so on return the body has executed past
        // delivery and its next statement is the inter-result gate await; settle so it reaches that
        // park before the test perturbs the teardown. The gate sources are set-before-await safe, so
        // this only narrows the schedule, it does not gate correctness.
        Assert.IsTrue(await first, "first MoveNextAsync did not deliver result 1");
        await SettleAsync();

        return new Scenario
        {
            Protocol = protocol,
            Flow = flow,
            Transport = transport,
            Clock = clock,
            Enumerator = e,
            Messages = msgs,
        };
    }

    // Count of leading wire messages up to (excluding) the first CommandComplete ('C'). Those carry
    // RowDescription + DataRows for result 1; releasing them lets the body deliver result 1 and park.
    static int MessagesBeforeCommandComplete(IReadOnlyList<byte[]> msgs)
    {
        for (var i = 0; i < msgs.Count; i++)
            if (msgs[i].Length > 0 && msgs[i][0] == (byte)'C')
                return i;
        return msgs.Count;
    }

    // Build a 3-command flow parked on the INTER-RESULT gate after command 1's result, with command 2
    // and 3 bytes HELD. The body delivered result 1 and is awaiting the consumer's next MoveNextAsync
    // (which opens the gate to drive the body to command 2). This is the multi-command lost-completion
    // window: a graceful close fired here must still converge the consumer (the body parked on the
    // inter-result gate must wake and drain, and the consumer's generation must get a completer).
    static async Task<Scenario> BuildMultiToFirstResultParked()
    {
        var clock = new FakeTimeProvider();
        var transport = new GatedReplayTransport(_handshake!);
        var protocolOptions = new PgClientProtocolOptions(_options!)
        {
            TimeProvider = clock,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            CompletionTimeout = TimeSpan.FromSeconds(30),
        };
        var protocol = PgClientProtocol.Create(protocolOptions);
        await protocol.StartAsync(_options!, transport);

        var aParked = transport.ArmReadPark();
        var flow = new CommandFlow(async: true,
            Command.Create("select 1"), Command.Create("select 2"), Command.Create("select 3"));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();

        var first = e.MoveNextAsync().AsTask();
        await aParked;

        // Release command 1's result (T, D, C) so the body delivers result 1 and parks on the
        // inter-result gate. Hold everything from command 2's RowDescription onward.
        var msgs = _multiMessages!;
        var release = MessagesThroughFirstCommandComplete(msgs);
        for (var i = 0; i < release; i++)
            transport.ReleaseSegment(msgs[i]);

        Assert.IsTrue(await first, "first MoveNextAsync did not deliver result 1");
        await SettleAsync();

        return new Scenario
        {
            Protocol = protocol,
            Flow = flow,
            Transport = transport,
            Clock = clock,
            Enumerator = e,
            Messages = msgs,
        };
    }

    // Count of wire messages through (including) the first CommandComplete ('C') - command 1's full
    // result. Releasing these delivers result 1 and parks the body on the inter-result gate.
    static int MessagesThroughFirstCommandComplete(IReadOnlyList<byte[]> msgs)
    {
        for (var i = 0; i < msgs.Count; i++)
            if (msgs[i].Length > 0 && msgs[i][0] == (byte)'C')
                return i + 1;
        return msgs.Count;
    }

    // The failing test's consumer pattern, split into its two phases so the test can interleave the
    // teardown precisely:
    //   RunLoop    - the read loop (`while (await MoveNextAsync()) {}` + catch PgClientClosedException).
    //   RunDispose - the trailing `await e.DisposeAsync()`. Returns the exception that escaped, or null.
    sealed class GranularConsumer(CommandFlow.Enumerator e)
    {
        public Exception? LoopException { get; private set; }

        public Task RunLoop() => Task.Run(async () =>
        {
            try
            {
                while (await e.MoveNextAsync()) { }
            }
            catch (Exception ex)
            {
                LoopException = ex;
            }
        });

        public Task<Exception?> RunDispose() => Task.Run<Exception?>(async () =>
        {
            try
            {
                await e.DisposeAsync();
                return null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        });
    }

    static Task<Exception?> StartSyncDispose(CommandFlow.Enumerator enumerator,
        out ManualResetEventSlim started)
    {
        started = new();
        var signal = started;
        return Task.Factory.StartNew<Exception?>(() =>
        {
            signal.Set();
            try
            {
                enumerator.Dispose();
                return null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    // =============================================================================================
    // Regression tests for the racing-teardown fix. Each pins an interleaving that produced an uncaught
    // PgClientClosedException out of DisposeAsync (or a hang) before the fix, and asserts the fixed
    // behavior: DisposeAsync converges cleanly. The fix collapses "close arrived while DisposeAsync was
    // draining" to ONE chokepoint - DisposeAsyncCore swallows PgClientClosedException (consumer-gone =>
    // a dead wire is the expected terminal), symmetric to the body catch's consumer-gone SetResult(null)
    // and to the consumer read loop's own catch. Only the close is swallowed; other faults still surface.
    //
    // To see the bug WITHOUT the fix, revert the two production edits (DisposeAsyncCore's catch and the
    // body closed-catch's consumer-gone branch); these tests then fail with an escaped close / hang.
    // =============================================================================================

    // Ordering 1 - BODY-DRIVEN THROW. The consumer's read loop consumed result-1, so DisposeAsync (not
    // the loop) drives the body: it opens the gate, drains, re-parks on the held 2nd read consumer-gone;
    // a forceful abort faults that read -> body's closed catch. Pre-fix: rethrow escaped DisposeAsync.
    // Now: DisposeAsyncCore swallows it.
    [TestMethod]
    public async Task Ordering1_BodyDrivenThrow_DisposeConverges()
    {
        await using var s = await BuildToFirstResultParked();
        var consumer = new GranularConsumer(s.Enumerator);

        var bodyReParked = s.Transport.ArmReadPark();
        var dispose = consumer.RunDispose();
        await bodyReParked;

        var protoDispose = s.Protocol.DisposeAsync();
        var escaped = await dispose;
        await protoDispose;

        Assert.IsNull(escaped,
            $"ordering 1: PgClientClosedException escaped DisposeAsync: {escaped?.GetType().Name}");
    }

    // Synchronous counterpart to ordering 1. Dispose takes over the async body and drives it until the
    // held read proves the takeover reached the decoder. Forceful shutdown then faults that exact read.
    // This isolates terminal publication and the continuation handoff without ThreadPool admission,
    // PostgreSQL timing, or an advisory lock.
    [TestMethod]
    public async Task SyncDispose_InFlightReadFault_Converges()
    {
        var iterations = Math.Clamp(StressEnv.Iterations(fallback: 1, cap: int.MaxValue), 1, 500);
        for (var i = 0; i < iterations; i++)
        {
            await using var s = await BuildToFirstResultParked();
            var bodyReParked = s.Transport.ArmReadPark();
            var dispose = StartSyncDispose(s.Enumerator, out var started);
            using (started)
                started.Wait();
            await bodyReParked;

            var abort = s.Protocol.DisposeAsync().AsTask();
            var escaped = await dispose;
            await abort;

            Assert.IsNull(escaped,
                $"iteration {i}: {escaped} escaped synchronous disposal");
        }
    }

    // Ordering 2 - GATE-FIRST GRACEFUL DRAIN. Body parked on the inter-result gate, graceful CompleteAsync
    // sets StoppingToken, a heartbeat tick faults the gate. The body switches to a graceful wire-drain
    // (AwaitResultGate) rather than throwing, so the next pipelined flow would read a clean wire. DisposeAsync
    // then waits for that drain (WaitForDrainOnDispose). The held bytes never arrive in-memory, so it is
    // bounded exactly as production bounds it - advance past CompletionTimeout (30s) so the graceful->abort
    // escalation faults the parked drain read and DisposeAsync converges, swallowing the close.
    [TestMethod]
    public async Task Ordering2_GateFirstThrow_DisposeConverges()
    {
        await using var s = await BuildToFirstResultParked();
        var consumer = new GranularConsumer(s.Enumerator);

        var complete = s.Protocol.CompleteAsync();

        s.Heartbeat();
        await SettleAsync();

        var dispose = consumer.RunDispose();
        await SettleAsync();
        s.Clock.Advance(TimeSpan.FromSeconds(120));
        var escaped = await dispose;
        await complete;

        Assert.IsNull(escaped,
            $"ordering 2: PgClientClosedException escaped DisposeAsync: {escaped?.GetType().Name}");
    }

    // A graceful close publishes gate progress before synchronous disposal begins. The disposer may
    // consume that progress without receiving a continuation and transfer to AwaitDrainOnDispose.
    // Escalation must still redrive/fault the held read and complete both teardown participants.
    [TestMethod]
    public async Task SyncDispose_GateProgressBeforeTakeover_Converges()
    {
        var iterations = Math.Clamp(StressEnv.Iterations(fallback: 1, cap: int.MaxValue), 1, 500);
        for (var i = 0; i < iterations; i++)
        {
            await using var s = await BuildToFirstResultParked();
            var complete = s.Protocol.CompleteAsync();
            s.Heartbeat();
            await SettleAsync();

            var dispose = StartSyncDispose(s.Enumerator, out var started);
            using (started)
                started.Wait();
            s.Clock.Advance(TimeSpan.FromSeconds(120));

            var escaped = await dispose;
            await complete;
            Assert.IsNull(escaped,
                $"iteration {i}: {escaped?.GetType().Name} escaped synchronous disposal");
        }
    }

    // The dump-captured obligation, made deterministic without a production hook. Dispose takes sync
    // ownership while the async body is parked on its initial read. A heartbeat then supplies a
    // progress-only wake; only afterwards does releasing the response let the body publish its sync
    // handoff continuation. Null progress cannot be mistaken for body termination.
    [TestMethod]
    public async Task SyncDispose_ProgressWakeBeforeLateHandoff_DrivesBodyToTermination()
    {
        var clock = new FakeTimeProvider();
        var transport = new GatedReplayTransport(_handshake!);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(_options!)
        {
            TimeProvider = clock,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            CompletionTimeout = TimeSpan.FromSeconds(30),
        });
        await protocol.StartAsync(_options!, transport);

        var readParked = transport.ArmReadPark();
        var flow = new CommandFlow(async: true, Command.Create("select generate_series(1, 3)"));
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetAsyncEnumerator();
        var first = enumerator.MoveNextAsync().AsTask();
        await readParked.WaitAsync(TestTimeout.Hang);

        var complete = protocol.CompleteAsync();
        var dispose = StartSyncDispose(enumerator, out var started);
        using (started)
            Assert.IsTrue(started.Wait(TestTimeout.Hang), "synchronous disposer did not start");

        Assert.IsTrue(SpinWait.SpinUntil(
                () => !flow.GetExecutionControl(protocol.FlowControl).IsAsync, TestTimeout.Hang),
            "synchronous disposer did not take body-drive ownership");
        clock.Advance(TimeSpan.FromSeconds(1));

        foreach (var message in _messages!)
            transport.ReleaseSegment(message);

        var escaped = await dispose.WaitAsync(TestTimeout.Hang);
        await complete.WaitAsync(TestTimeout.Hang);
        Assert.IsNull(escaped, $"{escaped} escaped synchronous disposal");
        try { await first.WaitAsync(TestTimeout.Hang); } catch (PgClientClosedException) { }
    }

    // A sync-at-bind body parked between command results still owns a continuation after close becomes
    // consumer-terminal. Dispose must drive that continuation before returning or surfacing the close.
    [TestMethod]
    public async Task SyncFlow_CloseAtInterResultPark_DisposeRetainsDriveObligation()
    {
        var clock = new FakeTimeProvider();
        var transport = new GatedReplayTransport(_handshake!);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(_options!)
        {
            TimeProvider = clock,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            CompletionTimeout = TimeSpan.FromSeconds(30),
        });
        await protocol.StartAsync(_options!, transport);

        var readParked = transport.ArmReadPark();
        var flow = new CommandFlow(async: false,
            Command.Create("select 1"), Command.Create("select 2"), Command.Create("select 3"));
        Assert.IsTrue(protocol.TryQueue(flow));
        var enumerator = flow.GetEnumerator();
        var first = Task.Factory.StartNew(enumerator.MoveNext, CancellationToken.None,
            TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await readParked.WaitAsync(TestTimeout.Hang);

        var release = MessagesThroughFirstCommandComplete(_multiMessages!);
        for (var i = 0; i < release; i++)
            transport.ReleaseSegment(_multiMessages![i]);
        Assert.IsTrue(await first.WaitAsync(TestTimeout.Hang), "first result was not delivered");

        var complete = protocol.CompleteAsync();
        clock.Advance(TimeSpan.FromSeconds(1));
        for (var i = release; i < _multiMessages!.Count; i++)
            transport.ReleaseSegment(_multiMessages[i]);

        var dispose = StartSyncDispose(enumerator, out var started);
        using (started)
            Assert.IsTrue(started.Wait(TestTimeout.Hang), "synchronous disposer did not start");
        var escaped = await dispose.WaitAsync(TestTimeout.Hang);
        await complete.WaitAsync(TestTimeout.Hang);
        Assert.IsNull(escaped, $"{escaped} escaped synchronous disposal");
    }

    // Ordering 3 (gate-fault) - the predicted lost-completion HANG is NOT reachable: the heartbeat-
    // faulted gate makes HandleException no-op on the consumed V0 generation, but the same gate-fault
    // published CancelException, so the consumer's next MoveNextAsync self-delivers the close and the
    // loop converges. Asserts convergence (a hang here would regress that protection).
    [TestMethod]
    public async Task Ordering3_GateFaultNoOp_SelfDeliverConverges()
    {
        await using var s = await BuildToFirstResultParked();
        var consumer = new GranularConsumer(s.Enumerator);

        var complete = s.Protocol.CompleteAsync();

        s.Heartbeat();
        await SettleAsync();

        var loop = consumer.RunLoop();
        await loop;

        _ = s.Protocol.DisposeAsync();
        s.Clock.Advance(TimeSpan.FromSeconds(120));
        try { await complete; } catch { }
        try { await consumer.RunDispose(); } catch { }

    }

    // Ordering 3 (read-fault) - the no-op never happens: with the body on its drain read, a forceful
    // abort lands HandleException on the LIVE (Reset) generation, because every body read in this flow
    // is preceded by a consumer Reset. Documents why the read-fault alone cannot produce the hang.
    [TestMethod]
    public async Task Ordering3_ReadFaultPath_NeverNoOps_Converges()
    {
        await using var s = await BuildToFirstResultParked();
        var consumer = new GranularConsumer(s.Enumerator);

        var bodyReParked = s.Transport.ArmReadPark();
        var loop = consumer.RunLoop();
        await bodyReParked;

        _ = s.Protocol.DisposeAsync();

        await loop;

        s.Clock.Advance(TimeSpan.FromSeconds(120));
        try { await consumer.RunDispose(); } catch { }

    }

    // Multi-command convergence under graceful close while the body is parked on the INTER-RESULT gate
    // (between command 1 and 2), with no heartbeat tick and no forceful abort. Asserts the consumer
    // converges (drains command 2/3 to a clean close). NOTE: several backstops can drive convergence
    // here (the consumer gate-open, the pipeline shutdown drain, and - in sync-continuation builds - the
    // inline ping-pong), so this is a convergence regression test, not an isolation of any one waker. The
    // never-started lost-completion (the actual reported hang) is gated by the stress repro + dump.
    [TestMethod]
    public async Task MultiCommand_GracefulCloseAtInterResultGate_Converges()
    {
        await using var s = await BuildMultiToFirstResultParked();
        var consumer = new GranularConsumer(s.Enumerator);

        // Graceful StoppingToken only - deliberately no heartbeat, no forceful DisposeAsync.
        var complete = s.Protocol.CompleteAsync();
        await SettleAsync();

        // Drive the consumer loop: its next MoveNextAsync must open the gate-parked body. Release the
        // held command 2/3 bytes so the woken body can drain to RFQ naturally (graceful path).
        var loop = consumer.RunLoop();
        for (var i = 0; i < s.Messages.Count; i++)
            s.Transport.ReleaseSegment(s.Messages[i]);

        await loop;

        s.Clock.Advance(TimeSpan.FromSeconds(120));
        try { await complete; } catch { }
        try { await consumer.RunDispose(); } catch { }

    }

    // Point-C outcome (deterministic replacement for ProtocolCompletionTests.DisposeAfterFirstResult_-
    // NextMoveNextSurfacesClosedException). Result 1 delivered, body parked on the inter-result gate; a
    // forceful teardown fires AbortToken and one heartbeat tick faults that gate BEFORE the consumer's
    // next MoveNextAsync (the live test's Task.Delay(50) window, made deterministic via the
    // FakeTimeProvider). The body's HandleException then no-ops on the already-consumed result-1
    // generation; the next MoveNextAsync must self-deliver the close (or complete) and NEVER re-yield
    // the stale result 1. Ordering3_GateFaultNoOp asserts the read loop converges; a stale re-yield does
    // not hang, so this pins the specific next-call outcome the loop cannot catch.
    [TestMethod]
    public async Task GateFaultBeforeNextMoveNext_SelfDeliversClose_NeverReYieldsStale()
    {
        await using var s = await BuildToFirstResultParked();

        var dispose = s.Protocol.DisposeAsync().AsTask();
        s.Heartbeat();
        await SettleAsync();

        bool more;
        try
        {
            more = await s.Enumerator.MoveNextAsync().AsTask();
        }
        catch (PgClientClosedException)
        {
            more = false;
        }
        Assert.IsFalse(more,
            "the next MoveNextAsync re-yielded the stale result 1 instead of surfacing close/complete");

        s.Clock.Advance(TimeSpan.FromSeconds(120));
        try { await dispose; } catch { }
        try { await s.Enumerator.DisposeAsync(); } catch { }
    }

    static async Task SettleAsync()
    {
        for (var i = 0; i < 16; i++)
            await Task.Yield();
    }

    // ---------------------------------------------------------------------------------------------
    // Capture machinery (shared shape with StoppingTokenInMemoryTests).
    // ---------------------------------------------------------------------------------------------
    static async Task<(byte[] handshake, byte[] response)> CaptureAsync(PgClientOptions options, params Command[] commands)
    {
        var sock = options.EndPoint is UnixDomainSocketEndPoint
            ? new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified)
            : new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        await sock.ConnectAsync(options.EndPoint);
        var recStream = new RecordingStream(new NetworkStream(sock, ownsSocket: true));
        var transport = new StreamTransport(PipeReader.Create(recStream), PipeWriter.Create(recStream));

        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);
        var handshakeLen = recStream.RecordedLength;

        var flow = new CommandFlow(async: true, commands);
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync())
        {
            await foreach (var _ in e.Current) { }
        }
        await e.DisposeAsync();
        var afterFlow = recStream.RecordedLength;

        var full = recStream.Snapshot();
        await protocol.CompleteAsync();

        var handshake = StartupTranscript.MakeReplayable(full.AsSpan(0, handshakeLen));
        var response = full.AsSpan(handshakeLen, afterFlow - handshakeLen).ToArray();
        Assert.IsTrue(response.Length > 0, "captured an empty response");
        return (handshake, response);
    }

    // Split a backend message stream into one segment per wire message (1 type byte + 4-byte BE
    // length; length excludes the type byte). Per-message granularity lets the test release
    // RowDescription/DataRow while holding CommandComplete+RFQ.
    static IReadOnlyList<byte[]> SplitMessages(byte[] response)
    {
        var messages = new List<byte[]>();
        var o = 0;
        while (o + 5 <= response.Length)
        {
            var len = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(response.AsSpan(o + 1, 4));
            var next = o + 1 + len;
            if (next > response.Length)
                break;
            messages.Add(response.AsSpan(o, next - o).ToArray());
            o = next;
        }
        return messages;
    }

    sealed class StreamTransport : TransportConnection
    {
        public StreamTransport(PipeReader reader, PipeWriter writer)
        {
            Reader = reader;
            Writer = writer;
        }

        public override PipeReader Reader { get; }
        public override PipeWriter Writer { get; }
        public override void WaitWritable() { }
    }

    sealed class RecordingStream : Stream
    {
        readonly Stream _inner;
        readonly ArrayBufferWriter<byte> _record = new();
        readonly object _lock = new();

        public RecordingStream(Stream inner) => _inner = inner;

        public int RecordedLength
        {
            get { lock (_lock) { return _record.WrittenCount; } }
        }

        public byte[] Snapshot()
        {
            lock (_lock) { return _record.WrittenSpan.ToArray(); }
        }

        void Record(ReadOnlySpan<byte> bytes)
        {
            if (bytes.Length == 0)
                return;
            lock (_lock)
                _record.Write(bytes);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var n = await _inner.ReadAsync(buffer, cancellationToken);
            Record(buffer.Span.Slice(0, n));
            return n;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var n = await _inner.ReadAsync(buffer, offset, count, cancellationToken);
            Record(buffer.AsSpan(offset, n));
            return n;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var n = _inner.Read(buffer, offset, count);
            Record(buffer.AsSpan(offset, n));
            return n;
        }

        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => _inner.WriteAsync(buffer, cancellationToken);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.WriteAsync(buffer, offset, count, cancellationToken);
        public override void Flush() => _inner.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void SetLength(long value) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();
        }
    }

    sealed class GatedReplayTransport : TransportConnection
    {
        readonly Pipe _toClient = new();
        readonly Pipe _toServer = new();
        readonly PipeReader _reader;
        readonly CompletingPipeWriter _writer;
        readonly ReadParkSignalingStream _readingStream;

        public override PipeReader Reader => _reader;
        public override PipeWriter Writer => _writer;
        public override void WaitWritable() { }
        public override void Abort()
        {
            try { _toClient.Writer.Complete(new IOException("The in-memory transport was aborted.")); }
            catch (InvalidOperationException) { }
        }

        public GatedReplayTransport(byte[] handshake)
        {
            _writer = new CompletingPipeWriter(_toServer.Writer);
            _readingStream = new ReadParkSignalingStream(_toClient.Reader.AsStream());
            _reader = new Slon.Pipelines.DefaultStreamPipeReader(_readingStream,
                new StreamPipeReaderOptions(bufferSize: 8192, useZeroByteReads: false),
                supportCancelPending: false);
            _toClient.Writer.WriteAsync(handshake).AsTask().GetAwaiter().GetResult();
            _ = DrainClient();
        }

        public void CompleteReadCleanly() => _toClient.Writer.Complete();

        public void CompleteWriteCleanly() => _writer.CompleteFlushes();

        public Task ArmReadPark() => _readingStream.ArmReadPark();

        public void ReleaseSegment(byte[] bytes)
        {
            try
            {
                _toClient.Writer.WriteAsync(bytes).AsTask().GetAwaiter().GetResult();
            }
            catch
            {
            }
        }

        async Task DrainClient()
        {
            try
            {
                while (true)
                {
                    var r = await _toServer.Reader.ReadAsync();
                    _toServer.Reader.AdvanceTo(r.Buffer.End);
                    if (r.IsCompleted)
                        break;
                }
            }
            catch
            {
            }
        }
    }

    sealed class CompletingPipeWriter(PipeWriter inner) : PipeWriter
    {
        int _completeFlushes;

        public void CompleteFlushes() => Volatile.Write(ref _completeFlushes, 1);
        public override void Advance(int bytes) => inner.Advance(bytes);
        public override void CancelPendingFlush() => inner.CancelPendingFlush();
        public override void Complete(Exception? exception = null) => inner.Complete(exception);
        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            => Volatile.Read(ref _completeFlushes) is not 0
                ? new(new FlushResult(isCanceled: false, isCompleted: true))
                : inner.FlushAsync(cancellationToken);
        public override long UnflushedBytes => inner.UnflushedBytes;
        public override Memory<byte> GetMemory(int sizeHint = 0) => inner.GetMemory(sizeHint);
        public override Span<byte> GetSpan(int sizeHint = 0) => inner.GetSpan(sizeHint);
    }

    sealed class ReadParkSignalingStream(Stream inner) : Stream
    {
        TaskCompletionSource? _park;

        public Task ArmReadPark()
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _park, tcs);
            return tcs.Task;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            Volatile.Read(ref _park)?.TrySetResult();
            return inner.Read(buffer, offset, count);
        }

        public override int Read(Span<byte> buffer)
        {
            Volatile.Read(ref _park)?.TrySetResult();
            return inner.Read(buffer);
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var vt = inner.ReadAsync(buffer, cancellationToken);
            if (!vt.IsCompleted)
                Volatile.Read(ref _park)?.TrySetResult();
            return vt;
        }

        public override void Flush() => inner.Flush();
        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
        public override void SetLength(long value) => inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => inner.CanSeek;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => inner.Position = value; }
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
