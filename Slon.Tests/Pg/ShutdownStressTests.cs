using System.Buffers.Binary;
using System.IO.Pipelines;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Hammers the dispatch-vs-shutdown race that surfaced a NullReferenceException instead of
// PgClientClosedException (the DrainInertItems torn-read in PgClientProtocol.Shutdown). A real
// connection per iteration saturates the server before the rare interleaving surfaces, so we
// simulate the wire: an in-memory transport replays only the startup handshake, command reads
// park (writer left open so reads block like an idle socket), and a tiny CompletionTimeout
// escalates to AbortToken to unblock them. The race is dispatch (executor dequeue) vs shutdown
// (Complete + inert-queue drain), independent of any query response. Scenario A pipelines two
// flows, scenario B is the single-flow graceful->abort path.
//
// Override count via SLON_STRESS_ITERATIONS (default 500). On any non-PgClientClosedException the
// full exception and stack are surfaced. WaitAsync caps turn a hang into a reported failure.
[TestClass]
[DoNotParallelize]
public class ShutdownStressTests
{
    // In-memory ReplayTransport (no Postgres), but each iteration spins up a protocol with a 5ms heartbeat
    // timer; those accumulate super-linearly past ~20k (timer/GC pressure), so cap there. SLON_UNCAPPED=1
    // drives the raw value for a deliberate soak (expect non-linear cost at very high counts).
    static int Iterations => StressEnv.Iterations(fallback: 500, cap: 20_000);

    static readonly TimeSpan Cap = TimeSpan.FromSeconds(10);

    // ~28-byte trust-auth startup handshake: AuthenticationOk 'R'/8/0, BackendKeyData 'K'/12/pid/key,
    // ReadyForQuery 'Z'/5/'I'. Lets StartAsync complete with no server.
    static byte[] Handshake()
    {
        var b = new byte[64];
        int o = 0;
        b[o++] = (byte)'R'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 0); o += 4;
        b[o++] = (byte)'K'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 12); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 4321); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8765); o += 4;
        b[o++] = (byte)'Z'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 5); o += 4; b[o++] = (byte)'I';
        return b.AsSpan(0, o).ToArray();
    }

    // ----- Scenario A: two pipelined flows, dispatch-vs-shutdown race -----
    //
    // Handshake-only replay: both flows' command reads park (no response in the pipe), so the race
    // under test is purely dispatch (executor dequeue) vs shutdown (Complete + the inert-queue
    // drain). A short CompletionTimeout escalates to AbortToken, and a short HeartbeatInterval
    // propagates it to the parked reads fast: parked-flow abort propagation is heartbeat-driven, so
    // the default 1s interval would otherwise leave each iteration parked up to a second (the suite's
    // residual ~1s variance). Faithfully reproducing the post-response pre-deliver path would
    // need wire latency we can't control per iteration; instant-replay there just desyncs the
    // pipelined decoder (a harness artifact, not the product fault). The torn-read NRE lives in the
    // dispatch/inert-drain coordination, which this exercises directly.

    [TestMethod]
    public async Task Stress_PipelinedFlows_StoppingTokenRace_NoNre()
    {
        var options = PgTestPool.NewOptions();
        var handshake = Handshake();

        await RunIterationsAsync(async i =>
        {
            var protocolOptions = new PgClientProtocolOptions(options) { CompletionTimeout = TimeSpan.FromMilliseconds(2), HeartbeatInterval = TimeSpan.FromMilliseconds(5) };
            var protocol = PgClientProtocol.Create(protocolOptions);
            await protocol.StartAsync(options, new ReplayTransport(handshake));
            await RunIterationAsync(i, async () =>
            {
                var flowA = new CommandFlow(async: true,
                    Command.Create("select generate_series(1, 20)"),
                    Command.Create("select 'a-two'"));
                var flowB = new CommandFlow(async: true, Command.Create("select 'b'"));
                Assert.IsTrue(protocol.TryQueue(flowA));
                Assert.IsTrue(protocol.TryQueue(flowB));

                var eA = flowA.GetAsyncEnumerator();
                var eB = flowB.GetAsyncEnumerator();

                var aFirstTask = eA.MoveNextAsync().AsTask();
                var completeTask = protocol.CompleteAsync();

                await aFirstTask.WaitAsync(Cap);
                await eB.MoveNextAsync().AsTask().WaitAsync(Cap);
                await eA.DisposeAsync();
                await eB.DisposeAsync();
                await completeTask.WaitAsync(Cap);
            });
            try { await protocol.CompleteAsync().WaitAsync(Cap); }
            catch { }
        });
    }

    // ----- Scenario B: graceful->abort, handshake-only (parked read) -----

    [TestMethod]
    public async Task Stress_GracefulEscalatesToAbort_NoNre()
    {
        var options = PgTestPool.NewOptions();
        var handshake = Handshake();

        await RunIterationsAsync(async i =>
        {
            var protocolOptions = new PgClientProtocolOptions(options) { CompletionTimeout = TimeSpan.FromMilliseconds(2), HeartbeatInterval = TimeSpan.FromMilliseconds(5) };
            var protocol = PgClientProtocol.Create(protocolOptions);
            await protocol.StartAsync(options, new ReplayTransport(handshake));
            await RunIterationAsync(i, async () =>
            {
                var flow = new CommandFlow(async: true, Command.Create("select 1"));
                Assert.IsTrue(protocol.TryQueue(flow));

                var e = flow.GetAsyncEnumerator();
                var moveNextTask = e.MoveNextAsync().AsTask();
                var completeTask = protocol.CompleteAsync();

                await moveNextTask.WaitAsync(Cap);
                await e.DisposeAsync();
                await completeTask.WaitAsync(Cap);
            });
            try { await protocol.CompleteAsync().WaitAsync(Cap); }
            catch { }
        });
    }

    // ----- Scenario C: exclusive-scope holder + pre-turn waiter vs stop -----
    //
    // Hammers the verdict-delivery race for a scope waiter that never won its turn: A holds the
    // scope (activated, idle), B is enqueued behind it, and the protocol stops without either scope
    // ending. B's HandoffReady must fault with PgClientClosedException - the close verdict has three
    // racing deliverers (the inert-queue drain, the heartbeat's cascade hooks, and the residual
    // pipeline drain) plus one wrong one (A's teardown retiring normally and advancing the executor
    // into B before the source's completion cutoff is visible). Each failure mode is classified:
    // a successful HandoffReady means B was PROMOTED onto a stopping protocol, a timeout means the
    // verdict was never delivered (B completed by the residual drain, which faults no caller gate).
    // Iteration parity alternates forceful DisposeAsync with graceful CompleteAsync; the pre-stop
    // delay sweeps the stop across the 5ms heartbeat phase, since the wrong-deliverer window opens
    // only when a tick lands between the close verdict firing and the source cutoff being installed.

    [TestMethod]
    public async Task Stress_PreTurnScopeWaiter_StopDeliversVerdict()
    {
        var options = PgTestPool.NewOptions();
        var handshake = Handshake();

        await RunIterationsAsync(async i =>
        {
            var protocolOptions = new PgClientProtocolOptions(options) { CompletionTimeout = TimeSpan.FromMilliseconds(2), HeartbeatInterval = TimeSpan.FromMilliseconds(5) };
            var protocol = PgClientProtocol.Create(protocolOptions);
            await protocol.StartAsync(options, new ReplayTransport(handshake));
            await RunIterationAsync(i, async () =>
            {
                var a = protocol.BeginExclusiveScope(async: true);
                try
                {
                    await a.HandoffReady.WaitAsync(Cap);
                }
                catch (TimeoutException)
                {
                    // Same self-classification as B's verdict wait: pin which population the holder
                    // died in before any stop was even requested.
                    Assert.Fail($"iter {i}: holder scope A STRANDED pre-handoff within {Cap} " +
                        $"[a: pending={a.IsPending} started={a.IsStarted} completed={a.IsCompleted}, " +
                        $"protocol: draining={protocol.IsDraining} completed={protocol.IsCompleted}]\n" +
                        $"{ProtocolDiag.Gauges(protocol)}\nsource: {ProtocolDiag.SourceState(protocol)}");
                }
                var b = protocol.BeginExclusiveScope(async: true);
                var bWait = b.HandoffReady;

                if (i % 7 != 0)
                    await Task.Delay(i % 7);

                if (i % 2 == 0)
                    await protocol.DisposeAsync();
                else
                    _ = protocol.CompleteAsync();

                try
                {
                    await bWait.WaitAsync(Cap);
                    Assert.Fail($"iter {i}: pre-turn waiter PROMOTED - HandoffReady resolved successfully on a stopping protocol");
                }
                catch (TimeoutException)
                {
                    // Self-classifying: pin which population the waiter died in - inert-in-queue
                    // (pending, sweep never ran), dispatched-never-completed (started, heartbeat
                    // missed), or completed-verdict-less (backstop hole).
                    Assert.Fail($"iter {i}: pre-turn waiter STRANDED - no verdict on HandoffReady within {Cap} " +
                        $"[b: pending={b.IsPending} started={b.IsStarted} completed={b.IsCompleted}, " +
                        $"protocol: draining={protocol.IsDraining} completed={protocol.IsCompleted}]\n" +
                        $"{ProtocolDiag.Gauges(protocol)}\nsource: {ProtocolDiag.SourceState(protocol)}");
                }
                catch (PgClientClosedException)
                {
                }

                // The latch contract: Completed is only guaranteed once the drain is awaited
                // (DisposeAsync is fire-and-forget); a false read after this await is a real latch bug.
                await ProtocolDiag.WhenAllOrDump(protocol, $"iter {i}: awaited drain did not converge", Cap,
                    ("complete", protocol.CompleteAsync()));
                Assert.IsTrue(protocol.IsCompleted, $"iter {i}: protocol not Completed after awaited drain");
            });
            try { await protocol.CompleteAsync().WaitAsync(Cap); }
            catch { }
        });
    }

    // The whole point is to catch a NullReferenceException (or any non-close fault) leaking from
    // the shutdown race. Every await inside an iteration may legitimately surface
    // PgClientClosedException (the protocol is shutting down), at any of several sites, so we
    // tolerate it globally and fail only on anything else.
    // Iterations are independent (a fresh protocol + transport each) and the per-iteration latency is
    // dominated by the abort escalation + heartbeat propagation that unblocks the parked reads (both
    // kept to a few ms) - a timer wait, not CPU. Running them with bounded concurrency overlaps those
    // waits, cutting wall-clock by ~DOP
    // without lowering the iteration count (the race coverage), and the added scheduling pressure
    // widens the dispatch-vs-shutdown interleavings the stress is hunting.
    static async Task RunIterationsAsync(Func<int, Task> iteration)
    {
        var dop = Math.Min(Iterations, Math.Max(32, Environment.ProcessorCount * 12));
        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = dop };
        await Parallel.ForEachAsync(Enumerable.Range(0, Iterations), parallelOptions, async (i, _) => await iteration(i));
    }

    static async Task RunIterationAsync(int iter, Func<Task> body)
    {
        try { await body(); }
        catch (PgClientClosedException) { }
        catch (Exception ex)
        {
            Assert.Fail($"iter {iter}: expected only PgClientClosedException but got {ex.GetType().FullName}: {ex.Message}\n{ex}");
        }
    }

    // In-memory transport: the read pipe is pre-filled with canned server bytes and the writer is
    // left OPEN so reads past the canned data park like an idle socket (rather than seeing EOF and
    // faulting as a closed connection). The write pipe has a huge pause threshold so the protocol's
    // flushes never block and need no draining consumer.
    sealed class ReplayTransport : TransportConnection
    {
        readonly Pipe _toClient = new();
        readonly Pipe _toServer = new(new PipeOptions(pauseWriterThreshold: 1 << 30, resumeWriterThreshold: 1 << 29));
        public override PipeReader Reader => _toClient.Reader;
        public override PipeWriter Writer => _toServer.Writer;
        public override void WaitWritable() { }
        public ReplayTransport(byte[] canned)
        {
            _toClient.Writer.WriteAsync(canned).AsTask().GetAwaiter().GetResult();
            // Intentionally NOT completed: reads past the canned bytes block (idle socket), they
            // do not surface EOF.
        }
    }
}
