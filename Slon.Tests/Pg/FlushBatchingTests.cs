using System.Buffers.Binary;
using System.IO.Pipelines;
using Draghi.Pipelining;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Pins cross-flow flush batching (write coalescing). Async pipelined writes defer their flush
// (PgEncoder.CanDeferFlush) and should coalesce into one wire write until the byte threshold - a "conga
// line" of P/B/D/E/S—flushed only when the executor genuinely parks. The
// in-memory transport counts physical flushes (each non-empty FlushAsync = one wire segment). Flows park
// on their reads (no responses fed), so we measure writes only and tear the protocol down at the end.
//
// The tests pause the configured execution scheduler before submitting the measured flows. This makes
// co-queueing a property of the fixture rather than a timing assumption, so physical flush count is a
// stable assertion and production code needs no test-only park/flush counters.
[TestClass]
public class FlushBatchingTests
{
    // 8 tiny "select 1" command frames stay under the 1000-byte flush threshold, so a coalesced run is one
    // wire segment.
    const int N = 8;
    static readonly TimeSpan SettleTimeout = TimeSpan.FromSeconds(5);

    // ~28-byte trust-auth handshake: AuthenticationOk, BackendKeyData, ReadyForQuery. Lets StartAsync finish.
    static byte[] Handshake()
    {
        var b = new byte[64];
        var o = 0;
        b[o++] = (byte)'R'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 0); o += 4;
        b[o++] = (byte)'K'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 12); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 4321); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8765); o += 4;
        b[o++] = (byte)'Z'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 5); o += 4; b[o++] = (byte)'I';
        return b.AsSpan(0, o).ToArray();
    }

    static async Task<(PgClientProtocol, FlushCountingTransport, PausableScheduler)> CreateAsync()
    {
        var options = new PgClientOptions { EndPoint = TestEndPoint.Default, Username = "postgres", Password = "postgres123", Database = "postgres" };
        var transport = new FlushCountingTransport(Handshake());
        var scheduler = new PausableScheduler();
        // Long timeouts so the parked (response-less) flows aren't aborted while we measure writes.
        var protocolOptions = new PgClientProtocolOptions(options)
        {
            CompletionTimeout = TimeSpan.FromSeconds(30),
            HeartbeatInterval = TimeSpan.FromSeconds(5),
            ExecutionScheduler = scheduler
        };
        var protocol = PgClientProtocol.Create(protocolOptions);
        await protocol.StartAsync(options, transport);
        return (protocol, transport, scheduler);
    }

    static CommandFlow Cmd() => new(async: true, Command.Create("select 1"));

    static async Task WaitForBytes(FlushCountingTransport t, long target)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (Volatile.Read(ref t.Counter.FlushedBytes) < target)
        {
            if (sw.Elapsed > SettleTimeout)
                Assert.Fail($"timed out waiting for {target} flushed bytes, saw {t.Counter.FlushedBytes} in {t.Counter.FlushCount} flushes");
            await Task.Delay(5);
        }
    }

    // Queue one flow, wait for its write to reach the wire (the executor flushes on park), return its size.
    static async Task<long> WarmAndMeasure(PgClientProtocol p, FlushCountingTransport t)
    {
        var before = t.Counter.FlushedBytes;
        Assert.IsTrue(p.TryQueue(Cmd()));
        await WaitForBytes(t, before + 1);
        return t.Counter.FlushedBytes - before;
    }

    [TestMethod]
    public async Task BurstEnqueue_Coalesces()
    {
        var (p, t, scheduler) = await CreateAsync();
        try
        {
            var perCmd = await WarmAndMeasure(p, t);
            var baseBytes = t.Counter.FlushedBytes;
            var baseFlush = t.Counter.FlushCount;
            await scheduler.PauseAsync();
            try
            {
                for (var i = 0; i < N; i++)
                    Assert.IsTrue(p.TryQueue(Cmd(), mustPipeline: true));
            }
            finally { scheduler.Resume(); }
            await WaitForBytes(t, baseBytes + N * perCmd);
            var flushes = t.Counter.FlushCount - baseFlush;
            Assert.AreEqual(1, flushes,
                $"a deterministically co-queued burst of {N} sub-threshold commands produced {flushes} wire segments");
        }
        finally
        {
            scheduler.Resume();
            await p.DisposeAsync();
        }
    }

    [TestMethod]
    public async Task ConcurrentProducers_WhenCoQueued_Coalesce()
    {
        var (p, t, scheduler) = await CreateAsync();
        try
        {
            var perCmd = await WarmAndMeasure(p, t);
            var baseBytes = t.Counter.FlushedBytes;
            var baseFlush = t.Counter.FlushCount;
            await scheduler.PauseAsync();
            try
            {
                await Task.WhenAll(Enumerable.Range(0, N).Select(_ => Task.Run(
                    () => Assert.IsTrue(p.TryQueue(Cmd(), mustPipeline: true)))));
            }
            finally { scheduler.Resume(); }
            await WaitForBytes(t, baseBytes + N * perCmd);
            var flushes = t.Counter.FlushCount - baseFlush;
            Assert.AreEqual(1, flushes,
                $"{N} concurrent producers, co-queued before execution, produced {flushes} wire segments");
        }
        finally
        {
            scheduler.Resume();
            await p.DisposeAsync();
        }
    }

    [TestMethod]
    public async Task NonPipelinedSuccessor_FlushesPipelinedPredecessor()
    {
        var (p, t, scheduler) = await CreateAsync();
        try
        {
            var baseFlush = t.Counter.FlushCount;
            await scheduler.PauseAsync();
            Assert.IsTrue(p.TryQueue(Cmd()));
            _ = p.BeginExclusiveScope(async: true);

            scheduler.Resume();
            await WaitForBytes(t, 1);
            Assert.AreEqual(1, t.Counter.FlushCount - baseFlush,
                "the exclusive activation boundary did not flush its pipelined predecessor");
        }
        finally
        {
            scheduler.Resume();
            await p.DisposeAsync();
        }
    }

    // An arrival after the executor has decided to perform its idle flush is a new batch, even if it
    // reaches the queue before WaitCore rechecks it. Pin that boundary explicitly instead of folding
    // this legitimate race into the deterministic coalescing assertions above.
    [TestMethod]
    public async Task EnqueueDuringIdleFlush_FormsNextBatch()
    {
        var (p, t, scheduler) = await CreateAsync();
        try
        {
            var perCmd = await WarmAndMeasure(p, t);
            var baseBytes = t.Counter.FlushedBytes;
            var baseFlush = t.Counter.FlushCount;
            var racedEnqueue = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            t.Counter.RunBeforeNextNonEmptyFlush(
                () => racedEnqueue.TrySetResult(p.TryQueue(Cmd(), mustPipeline: true)));
            Assert.IsTrue(p.TryQueue(Cmd(), mustPipeline: true));
            Assert.IsTrue(await racedEnqueue.Task.WaitAsync(SettleTimeout));
            await WaitForBytes(t, baseBytes + 2 * perCmd);
            Assert.AreEqual(2, t.Counter.FlushCount - baseFlush);
        }
        finally
        {
            scheduler.Resume();
            await p.DisposeAsync();
        }
    }

    sealed class PausableScheduler : PipelineScheduler
    {
        readonly object _sync = new();
        readonly Queue<Work> _held = new();
        TaskCompletionSource? _idle;
        bool _paused;
        int _running;

        public Task PauseAsync()
        {
            lock (_sync)
            {
                _paused = true;
                return _running is 0
                    ? Task.CompletedTask
                    : (_idle ??= new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
            }
        }

        public void Resume()
        {
            Work[] held;
            lock (_sync)
            {
                _paused = false;
                held = _held.ToArray();
                _held.Clear();
            }
            foreach (var work in held)
                SubmitDetached(work.Action, work.State, work.PreferLocal);
        }

        public override void SubmitDetached(Action<object?> action, object? state, bool preferLocal = true)
        {
            var work = new Work(action, state, preferLocal);
            lock (_sync)
            {
                if (_paused)
                {
                    _held.Enqueue(work);
                    return;
                }
                _running++;
            }
            PipelineScheduler.ThreadPool.SubmitDetached(static state => ((Dispatch)state!).Run(), new Dispatch(this, work), preferLocal);
        }

        void Finished()
        {
            TaskCompletionSource? idle = null;
            lock (_sync)
            {
                if (--_running is 0)
                {
                    idle = _idle;
                    _idle = null;
                }
            }
            idle?.TrySetResult();
        }

        readonly record struct Work(Action<object?> Action, object? State, bool PreferLocal);

        sealed class Dispatch(PausableScheduler scheduler, Work work)
        {
            public void Run()
            {
                try { work.Action(work.State); }
                finally { scheduler.Finished(); }
            }
        }
    }

    sealed class FlushCountingTransport : TransportConnection
    {
        readonly Pipe _toClient = new();
        readonly Pipe _toServer = new(new PipeOptions(pauseWriterThreshold: 1 << 30, resumeWriterThreshold: 1 << 29));
        public readonly CountingWriter Counter;
        public override PipeReader Reader => _toClient.Reader;
        public override PipeWriter Writer => Counter;
        public override void WaitWritable() { }

        public FlushCountingTransport(byte[] handshake)
        {
            Counter = new CountingWriter(_toServer.Writer);
            _toClient.Writer.WriteAsync(handshake).AsTask().GetAwaiter().GetResult();
        }

        // Counts a wire segment per non-empty FlushAsync. The protocol defers flushes (returns no flush);
        // a real flush flows through here, so FlushCount == number of physical writes to the wire.
        public sealed class CountingWriter(PipeWriter inner) : PipeWriter
        {
            long _unflushed;
            Action? _beforeNextNonEmptyFlush;
            public int FlushCount;
            public long FlushedBytes;
            public void RunBeforeNextNonEmptyFlush(Action action) => Volatile.Write(ref _beforeNextNonEmptyFlush, action);
            // The protocol reads UnflushedBytes for the flush-threshold gate; the base PipeWriter throws
            // unless these delegate to the real (unflushed-tracking) pipe writer.
            public override bool CanGetUnflushedBytes => inner.CanGetUnflushedBytes;
            public override long UnflushedBytes => inner.UnflushedBytes;
            public override void Advance(int bytes) { _unflushed += bytes; inner.Advance(bytes); }
            public override Memory<byte> GetMemory(int sizeHint = 0) => inner.GetMemory(sizeHint);
            public override Span<byte> GetSpan(int sizeHint = 0) => inner.GetSpan(sizeHint);
            public override void CancelPendingFlush() => inner.CancelPendingFlush();
            public override void Complete(Exception? exception = null) => inner.Complete(exception);
            public override ValueTask CompleteAsync(Exception? exception = null) => inner.CompleteAsync(exception);
            public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                if (_unflushed > 0)
                {
                    Interlocked.Exchange(ref _beforeNextNonEmptyFlush, null)?.Invoke();
                    FlushCount++;
                    Volatile.Write(ref FlushedBytes, FlushedBytes + _unflushed);
                    _unflushed = 0;
                }
                return inner.FlushAsync(cancellationToken);
            }
        }
    }
}
