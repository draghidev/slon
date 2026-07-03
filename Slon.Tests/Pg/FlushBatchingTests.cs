using System.Buffers.Binary;
using System.IO.Pipelines;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Pins cross-flow flush batching (write coalescing). Async pipelined writes defer their flush
// (PgEncoder.CanDelayFlush) and should coalesce into one wire write until the byte threshold - a "conga
// line" of P/B/D/E/S - flushed only when the executor genuinely parks (FlushGate.FlushBeforePark). The
// in-memory transport counts physical flushes (each non-empty FlushAsync = one wire segment). Flows park
// on their reads (no responses fed), so we measure writes only and tear the protocol down at the end.
//
// Two feed shapes: a synchronous burst (all N co-queued before the executor drains -> should be 1 segment)
// and concurrent producers racing the executor (the multiplexed-pool condition). The contrast tells us
// whether the mechanism works and the suite just isn't densely pipelined, or the coalescing itself is off.
[TestClass]
[DoNotParallelize]
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

    static async Task<(PgClientProtocol, FlushCountingTransport)> CreateAsync()
    {
        var options = new PgClientOptions { EndPoint = TestEndPoint.Default, Username = "postgres", Password = "postgres123", Database = "postgres" };
        var transport = new FlushCountingTransport(Handshake());
        // Long timeouts so the parked (response-less) flows aren't aborted while we measure writes.
        var protocolOptions = new PgClientProtocolOptions(options) { CompletionTimeout = TimeSpan.FromSeconds(30), HeartbeatInterval = TimeSpan.FromSeconds(5) };
        var protocol = PgClientProtocol.Create(protocolOptions);
        await protocol.StartAsync(options, transport);
        return (protocol, transport);
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
        var (p, t) = await CreateAsync();
        try
        {
            var perCmd = await WarmAndMeasure(p, t);
            var baseBytes = t.Counter.FlushedBytes;
            var baseFlush = t.Counter.FlushCount;
            for (var i = 0; i < N; i++)
                Assert.IsTrue(p.TryQueue(Cmd()));               // co-queued before the executor drains
            await WaitForBytes(t, baseBytes + N * perCmd);
            var flushes = t.Counter.FlushCount - baseFlush;
            Assert.IsTrue(flushes <= 2, $"a co-queued burst of {N} sub-threshold commands should coalesce to ~1 wire segment, saw {flushes}");
        }
        finally { await p.DisposeAsync(); }
    }

    [TestMethod, Ignore]
    public async Task ConcurrentProducers_Coalesce()
    {
        var (p, t) = await CreateAsync();
        try
        {
            var perCmd = await WarmAndMeasure(p, t);
            var baseBytes = t.Counter.FlushedBytes;
            var baseFlush = t.Counter.FlushCount;
            // N producers racing the executor - the multiplexed-pool condition (flows arrive from separate
            // callers onto one wire). If the pre-park flush fires on a TryGetNext miss that immediately
            // finds an item on the WaitCore retry, each lands as its own segment.
            await Task.WhenAll(Enumerable.Range(0, N).Select(_ => Task.Run(() => Assert.IsTrue(p.TryQueue(Cmd())))));
            await WaitForBytes(t, baseBytes + N * perCmd);
            var flushes = t.Counter.FlushCount - baseFlush;
            Assert.IsTrue(flushes <= 3, $"concurrent producers of {N} sub-threshold commands should still mostly coalesce, saw {flushes} segments (one-per-command = the pre-park flush firing before the has-item recheck)");
        }
        finally { await p.DisposeAsync(); }
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
            public int FlushCount;
            public long FlushedBytes;
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
                    FlushCount++;
                    Volatile.Write(ref FlushedBytes, FlushedBytes + _unflushed);
                    _unflushed = 0;
                }
                return inner.FlushAsync(cancellationToken);
            }
        }
    }
}
