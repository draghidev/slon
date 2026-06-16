using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Reproduces the StoppingToken-pipelined NRE in the post-response -> pre-deliver window,
// deterministically. Server bytes are captured once from a real connection and replayed with the
// handshake pre-filled but response segments released on demand, while a FakeTimeProvider fires the
// heartbeat and abort escalation at chosen instants. This pins the interleaving rather than sampling
// it: flowA parks on its read, graceful shutdown sets StoppingToken (deferring AbortToken, so the read
// isn't cancelled), the heartbeat faults sibling flowB out of order, flowB completes before the
// still-reading head flowA, and the Pipeline clears _activatedItem on that non-head completion -
// nulling flowA's live slot so its woken read derefs null.
//
// Regression test for the depth-0 fix (the Pipeline clears _activatedItem only when completion drains to empty).
[TestClass]
[DoNotParallelize]
public class StoppingTokenInMemoryRace
{
    static readonly TimeSpan Cap = TimeSpan.FromSeconds(10);

    static readonly object _logLock = new();
    // Off by default; set SLON_INMEM_LOG to capture the phase timeline for this timing-sensitive race.
    static readonly bool _logEnabled = Environment.GetEnvironmentVariable("SLON_INMEM_LOG") is not null;

    static void Log(string phase)
    {
        if (!_logEnabled)
            return;
        lock (_logLock)
            System.IO.File.AppendAllText("/tmp/inmem_phase.txt", $"{DateTime.Now:HH:mm:ss.fff} {phase}\n");
    }

    // Repeats per ordering variant. Override with SLON_INMEM_REPEATS for stress.
    static int Repeats
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("SLON_INMEM_REPEATS");
            return int.TryParse(raw, out var n) && n > 0 ? n : 3;
        }
    }

    [TestMethod]
    public async Task Deterministic_StoppingTokenRace_WithResponse_NoNre()
    {
        var options = PgTestPool.NewOptions();
        var (handshake, segments) = await CaptureAsync(options);

        string? failure = null;
        void Fail(string m) => Interlocked.CompareExchange(ref failure, m, null);

        // Ordering variants over the four controlled events. Each pins a different interleaving of
        // the heartbeat tick and flowA's byte release relative to the StoppingToken (CompleteAsync).
        foreach (var variant in new[] { Variant.HeartbeatThenRelease, Variant.ReleaseThenHeartbeat, Variant.ReleaseNoHeartbeat, Variant.HeartbeatConcurrentRelease })
        {
            for (var rep = 0; rep < Repeats && failure is null; rep++)
            {
                var msg = await RunOne(options, handshake, segments, variant, rep);
                if (msg is not null)
                {
                    Fail($"[{variant}] rep {rep}: {msg}");
                    break;
                }
            }
        }

        if (failure is not null)
            Assert.Fail(failure);
    }

    enum Variant
    {
        HeartbeatThenRelease,
        ReleaseThenHeartbeat,
        ReleaseNoHeartbeat,
        HeartbeatConcurrentRelease,
    }

    // Returns null on a clean (close-or-deliver) outcome, or a failure message (with full stack) on
    // any non-PgClientClosedException - the NRE we are hunting.
    static async Task<string?> RunOne(PgClientOptions options, byte[] handshake, IReadOnlyList<byte[]> segments, Variant variant, int rep)
    {
        var fake = new FakeTimeProvider();
        var transport = new GatedReplayTransport(handshake);
        var protocolOptions = new PgClientProtocolOptions(options)
        {
            TimeProvider = fake,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            CompletionTimeout = TimeSpan.FromSeconds(30),
        };
        var protocol = PgClientProtocol.Create(protocolOptions);
        Log($"{variant} rep{rep} start");
        await protocol.StartAsync(options, transport);
        Log($"{variant} rep{rep} started");

        var flowA = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 20)"),
            Command.Create("select 'a-two'"));
        var flowB = new CommandFlow(async: true, Command.Create("select 'b'"));
        Assert.IsTrue(protocol.TryQueue(flowA));
        Assert.IsTrue(protocol.TryQueue(flowB));

        var eA = flowA.GetAsyncEnumerator();
        var eB = flowB.GetAsyncEnumerator();

        // Fire A's first MoveNext. A's command bytes don't flush until CompleteAsync arms the flush
        // gate (the pipelined-write seam), so the body can't reach its read-park until then.
        var aFirstTask = eA.MoveNextAsync().AsTask();

        // StoppingToken + flush. Side effect: A's command bytes hit the wire; the body then parks on
        // its response read.
        var completeTask = protocol.CompleteAsync().AsTask();
        Log($"{variant} rep{rep} completeAsync called, waiting quiesce");
        await transport.WaitClientQuiesceAsync();
        Log($"{variant} rep{rep} quiesced");

        string? captured = null;
        void Catch(string where, Exception ex)
        {
            if (ex is PgClientClosedException)
                return;
            captured ??= $"{where} unexpected {ex.GetType().Name}:\n{ex}";
        }

        // The pinned interleaving. ReleaseSegment(0) delivers flowA's first CommandResult; advancing
        // the fake clock by the heartbeat interval fires OnStopping (faults flowB, runs the decoder
        // heartbeat) - the perturbation that may tear the shared decoder/gate/ReadState flowA reads.
        switch (variant)
        {
            case Variant.HeartbeatThenRelease:
                fake.Advance(TimeSpan.FromSeconds(1));
                await SettleAsync();
                transport.ReleaseSegment(segments[0]);
                break;
            case Variant.ReleaseThenHeartbeat:
                transport.ReleaseSegment(segments[0]);
                fake.Advance(TimeSpan.FromSeconds(1));
                break;
            case Variant.ReleaseNoHeartbeat:
                transport.ReleaseSegment(segments[0]);
                break;
            case Variant.HeartbeatConcurrentRelease:
                var hb = Task.Run(() => fake.Advance(TimeSpan.FromSeconds(1)));
                var rel = Task.Run(() => transport.ReleaseSegment(segments[0]));
                await Task.WhenAll(hb, rel);
                break;
        }

        Log($"{variant} rep{rep} switch done, awaiting aFirst");
        try { await aFirstTask.WaitAsync(Cap); } catch (Exception ex) { Catch("aFirst", ex); }
        Log($"{variant} rep{rep} aFirst settled");

        // Release the remaining segments and escalate the abort so the protocol drains to a clean
        // exit regardless of how the race resolved.
        for (var i = 1; i < segments.Count; i++)
            transport.ReleaseSegment(segments[i]);
        fake.Advance(TimeSpan.FromSeconds(31));
        Log($"{variant} rep{rep} abort escalated");

        try { await eB.MoveNextAsync().AsTask().WaitAsync(Cap); } catch (Exception ex) { Catch("eB", ex); }
        Log($"{variant} rep{rep} eB settled");
        try { await eA.DisposeAsync(); } catch (Exception ex) { Catch("eA dispose", ex); }
        try { await eB.DisposeAsync(); } catch (Exception ex) { Catch("eB dispose", ex); }
        Log($"{variant} rep{rep} disposed");
        try { await completeTask.WaitAsync(Cap); } catch (Exception ex) { Catch("complete", ex); }
        Log($"{variant} rep{rep} complete settled -> {(captured is null ? "clean" : "FAILURE")}");

        return captured;

        static async Task SettleAsync()
        {
            // Let the heartbeat's OnStopping actions (scheduled off the PeriodicTimer continuation)
            // run to completion before the next controlled step.
            for (var i = 0; i < 5; i++)
                await Task.Yield();
            await Task.Delay(2);
        }
    }

    // Capture the handshake + RFQ-delimited response segments for flowA(2 cmds)+flowB(1 cmd) from ONE
    // real connection.
    static async Task<(byte[] handshake, IReadOnlyList<byte[]> segments)> CaptureAsync(PgClientOptions options)
    {
        Log("capture: connecting");
        var sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        await sock.ConnectAsync((IPEndPoint)options.EndPoint);
        var recStream = new RecordingStream(new NetworkStream(sock, ownsSocket: true));
        var transport = new StreamTransport(PipeReader.Create(recStream), PipeWriter.Create(recStream));

        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);
        var handshakeLen = recStream.RecordedLength;
        Log($"capture: started, handshakeLen={handshakeLen}");

        // Capture each flow SOLO and sequentially. A single queued+drained flow flushes its own
        // command writes (the normal drive); queuing both and pipelining would strand flowA's writes
        // until a flush gate we never arm here. The per-command response bytes are identical whether
        // a flow is pipelined or run alone, so solo capture is faithful for replay.
        var flowA = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 20)"),
            Command.Create("select 'a-two'"));
        Assert.IsTrue(protocol.TryQueue(flowA));
        await Drain(flowA).WaitAsync(Cap);
        var afterA = recStream.RecordedLength;
        Log($"capture: flowA drained, recorded={afterA}");

        var flowB = new CommandFlow(async: true, Command.Create("select 'b'"));
        Assert.IsTrue(protocol.TryQueue(flowB));
        await Drain(flowB).WaitAsync(Cap);
        var afterB = recStream.RecordedLength;
        Log($"capture: flowB drained, recorded={afterB}");

        var full = recStream.Snapshot();
        await protocol.CompleteAsync().AsTask().WaitAsync(Cap);
        Log($"capture: complete, total={full.Length}");

        var handshake = full.AsSpan(0, handshakeLen).ToArray();
        var flowAResponse = full.AsSpan(handshakeLen, afterA - handshakeLen).ToArray();
        var flowBResponse = full.AsSpan(afterA, afterB - afterA).ToArray();
        Assert.IsTrue(flowAResponse.Length > 0, "captured an empty flowA response");
        Assert.IsTrue(flowBResponse.Length > 0, "captured an empty flowB response");

        // segment[0] = flowA's full response (released to drive flowA's first delivery in the race),
        // segment[1] = flowB's response (released during cleanup).
        var segments = new[] { flowAResponse, flowBResponse };
        return (handshake, segments);

        static async Task Drain(CommandFlow flow)
        {
            var e = flow.GetAsyncEnumerator();
            while (await e.MoveNextAsync())
            {
                var result = e.Current;
                await foreach (var _ in result) { }
            }
            await e.DisposeAsync();
        }
    }

    // Walk the backend message stream (1 type byte + 4-byte BE length, length excludes the type byte)
    // and cut a new segment after every ReadyForQuery ('Z'). Each segment is a self-contained, in-order
    // slice that ends a flow's Sync, so releasing them one at a time preserves pipelined sequencing.
    static IReadOnlyList<byte[]> SplitAtReadyForQuery(byte[] response)
    {
        var segments = new List<byte[]>();
        var start = 0;
        var o = 0;
        while (o + 5 <= response.Length)
        {
            var type = response[o];
            var len = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(response.AsSpan(o + 1, 4));
            var next = o + 1 + len;
            if (next > response.Length)
                break;
            if (type == (byte)'Z')
            {
                segments.Add(response.AsSpan(start, next - start).ToArray());
                start = next;
            }
            o = next;
        }
        if (start < response.Length)
            segments.Add(response.AsSpan(start).ToArray());
        return segments;
    }

    // Transport over a raw stream via the BCL PipeReader/PipeWriter. Async-only (the sync MoveNext
    // path requires a StreamPipeReader; capture uses async flows), which keeps this minimal.
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

    // Records every byte READ from the socket (the raw server stream), in order. Stream-level teeing
    // sidesteps all PipeReader buffering/examined subtleties: PipeReader.Create pulls bytes through
    // Read, and we record exactly those. Writes delegate untouched.
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

    // In-memory transport: read pipe starts pre-filled with the handshake only; response segments are
    // released on demand. Client writes are drained on a background loop so WaitClientQuiesceAsync can
    // detect when the body has flushed its command bytes and re-parked on its read.
    sealed class GatedReplayTransport : TransportConnection
    {
        readonly Pipe _toClient = new();
        readonly Pipe _toServer = new();
        long _clientBytes;

        public override PipeReader Reader => _toClient.Reader;
        public override PipeWriter Writer => _toServer.Writer;
        public override void WaitWritable() { }

        public GatedReplayTransport(byte[] handshake)
        {
            _toClient.Writer.WriteAsync(handshake).AsTask().GetAwaiter().GetResult();
            _ = DrainClient();
        }

        public void ReleaseSegment(byte[] bytes)
        {
            try
            {
                _toClient.Writer.WriteAsync(bytes).AsTask().GetAwaiter().GetResult();
            }
            catch
            {
                // Reader completed during shutdown - the released bytes have no consumer.
            }
        }

        // Wait until the client has written some bytes and then gone quiet (body parked on its read).
        public async Task WaitClientQuiesceAsync(int quietMs = 5, int timeoutMs = 5000)
        {
            var sw = Stopwatch.StartNew();
            long last = -1;
            var stable = 0;
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                var cur = Interlocked.Read(ref _clientBytes);
                if (cur > 0 && cur == last)
                {
                    if (++stable >= quietMs)
                        return;
                }
                else
                {
                    stable = 0;
                }
                last = cur;
                await Task.Delay(1);
            }
        }

        async Task DrainClient()
        {
            try
            {
                while (true)
                {
                    var r = await _toServer.Reader.ReadAsync();
                    if (r.Buffer.Length > 0)
                        Interlocked.Add(ref _clientBytes, r.Buffer.Length);
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
}
