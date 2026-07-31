using System.Buffers;
using System.Diagnostics;
using System.Net.Sockets;
using System.IO.Pipelines;
using Microsoft.Extensions.Time.Testing;
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
public class StoppingTokenInMemoryTests
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

        // Arm the deterministic read-park gate before any flow is queued. The first read-park the
        // protocol takes after the handshake is A's body waiting for its response; the executor may
        // activate A autonomously (before MoveNext), so arm ahead of TryQueue to never miss it.
        var aParkedOnRead = transport.ArmReadPark();

        var flowA = new CommandFlow(async: true,
            Command.Create("select generate_series(1, 20)"),
            Command.Create("select 'a-two'"));
        var flowB = new CommandFlow(async: true, Command.Create("select 'b'"));
        Assert.IsTrue(protocol.TryQueue(flowA));
        Assert.IsTrue(protocol.TryQueue(flowB));

        var eA = flowA.GetAsyncEnumerator();
        var eB = flowB.GetAsyncEnumerator();

        // Fire A's first MoveNext. A writes its command (buffered; the flush is deferred until
        // CompleteAsync) and parks on its response read.
        var aFirstTask = eA.MoveNextAsync().AsTask();

        // Pin the interleaving deterministically: wait until A has actually parked mid-read BEFORE
        // completing, rather than racing A's dispatch against source-completion (the race that let A
        // drain inert ~0.6% of reps, which the old byte-poll masked as 5s of silent green). A timeout
        // here is a real "scenario never set up" failure, not slowness.
        Log($"{variant} rep{rep} awaiting A read-park");
        try { await aParkedOnRead.WaitAsync(Cap); }
        catch (TimeoutException) { return $"flowA never parked on its read within {Cap} - scenario not set up (dispatch raced completion)"; }

        // StoppingToken + flush. A is parked mid-read; completion can no longer drain it inert.
        var completeTask = protocol.CompleteAsync();
        Log($"{variant} rep{rep} A parked, completeAsync called");

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
        var sock = options.EndPoint is UnixDomainSocketEndPoint
            ? new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified)
            : new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        await sock.ConnectAsync(options.EndPoint);
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
        await protocol.CompleteAsync().WaitAsync(Cap);
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
        readonly ReadParkSignalingReader _reader;

        public override PipeReader Reader => _reader;
        public override PipeWriter Writer => _toServer.Writer;
        public override void WaitWritable() { }

        public GatedReplayTransport(byte[] handshake)
        {
            _reader = new ReadParkSignalingReader(_toClient.Reader);
            _toClient.Writer.WriteAsync(handshake).AsTask().GetAwaiter().GetResult();
            _ = DrainClient();
        }

        // Arm a one-shot signal that completes when the protocol next parks on a read (ReadAsync
        // returns pending) - i.e. an in-flight flow has written its command and is waiting for its
        // response. Deterministic replacement for byte-polling: guarantees the flow is dispatched and
        // parked mid-read before the test perturbs completion, instead of racing dispatch.
        public Task ArmReadPark() => _reader.ArmReadPark();

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

    // Delegating PipeReader that signals an armed one-shot TCS when ReadAsync returns pending (the
    // protocol parked waiting for data). Lets the harness gate deterministically on "a flow is parked
    // mid-read" instead of polling write-byte counts.
    sealed class ReadParkSignalingReader : PipeReader
    {
        readonly PipeReader _inner;
        TaskCompletionSource? _park;

        public ReadParkSignalingReader(PipeReader inner) => _inner = inner;

        public Task ArmReadPark()
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _park, tcs);
            return tcs.Task;
        }

        // The protocol's transport read (PipeSegmentEnumerator) funnels through ReadAsync, including
        // via the base ReadAtLeastAsync default; signaling here covers both. AdvanceTo is delegated
        // verbatim (no teeing), so the base default's position tracking stays correct.
        public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            var vt = _inner.ReadAsync(cancellationToken);
            if (!vt.IsCompleted)
                Volatile.Read(ref _park)?.TrySetResult();
            return vt;
        }

        public override bool TryRead(out ReadResult result) => _inner.TryRead(out result);
        public override void AdvanceTo(SequencePosition consumed) => _inner.AdvanceTo(consumed);
        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) => _inner.AdvanceTo(consumed, examined);
        public override void CancelPendingRead() => _inner.CancelPendingRead();
        public override void Complete(Exception? exception = null) => _inner.Complete(exception);
        public override ValueTask CompleteAsync(Exception? exception = null) => _inner.CompleteAsync(exception);
    }
}
