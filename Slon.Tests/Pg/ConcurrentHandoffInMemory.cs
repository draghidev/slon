using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net.Sockets;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Tests.Pg;

// In-memory variant of ConcurrentSyncAndAsync_NoSharedPromiseCollision. Captures the handshake + one
// "select 1" response from a real backend ONCE, then replays it against an in-memory echo-server
// transport (responds with the canned response per client Sync). No network, so the concurrent
// sync/async handoff race runs far faster than the real-PG soak. Same per-iteration timeout so a
// read-side hang self-reports.
//
// Caveat: the hang is timing-dependent; in-memory timing differs from network, so reproduction is not
// guaranteed. This is a fast-iteration companion to the real soak, not a replacement.
[TestClass, Ignore]
[DoNotParallelize]
public class ConcurrentHandoffInMemory
{
    static int Iters => int.TryParse(Environment.GetEnvironmentVariable("SLON_INMEM_ITERS"), out var n) && n > 0 ? n : 200_000;

    // FORCED overlap: every iteration puts an async flow AND a sync flow on the wire at the same time,
    // so the sync<->async read-baton handoff is exercised each iteration (vs the loose loops above where
    // overlap is timing-incidental and rare). This is the deterministic driver for the read-order race.
    [TestMethod]
    public async Task I ()
    {
        var (handshake, response) = await CaptureOnce();
        var options = PgTestPool.NewOptions();
        var transport = new EchoServerTransport(handshake, response);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options)
        {
            CompletionTimeout = TimeSpan.FromSeconds(60),
            HeartbeatInterval = TimeSpan.FromSeconds(5),
        });
        await protocol.StartAsync(options, transport);

        var failure = new Exception?[1];
        void Capture(Exception ex) => Interlocked.CompareExchange(ref failure[0], ex, null);
        var iters = Iters;

        // Persistent sync worker: on each release, queues+drives ONE sync flow via the handoff on its
        // own OS thread (a fresh thread per iter is too costly). Signals back when its flow completes.
        var runSync = new SemaphoreSlim(0);
        var syncDone = new SemaphoreSlim(0);
        var stop = false;
        var worker = new Thread(() =>
        {
            while (true)
            {
                runSync.Wait();
                if (Volatile.Read(ref stop)) return;
                try
                {
                    var s = new CommandFlow(async: false, Command.Create("select 1"));
                    if (protocol.TryQueue(s))
                    {
                        var e = s.GetEnumerator();
                        while (e.MoveNext()) { }
                        e.Dispose();
                    }
                }
                catch (Exception ex) { Capture(ex); }
                syncDone.Release();
            }
        })
        { IsBackground = true, Name = "forced-sync-worker" };
        worker.Start();

        for (int i = 0; i < iters && Volatile.Read(ref failure[0]) is null; i++)
        {
            // Async A: queue + start draining (writes A's query, parks on its read).
            var a = new CommandFlow(async: true, Command.Create("select 1"));
            protocol.TryQueue(a);
            var aDrain = DrainAsync(a);
            // Trigger sync S concurrently - now A and S are both in flight on one wire.
            runSync.Release();
            try
            {
                await aDrain.WaitAsync(TimeSpan.FromSeconds(15));
                if (!await Task.Run(() => syncDone.Wait(TimeSpan.FromSeconds(15))))
                    throw new TimeoutException("sync flow");
            }
            catch (Exception ex) when (ex is TimeoutException)
            {
                Capture(new Exception($"FORCED-OVERLAP HUNG at iter {i} ({ex.Message})."));
                break;
            }
        }

        Volatile.Write(ref stop, true);
        runSync.Release();
        try { await protocol.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
        if (failure[0] is { } f)
            Assert.Fail($"forced-overlap (in-memory) raised {f.GetType().Name}: {f.Message}\n{f}");

        static async Task DrainAsync(CommandFlow flow)
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

    [TestMethod]
    public async Task ConcurrentSyncAsync_InMemory()
    {
        var (handshake, response) = await CaptureOnce();
        var options = PgTestPool.NewOptions();
        var transport = new EchoServerTransport(handshake, response);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options)
        {
            CompletionTimeout = TimeSpan.FromSeconds(60),
            HeartbeatInterval = TimeSpan.FromSeconds(5),
        });
        await protocol.StartAsync(options, transport);

        var failure = new Exception?[1];
        void Capture(Exception ex) => Interlocked.CompareExchange(ref failure[0], ex, null);
        var iters = Iters;

        var asyncLoop = Task.Run(async () =>
        {
            try
            {
                for (int i = 0; i < iters && Volatile.Read(ref failure[0]) is null; i++)
                {
                    try { await PgTestPool.RunAsync(protocol, "select 1").WaitAsync(TimeSpan.FromSeconds(15)); }
                    catch (TimeoutException)
                    {
                        Capture(new Exception($"async RunAsync HUNG at iter {i}."));
                        break;
                    }
                }
            }
            catch (Exception ex) { Capture(ex); }
        });

        var syncThread = new Thread(() =>
        {
            try
            {
                for (int i = 0; i < iters && Volatile.Read(ref failure[0]) is null; i++)
                {
                    var flow = new CommandFlow(async: false, Command.Create("select 1"));
                    if (!protocol.TryQueue(flow))
                        break;
                    var e = flow.GetEnumerator();
                    while (e.MoveNext()) { }
                    e.Dispose();
                }
            }
            catch (Exception ex) { Capture(ex); }
        })
        { IsBackground = true, Name = "inmem-sync-loop" };
        syncThread.Start();

        await asyncLoop;
        syncThread.Join(TimeSpan.FromSeconds(30));
        try { await protocol.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10)); } catch { }

        if (failure[0] is { } ex2)
            Assert.Fail($"concurrent sync/async (in-memory) raised {ex2.GetType().Name}: {ex2.Message}\n{ex2}");
    }

    // Capture handshake + one "select 1" async response from a real backend.
    static async Task<(byte[] handshake, byte[] response)> CaptureOnce()
    {
        var options = PgTestPool.NewOptions();
        var sock = options.EndPoint is UnixDomainSocketEndPoint
            ? new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified)
            : new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        await sock.ConnectAsync(options.EndPoint);
        var rec = new RecordingStream(new NetworkStream(sock, ownsSocket: true));
        var transport = new StreamTransport(PipeReader.Create(rec), PipeWriter.Create(rec));

        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);
        var handshakeLen = rec.RecordedLength;

        var flow = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync())
        {
            var result = e.Current;
            await foreach (var _ in result) { }
        }
        await e.DisposeAsync();
        var total = rec.RecordedLength;
        var full = rec.Snapshot();
        await protocol.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10));

        var handshake = full.AsSpan(0, handshakeLen).ToArray();
        var response = full.AsSpan(handshakeLen, total - handshakeLen).ToArray();
        Assert.IsTrue(response.Length > 0, "captured an empty select-1 response");
        return (handshake, response);
    }

    // Transport over a raw stream via the BCL PipeReader/PipeWriter (capture only).
    sealed class StreamTransport : TransportConnection
    {
        public StreamTransport(PipeReader reader, PipeWriter writer) { Reader = reader; Writer = writer; }
        public override PipeReader Reader { get; }
        public override PipeWriter Writer { get; }
        public override void WaitWritable() { }
    }

    // Records every byte read from the server stream (for capture).
    sealed class RecordingStream : Stream
    {
        readonly Stream _inner;
        readonly System.Buffers.ArrayBufferWriter<byte> _record = new();
        readonly object _lock = new();
        public RecordingStream(Stream inner) => _inner = inner;
        public int RecordedLength { get { lock (_lock) { return _record.WrittenCount; } } }
        public byte[] Snapshot() { lock (_lock) { return _record.WrittenSpan.ToArray(); } }
        void Record(ReadOnlySpan<byte> b) { if (b.Length == 0) return; lock (_lock) _record.Write(b); }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        { var n = await _inner.ReadAsync(buffer, ct); Record(buffer.Span.Slice(0, n)); return n; }
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        { var n = await _inner.ReadAsync(buffer, offset, count, ct); Record(buffer.AsSpan(offset, n)); return n; }
        public override int Read(byte[] buffer, int offset, int count)
        { var n = _inner.Read(buffer, offset, count); Record(buffer.AsSpan(offset, n)); return n; }
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) => _inner.WriteAsync(buffer, ct);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) => _inner.WriteAsync(buffer, offset, count, ct);
        public override void Flush() => _inner.Flush();
        public override Task FlushAsync(CancellationToken ct) => _inner.FlushAsync(ct);
        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void SetLength(long value) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        protected override void Dispose(bool disposing) { if (disposing) _inner.Dispose(); }
    }

    // In-memory echo server: pre-fills the handshake, then a background loop frames client frontend
    // messages and writes one canned response per Sync ('S'). The first client message is the untyped
    // StartupMessage (length-prefixed, no type byte) - consumed specially; subsequent messages (auth
    // PasswordMessage, Parse/Bind/Describe/Execute/Sync) are typed and framed by 1-byte type + 4-byte len.
    sealed class EchoServerTransport : TransportConnection
    {
        readonly Pipe _toClient = new(new PipeOptions(pauseWriterThreshold: 1 << 30, resumeWriterThreshold: 1 << 29));
        readonly Pipe _toServer = new(new PipeOptions(pauseWriterThreshold: 1 << 30, resumeWriterThreshold: 1 << 29));
        readonly byte[] _response;
        // Client read side must be a StreamPipeReader (supports the sync-flow synchronous read path,
        // like the real socket transport); a raw Pipe.Reader throws "does not support synchronous reads".
        readonly PipeReader _clientReader;
        public override PipeReader Reader => _clientReader;
        public override PipeWriter Writer => _toServer.Writer;
        public override void WaitWritable() { }

        public EchoServerTransport(byte[] handshake, byte[] response)
        {
            _response = response;
            // Slon's PipeSegmentEnumerator requires its OWN StreamPipeReader for the sync path
            // (not the BCL PipeReader.Create). Wrap the in-memory pipe as a stream, same as the socket transport.
            _clientReader = new Slon.Pipelines.DefaultStreamPipeReader(
                _toClient.Reader.AsStream(),
                new StreamPipeReaderOptions(bufferSize: 8192, useZeroByteReads: false),
                supportCancelPending: false);
            _toClient.Writer.WriteAsync(handshake).AsTask().GetAwaiter().GetResult();
            _ = ServerLoop();
        }

        async Task ServerLoop()
        {
            var buf = new List<byte>();
            var startupConsumed = false;
            try
            {
                while (true)
                {
                    var r = await _toServer.Reader.ReadAsync();
                    foreach (var seg in r.Buffer)
                        buf.AddRange(seg.ToArray());
                    _toServer.Reader.AdvanceTo(r.Buffer.End);

                    var off = 0;
                    if (!startupConsumed)
                    {
                        if (buf.Count - off >= 4)
                        {
                            var len = ReadInt32BE(buf, off);         // startup length INCLUDES the 4 length bytes
                            if (buf.Count - off >= len) { off += len; startupConsumed = true; }
                        }
                    }
                    if (startupConsumed)
                    {
                        var responses = 0;
                        while (buf.Count - off >= 5)
                        {
                            var type = buf[off];
                            var len = ReadInt32BE(buf, off + 1);      // typed length EXCLUDES the type byte
                            if (buf.Count - off < 1 + len) break;     // incomplete message
                            if (type == (byte)'S') responses++;       // Sync -> emit one canned response
                            off += 1 + len;
                        }
                        // Yield before responding so the flow actually PARKS on its read first - that
                        // opens the sync/async read-baton race window (an instant response closes it).
                        if (responses > 0)
                        {
                            await Task.Yield();
                            for (var i = 0; i < responses; i++)
                                await _toClient.Writer.WriteAsync(_response);
                        }
                    }
                    if (off > 0)
                        buf.RemoveRange(0, off);
                    if (r.IsCompleted)
                        break;
                }
            }
            catch { /* pipe completed on teardown */ }
        }

        static int ReadInt32BE(List<byte> b, int o)
            => (b[o] << 24) | (b[o + 1] << 16) | (b[o + 2] << 8) | b[o + 3];
    }
}
