using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Slon.Pipelines;

namespace Slon.Transport;

sealed class SocketStreamConnection : TransportConnection
{
    readonly SealedNetworkStream _stream;
    bool _aborted;

    SocketStreamConnection(SealedNetworkStream stream, TransportConnectionOptions options)
    {
        _stream = stream;
        // NetworkStream cancels natively from the token passed to Read/Write, so
        // CancelPending* (and its per-op token-source registration) is dead weight here.
        Reader = new DefaultStreamPipeReader(stream, new StreamPipeReaderOptions(bufferSize: options.ReaderSegmentSize, useZeroByteReads: options.UseZeroByteReads), supportCancelPending: false);
        Writer = new DefaultStreamPipeWriter(stream, new StreamPipeWriterOptions(minimumBufferSize: options.WriterSegmentSize), supportCancelPending: false);
    }

    static Socket CreateUnconnectedSocket(AddressFamily addressFamily)
    {
        var protocolType =
            addressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6
                ? ProtocolType.Tcp
                : ProtocolType.IP;
        return WithDefaultSocketOptions(new Socket(addressFamily, SocketType.Stream, protocolType));

        static Socket WithDefaultSocketOptions(Socket socket)
        {
            if (socket.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
                socket.NoDelay = true;
            return socket;
        }
    }

    public override PipeReader Reader { get; }
    public override PipeWriter Writer { get; }
    public override void WaitWritable() => _stream.WaitWritable();

    public static Factory CreateFactory(EndPoint endPoint, TransportConnectionOptions? options = null) => new(endPoint, options);

    public static ValueTask<SocketStreamConnection> ConnectAsync(EndPoint endPoint, TransportConnectionOptions? options = null, CancellationToken cancellationToken = default)
        => ConnectAsync<SocketStreamConnection>(endPoint, options ?? new(), cancellationToken);

    static async ValueTask<T> ConnectAsync<T>(EndPoint endPoint, TransportConnectionOptions options, CancellationToken cancellationToken)
        where T : TransportConnection
    {
        var resolvedEndpoint = await ResolveEndPointAsync(endPoint, cancellationToken).ConfigureAwait(false);
        var socket = CreateUnconnectedSocket(resolvedEndpoint.AddressFamily);
        await socket.ConnectAsync(resolvedEndpoint, cancellationToken).ConfigureAwait(false);
        return (T)(object)new SocketStreamConnection(new SealedNetworkStream(socket, ownsSocket: true), options);
    }

    public static SocketStreamConnection Connect(EndPoint endPoint, TransportConnectionOptions? options = null, TimeSpan timeout = default)
    {
        options ??= new();
        var deadline = new Deadline(timeout);
        var resolvedEndpoint = ResolveEndPoint(endPoint);
        var socket = CreateUnconnectedSocket(resolvedEndpoint.AddressFamily);
        if (deadline.TotalDuration == Timeout.InfiniteTimeSpan)
            socket.Connect(resolvedEndpoint);
        else
            ConnectWithTimeout(socket, resolvedEndpoint, deadline.GetRemaining());

        return new(new SealedNetworkStream(socket, ownsSocket: true), options);

        static void ConnectWithTimeout(Socket socket, EndPoint endPoint, TimeSpan timeout)
        {
            socket.Blocking = false;
            try
            {
                socket.Connect(endPoint);
            }
            catch (SocketException e)
            {
                if (e.SocketErrorCode != SocketError.WouldBlock)
                    throw;
            }
            var write = new List<Socket> {socket};
            var error = new List<Socket> {socket};
            Socket.Select(null, write, error, checked((int)timeout.Ticks / (int)TimeSpan.TicksPerMicrosecond));
            var errorCode = (int) socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error)!;
            if (errorCode != 0)
                throw new SocketException(errorCode);
            if (!write.Any())
                throw new TimeoutException("Timeout during connection attempt");
            socket.Blocking = true;
        }
    }

    // No connection-level DISPOSAL: the Reader and Writer own the stream (LeaveOpen is false), so
    // completing them closes the socket. The one socket-specific teardown is the abortive close:
    // socket.Close(0) sets a 0-linger close -> sends RST. It never blocks (a graceful Dispose can hang
    // flushing the FIN against a wedged peer), faults any parked sync read (fd gone -> the read throws),
    // and skips TIME_WAIT. Leaves the reader/writer buffers for the later Complete (a parked read may
    // hold a reserved segment). The generic finalize stays on the reader/writer's Complete.
    // Idempotent: Close(0) sets LingerState then disposes, and setting LingerState on an already-disposed
    // socket throws, so gate behind a flag - multiple release sites (the factory's pre-Start cleanup and
    // the protocol's start-failure cleanup) can both reach here.
    public override void Abort()
    {
        if (!Interlocked.Exchange(ref _aborted, true))
            _stream.Socket.Close(0);
    }

    static EndPoint ResolveEndPoint(EndPoint endPoint)
        => endPoint switch
        {
            DnsEndPoint dnsEndPoint
                => new IPEndPoint(Dns.GetHostAddresses(dnsEndPoint.Host, dnsEndPoint.AddressFamily)[0], dnsEndPoint.Port),
            IPEndPoint value => value,
            UnixDomainSocketEndPoint value => value,
            _ => throw new NotSupportedException("EndPoint not supported")
        };

    static async ValueTask<EndPoint> ResolveEndPointAsync(EndPoint endPoint, CancellationToken cancellationToken = default)
        => endPoint switch
        {
            DnsEndPoint dnsEndPoint
                => new IPEndPoint((await Dns.GetHostAddressesAsync(dnsEndPoint.Host, dnsEndPoint.AddressFamily, cancellationToken).ConfigureAwait(false))[0], dnsEndPoint.Port),
            IPEndPoint value => value,
            UnixDomainSocketEndPoint value => value,
            _ => throw new NotSupportedException("EndPoint not supported")
        };

    public new sealed class Factory : TransportConnection.Factory
    {
        readonly EndPoint _endPoint;

        internal Factory(EndPoint endPoint, TransportConnectionOptions? options = null) : base(options)
        {
            _endPoint = endPoint;
        }

        public override bool SupportsSynchronousIO => true;

        public override TransportConnection Connect(TimeSpan timeout = default)
            => SocketStreamConnection.Connect(_endPoint, Options, timeout);

        public override ValueTask<TransportConnection> ConnectAsync(CancellationToken cancellationToken = default)
            => SocketStreamConnection.ConnectAsync<TransportConnection>(_endPoint, Options, cancellationToken);
    }

    // Reads TransportConnection.SyncNonBlockingSignal to decide whether WriteAsync does sync
    // non-blocking syscalls. With a signal set it returns a pending ValueTask backed by that
    // signal on WouldBlock, never throwing. Without one it falls through to base
    // NetworkStream's normal async I/O. The "lie" lets SslStream and any other async wrappers
    // above pass our ValueTasks through faithfully. The signal is owned by the flow that set
    // it. The transport never signals it.
    internal sealed class SealedNetworkStream : NetworkStream
    {
        public SealedNetworkStream(Socket socket, bool ownsSocket) : base(socket, ownsSocket)
        {
            // NetworkStream's constructor validates that socket.Blocking is true and refuses
            // non-blocking sockets. Flip it AFTER base construction so our WriteAsync override
            // gets non-blocking semantics. The read path needs the same treatment before this
            // is safe for full integration use, but it works for the write-only tests below.
            socket.Blocking = false;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            // No TLS signal == normal async I/O path (TP completion via SocketAsyncEngine).
            // This is the async-flow path, unaffected by the coroutine pattern.
            var signal = SyncNonBlockingSignal;
            if (signal is null)
                return base.WriteAsync(buffer, cancellationToken);

            // Hot path: try sync non-blocking send inline. If the whole buffer goes out
            // without WouldBlock, return sync-completed, no state machine, no allocation.
            while (buffer.Length > 0)
            {
                var sent = Socket.Send(buffer.Span, SocketFlags.None, out var errorCode);
                if (errorCode == SocketError.WouldBlock)
                {
                    // Cold path: rest of the buffer needs to suspend. Hand off to the coroutine
                    // capturing the signal (so the flow's driver can clear TLS after this
                    // initial call returns without affecting the in-flight coroutine).
                    return WriteAsyncCoroutine(buffer, signal, cancellationToken);
                }
                if (errorCode != SocketError.Success)
                    return ValueTask.FromException(new SocketException((int)errorCode));
                buffer = buffer.Slice(sent);
            }
            return ValueTask.CompletedTask;
        }

        async ValueTask WriteAsyncCoroutine(ReadOnlyMemory<byte> buffer, WriteResumeSignal signal, CancellationToken cancellationToken)
        {
            while (buffer.Length > 0)
            {
                var sent = Socket.Send(buffer.Span, SocketFlags.None, out var errorCode);
                if (errorCode == SocketError.WouldBlock)
                {
                    await signal.Pending().ConfigureAwait(false);
                    continue;
                }
                if (errorCode != SocketError.Success)
                    throw new SocketException((int)errorCode);
                buffer = buffer.Slice(sent);
            }
        }

        // Exposed so the flow-level driver can park its thread until the kernel reports
        // writability. Honors the TLS-carried SyncNonBlockingDeadline set by ResumableScope
        // so per-command timeouts work end-to-end without a separate signature. Throws
        // TimeoutException on expiry so a dead peer doesn't park the driver thread forever.
        public void WaitWritable() => PollWritableOrThrow(SyncNonBlockingDeadline);

        // Sync Write. The socket is non-blocking (set at construction), so we own the local
        // reactor loop here. Sources the deadline from the Stream's WriteTimeout property
        // (the standard sync-stream timeout knob) rather than the TLS slot, since sync Write
        // callers aren't going through ResumableScope. One Deadline per Write call so the
        // whole operation must complete within WriteTimeout, matching the NetworkStream
        // contract.
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            var deadline = WriteTimeout <= 0
                ? (Deadline?)null
                : new Deadline(TimeSpan.FromMilliseconds(WriteTimeout));
            while (buffer.Length > 0)
            {
                var sent = Socket.Send(buffer, SocketFlags.None, out var errorCode);
                if (errorCode == SocketError.Success)
                {
                    buffer = buffer.Slice(sent);
                    continue;
                }
                if (errorCode == SocketError.WouldBlock)
                {
                    PollWritableOrThrow(deadline);
                    continue;
                }
                throw new SocketException((int)errorCode);
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
            => Write(buffer.AsSpan(offset, count));

        // Sync Read. Mirrors the sync Write reactor loop. Once writes shunt off to a
        // LongRunning thread the caller thread is free to block here in Poll waiting for
        // data, so we don't need TLS-driven async-read coroutines for sync flows. Sources
        // the deadline from Stream.ReadTimeout, fresh Deadline per Read call to match the
        // NetworkStream contract.
        public override int Read(Span<byte> buffer)
        {
            var deadline = ReadTimeout <= 0
                ? (Deadline?)null
                : new Deadline(TimeSpan.FromMilliseconds(ReadTimeout));
            while (true)
            {
                var received = Socket.Receive(buffer, SocketFlags.None, out var errorCode);
                if (errorCode == SocketError.Success)
                    return received;
                if (errorCode == SocketError.WouldBlock)
                {
                    PollOrThrow(deadline, SelectMode.SelectRead, "Read");
                    continue;
                }
                throw new SocketException((int)errorCode);
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
            => Read(buffer.AsSpan(offset, count));

        void PollWritableOrThrow(Deadline? deadline) => PollOrThrow(deadline, SelectMode.SelectWrite, "Write");

        void PollOrThrow(Deadline? deadline, SelectMode mode, string opName)
        {
            // Null deadline means infinite. Otherwise compute remaining (which itself throws
            // TimeoutException if already elapsed). Socket.Poll uses -1 for infinite and
            // positive value as microseconds. Clamp ms-to-us conversion at int.MaxValue for
            // very large remaining intervals.
            int pollUs;
            if (deadline is { } d)
            {
                var remaining = d.GetRemaining();
                pollUs = remaining == Timeout.InfiniteTimeSpan
                    ? -1
                    : (int)Math.Min((long)remaining.TotalMilliseconds * 1000, int.MaxValue);
            }
            else
            {
                pollUs = -1;
            }
            if (!Socket.Poll(pollUs, mode))
                throw new IOException($"{opName} timed out waiting for socket readiness.", new SocketException((int)SocketError.TimedOut));
        }
    }
}
