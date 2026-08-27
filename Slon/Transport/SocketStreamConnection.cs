using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Slon.Pipelines;
using Slon.Runtime;

namespace Slon.Transport;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed class SocketStreamConnection : TransportConnection
{
    readonly Factory _factory;
    readonly SealedNetworkStream _networkStream;
    readonly TransportConnectionOptions _options;
    DefaultStreamPipeReader _reader;
    DefaultStreamPipeWriter _writer;
    PipeStreamOwner _pipeStreamOwner;
    Stream _stream;
    bool _aborted;

    SocketStreamConnection(Factory factory, SealedNetworkStream networkStream, Stream stream, TransportConnectionOptions options)
    {
        _factory = factory;
        _networkStream = networkStream;
        _stream = stream;
        _options = options;
        (_reader, _writer, _pipeStreamOwner) = CreatePipes(stream, options);
    }

    static (DefaultStreamPipeReader Reader, DefaultStreamPipeWriter Writer, PipeStreamOwner Owner) CreatePipes(Stream stream, TransportConnectionOptions options)
    {
        var owner = new PipeStreamOwner(stream);
        // NetworkStream cancels natively from the token passed to Read/Write, so
        // CancelPending* (and its per-op token-source registration) is dead weight here.
        var reader = new DefaultStreamPipeReader(stream, new StreamPipeReaderOptions(bufferSize: options.ReaderSegmentSize, useZeroByteReads: options.UseZeroByteReads), supportCancelPending: false, owner);
        var writer = new DefaultStreamPipeWriter(stream, new StreamPipeWriterOptions(minimumBufferSize: options.WriterSegmentSize), supportCancelPending: false, owner)
        {
            RetainBuffer = !options.UseZeroByteReads
        };
        return (reader, writer, owner);
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

    public override PipeReader Reader => _reader;
    public override PipeWriter Writer => _writer;
    public override bool IsConnectionLost(Exception exception)
        => exception is SocketException or IOException { InnerException: SocketException };
    public override void WaitUntilWritable(TimeSpan timeout = default) => _networkStream.WaitUntilWritable(timeout);

    public static Factory CreateFactory(EndPoint endPoint, TransportConnectionOptions? options = null) => new(endPoint, options);

    public static ValueTask<SocketStreamConnection> ConnectAsync(EndPoint endPoint, TransportConnectionOptions? options = null, CancellationToken cancellationToken = default)
    {
        var factory = new Factory(endPoint, options);
        return ConnectAsync(factory, cancellationToken);

        static async ValueTask<SocketStreamConnection> ConnectAsync(Factory factory, CancellationToken cancellationToken)
            => (SocketStreamConnection)await factory.ConnectAsync(cancellationToken).ConfigureAwait(false);
    }

    public static SocketStreamConnection Connect(EndPoint endPoint, TransportConnectionOptions? options = null, TimeSpan timeout = default)
        => (SocketStreamConnection)new Factory(endPoint, options).Connect(timeout);

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
        Socket.Select(null, write, error, Deadline.ToTimeoutMicroseconds(timeout));
        var errorCode = (int) socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error)!;
        if (errorCode != 0)
            throw new SocketException(errorCode);
        if (!write.Any())
            throw new TimeoutException("Timeout during connection attempt");
        socket.Blocking = true;
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
            _networkStream.Close(0);
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

        public Factory(EndPoint endPoint, TransportConnectionOptions? options = null) : base(options)
        {
            _endPoint = endPoint;
        }

        public override TransportConnection ConnectTransformed(Func<Stream, Stream> transform, TimeSpan timeout = default)
        {
            ArgumentNullException.ThrowIfNull(transform);
            var deadline = new Deadline(timeout);
            var resolvedEndpoint = ResolveEndPoint(_endPoint);
            var socket = CreateUnconnectedSocket(resolvedEndpoint.AddressFamily);
            try
            {
                if (deadline.TotalDuration == Timeout.InfiniteTimeSpan)
                    socket.Connect(resolvedEndpoint);
                else
                    ConnectWithTimeout(socket, resolvedEndpoint, deadline.GetRemaining());

                return CreateTransformed(new SealedNetworkStream(socket, ownsSocket: true), transform);
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }

        public override async ValueTask<TransportConnection> ConnectTransformedAsync(Func<Stream, Stream> transform, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(transform);
            var resolvedEndpoint = await ResolveEndPointAsync(_endPoint, cancellationToken).ConfigureAwait(false);
            var socket = CreateUnconnectedSocket(resolvedEndpoint.AddressFamily);
            try
            {
                await socket.ConnectAsync(resolvedEndpoint, cancellationToken).ConfigureAwait(false);
                return CreateTransformed(new SealedNetworkStream(socket, ownsSocket: true), transform);
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }

        public override TransportConnection Upgrade(TransportConnection connection, Func<Stream, Stream> transform)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(transform);
            if (connection is not SocketStreamConnection socketConnection || !ReferenceEquals(socketConnection._factory, this))
                throw new ArgumentException("The connection was not created by this factory.", nameof(connection));

            socketConnection._reader.EnsureCanUpgradeStream();
            socketConnection._writer.EnsureCanUpgradeStream();
            var transformed = ApplyTransform(socketConnection._stream, transform);
            try
            {
                socketConnection._pipeStreamOwner.LeaveOpen();
                socketConnection._reader.Complete();
                socketConnection._writer.Complete();
                (socketConnection._reader, socketConnection._writer, socketConnection._pipeStreamOwner) = CreatePipes(transformed, socketConnection._options);
                socketConnection._stream = transformed;
                return socketConnection;
            }
            catch
            {
                socketConnection.Abort();
                throw;
            }
        }

        SocketStreamConnection CreateTransformed(SealedNetworkStream networkStream, Func<Stream, Stream> transform)
        {
            var transformed = ApplyTransform(networkStream, transform);
            return new(this, networkStream, transformed, Options);
        }

        static Stream ApplyTransform(Stream stream, Func<Stream, Stream> transform)
            => transform(stream) ?? throw new InvalidOperationException("The connection transform returned null.");
    }

    sealed class PipeStreamOwner(Stream stream) : IStreamOwner
    {
        bool _leaveOpen;
        int _disposed;

        internal void LeaveOpen() => _leaveOpen = true;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) is 0 && !_leaveOpen)
                stream.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) is not 0 || _leaveOpen)
                return default;
            return stream.DisposeAsync();
        }
    }

    // Reads TransportConnection.CurrentResumableWrite to decide whether WriteAsync does sync
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
            // non-blocking sockets. Flip it after base construction; the synchronous Read/Write
            // overrides below turn WouldBlock into readiness waits.
            socket.Blocking = false;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            // No TLS binding == normal async I/O path (TP completion via SocketAsyncEngine).
            // This is the async-flow path, unaffected by the coroutine pattern.
            var resumableWrite = CurrentResumableWrite;
            var signal = resumableWrite.Signal;
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
                    return WriteAsyncCoroutine(buffer, signal,
                        ResumeSignal.CreateDeadline(resumableWrite.Timeout), cancellationToken);
                }
                if (errorCode != SocketError.Success)
                    return ValueTask.FromException(new SocketException((int)errorCode));
                buffer = buffer.Slice(sent);
            }
            return ValueTask.CompletedTask;
        }

        async ValueTask WriteAsyncCoroutine(ReadOnlyMemory<byte> buffer, ResumeSignal signal,
            Deadline? deadline, CancellationToken cancellationToken)
        {
            while (buffer.Length > 0)
            {
                var sent = Socket.Send(buffer.Span, SocketFlags.None, out var errorCode);
                if (errorCode == SocketError.WouldBlock)
                {
                    await signal.Pending(deadline).ConfigureAwait(false);
                    continue;
                }
                if (errorCode != SocketError.Success)
                    throw new SocketException((int)errorCode);
                buffer = buffer.Slice(sent);
            }
        }

        public void WaitUntilWritable(TimeSpan timeout = default)
            => PollOrThrow(timeout, SelectMode.SelectWrite, "Write");

        // Sync Write. The socket is non-blocking (set at construction), so we own the local
        // reactor loop here. Sources the deadline from the Stream's WriteTimeout property
        // (the standard sync-stream timeout knob) rather than the TLS slot, since sync Write
        // callers aren't going through ResumableWriteScope. One Deadline per Write call so the
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
            => PollOrThrow(deadline?.GetRemaining() ?? Timeout.InfiniteTimeSpan, mode, opName);

        void PollOrThrow(TimeSpan timeout, SelectMode mode, string opName)
        {
            if (!Socket.Poll(Deadline.ToTimeoutMicroseconds(timeout), mode))
                throw new IOException($"{opName} timed out waiting for socket readiness.", new SocketException((int)SocketError.TimedOut));
        }
    }
}
