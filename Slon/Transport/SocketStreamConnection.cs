using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Slon.Pipelines;

namespace Slon.Transport;

sealed class SocketStreamConnection : TransportConnection, IDisposable, IAsyncDisposable
{
    readonly SealedNetworkStream _stream;

    SocketStreamConnection(SealedNetworkStream stream, TransportConnectionOptions options)
    {
        _stream = stream;
        Reader = new DefaultStreamPipeReader(stream, new StreamPipeReaderOptions(bufferSize: options.ReaderSegmentSize, useZeroByteReads: options.UseZeroByteReads));
        Writer = new DefaultStreamPipeWriter(stream, new StreamPipeWriterOptions(minimumBufferSize: options.WriterSegmentSize));
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

        static void ConnectWithTimeout(Socket socket, IPEndPoint endPoint, TimeSpan timeout)
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

    public void Dispose()
    {
        Reader.Complete();
        Writer.Complete();
        _stream.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await Reader.CompleteAsync().ConfigureAwait(false);
        await Writer.CompleteAsync().ConfigureAwait(false);
        await _stream.DisposeAsync().ConfigureAwait(false);
    }

    static IPEndPoint ResolveEndPoint(EndPoint endPoint)
        => endPoint switch
        {
            DnsEndPoint dnsEndPoint
                => new IPEndPoint(Dns.GetHostAddresses(dnsEndPoint.Host, dnsEndPoint.AddressFamily)[0], dnsEndPoint.Port),
            IPEndPoint value => value,
            _ => throw new NotSupportedException("EndPoint not supported")
        };

    static async ValueTask<IPEndPoint> ResolveEndPointAsync(EndPoint endPoint, CancellationToken cancellationToken = default)
        => endPoint switch
        {
            DnsEndPoint dnsEndPoint
                => new IPEndPoint((await Dns.GetHostAddressesAsync(dnsEndPoint.Host, dnsEndPoint.AddressFamily, cancellationToken).ConfigureAwait(false))[0], dnsEndPoint.Port),
            IPEndPoint value => value,
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

    sealed class SealedNetworkStream(Socket socket, bool ownsSocket) : NetworkStream(socket, ownsSocket);
}
