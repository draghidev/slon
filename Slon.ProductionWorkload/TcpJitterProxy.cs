using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Slon.ProductionWorkload;

sealed class TcpJitterProxy : IAsyncDisposable
{
    readonly string _upstreamHost;
    readonly int _upstreamPort;
    readonly int _maximumDelayMilliseconds;
    readonly int _maximumChunkBytes;
    readonly int _seed;
    readonly TcpListener _listener;
    readonly CancellationTokenSource _stop = new();
    readonly ConcurrentDictionary<long, Task> _connections = new();
    readonly Task _acceptLoop;
    long _connectionId;
    long _bytesForwarded;

    public TcpJitterProxy(string upstreamHost, int upstreamPort, int maximumDelayMilliseconds,
        int maximumChunkBytes, int seed)
    {
        _upstreamHost = upstreamHost;
        _upstreamPort = upstreamPort;
        _maximumDelayMilliseconds = maximumDelayMilliseconds;
        _maximumChunkBytes = maximumChunkBytes;
        _seed = seed;
        _listener = new(IPAddress.Loopback, 0);
        _listener.Start();
        EndPoint = (IPEndPoint)_listener.LocalEndpoint;
        _acceptLoop = AcceptConnectionsAsync();
    }

    public IPEndPoint EndPoint { get; }
    public long Connections => Volatile.Read(ref _connectionId);
    public long BytesForwarded => Volatile.Read(ref _bytesForwarded);

    async Task AcceptConnectionsAsync()
    {
        try
        {
            while (true)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                var id = Interlocked.Increment(ref _connectionId);
                var task = RunConnectionAsync(client, id);
                _connections[id] = task;
                _ = task.ContinueWith(static (_, state) =>
                {
                    var (connections, connectionId) = ((ConcurrentDictionary<long, Task>, long))state!;
                    connections.TryRemove(connectionId, out Task? _);
                }, (_connections, id), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (ObjectDisposedException) when (_stop.IsCancellationRequested) { }
    }

    async Task RunConnectionAsync(TcpClient client, long id)
    {
        using (client)
        using (var upstream = new TcpClient())
        {
            try
            {
                await upstream.ConnectAsync(_upstreamHost, _upstreamPort, _stop.Token).ConfigureAwait(false);
                using var clientStream = client.GetStream();
                using var upstreamStream = upstream.GetStream();
                var upstreamPump = PumpAsync(clientStream, upstreamStream, CreateRandom(id, 0));
                var downstreamPump = PumpAsync(upstreamStream, clientStream, CreateRandom(id, 1));
                await Task.WhenAny(upstreamPump, downstreamPump).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
            catch (IOException) when (_stop.IsCancellationRequested) { }
            catch (SocketException) when (_stop.IsCancellationRequested) { }
        }
    }

    Random CreateRandom(long connectionId, int direction)
        => new(HashCode.Combine(_seed, connectionId, direction));

    async Task PumpAsync(NetworkStream source, NetworkStream destination, Random random)
    {
        var buffer = GC.AllocateUninitializedArray<byte>(_maximumChunkBytes);
        while (true)
        {
            var requested = random.Next(1, buffer.Length + 1);
            var read = await source.ReadAsync(buffer.AsMemory(0, requested), _stop.Token).ConfigureAwait(false);
            if (read == 0)
                return;

            if (_maximumDelayMilliseconds != 0)
            {
                var delay = random.Next(_maximumDelayMilliseconds + 1);
                if (delay != 0)
                    await Task.Delay(delay, _stop.Token).ConfigureAwait(false);
            }

            await destination.WriteAsync(buffer.AsMemory(0, read), _stop.Token).ConfigureAwait(false);
            Interlocked.Add(ref _bytesForwarded, read);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        try
        {
            await _acceptLoop.ConfigureAwait(false);
            await Task.WhenAll(_connections.Values).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        _stop.Dispose();
    }
}
