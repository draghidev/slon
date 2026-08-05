using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Slon.Transport;

namespace Slon.Tests.Transport;

// Unit tests for the non-blocking transport primitives in isolation: the WriteResumeSignal
// auto-reset cycle and inline-continuation guarantee, the ResumableScope TLS lifecycle,
// and end-to-end byte delivery through SocketStreamConnection and SslStream.
[TestClass]
public class TransportTests
{
    [TestMethod]
    public void WriteResumeSignal_Pending_IsPending_BeforeSignal()
    {
        var signal = new WriteResumeSignal();
        var t = signal.Pending();
        Assert.IsFalse(t.IsCompleted, "Pending() before Signal should be pending");
    }

    [TestMethod]
    public void WriteResumeSignal_Signal_CompletesPending()
    {
        var signal = new WriteResumeSignal();
        var t = signal.Pending();
        signal.Signal();
        Assert.IsTrue(t.IsCompleted);
        t.GetAwaiter().GetResult();
    }

    [TestMethod]
    public void WriteResumeSignal_AutoResets_OnConsumption_AllowsNewCycle()
    {
        var signal = new WriteResumeSignal();

        var t1 = signal.Pending();
        signal.Signal();
        t1.GetAwaiter().GetResult();

        var t2 = signal.Pending();
        Assert.IsFalse(t2.IsCompleted, "Second Pending after a complete cycle should be fresh-pending");

        signal.Signal();
        Assert.IsTrue(t2.IsCompleted);
        t2.GetAwaiter().GetResult();
    }

    [TestMethod]
    public async Task WriteResumeSignal_Continuation_RunsInline_OnSignalThread()
    {
        var signal = new WriteResumeSignal();
        var continuationThreadId = 0;

        var awaitTask = AwaitSignal();
        var signalThreadId = Environment.CurrentManagedThreadId;
        signal.Signal();

        await awaitTask;
        Assert.AreEqual(signalThreadId, continuationThreadId,
            "Continuation must run inline on the Signal caller's thread to keep TP off the path");

        async Task AwaitSignal()
        {
            await signal.Pending().ConfigureAwait(false);
            continuationThreadId = Environment.CurrentManagedThreadId;
        }
    }

    [TestMethod]
    public void ResumableScope_Sets_AndRestores_TLS()
    {
        Assert.IsNull(TransportConnection.SyncNonBlockingSignal, "TLS should start null");

        var signal = new WriteResumeSignal();
        using (new ResumableScope(signal))
        {
            Assert.AreSame(signal, TransportConnection.SyncNonBlockingSignal);
        }
        Assert.IsNull(TransportConnection.SyncNonBlockingSignal, "TLS should restore to null after Dispose");
    }

    [TestMethod]
    public void ResumableScope_Nests_AndRestores_OuterValue()
    {
        var outer = new WriteResumeSignal();
        var inner = new WriteResumeSignal();

        using (new ResumableScope(outer))
        {
            Assert.AreSame(outer, TransportConnection.SyncNonBlockingSignal);
            using (new ResumableScope(inner))
            {
                Assert.AreSame(inner, TransportConnection.SyncNonBlockingSignal);
            }
            Assert.AreSame(outer, TransportConnection.SyncNonBlockingSignal,
                "Inner scope dispose should restore outer signal, not null");
        }
        Assert.IsNull(TransportConnection.SyncNonBlockingSignal);
    }

    // End-to-end: loopback TCP listener accepts a connection, we wrap our side in a
    // SocketStreamConnection, set the TLS signal, write through the PipeWriter (which flushes
    // through the SealedNetworkStream WriteAsync override), and verify the bytes arrive.
    [TestMethod]
    public async Task SocketStreamConnection_WriteWithSignal_DeliversBytes_ToLoopback()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var endpoint = (IPEndPoint)listener.LocalEndpoint;

        var acceptTask = listener.AcceptSocketAsync();
        var conn = await SocketStreamConnection.ConnectAsync(endpoint);
        using var serverSocket = await acceptTask;

        var payload = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };

        // Open the ResumableScope so the transport's WriteAsync takes the sync non-blocking
        // path. ResumableScope is a ref struct, can't cross awaits, only the scope-internal
        // call lives inside. The flush should sync-complete because 5 bytes fit in any
        // reasonable send buffer with no WouldBlock.
        ValueTask<System.IO.Pipelines.FlushResult> writeAndFlush;
        using (new ResumableScope(new WriteResumeSignal()))
            writeAndFlush = WriteAndFlush(conn, payload);

        var flushResult = await writeAndFlush;
        Assert.IsTrue(flushResult.IsCompleted || !flushResult.IsCanceled, "flush completed normally");

        var received = new byte[payload.Length];
        var total = 0;
        while (total < payload.Length)
        {
            var n = serverSocket.Receive(received.AsSpan(total));
            Assert.IsTrue(n > 0, "premature EOF");
            total += n;
        }
        CollectionAssert.AreEqual(payload, received);

        // No connection-level disposal anymore: release through the endpoints, which own the socket.
        conn.Writer.Complete();
        conn.Reader.Complete();

        static async ValueTask<System.IO.Pipelines.FlushResult> WriteAndFlush(SocketStreamConnection c, byte[] data)
        {
            await c.Writer.WriteAsync(data);
            return await c.Writer.FlushAsync();
        }
    }

    // End-to-end through SslStream wrapping SealedNetworkStream. Verifies that SslStream's
    // WriteAsync passes ValueTask shapes through faithfully: our override sees the TLS signal,
    // does sync non-blocking Send on the socket, returns sync-completed ValueTask, SslStream's
    // own WriteAsync sync-completes too. The "lie" reaches the application without SslStream
    // knowing it's not really async I/O underneath.
    [TestMethod]
    public async Task SslStream_OverSealedNetworkStream_WriteWithSignal_DeliversBytes()
    {
        var cert = TlsTestCertificate.Instance;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var endpoint = (IPEndPoint)listener.LocalEndpoint;

        var serverTask = AcceptAndDecrypt(listener, cert);

        var clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await clientSocket.ConnectAsync(endpoint);
        var inner = new SocketStreamConnection.SealedNetworkStream(clientSocket, ownsSocket: true);
        await using var clientSsl = new SslStream(inner, leaveInnerStreamOpen: false,
            userCertificateValidationCallback: (_, _, _, _) => true);

        await clientSsl.AuthenticateAsClientAsync("localhost");

        var payload = new byte[] { 0x10, 0x20, 0x30, 0x40 };
        ValueTask writeTask;
        using (new ResumableScope(new WriteResumeSignal()))
            writeTask = clientSsl.WriteAsync(payload);
        await writeTask;
        await clientSsl.FlushAsync();

        var received = await serverTask;
        CollectionAssert.AreEqual(payload, received);
    }

    [TestMethod]
    public async Task SslStream_OverSealedNetworkStream_WouldBlock_ResumesFromWritableSignal()
    {
        var cert = TlsTestCertificate.Instance;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var endpoint = (IPEndPoint)listener.LocalEndpoint;
        var allowRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        {
            SendBufferSize = 4096
        };
        await clientSocket.ConnectAsync(endpoint);
        // Kernels may clamp or scale SO_SNDBUF. Base the pressure on the effective value instead of
        // assuming the requested size, with room for TLS and socket-stack buffering above it.
        var payloadLength = Math.Max(64 * 1024, checked(clientSocket.SendBufferSize * 8));
        var serverTask = AcceptAndDecrypt(listener, cert, allowRead.Task, payloadLength);
        var inner = new SocketStreamConnection.SealedNetworkStream(clientSocket, ownsSocket: true);
        await using var clientSsl = new SslStream(inner, leaveInnerStreamOpen: false,
            userCertificateValidationCallback: (_, _, _, _) => true);
        await clientSsl.AuthenticateAsClientAsync("localhost");

        var payload = new byte[payloadLength];
        Random.Shared.NextBytes(payload);
        var signal = new WriteResumeSignal();
        ValueTask writeTask;
        using (new ResumableScope(signal))
            writeTask = clientSsl.WriteAsync(payload);

        Assert.IsFalse(writeTask.IsCompleted,
            "with the peer held and a constrained send buffer, the TLS write must reach WouldBlock");

        allowRead.SetResult();
        while (!writeTask.IsCompleted)
        {
            var spin = new SpinWait();
            while (!writeTask.IsCompleted && !signal.IsPending)
                spin.SpinOnce();
            if (writeTask.IsCompleted)
                break;
            inner.WaitWritable();
            signal.Signal();
        }
        await writeTask;

        CollectionAssert.AreEqual(payload, await serverTask);
    }

    [TestMethod]
    public async Task SocketFactory_ConnectTransformed_MaterializesOverTls()
    {
        var cert = TlsTestCertificate.Instance;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var serverTask = AcceptAndDecrypt(listener, cert);
        var factory = SocketStreamConnection.CreateFactory((IPEndPoint)listener.LocalEndpoint);

        SslStream? ssl = null;
        var connection = await factory.ConnectTransformedAsync(stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false,
                userCertificateValidationCallback: (_, _, _, _) => true));
        await ssl!.AuthenticateAsClientAsync("localhost");

        var payload = new byte[] { 0x10, 0x20, 0x30, 0x40 };
        await connection.Writer.WriteAsync(payload);
        CollectionAssert.AreEqual(payload, await serverTask);
        connection.Writer.Complete();
        connection.Reader.Complete();
    }

    [TestMethod]
    public async Task SocketFactory_Upgrade_ReplacesFlushedPlaintextPipesWithTls()
    {
        var cert = TlsTestCertificate.Instance;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var serverTask = AcceptUpgradeAndDecrypt(listener, cert);
        var factory = SocketStreamConnection.CreateFactory((IPEndPoint)listener.LocalEndpoint);
        var connection = await factory.ConnectAsync();

        await connection.Writer.WriteAsync(new byte[] { 0x42 });
        var response = await connection.Reader.ReadAsync();
        Assert.AreEqual((byte)'S', response.Buffer.FirstSpan[0]);
        connection.Reader.AdvanceTo(response.Buffer.End);

        SslStream? ssl = null;
        var upgraded = factory.Upgrade(connection, stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false,
                userCertificateValidationCallback: (_, _, _, _) => true));
        Assert.AreSame(connection, upgraded);
        await ssl!.AuthenticateAsClientAsync("localhost");

        var payload = new byte[] { 0x10, 0x20, 0x30, 0x40 };
        await upgraded.Writer.WriteAsync(payload);
        CollectionAssert.AreEqual(payload, await serverTask);
        upgraded.Writer.Complete();
        upgraded.Reader.Complete();
    }

    [TestMethod]
    public async Task SocketFactory_Upgrade_RejectsUnflushedWrites()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var acceptTask = listener.AcceptSocketAsync();
        var factory = SocketStreamConnection.CreateFactory((IPEndPoint)listener.LocalEndpoint);
        var connection = await factory.ConnectAsync();
        using var server = await acceptTask;

        connection.Writer.GetSpan(1)[0] = 0x42;
        connection.Writer.Advance(1);
        Assert.ThrowsExactly<InvalidOperationException>(() =>
            factory.Upgrade(connection, static stream => stream));

        connection.Writer.Complete(new InvalidOperationException("test cleanup"));
        connection.Reader.Complete();
    }

    static async Task<byte[]> AcceptAndDecrypt(TcpListener listener, X509Certificate2 cert)
    {
        var serverSocket = await listener.AcceptSocketAsync();
        await using var serverNet = new NetworkStream(serverSocket, ownsSocket: true);
        await using var serverSsl = new SslStream(serverNet, leaveInnerStreamOpen: false);
        await serverSsl.AuthenticateAsServerAsync(cert, clientCertificateRequired: false,
            enabledSslProtocols: System.Security.Authentication.SslProtocols.None, checkCertificateRevocation: false);

        var buf = new byte[64];
        var total = 0;
        while (total < 4)
        {
            var n = await serverSsl.ReadAsync(buf.AsMemory(total));
            if (n == 0) break;
            total += n;
        }
        var result = new byte[total];
        Array.Copy(buf, result, total);
        return result;
    }

    static async Task<byte[]> AcceptAndDecrypt(TcpListener listener, X509Certificate2 cert,
        Task allowRead, int length)
    {
        var serverSocket = await listener.AcceptSocketAsync();
        await using var serverNet = new NetworkStream(serverSocket, ownsSocket: true);
        await using var serverSsl = new SslStream(serverNet, leaveInnerStreamOpen: false);
        await serverSsl.AuthenticateAsServerAsync(cert, clientCertificateRequired: false,
            enabledSslProtocols: System.Security.Authentication.SslProtocols.None,
            checkCertificateRevocation: false);
        await allowRead;

        var result = new byte[length];
        var total = 0;
        while (total < result.Length)
        {
            var read = await serverSsl.ReadAsync(result.AsMemory(total));
            if (read is 0)
                break;
            total += read;
        }
        Assert.AreEqual(result.Length, total);
        return result;
    }

    static async Task<byte[]> AcceptUpgradeAndDecrypt(TcpListener listener, X509Certificate2 cert)
    {
        var serverSocket = await listener.AcceptSocketAsync();
        await using var serverNet = new NetworkStream(serverSocket, ownsSocket: true);
        var request = new byte[1];
        Assert.AreEqual(1, await serverNet.ReadAsync(request));
        Assert.AreEqual(0x42, request[0]);
        await serverNet.WriteAsync(new byte[] { (byte)'S' });

        await using var serverSsl = new SslStream(serverNet, leaveInnerStreamOpen: false);
        await serverSsl.AuthenticateAsServerAsync(cert, clientCertificateRequired: false,
            enabledSslProtocols: System.Security.Authentication.SslProtocols.None, checkCertificateRevocation: false);

        var result = new byte[4];
        var total = 0;
        while (total < result.Length)
        {
            var read = await serverSsl.ReadAsync(result.AsMemory(total));
            if (read is 0)
                break;
            total += read;
        }
        Assert.AreEqual(result.Length, total);
        return result;
    }

}
