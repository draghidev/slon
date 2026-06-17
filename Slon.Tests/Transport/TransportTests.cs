using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
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
        await using var conn = await SocketStreamConnection.ConnectAsync(endpoint);
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
        using var cert = CreateSelfSignedCert();
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

    static X509Certificate2 CreateSelfSignedCert()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var sanBuilder = new SubjectAlternativeNameBuilder();
        sanBuilder.AddDnsName("localhost");
        req.CertificateExtensions.Add(sanBuilder.Build());
        var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var pfx = cert.Export(X509ContentType.Pfx);
        return X509CertificateLoader.LoadPkcs12(pfx, password: null);
    }
}
