using System.Buffers.Binary;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Transport;

namespace Slon.Tests.Pg;

[TestClass]
public class PostgreSqlSslTests
{
    [TestMethod]
    public void DefaultMode_PrefersTls()
        => Assert.AreEqual(PostgreSqlSslMode.Prefer, new PostgreSqlSslOptions().Mode);

    [TestMethod]
    [DataRow(PostgreSqlSslMode.Allow)]
    [DataRow(PostgreSqlSslMode.Prefer)]
    [DataRow(PostgreSqlSslMode.Require)]
    [DataRow(PostgreSqlSslMode.VerifyCA)]
    [DataRow(PostgreSqlSslMode.VerifyFull)]
    public void UnixDomainSocket_DoesNotNegotiateTls(PostgreSqlSslMode mode)
    {
        var options = new PostgreSqlSslOptions { Mode = mode };
        var endpoint = new UnixDomainSocketEndPoint("/tmp/.s.PGSQL.5432");
        Assert.IsFalse(options.ShouldNegotiateTls(endpoint));
        Assert.IsFalse(options.ShouldUseDirectTls(endpoint));
    }

    [TestMethod]
    public async Task PostgreSqlNegotiation_UpgradesBeforeStartup()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeAsync(listener, cert, direct: false);

        var connection = await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql).CreateAsync();
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task DirectNegotiation_StartsWithTls()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeAsync(listener, cert, direct: true);

        var connection = await CreateFactory(listener, PostgreSqlSslNegotiation.Direct).CreateAsync();
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task AutomaticNegotiation_UsesDirectTlsForPostgreSql17()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeAsync(listener, cert, direct: true);

        var connection = await CreateFactory(listener, PostgreSqlSslNegotiation.Automatic,
            endpointVersion: new Version(17, 0)).CreateAsync();
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task PostgreSqlNegotiation_SynchronousUpgradeBeforeStartup()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeAsync(listener, cert, direct: false);

        var connection = CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql).Create(TimeSpan.FromSeconds(5));
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task DirectNegotiation_SynchronousTlsBeforeStartup()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeAsync(listener, cert, direct: true);

        var connection = CreateFactory(listener, PostgreSqlSslNegotiation.Direct).Create(TimeSpan.FromSeconds(5));
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task PostgreSqlNegotiation_RejectionFailsConnection()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = RejectAsync(listener, (byte)'N');

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql).CreateAsync());
        await server;
    }

    [TestMethod]
    public async Task PostgreSqlNegotiation_MalformedReplyFailsConnection()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = RejectAsync(listener, (byte)'X');

        var exception = await Assert.ThrowsExactlyAsync<PgProtocolException>(async () =>
            await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql).CreateAsync());
        Assert.IsInstanceOfType<InvalidDataException>(exception.InnerException);
        await server;
    }

    [TestMethod]
    public async Task PostgreSqlNegotiation_EofFailsConnection()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = CloseAfterRequestAsync(listener);

        await Assert.ThrowsExactlyAsync<EndOfStreamException>(async () =>
            await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql).CreateAsync());
        await server;
    }

    [TestMethod]
    public async Task VerifyFull_RejectsUntrustedCertificate()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeAsync(listener, cert, direct: true);

        await Assert.ThrowsExactlyAsync<System.Security.Authentication.AuthenticationException>(async () =>
            await CreateFactory(listener, PostgreSqlSslNegotiation.Direct,
                PostgreSqlSslMode.VerifyFull).CreateAsync());
        try { await server; }
        catch (IOException) { }
    }

    [TestMethod]
    public async Task VerifyCA_RejectsUntrustedCertificate()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeAsync(listener, cert, direct: true);

        await Assert.ThrowsExactlyAsync<System.Security.Authentication.AuthenticationException>(async () =>
            await CreateFactory(listener, PostgreSqlSslNegotiation.Direct,
                PostgreSqlSslMode.VerifyCA).CreateAsync());
        try { await server; }
        catch (IOException) { }
    }

    [TestMethod]
    [DataRow(PostgreSqlSslMode.VerifyCA)]
    [DataRow(PostgreSqlSslMode.VerifyFull)]
    public void VerifyMode_RejectsReplacementCertificateValidation(PostgreSqlSslMode mode)
    {
        var options = new PostgreSqlSslOptions
        {
            Mode = mode,
            ConfigureClientAuthenticationOptions = options =>
                options.RemoteCertificateValidationCallback = static (_, _, _, _) => true
        };

        Assert.ThrowsExactly<InvalidOperationException>(() =>
            options.CreateAuthenticationOptions(new DnsEndPoint("localhost", 5432)));
    }

    [TestMethod]
    public void DirectNegotiation_RejectsReplacementAlpn()
    {
        var options = new PostgreSqlSslOptions
        {
            Mode = PostgreSqlSslMode.Require,
            Negotiation = PostgreSqlSslNegotiation.Direct,
            ConfigureClientAuthenticationOptions = options => options.ApplicationProtocols = []
        };

        Assert.ThrowsExactly<InvalidOperationException>(() =>
            options.CreateAuthenticationOptions(new DnsEndPoint("localhost", 5432)));
    }

    [TestMethod]
    public void DirectNegotiation_AllowsAdditionalAlpnProtocols()
    {
        var options = new PostgreSqlSslOptions
        {
            Mode = PostgreSqlSslMode.Require,
            Negotiation = PostgreSqlSslNegotiation.Direct,
            ConfigureClientAuthenticationOptions = options =>
                options.ApplicationProtocols!.Add(new SslApplicationProtocol("proxy"))
        };

        var authentication = options.CreateAuthenticationOptions(new DnsEndPoint("localhost", 5432));
        CollectionAssert.Contains(authentication.ApplicationProtocols, new SslApplicationProtocol("postgresql"));
        CollectionAssert.Contains(authentication.ApplicationProtocols, new SslApplicationProtocol("proxy"));
    }

    [TestMethod]
    public async Task AutomaticNegotiation_PreferStillUsesSslRequestOnPostgreSql17()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServePreferRejectedAsync(listener);

        var connection = await CreateFactory(listener, PostgreSqlSslNegotiation.Automatic,
            PostgreSqlSslMode.Prefer, new Version(17, 0)).CreateAsync();
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task Prefer_FailedTlsHandshake_DoesNotDowngrade()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var secondConnection = RejectTlsHandshakeAndObserveReconnectAsync(listener);

        Exception? error = null;
        try
        {
            await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql,
                PostgreSqlSslMode.Prefer).CreateAsync();
        }
        catch (Exception ex)
        {
            error = ex;
        }
        Assert.IsNotNull(error);
        Assert.IsFalse(await secondConnection, "A failed TLS handshake must not trigger plaintext fallback.");
    }

    [TestMethod]
    [DataRow(PostgreSqlSslMode.Disable)]
    [DataRow(PostgreSqlSslMode.Allow)]
    [DataRow(PostgreSqlSslMode.Prefer)]
    public void DirectNegotiation_RejectsModesWithPlaintextFallback(PostgreSqlSslMode mode)
    {
        var options = new SlonDataSourceOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "postgres",
            Ssl = new() { Mode = mode, Negotiation = PostgreSqlSslNegotiation.Direct }
        };
        Assert.ThrowsExactly<InvalidOperationException>(() => new SlonDataSource(options));
    }

    [TestMethod]
    public async Task Allow_InitialConnectFailure_DoesNotRetry()
    {
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "postgres",
            Ssl = new() { Mode = PostgreSqlSslMode.Allow }
        };
        var transportFactory = new FailingTransportFactory();
        var factory = new PgConnectionFactory(options, transportFactory);

        await Assert.ThrowsExactlyAsync<IOException>(async () => await factory.CreateAsync());
        Assert.AreEqual(1, transportFactory.Attempts);
    }

    [TestMethod]
    public async Task Prefer_CancellationAfterTls_DoesNotRetry()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var secondConnection = HoldEncryptedStartupAndObserveReconnectAsync(listener, cert);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql,
                PostgreSqlSslMode.Prefer).CreateAsync(cancellation.Token));
        Assert.IsFalse(await secondConnection, "Cancellation must not trigger plaintext fallback.");
    }

    [TestMethod]
    public async Task Prefer_EncryptedStartupFailure_RetriesPlaintext()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServePreferFallbackAsync(listener, cert);

        var connection = await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql,
            PostgreSqlSslMode.Prefer).CreateAsync();
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task Prefer_EncryptedStartupFailure_RetriesPlaintextSynchronously()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServePreferFallbackAsync(listener, cert);

        var connection = CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql,
            PostgreSqlSslMode.Prefer).Create(TimeSpan.FromSeconds(5));
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task Allow_PlaintextStartupFailure_RetriesTls()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeAllowFallbackAsync(listener, cert);

        var connection = await CreateFactory(listener, PostgreSqlSslNegotiation.Automatic,
            PostgreSqlSslMode.Allow).CreateAsync();
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task Allow_PlaintextStartupFailure_RetriesTlsSynchronously()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeAllowFallbackAsync(listener, cert);

        var connection = CreateFactory(listener, PostgreSqlSslNegotiation.Automatic,
            PostgreSqlSslMode.Allow).Create(TimeSpan.FromSeconds(5));
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    public async Task Prefer_ServerRejectsTls_ContinuesPlaintextOnSameConnection()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServePreferRejectedAsync(listener);

        var connection = await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql,
            PostgreSqlSslMode.Prefer).CreateAsync();
        await server;
        await connection.CompleteAsync();
    }

    [TestMethod]
    [DataRow((byte)'S')]
    [DataRow((byte)'N')]
    public async Task PostgreSqlNegotiation_AdditionalPlaintextData_FailsConnection(byte response)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = RejectWithAdditionalDataAsync(listener, response);

        var exception = await Assert.ThrowsExactlyAsync<PgProtocolException>(async () =>
            await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql,
                PostgreSqlSslMode.Prefer).CreateAsync());
        Assert.IsInstanceOfType<InvalidDataException>(exception.InnerException);
        await server;
    }

    [TestMethod]
    public async Task PostgreSqlNegotiation_EncryptsCancelRequest()
    {
        using var cert = CreateSelfSignedCert();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var server = ServeEncryptedCancelAsync(listener, cert, 123, 456);

        var delivery = await CreateFactory(listener, PostgreSqlSslNegotiation.PostgreSql)
            .SendCancelAsync(123, 456, CancellationToken.None);

        Assert.AreEqual(CancelRequestState.Sent, delivery);
        await server;
    }

    static PgConnectionFactory CreateFactory(TcpListener listener, PostgreSqlSslNegotiation negotiation,
        PostgreSqlSslMode mode = PostgreSqlSslMode.Require, Version? endpointVersion = null)
    {
        var endpoint = (IPEndPoint)listener.LocalEndpoint;
        var options = new PgClientOptions
        {
            EndPoint = endpoint,
            Username = "postgres",
            Ssl = new()
            {
                Mode = mode,
                Negotiation = negotiation,
                EndpointVersion = endpointVersion,
                ConfigureClientAuthenticationOptions = options => options.TargetHost = "localhost"
            },
            ConnectionTimeout = TimeSpan.FromSeconds(5)
        };
        return new PgConnectionFactory(options, SocketStreamConnection.CreateFactory(endpoint));
    }

    static async Task ServeAsync(TcpListener listener, X509Certificate2 cert, bool direct)
    {
        using var socket = await listener.AcceptSocketAsync();
        await using var network = new NetworkStream(socket, ownsSocket: false);
        if (!direct)
        {
            var sslRequest = new byte[8];
            await network.ReadExactlyAsync(sslRequest);
            Assert.AreEqual(8, BinaryPrimitives.ReadInt32BigEndian(sslRequest));
            Assert.AreEqual(80877103, BinaryPrimitives.ReadInt32BigEndian(sslRequest.AsSpan(4)));
            await network.WriteAsync(new byte[] { (byte)'S' });
        }

        await using var ssl = new SslStream(network, leaveInnerStreamOpen: true);
        await AuthenticateServerAsync(ssl, cert);
        Assert.AreEqual(new SslApplicationProtocol("postgresql"), ssl.NegotiatedApplicationProtocol);

        await ReadStartupAndRespondAsync(ssl);
    }

    static async Task RejectAsync(TcpListener listener, byte response)
    {
        using var socket = await listener.AcceptSocketAsync();
        await using var stream = new NetworkStream(socket, ownsSocket: false);
        var request = new byte[8];
        await stream.ReadExactlyAsync(request);
        await stream.WriteAsync(new[] { response });
    }

    static async Task RejectWithAdditionalDataAsync(TcpListener listener, byte response)
    {
        using var socket = await listener.AcceptSocketAsync();
        await using var stream = new NetworkStream(socket, ownsSocket: false);
        await ExpectSslRequestAsync(stream);
        await stream.WriteAsync(new[] { response, (byte)0 });
    }

    static async Task ServeEncryptedCancelAsync(TcpListener listener, X509Certificate2 certificate,
        int processId, int secretKey)
    {
        using var socket = await listener.AcceptSocketAsync();
        await using var network = new NetworkStream(socket, ownsSocket: false);
        await ExpectSslRequestAsync(network);
        await network.WriteAsync(new byte[] { (byte)'S' });
        await using var ssl = new SslStream(network, leaveInnerStreamOpen: true);
        await AuthenticateServerAsync(ssl, certificate);

        var request = new byte[16];
        await ssl.ReadExactlyAsync(request);
        Assert.AreEqual(16, BinaryPrimitives.ReadInt32BigEndian(request));
        Assert.AreEqual(80877102, BinaryPrimitives.ReadInt32BigEndian(request.AsSpan(4)));
        Assert.AreEqual(processId, BinaryPrimitives.ReadInt32BigEndian(request.AsSpan(8)));
        Assert.AreEqual(secretKey, BinaryPrimitives.ReadInt32BigEndian(request.AsSpan(12)));
    }

    static async Task CloseAfterRequestAsync(TcpListener listener)
    {
        using var socket = await listener.AcceptSocketAsync();
        await using var stream = new NetworkStream(socket, ownsSocket: false);
        await stream.ReadExactlyAsync(new byte[8]);
    }

    static async Task ServePreferFallbackAsync(TcpListener listener, X509Certificate2 cert)
    {
        using (var first = await listener.AcceptSocketAsync())
        await using (var network = new NetworkStream(first, ownsSocket: false))
        {
            await ExpectSslRequestAsync(network);
            await network.WriteAsync(new byte[] { (byte)'S' });
            await using var ssl = new SslStream(network, leaveInnerStreamOpen: true);
            await AuthenticateServerAsync(ssl, cert);
            await ReadStartupAsync(ssl);
        }

        using var second = await listener.AcceptSocketAsync();
        await using var plaintext = new NetworkStream(second, ownsSocket: false);
        await ReadStartupAndRespondAsync(plaintext);
    }

    static async Task ServeAllowFallbackAsync(TcpListener listener, X509Certificate2 cert)
    {
        using (var first = await listener.AcceptSocketAsync())
        await using (var plaintext = new NetworkStream(first, ownsSocket: false))
            await ReadStartupAsync(plaintext);

        using var second = await listener.AcceptSocketAsync();
        await using var network = new NetworkStream(second, ownsSocket: false);
        await ExpectSslRequestAsync(network);
        await network.WriteAsync(new byte[] { (byte)'S' });
        await using var ssl = new SslStream(network, leaveInnerStreamOpen: true);
        await AuthenticateServerAsync(ssl, cert);
        await ReadStartupAndRespondAsync(ssl);
    }

    static async Task ServePreferRejectedAsync(TcpListener listener)
    {
        using var socket = await listener.AcceptSocketAsync();
        await using var stream = new NetworkStream(socket, ownsSocket: false);
        await ExpectSslRequestAsync(stream);
        await stream.WriteAsync(new byte[] { (byte)'N' });
        await ReadStartupAndRespondAsync(stream);
    }

    static async Task<bool> RejectTlsHandshakeAndObserveReconnectAsync(TcpListener listener)
    {
        using (var socket = await listener.AcceptSocketAsync())
        await using (var stream = new NetworkStream(socket, ownsSocket: false))
        {
            await ExpectSslRequestAsync(stream);
            await stream.WriteAsync(new byte[] { (byte)'S' });
        }

        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        try
        {
            using var second = await listener.AcceptSocketAsync(timeout.Token);
            return true;
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            return false;
        }
    }

    static async Task<bool> HoldEncryptedStartupAndObserveReconnectAsync(TcpListener listener, X509Certificate2 cert)
    {
        using var socket = await listener.AcceptSocketAsync();
        await using var network = new NetworkStream(socket, ownsSocket: false);
        await ExpectSslRequestAsync(network);
        await network.WriteAsync(new byte[] { (byte)'S' });
        await using var ssl = new SslStream(network, leaveInnerStreamOpen: true);
        await AuthenticateServerAsync(ssl, cert);
        await ReadStartupAsync(ssl);

        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        try
        {
            using var second = await listener.AcceptSocketAsync(timeout.Token);
            return true;
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            return false;
        }
    }

    static async Task ExpectSslRequestAsync(Stream stream)
    {
        var request = new byte[8];
        await stream.ReadExactlyAsync(request);
        Assert.AreEqual(8, BinaryPrimitives.ReadInt32BigEndian(request));
        Assert.AreEqual(80877103, BinaryPrimitives.ReadInt32BigEndian(request.AsSpan(4)));
    }

    static async Task ReadStartupAndRespondAsync(Stream stream)
    {
        await ReadStartupAsync(stream);
        await stream.WriteAsync(StartupResponse());
        await stream.FlushAsync();
    }

    static Task AuthenticateServerAsync(SslStream stream, X509Certificate2 certificate)
        => stream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
        {
            ServerCertificate = certificate,
            ApplicationProtocols = [new SslApplicationProtocol("postgresql")]
        });

    static async Task ReadStartupAsync(Stream stream)
    {
        var lengthBytes = new byte[4];
        await stream.ReadExactlyAsync(lengthBytes);
        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);
        Assert.IsGreaterThanOrEqualTo(8, length);
        await stream.ReadExactlyAsync(new byte[length - 4]);
    }

    static byte[] StartupResponse()
    {
        var response = new byte[28];
        var offset = 0;
        response[offset++] = (byte)'R';
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(offset), 8); offset += 4;
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(offset), 0); offset += 4;
        response[offset++] = (byte)'K';
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(offset), 12); offset += 4;
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(offset), 123); offset += 4;
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(offset), 456); offset += 4;
        response[offset++] = (byte)'Z';
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(offset), 5); offset += 4;
        response[offset] = (byte)'I';
        return response;
    }

    static X509Certificate2 CreateSelfSignedCert()
    {
        using var ecdsa = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=localhost", ecdsa, HashAlgorithmName.SHA256);
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        request.CertificateExtensions.Add(names.Build());
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }

    sealed class FailingTransportFactory : TransportConnection.Factory
    {
        public int Attempts { get; private set; }
        public override bool SupportsSynchronousIO => true;

        public override TransportConnection ConnectTransformed(Func<Stream, Stream> transform, TimeSpan timeout = default)
        {
            Attempts++;
            throw new IOException("connect failed");
        }

        public override ValueTask<TransportConnection> ConnectTransformedAsync(Func<Stream, Stream> transform,
            CancellationToken cancellationToken = default)
        {
            Attempts++;
            return ValueTask.FromException<TransportConnection>(new IOException("connect failed"));
        }

        public override TransportConnection Upgrade(TransportConnection connection, Func<Stream, Stream> transform)
            => throw new InvalidOperationException();
    }
}
