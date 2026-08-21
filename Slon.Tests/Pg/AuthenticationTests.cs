using System.Net;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pipelines;
using Slon.Transport;
using GssAuthentication = Slon.Pg.Protocol.Flows.StartupFlow.GssAuthentication;
using ScramSha256 = Slon.Pg.Protocol.Flows.StartupFlow.ScramSha256;

namespace Slon.Tests.Pg;

[TestClass]
public class AuthenticationTests
{
    [TestMethod]
    public void ScramSha256_ValidatesServerTranscript()
    {
        const string password = "pencil";
        using var scram = ScramSha256.Create(["SCRAM-SHA-256"], password,
            PostgreSqlChannelBinding.Prefer, null);
        var initial = Encoding.UTF8.GetString(scram.CreateInitialResponse());
        var clientFirstBare = initial[3..];
        var clientNonce = clientFirstBare[(clientFirstBare.IndexOf("r=", StringComparison.Ordinal) + 2)..];
        var salt = Convert.ToBase64String("salt"u8);
        var serverFirst = $"r={clientNonce}server,s={salt},i=4096";

        var clientFinal = Encoding.UTF8.GetString(scram.ProcessServerFirst(Encoding.UTF8.GetBytes(serverFirst)));
        var clientFinalWithoutProof = clientFinal[..clientFinal.LastIndexOf(",p=", StringComparison.Ordinal)];
        var authMessage = $"{clientFirstBare},{serverFirst},{clientFinalWithoutProof}";
        var saltedPassword = Rfc2898DeriveBytes.Pbkdf2(password, "salt"u8, 4096,
            HashAlgorithmName.SHA256, SHA256.HashSizeInBytes);
        var serverKey = HMACSHA256.HashData(saltedPassword, "Server Key"u8);
        var expected = HMACSHA256.HashData(serverKey, Encoding.UTF8.GetBytes(authMessage));

        scram.ValidateServerFinal(Encoding.UTF8.GetBytes($"v={Convert.ToBase64String(expected)}"));
        CryptographicOperations.ZeroMemory(saltedPassword);
        CryptographicOperations.ZeroMemory(serverKey);
        CryptographicOperations.ZeroMemory(expected);
    }

    [TestMethod]
    public void ScramSha256_RejectsInvalidServerSignature()
    {
        using var scram = ScramSha256.Create(["SCRAM-SHA-256"], "password",
            PostgreSqlChannelBinding.Prefer, null);
        var initial = Encoding.UTF8.GetString(scram.CreateInitialResponse());
        var nonce = initial[(initial.IndexOf("r=", StringComparison.Ordinal) + 2)..];
        scram.ProcessServerFirst(Encoding.UTF8.GetBytes(
            $"r={nonce}server,s={Convert.ToBase64String("salt"u8)},i=4096"));

        var exception = Assert.ThrowsExactly<PgProtocolException>(() =>
            scram.ValidateServerFinal("v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="u8));
        StringAssert.Contains(exception.Message, "server signature");
        Assert.IsNull(exception.InnerException);
    }

    [TestMethod]
    public void ScramSha256_RejectsExcessiveIterationCount()
    {
        using var scram = ScramSha256.Create(["SCRAM-SHA-256"], "password",
            PostgreSqlChannelBinding.Prefer, null);
        var initial = Encoding.UTF8.GetString(scram.CreateInitialResponse());
        var nonce = initial[(initial.IndexOf("r=", StringComparison.Ordinal) + 2)..];

        var exception = Assert.ThrowsExactly<PgProtocolException>(() =>
            scram.ProcessServerFirst(Encoding.UTF8.GetBytes(
                $"r={nonce}server,s={Convert.ToBase64String("salt"u8)},i={ScramSha256.MaximumIterationCount + 1}")));
        StringAssert.Contains(exception.Message, "iteration count");
        Assert.IsNull(exception.InnerException);
    }

    [TestMethod]
    public void ScramSha256Plus_UsesTlsServerEndPoint()
    {
        var certificate = TlsTestCertificate.Instance;

        using var scram = ScramSha256.Create(["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"], "password",
            PostgreSqlChannelBinding.Prefer, certificate);

        Assert.AreEqual("SCRAM-SHA-256-PLUS", scram.Mechanism);
        var initial = Encoding.UTF8.GetString(scram.CreateInitialResponse());
        StringAssert.StartsWith(initial, "p=tls-server-end-point,,");
        var nonce = initial[(initial.IndexOf("r=", StringComparison.Ordinal) + 2)..];
        var final = Encoding.UTF8.GetString(scram.ProcessServerFirst(Encoding.UTF8.GetBytes(
            $"r={nonce}server,s={Convert.ToBase64String("salt"u8)},i=4096")));
        var bindingText = final[2..final.IndexOf(',', 2)];
        var expectedBinding = Encoding.UTF8.GetBytes("p=tls-server-end-point,,")
            .Concat(certificate.GetCertHash(HashAlgorithmName.SHA256)).ToArray();
        CollectionAssert.AreEqual(expectedBinding, Convert.FromBase64String(bindingText));
    }

    [TestMethod]
    public void ScramSha256Plus_RequiredWithoutTls_Throws()
    {
        var exception = Assert.ThrowsExactly<PgClientException>(() =>
            ScramSha256.Create(["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"], "password",
                PostgreSqlChannelBinding.Require, null));
        Assert.IsInstanceOfType<AuthenticationException>(exception.InnerException);
    }

    [TestMethod]
    public void ScramSha256Plus_RequiredWhenServerDoesNotOfferPlus_Throws()
    {
        var certificate = TlsTestCertificate.Instance;

        var exception = Assert.ThrowsExactly<PgClientException>(() =>
            ScramSha256.Create(["SCRAM-SHA-256"], "password",
                PostgreSqlChannelBinding.Require, certificate));
        Assert.IsInstanceOfType<AuthenticationException>(exception.InnerException);
    }

    [TestMethod]
    public void ScramSha256Plus_PreferWithoutTls_FallsBackWithUnsupportedFlag()
    {
        using var scram = ScramSha256.Create(["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"], "password",
            PostgreSqlChannelBinding.Prefer, null);

        Assert.AreEqual("SCRAM-SHA-256", scram.Mechanism);
        StringAssert.StartsWith(Encoding.UTF8.GetString(scram.CreateInitialResponse()), "n,,");
    }

    [TestMethod]
    public void ScramSha256Plus_PreferWhenServerDoesNotOfferPlus_UsesSupportedFlag()
    {
        var certificate = TlsTestCertificate.Instance;
        using var scram = ScramSha256.Create(["SCRAM-SHA-256"], "password",
            PostgreSqlChannelBinding.Prefer, certificate);

        Assert.AreEqual("SCRAM-SHA-256", scram.Mechanism);
        StringAssert.StartsWith(Encoding.UTF8.GetString(scram.CreateInitialResponse()), "y,,");
    }

    [TestMethod]
    public void ScramSha256Plus_Disabled_IgnoresAvailableBinding()
    {
        var certificate = TlsTestCertificate.Instance;
        using var scram = ScramSha256.Create(["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"], "password",
            PostgreSqlChannelBinding.Disable, certificate);

        Assert.AreEqual("SCRAM-SHA-256", scram.Mechanism);
        StringAssert.StartsWith(Encoding.UTF8.GetString(scram.CreateInitialResponse()), "n,,");
    }

    [TestMethod]
    [DataRow("1.2.840.113549.1.1.4", "SHA256")] // md5WithRSAEncryption
    [DataRow("1.2.840.113549.1.1.5", "SHA256")] // sha1WithRSAEncryption
    [DataRow("1.2.840.10040.4.3", "SHA256")] // dsaWithSha1
    [DataRow("1.2.840.10045.4.1", "SHA256")] // ecdsaWithSha1
    [DataRow("1.2.840.113549.1.1.11", "SHA256")]
    [DataRow("1.2.840.10045.4.3.2", "SHA256")]
    [DataRow("1.2.840.113549.1.1.12", "SHA384")]
    [DataRow("1.2.840.10045.4.3.3", "SHA384")]
    [DataRow("1.2.840.113549.1.1.13", "SHA512")]
    [DataRow("1.2.840.10045.4.3.4", "SHA512")]
    public void TlsServerEndPoint_MapsCertificateSignatureHash(string oid, string expected)
        => Assert.AreEqual(expected, ScramSha256.GetTlsServerEndPointHashAlgorithm(oid).Name);

    [TestMethod]
    [DataRow(null)]
    [DataRow("")]
    [DataRow("1.2.840.113549.1.1.10")] // RSASSA-PSS: hash is carried in parameters.
    [DataRow("1.3.101.112")] // Ed25519
    [DataRow("1.2.3.4")]
    public void TlsServerEndPoint_RejectsUnsupportedCertificateSignatureHash(string? oid)
        => Assert.AreEqual(default, ScramSha256.GetTlsServerEndPointHashAlgorithm(oid));

    [TestMethod]
    [DataRow(true, false, "Kerberos")]
    [DataRow(true, true, "Kerberos")]
    [DataRow(false, false, "Kerberos")]
    [DataRow(false, true, "Negotiate")]
    public void IntegratedAuthentication_SelectsPackageForServerRequest(
        bool requiresKerberos, bool allowNtlm, string expected)
        => Assert.AreEqual(expected, GssAuthentication.GetPackage(requiresKerberos, allowNtlm));

    [TestMethod]
    [DataRow(0)] // trust
    [DataRow(3)] // cleartext password
    [DataRow(5)] // MD5 password
    [DataRow(7)] // GSS
    [DataRow(9)] // SSPI
    public async Task ChannelBindingRequired_RejectsOtherAuthenticationBeforeSendingCredentials(int authenticationType)
    {
        // Construct the internal options directly to bypass public validation and pin the flow's
        // defense in depth: no credential may leave even with an impossible configuration.
        var certificate = TlsTestCertificate.Instance;
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "user",
            Password = "secret",
            IntegratedSecurity = new(),
            Ssl = new()
            {
                Mode = PostgreSqlSslMode.Disable,
                ChannelBinding = PostgreSqlChannelBinding.Require
            }
        };
        var transport = new StartupTransport(Authentication(authenticationType, authenticationType == 5 ? new byte[4] : []),
            certificate);
        var protocol = PgClientProtocol.Create(new(options));

        await AssertChannelBindingRejectedAsync(protocol.StartAsync(options, transport));

        AssertNoAuthenticationResponse(transport.WrittenBytes);
    }

    [TestMethod]
    public async Task ChannelBindingRequired_DoesNotSendOAuthBearerToken()
    {
        // Deliberately bypass public validation to exercise the flow-level downgrade guard.
        var certificate = TlsTestCertificate.Instance;
        var providerCalls = 0;
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "user",
            OAuthTokens = new OAuthTokenCache(new()
            {
                TokenProvider = (_, _) =>
                {
                    providerCalls++;
                    return new(new PostgreSqlOAuthToken("secret-token"));
                }
            }, new(new DnsEndPoint("localhost", 5432), "user", "database")),
            Ssl = new()
            {
                Mode = PostgreSqlSslMode.Disable,
                ChannelBinding = PostgreSqlChannelBinding.Require
            }
        };
        var transport = new StartupTransport(Authentication(10, "OAUTHBEARER\0\0"u8), certificate);
        var protocol = PgClientProtocol.Create(new(options));

        await AssertChannelBindingRejectedAsync(protocol.StartAsync(options, transport));

        Assert.AreEqual(0, providerCalls);
        AssertNoAuthenticationResponse(transport.WrittenBytes);
    }

    [TestMethod]
    public async Task UnsupportedAuthentication_IsAClientFailure()
    {
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "user",
            Ssl = new() { Mode = PostgreSqlSslMode.Disable }
        };
        var transport = new StartupTransport(Authentication(123, []));
        var protocol = PgClientProtocol.Create(new(options));

        var exception = await Assert.ThrowsExactlyAsync<PgClientException>(async () =>
            await protocol.StartAsync(options, transport));

        Assert.IsFalse(ContainsException<PgProtocolException>(exception));
        Assert.IsInstanceOfType<NotSupportedException>(exception.InnerException);
    }

    [TestMethod]
    public async Task TruncatedAuthenticationMessage_IsAProtocolFailure()
    {
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "user",
            Ssl = new() { Mode = PostgreSqlSslMode.Disable }
        };
        var transport = new StartupTransport([(byte)'R', 0, 0, 0, 4]);
        var protocol = PgClientProtocol.Create(new(options));

        var exception = await Assert.ThrowsExactlyAsync<PgClientException>(async () =>
            await protocol.StartAsync(options, transport));

        Assert.IsInstanceOfType<PgProtocolException>(exception.InnerException);
    }

    [TestMethod]
    public async Task OAuthTokenCache_CoalescesRefreshes()
    {
        var calls = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = new OAuthTokenCache(new()
        {
            TokenProvider = async (_, cancellationToken) =>
            {
                Interlocked.Increment(ref calls);
                await release.Task.WaitAsync(cancellationToken);
                return new("token", DateTimeOffset.UtcNow.AddMinutes(10));
            }
        }, new(new DnsEndPoint("localhost", 5432), "user", "database"));

        var first = cache.GetAsync(async: true, CancellationToken.None).AsTask();
        var second = cache.GetAsync(async: true, CancellationToken.None).AsTask();
        release.SetResult();

        await Task.WhenAll(first, second);
        Assert.AreEqual(1, calls);
        Assert.AreEqual("token", first.Result.AccessToken);
    }

    [TestMethod]
    public async Task OAuthTokenCache_UsesStillValidTokenWhenRefreshFails()
    {
        var calls = 0;
        var cache = new OAuthTokenCache(new()
        {
            RefreshBeforeExpiration = TimeSpan.FromMinutes(5),
            TokenProvider = (_, _) => ++calls == 1
                ? new(new PostgreSqlOAuthToken("token", DateTimeOffset.UtcNow.AddMinutes(2)))
                : ValueTask.FromException<PostgreSqlOAuthToken>(new IOException("refresh failed"))
        }, new(new DnsEndPoint("localhost", 5432), "user", "database"));

        Assert.AreEqual("token", (await cache.GetAsync(true, CancellationToken.None)).AccessToken);
        Assert.AreEqual("token", (await cache.GetAsync(true, CancellationToken.None)).AccessToken);
        Assert.AreEqual(2, calls);
    }

    [TestMethod]
    public void OAuthTokenCache_SyncColdOpenRunsProviderOffCallerThread()
    {
        var callerThread = Environment.CurrentManagedThreadId;
        var providerThread = 0;
        var cache = new OAuthTokenCache(new()
        {
            TokenProvider = (_, _) =>
            {
                providerThread = Environment.CurrentManagedThreadId;
                return new(new PostgreSqlOAuthToken("token"));
            }
        }, new(new DnsEndPoint("localhost", 5432), "user", "database"));

        var token = cache.GetAsync(async: false, CancellationToken.None).GetAwaiter().GetResult();

        Assert.AreEqual("token", token.AccessToken);
        Assert.AreNotEqual(callerThread, providerThread);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task OAuthBearer_UsesSaslInitialResponse(bool sendsFinal)
    {
        var cache = new OAuthTokenCache(new()
        {
            TokenProvider = (_, _) => new(new PostgreSqlOAuthToken(
                "access-token", DateTimeOffset.UtcNow.AddMinutes(10)))
        }, new(new DnsEndPoint("localhost", 5432), "user", "database"));
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "user",
            Database = "database",
            OAuthTokens = cache,
            AllowInsecureTransport = true,
            Ssl = new() { Mode = PostgreSqlSslMode.Disable }
        };
        var transcript = new List<byte>();
        transcript.AddRange(Authentication(10, "OAUTHBEARER\0\0"u8));
        if (sendsFinal)
            transcript.AddRange(Authentication(12, []));
        transcript.AddRange(Authentication(0, []));
        transcript.AddRange(BackendKeyData());
        transcript.AddRange(ReadyForQuery());
        var transport = new StartupTransport(transcript.ToArray());
        var protocol = PgClientProtocol.Create(new(options));

        await protocol.StartAsync(options, transport);
        var response = FirstTypedMessageAfterStartup(transport.WrittenBytes);
        Assert.AreEqual((byte)'p', response.Type);
        var mechanismEnd = response.Body.IndexOf((byte)0);
        Assert.AreEqual("OAUTHBEARER", Encoding.UTF8.GetString(response.Body[..mechanismEnd]));
        var initialLength = BinaryPrimitives.ReadInt32BigEndian(response.Body[(mechanismEnd + 1)..]);
        var initial = response.Body.AsSpan(mechanismEnd + 5, initialLength);
        Assert.AreEqual("n,,\u0001auth=Bearer access-token\u0001\u0001", Encoding.UTF8.GetString(initial));
        await protocol.CompleteAsync();
    }

    [TestMethod]
    public async Task OAuthBearer_IsRefusedOverUnencryptedTcp()
    {
        var providerCalls = 0;
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "user",
            OAuthTokens = new OAuthTokenCache(new()
            {
                TokenProvider = (_, _) =>
                {
                    providerCalls++;
                    return new(new PostgreSqlOAuthToken("access-token"));
                }
            }, new(new DnsEndPoint("localhost", 5432), "user", null)),
            Ssl = new() { Mode = PostgreSqlSslMode.Disable }
        };
        var transport = new StartupTransport(Authentication(10, "OAUTHBEARER\0\0"u8));
        var protocol = PgClientProtocol.Create(new(options));

        var exception = await Assert.ThrowsExactlyAsync<PgClientException>(async () =>
            await protocol.StartAsync(options, transport));

        Assert.IsTrue(ContainsException<AuthenticationException>(exception));
        Assert.AreEqual(0, providerCalls);
    }

    [TestMethod]
    public async Task OAuthBearer_ErrorJson_IsPreserved()
    {
        const string errorJson = "{\"status\":\"invalid_token\"}";
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "user",
            OAuthTokens = new OAuthTokenCache(new()
            {
                TokenProvider = (_, _) => new(new PostgreSqlOAuthToken("access-token"))
            }, new(new DnsEndPoint("localhost", 5432), "user", null)),
            AllowInsecureTransport = true,
            Ssl = new() { Mode = PostgreSqlSslMode.Disable }
        };
        var transport = new StartupTransport([
            .. Authentication(10, "OAUTHBEARER\0\0"u8),
            .. Authentication(11, Encoding.UTF8.GetBytes(errorJson)),
            .. ErrorResponse("OAuth rejected")
        ]);
        var protocol = PgClientProtocol.Create(new(options));

        var exception = await Assert.ThrowsExactlyAsync<PgClientException>(async () =>
            await protocol.StartAsync(options, transport));

        Assert.IsTrue(ContainsMessage(exception, errorJson));
        Assert.IsTrue(ContainsException<PgErrorException>(exception));
    }

    [TestMethod]
    public async Task CleartextPassword_IsRefusedOverUnencryptedTcp()
    {
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "user",
            Password = "secret",
            Ssl = new() { Mode = PostgreSqlSslMode.Disable }
        };
        var transport = new StartupTransport(Authentication(3, []));
        var protocol = PgClientProtocol.Create(new(options));

        var exception = await Assert.ThrowsExactlyAsync<PgClientException>(async () =>
            await protocol.StartAsync(options, transport));

        Assert.IsTrue(ContainsException<AuthenticationException>(exception));
        AssertNoAuthenticationResponse(transport.WrittenBytes);
    }

    [TestMethod]
    public async Task CleartextPassword_IsAllowedOverUnixSocket()
    {
        var options = new PgClientOptions
        {
            EndPoint = new System.Net.Sockets.UnixDomainSocketEndPoint("/tmp/postgres"),
            Username = "user",
            Password = "secret",
            Ssl = new() { Mode = PostgreSqlSslMode.Disable }
        };
        var transport = new StartupTransport([
            .. Authentication(3, []),
            .. Authentication(0, []),
            .. BackendKeyData(),
            .. ReadyForQuery()
        ]);
        var protocol = PgClientProtocol.Create(new(options));

        await protocol.StartAsync(options, transport);
        var response = FirstTypedMessageAfterStartup(transport.WrittenBytes);
        Assert.AreEqual((byte)'p', response.Type);
        CollectionAssert.AreEqual("secret\0"u8.ToArray(), response.Body.ToArray());
        await protocol.CompleteAsync();
    }

    [TestMethod]
    public async Task InsecureTransportPolicy_AllowsCleartextPasswordOverTcp()
    {
        var options = new PgClientOptions
        {
            EndPoint = new DnsEndPoint("localhost", 5432),
            Username = "user",
            Password = "secret",
            AllowInsecureTransport = true,
            Ssl = new() { Mode = PostgreSqlSslMode.Disable }
        };
        var transport = new StartupTransport([
            .. Authentication(3, []),
            .. Authentication(0, []),
            .. BackendKeyData(),
            .. ReadyForQuery()
        ]);
        var protocol = PgClientProtocol.Create(new(options));

        await protocol.StartAsync(options, transport);

        var response = FirstTypedMessageAfterStartup(transport.WrittenBytes);
        CollectionAssert.AreEqual("secret\0"u8.ToArray(), response.Body.ToArray());
        await protocol.CompleteAsync();
    }

    static async Task AssertChannelBindingRejectedAsync(ValueTask start)
    {
        var exception = await Assert.ThrowsAsync<Exception>(async () => await start);
        Assert.IsTrue(ContainsMessage(exception, "channel binding"));
    }

    static bool ContainsMessage(Exception exception, string value)
        => exception.Message.Contains(value, StringComparison.OrdinalIgnoreCase)
            || exception is AggregateException aggregate
            && aggregate.InnerExceptions.Any(error => ContainsMessage(error, value))
            || exception.InnerException is { } inner
            && ContainsMessage(inner, value);

    static bool ContainsException<T>(Exception exception) where T : Exception
        => exception is T
            || exception is AggregateException aggregate
            && aggregate.InnerExceptions.Any(ContainsException<T>)
            || exception.InnerException is { } inner
            && ContainsException<T>(inner);

    static void AssertNoAuthenticationResponse(byte[] written)
    {
        var offset = BinaryPrimitives.ReadInt32BigEndian(written);
        while (offset < written.Length)
        {
            var type = written[offset];
            var length = BinaryPrimitives.ReadInt32BigEndian(written.AsSpan(offset + 1));
            Assert.AreNotEqual((byte)'p', type,
                "No credential-bearing authentication response may be sent before rejecting the downgrade.");
            offset = checked(offset + 1 + length);
        }
        Assert.AreEqual(written.Length, offset);
    }

    static byte[] Authentication(int type, ReadOnlySpan<byte> payload)
    {
        var result = new byte[9 + payload.Length];
        result[0] = (byte)'R';
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(1), 8 + payload.Length);
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(5), type);
        payload.CopyTo(result.AsSpan(9));
        return result;
    }

    static byte[] ErrorResponse(string message)
    {
        var fields = Encoding.UTF8.GetBytes($"SERROR\0C28000\0M{message}\0\0");
        var result = new byte[5 + fields.Length];
        result[0] = (byte)'E';
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(1), 4 + fields.Length);
        fields.CopyTo(result.AsSpan(5));
        return result;
    }

    static byte[] BackendKeyData()
    {
        var result = new byte[13];
        result[0] = (byte)'K';
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(1), 12);
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(5), 123);
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(9), 456);
        return result;
    }

    static byte[] ReadyForQuery() => [(byte)'Z', 0, 0, 0, 5, (byte)'I'];

    static (byte Type, byte[] Body) FirstTypedMessageAfterStartup(byte[] bytes)
    {
        var startupLength = BinaryPrimitives.ReadInt32BigEndian(bytes);
        var type = bytes[startupLength];
        var length = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(startupLength + 1));
        return (type, bytes.AsSpan(startupLength + 5, length - 4).ToArray());
    }

    sealed class StartupTransport : TransportConnection
    {
        readonly MemoryStream _written = new();
        readonly X509Certificate? _remoteCertificate;

        public StartupTransport(byte[] responses, X509Certificate? remoteCertificate = null)
        {
            _remoteCertificate = remoteCertificate;
            Reader = new DefaultStreamPipeReader(new MemoryStream(responses, writable: false),
                new StreamPipeReaderOptions(useZeroByteReads: false), supportCancelPending: false);
            Writer = new DefaultStreamPipeWriter(_written, new StreamPipeWriterOptions(), supportCancelPending: false);
        }

        public byte[] WrittenBytes => _written.ToArray();
        public override X509Certificate? RemoteCertificate => _remoteCertificate;
        public override PipeReader Reader { get; }
        public override PipeWriter Writer { get; }
        public override void WaitWritable() { }
    }
}
