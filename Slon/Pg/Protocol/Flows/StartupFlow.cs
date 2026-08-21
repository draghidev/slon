using System.Buffers;
using System.Net.Sockets;
using System.Security.Authentication;
using Slon.Buffers;
using Slon.Buffers.Binary;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol.Flows;

sealed partial class StartupFlow : PgClientFlow
{
    readonly PgClientOptions _options;
    readonly TimeSpan _startupTimeout;
    readonly List<KeyValuePair<string, string>> _parameters;
    readonly X509Certificate? _remoteCertificate;
    // Sync startup hands the body off to the Start() caller's thread: that caller parks in
    // WaitForExecutor on this MRES until the executor reaches and holds this flow, then drives the
    // handshake inline on its own thread (no TP escape during connection open). One-shot flow (fresh per
    // open, not pooled), so the MRES is allocated eagerly when sync; an async startup never parks => null,
    // the waiter-presence gate fec0355 keys on. The scripted body runs straight through (no consumer gate /
    // WaitForContinuation), so the MRES is only the handoff park, never reused for body rendezvous.
    protected override ManualResetEventSlim? HandoffEvent { get; }

    // Parsed from the BackendKeyData wire message. Pulled by PgClientProtocol after the flow
    // completes; the protocol stores them as its own fields and exposes them via Control.
    // 0 = not yet parsed (the wire format guarantees ProcessId is a non-zero OS PID, so 0 is a
    // safe "not received" sentinel; SecretKey is opaque 32-bit so ProcessId is the indicator).
    internal int BackendProcessId { get; private set; }
    internal int BackendSecretKey { get; private set; }
    internal PgClientOptions Options => _options;

    public StartupFlow(bool async, PgClientOptions options, X509Certificate? remoteCertificate,
        TimeSpan startupTimeout = default)
    {
        _options = options;
        _remoteCertificate = remoteCertificate;
        _startupTimeout = startupTimeout;
        _parameters = [
            new("user", options.Username),
            new("client_encoding", options.Encoding.WebName)
        ];
        if (options.Database is not null)
            _parameters.Add(new KeyValuePair<string, string>("database", options.Database));
        IsAsync = async;
        if (!async)
            HandoffEvent = new(false);
    }

    protected override async ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        var spanWriter = new SpanWriter<ArrayBufferWriter<byte>, byte>(new ArrayBufferWriter<byte>(4096));
        var encoder = context.GetEncoder();
        const int protocolVersion3 = 3 << 16; // 196608
        var startSegment = spanWriter;
        // Empty space for the length.
        spanWriter.WriteUInt32BigEndian(0);
        spanWriter.WriteInt32BigEndian(protocolVersion3);

        foreach (var kv in _parameters)
        {
            spanWriter.WriteStringWithNullTerminator(kv.Key, PgClientOptions.PreStartupEncoding);
            spanWriter.WriteStringWithNullTerminator(kv.Value, PgClientOptions.PreStartupEncoding);
        }

        spanWriter.WriteByte(0);
        spanWriter.Commit();
        // Write the length into the empty space left previously.
        startSegment.WriteUInt32BigEndian((uint)spanWriter.Committed);

        encoder.CopyStartupBuffer(spanWriter.InnerWriter.WrittenSpan);
        await encoder.FlushAuto().ConfigureAwait(false);

        var decoder = await context.GetDecoderAuto().ConfigureAwait(false);
        decoder.UseReadTimeout(_startupTimeout);
        var message = await decoder.GetNextAuto().ConfigureAwait(false);
        var authType = ParseAuthMessage(message, out var reader);
        var requireChannelBinding = _options.Ssl.ChannelBinding is PostgreSqlChannelBinding.Require;
        var channelBound = false;
        if (requireChannelBinding && authType is not AuthenticationType.SASL)
            throw new PgClientException(new AuthenticationException(
                "SCRAM channel binding was required but PostgreSQL selected another authentication method."));
        switch (authType)
        {
            case AuthenticationType.Ok:
                // No auth required, nothing more to do.
                break;
            case AuthenticationType.CleartextPassword:
                if (_options.Password is null)
                    throw new InvalidOperationException("No password given, connection expects password authentication.");
                if (_remoteCertificate is null && _options.EndPoint is not UnixDomainSocketEndPoint
                    && !_options.AllowInsecureTransport)
                    throw new PgClientException(new AuthenticationException(
                        "Refusing to send a cleartext password over an unencrypted TCP connection."));
                encoder.WritePasswordResponse(_options.Password, _options.PasswordEncoding);
                await encoder.FlushAuto().ConfigureAwait(false);
                await ExpectAuthOkAsync(decoder, "after the password response").ConfigureAwait(false);
                break;
            case AuthenticationType.MD5Password:
                Span<byte> salt = stackalloc byte[4];
                if (!reader.TryCopyTo(salt))
                    throw PgProtocolException.NotEnoughData(nameof(salt));
                reader.Advance(4);

                if (_options.Password is null)
                    throw new InvalidOperationException("No password given, connection expects password.");

                encoder.WritePasswordResponse(
                    Md5Password.CreateResponse(_options.Username, _options.Password, salt, _options.PasswordEncoding),
                    _options.PasswordEncoding);
                await encoder.FlushAuto().ConfigureAwait(false);

                await ExpectAuthOkAsync(decoder, "after the password response").ConfigureAwait(false);
                break;
            case AuthenticationType.GSS:
            case AuthenticationType.SSPI:
                if (_options.IntegratedSecurity is null)
                    throw new InvalidOperationException("PostgreSQL requested integrated authentication, but it is not configured.");
                using (var gss = new GssAuthentication(_options.IntegratedSecurity, _options.EndPoint,
                           requiresKerberos: authType is AuthenticationType.GSS))
                {
                    var outgoing = gss.GetOutgoingBlob([])
                        ?? throw new PgClientException(
                            new AuthenticationException("Integrated authentication produced no initial token."));
                    encoder.WriteAuthenticationResponse(outgoing);
                    await encoder.FlushAuto().ConfigureAwait(false);
                    while (true)
                    {
                        message = await decoder.GetNextAuto().ConfigureAwait(false);
                        authType = ParseAuthMessage(message, out reader);
                        if (authType is AuthenticationType.Ok)
                        {
                            if (!gss.IsAuthenticated)
                                throw new PgProtocolException(
                                    "PostgreSQL completed authentication before the GSS context was authenticated.");
                            break;
                        }
                        if (authType is not AuthenticationType.GSSContinue)
                            throw new PgProtocolException("Expected a GSS authentication continuation.");
                        outgoing = gss.GetOutgoingBlob(ReadRemaining(ref reader));
                        if (outgoing is not null)
                        {
                            encoder.WriteAuthenticationResponse(outgoing);
                            await encoder.FlushAuto().ConfigureAwait(false);
                        }
                    }
                }
                break;
            case AuthenticationType.SASL:
                if (_options.Password is null && _options.OAuthTokens is null)
                    throw new InvalidOperationException("No authentication credential was provided for SASL.");
                var mechanisms = ReadMechanisms(ref reader);
                if (!requireChannelBinding
                    && _options.OAuthTokens is not null && mechanisms.Contains("OAUTHBEARER"))
                {
                    if (_remoteCertificate is null && _options.EndPoint is not UnixDomainSocketEndPoint
                        && !_options.AllowInsecureTransport)
                        throw new PgClientException(new AuthenticationException(
                            "Refusing to send an OAuth bearer token over an unencrypted connection."));
                    var token = await _options.OAuthTokens.GetAsync(IsAsync, context.StoppingToken).ConfigureAwait(false);
                    var response = Encoding.UTF8.GetBytes($"n,,\u0001auth=Bearer {token.AccessToken}\u0001\u0001");
                    encoder.WriteSaslInitialResponse("OAUTHBEARER", response);
                    await encoder.FlushAuto().ConfigureAwait(false);
                    message = await decoder.GetNextAuto().ConfigureAwait(false);
                    authType = ParseAuthMessage(message, out reader);
                    if (authType is AuthenticationType.SASLContinue)
                    {
                        var error = Encoding.UTF8.GetString(ReadRemaining(ref reader));
                        encoder.WriteAuthenticationResponse([1]);
                        await encoder.FlushAuto().ConfigureAwait(false);
                        message = await decoder.GetNextAuto().ConfigureAwait(false);
                        try
                        {
                            ParseAuthMessage(message, out reader);
                        }
                        catch (PgErrorException exception)
                        {
                            throw new PgClientException(new AuthenticationException(
                                $"PostgreSQL rejected OAuth authentication: {error}", exception));
                        }
                        throw new PgClientException(
                            new AuthenticationException($"PostgreSQL rejected OAuth authentication: {error}"));
                    }
                    if (authType is AuthenticationType.SASLFinal)
                        await ExpectAuthOkAsync(decoder, "after OAuth").ConfigureAwait(false);
                    else
                        EnsureAuthOk(authType, "after OAuth");
                    break;
                }
                if (_options.Password is null)
                    throw new InvalidOperationException(requireChannelBinding
                        ? "SCRAM channel binding was required but no password was provided for SCRAM authentication."
                        : "No password was provided for SCRAM authentication.");
                using (var scram = ScramSha256.Create(mechanisms, _options.Password,
                           _options.Ssl.ChannelBinding, _remoteCertificate))
                {
                    channelBound = scram.IsChannelBound;
                    var initial = scram.CreateInitialResponse();
                    encoder.WriteSaslInitialResponse(scram.Mechanism, initial);
                    await encoder.FlushAuto().ConfigureAwait(false);

                    message = await decoder.GetNextAuto().ConfigureAwait(false);
                    authType = ParseAuthMessage(message, out reader);
                    if (authType is not AuthenticationType.SASLContinue)
                        throw new PgProtocolException("Expected a SASL continuation response.");
                    var final = scram.ProcessServerFirst(ReadRemaining(ref reader));
                    encoder.WriteAuthenticationResponse(final);
                    await encoder.FlushAuto().ConfigureAwait(false);

                    message = await decoder.GetNextAuto().ConfigureAwait(false);
                    authType = ParseAuthMessage(message, out reader);
                    if (authType is not AuthenticationType.SASLFinal)
                        throw new PgProtocolException("Expected a SASL final response.");
                    scram.ValidateServerFinal(ReadRemaining(ref reader));

                    await ExpectAuthOkAsync(decoder, "after SASL").ConfigureAwait(false);
                }
                break;
            case AuthenticationType.SASLContinue:
            case AuthenticationType.SASLFinal:
            default:
                throw new PgClientException(new NotSupportedException(
                    $"PostgreSQL requested unsupported authentication type {(int)authType}."));
        }

        if (requireChannelBinding && !channelBound)
            throw new PgClientException(new AuthenticationException(
                "SCRAM channel binding was required but PostgreSQL authenticated without it."));

        // PgClientFlow will handle ParameterStatus messages, so we just need to handle BackendKeyData and RFQ.
        message = await decoder.GetNextAuto().ConfigureAwait(false);
        if (message.EnsureExpectedOrError(BackendType.BackendKeyData) is { } keyDataError)
            PgErrorException.Throw(keyDataError);
        message.DebugEnsureBuffered();
        var keyReader = message.BodyReader;
        if (!keyReader.TryReadBigEndian(out int processId))
            throw PgProtocolException.NotEnoughData(nameof(processId));
        if (!keyReader.TryReadBigEndian(out int secretKey))
            throw PgProtocolException.NotEnoughData(nameof(secretKey));
        BackendProcessId = processId;
        BackendSecretKey = secretKey;

        message = await decoder.GetNextAuto().ConfigureAwait(false);
        if (message.EnsureExpectedOrError(BackendType.ReadyForQuery) is { } rfqError)
            PgErrorException.Throw(rfqError);

        return ValueTask.CompletedTask;
    }

    static List<string> ReadMechanisms(ref SequenceReader<byte> reader)
    {
        var mechanisms = new List<string>();
        while (reader.TryReadTo(out ReadOnlySequence<byte> value, (byte)0, advancePastDelimiter: true))
        {
            if (value.IsEmpty)
                return mechanisms.Count != 0
                    ? mechanisms
                    : throw new PgProtocolException("PostgreSQL offered no SASL mechanisms.");
            mechanisms.Add(Encoding.UTF8.GetString(value));
        }
        throw new PgProtocolException("The SASL mechanism list is not terminated.");
    }

    static byte[] ReadRemaining(ref SequenceReader<byte> reader)
    {
        var remaining = reader.Remaining;
        var result = reader.UnreadSequence.ToArray();
        reader.Advance(remaining);
        return result;
    }

    async ValueTask ExpectAuthOkAsync(PgDecoder decoder, string context)
    {
        var message = await decoder.GetNextAuto().ConfigureAwait(false);
        var authType = ParseAuthMessage(message, out _);
        EnsureAuthOk(authType, context);
    }

    static void EnsureAuthOk(AuthenticationType authType, string context)
    {
        if (authType is not AuthenticationType.Ok)
            throw new PgProtocolException($"Expected authentication completion {context}.");
    }

    AuthenticationType ParseAuthMessage(BackendMessage message, out SequenceReader<byte> reader)
    {
        if (message.EnsureExpectedOrError(BackendType.AuthenticationRequest) is { } startupError)
            PgErrorException.Throw(startupError);

        message.DebugEnsureBuffered();

        reader = message.BodyReader;
        if (!reader.TryReadBigEndian(out int rq))
            throw PgProtocolException.NotEnoughData(nameof(AuthenticationType));
        return (AuthenticationType)rq;
    }

    enum AuthenticationType
    {
        Ok = 0,
        CleartextPassword = 3,
        MD5Password = 5,
        GSS = 7,
        GSSContinue = 8,
        SSPI = 9,
        SASL = 10,
        SASLContinue = 11,
        SASLFinal = 12
    }
}
