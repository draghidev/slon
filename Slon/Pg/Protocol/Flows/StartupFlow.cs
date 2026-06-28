using System.Buffers;
using Slon.Buffers;
using Slon.Buffers.Binary;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol.Flows;

sealed class StartupFlow : PgClientFlow
{
    readonly PgClientOptions _options;
    readonly TimeSpan _startupTimeout;
    readonly List<KeyValuePair<string, string>> _parameters;
    // Sync startup hands the body off to the Start() caller's thread: that caller parks in
    // WaitForExecutor on this MRES until the executor reaches and holds this flow, then drives the
    // handshake inline on its own thread (no TP escape during connection open). One-shot flow (fresh per
    // open, not pooled), so the MRES is allocated eagerly when sync; an async startup never parks => null,
    // the waiter-presence gate fec0355 keys on. The scripted body runs straight through (no consumer gate /
    // WaitForContinuation), so the MRES is only the handoff park, never reused for body rendezvous.
    readonly ManualResetEventSlim? _handoffMres;

    // Parsed from the BackendKeyData wire message. Pulled by PgClientProtocol after the flow
    // completes; the protocol stores them as its own fields and exposes them via Control.
    // 0 = not yet parsed (the wire format guarantees ProcessId is a non-zero OS PID, so 0 is a
    // safe "not received" sentinel; SecretKey is opaque 32-bit so ProcessId is the indicator).
    internal int BackendProcessId { get; private set; }
    internal int BackendSecretKey { get; private set; }

    public StartupFlow(bool async, PgClientOptions options, TimeSpan startupTimeout = default) : base(supportsPipelining: false)
    {
        _options = options;
        _startupTimeout = startupTimeout;
        _parameters = [
            new("user", options.Username),
            new("client_encoding", options.Encoding.WebName)
        ];
        if (options.Database is not null)
            _parameters.Add(new KeyValuePair<string, string>("database", options.Database));
        IsAsync = async;
        if (!async)
            _handoffMres = new(false);
    }

    internal override ManualResetEventSlim? GetHandoffMres() => _handoffMres;

    protected override async ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        var spanWriter = new SpanWriter<MemoryBufferWriter, byte>(new MemoryBufferWriter());
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

        encoder.CopyStartupBuffer(spanWriter.InnerWriter);
        await encoder.FlushAuto().ConfigureAwait(false);

        var decoder = await context.GetDecoderAsync().ConfigureAwait(false);
        decoder.ReadTimeout = _startupTimeout;
        var message = await decoder.GetNextAsync().ConfigureAwait(false);
        var authType = ParseAuthMessage(message, out var reader);
        switch (authType)
        {
            case AuthenticationType.Ok:
                // No auth required, nothing more to do.
                break;
            case AuthenticationType.CleartextPassword:
                throw new NotSupportedException("Refusing to send password as cleartext.");
            case AuthenticationType.MD5Password:
                Span<byte> salt = stackalloc byte[4];
                if (!reader.TryCopyTo(salt))
                    ThrowHelper.ThrowNotEnoughData(nameof(salt));
                reader.Advance(4);

                if (_options.Password is null)
                    throw new InvalidOperationException("No password given, connection expects password.");

                encoder.WriteStartupMD5Password(_options.Username, _options.Password, salt, _options.PasswordEncoding);
                await encoder.FlushAuto().ConfigureAwait(false);

                message = await decoder.GetNextAsync().ConfigureAwait(false);
                authType = ParseAuthMessage(message, out reader);
                if (authType != AuthenticationType.Ok)
                    throw new InvalidOperationException("Unexpected authentication response.");
                break;
            case AuthenticationType.GSS:
                break;
            case AuthenticationType.GSSContinue:
                break;
            case AuthenticationType.SSPI:
                break;
            case AuthenticationType.SASL:
                break;
            case AuthenticationType.SASLContinue:
                break;
            case AuthenticationType.SASLFinal:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(authType), authType, "Unknown authentication type");
        }

        // PgClientFlow will handle ParameterStatus messages, so we just need to handle BackendKeyData and RFQ.
        message = await decoder.GetNextAsync().ConfigureAwait(false);
        if (message.EnsureExpectedOrError(BackendType.BackendKeyData) is { } keyDataError)
            PostgresException.Throw(keyDataError);
        message.DebugEnsureBuffered();
        var keyReader = message.BodyReader;
        if (!keyReader.TryReadBigEndian(out int processId))
            ThrowHelper.ThrowNotEnoughData(nameof(processId));
        if (!keyReader.TryReadBigEndian(out int secretKey))
            ThrowHelper.ThrowNotEnoughData(nameof(secretKey));
        BackendProcessId = processId;
        BackendSecretKey = secretKey;

        message = await decoder.GetNextAsync().ConfigureAwait(false);
        if (message.EnsureExpectedOrError(BackendType.ReadyForQuery) is { } rfqError)
            PostgresException.Throw(rfqError);

        return ValueTask.CompletedTask;
    }

    AuthenticationType ParseAuthMessage(BackendMessage message, out SequenceReader<byte> reader)
    {
        if (message.EnsureExpectedOrError(BackendType.AuthenticationRequest) is { } startupError)
            PostgresException.Throw(startupError);

        message.DebugEnsureBuffered();

        reader = message.BodyReader;
        if (!reader.TryReadBigEndian(out int rq))
            ThrowHelper.ThrowNotEnoughData(nameof(AuthenticationType));
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
