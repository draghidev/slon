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
    }

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
        // TODO store on protocol
        if (message.EnsureExpectedOrError(BackendType.BackendKeyData) is { } keyDataError)
            PostgresException.Throw(keyDataError);

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
