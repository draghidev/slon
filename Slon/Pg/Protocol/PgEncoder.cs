using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Slon.Buffers;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol;

// TODO wire in protocol abort cancellation.
readonly struct PgEncoder
{
    readonly PgClientFlow.ExecutionControl _executionControl;
    readonly PgProtocolDataWriter _writer;

    internal PgEncoder(PgClientFlow.ExecutionControl executionControl, PgProtocolDataWriter writer)
    {
        _executionControl = executionControl;
        _writer = writer;
    }

    internal Encoding ClientEncoding => _writer.ClientEncoding;

    public bool LastMessageInducesRfq => _executionControl.LastMessageInducesRfq;

    public ValueTask WriteQueryAuto(string commandText)
    {
        if (_executionControl.IsAsync)
            return WriteQueryAsync(commandText);
        WriteQuery(commandText);
        return new();
    }

    // Today identical to WriteQuery in body. Once the serializer / large-query path lands,
    // this takes the async-flush route when the text exceeds buffer capacity.
    public ValueTask WriteQueryAsync(string commandText)
    {
        WriteQuery(commandText);
        return new();
    }

    public void WriteQuery(string commandText)
    {
        var encoding = ClientEncoding;
        var commandTextLength = GetStringWithNullTerminatorByteCount(commandText, encoding);
        StartMessage(FrontendType.Query, bodyLength: commandTextLength);
        _writer.WriteStringWithNullTerminator(commandText, encoding, commandTextLength);
    }

    public ValueTask WriteParseAuto(string commandText, EncodedString commandName = default, ParameterTypeList parameterTypes = default, CancellationToken cancellationToken = default)
    {
        if (_executionControl.IsAsync)
            return WriteParseAsync(commandText, commandName, parameterTypes, cancellationToken);
        WriteParse(commandText, commandName, parameterTypes);
        return new();
    }

    public async ValueTask WriteParseAsync(string commandText, EncodedString commandName = default, ParameterTypeList parameterTypes = default, CancellationToken cancellationToken = default)
    {
        var encoding = ClientEncoding;
        var commandTextLength = GetStringWithNullTerminatorByteCount(commandText, encoding);
        var commandNameBytes = commandName.AsNullTerminatedSpan(encoding);
        var parameterCount = parameterTypes.PgCount;
        StartMessage(FrontendType.Parse, bodyLength:
            commandNameBytes.Length + // Null-terminated command name
            commandTextLength + // Null-terminated query string
            sizeof(ushort) + // Number of parameters
            parameterCount * sizeof(uint)  // Parameter OIDs
        );

        _writer.WriteRaw(commandNameBytes);
        await _writer.WriteStringWithNullTerminatorAsync(commandText, encoding, commandTextLength, cancellationToken).ConfigureAwait(false);
        _writer.WriteUShort(parameterCount);

        // We're at most buffering 260kb across a few segments (2^16 * sizeof(uint)) for the maximum number of params, seems fine.
        using var enumerator = parameterTypes.GetEnumerator(_writer.OidLookup); // TODO should probably come from the flow.
        while (enumerator.MoveNext())
            _writer.WriteUInt(enumerator.Current.Oid.Value);
    }

    public void WriteParse(string commandText, EncodedString commandName = default, ParameterTypeList parameterTypes = default)
    {
        var encoding = ClientEncoding;
        var commandTextLength = GetStringWithNullTerminatorByteCount(commandText, encoding);
        var commandNameBytes = commandName.AsNullTerminatedSpan(encoding);
        var parameterCount = parameterTypes.PgCount;
        StartMessage(FrontendType.Parse, bodyLength:
            commandNameBytes.Length +
            commandTextLength +
            sizeof(ushort) +
            parameterCount * sizeof(uint)
        );

        _writer.WriteRaw(commandNameBytes);
        _writer.WriteStringWithNullTerminator(commandText, encoding, commandTextLength);
        _writer.WriteUShort(parameterCount);

        using var enumerator = parameterTypes.GetEnumerator(_writer.OidLookup);
        while (enumerator.MoveNext())
            _writer.WriteUInt(enumerator.Current.Oid.Value);
    }

    public ValueTask WriteBindAuto(EncodedString commandName = default, EncodedString portalName = default, ImmutableArray<Parameter> parameters = default, CancellationToken cancellationToken = default)
    {
        if (_executionControl.IsAsync)
            return WriteBindAsync(commandName, portalName, parameters, cancellationToken);
        WriteBind(commandName, portalName, parameters);
        return new();
    }

    // Today identical to WriteBind in body. The full serializer hasn't landed yet, so
    // parameter writes are just buffer fills with no flush points. Once the serializer is in
    // and large parameter payloads need to flush mid-write, this method takes the async-flush
    // path (FlushAsync) while WriteBind takes the sync-flush path (Flush).
    public ValueTask WriteBindAsync(EncodedString commandName = default, EncodedString portalName = default, ImmutableArray<Parameter> parameters = default, CancellationToken cancellationToken = default)
    {
        WriteBind(commandName, portalName, parameters);
        return new();
    }

    public void WriteBind(EncodedString commandName = default, EncodedString portalName = default, ImmutableArray<Parameter> parameters = default)
    {
        var encoding = ClientEncoding;
        var portalNameBytes = portalName.AsNullTerminatedSpan(encoding);
        var commandNameBytes = commandName.AsNullTerminatedSpan(encoding);

        var totalParameterSize = sizeof(ushort);
        var parameterCount = checked((ushort)parameters.Length);
        if (parameterCount > 0)
        {
            foreach (var p in parameters)
            {
                var size = p.GetSize();
                totalParameterSize += sizeof(int) + (size > 0 ? size : 0); // length + value
            }
        }

        var totalFormatCodeSize = parameterCount is 0 ? sizeof(ushort) : sizeof(ushort) + sizeof(ushort);

        StartMessage(FrontendType.Bind, bodyLength:
            commandNameBytes.Length + // Null-terminated command name
            portalNameBytes.Length + // Null-terminated portal name
            totalFormatCodeSize +
            totalParameterSize +
            sizeof(ushort) + // Number of result format codes
            sizeof(ushort) // Result format codes
        );

        _writer.WriteRaw(portalNameBytes);
        _writer.WriteRaw(commandNameBytes);

        if (parameterCount is 0)
        {
            _writer.WriteUShort(0); // format codes
            _writer.WriteUShort(parameterCount);
        }
        else
        {
            _writer.WriteUShort(1);
            _writer.WriteUShort(1); // all binary for now

            _writer.WriteUShort(parameterCount);
            foreach (var p in parameters)
            {
                if (p.Value is null)
                {
                    _writer.WriteInt(-1);
                }
                else
                {
                    _writer.WriteInt(p.GetSize());
                    if (p.ResolvedValueType == typeof(int))
                    {
                        _writer.WriteInt((int)p.Value);
                    }
                    else
                    {
                        ThrowHelper.ThrowNotSupported("Only int parameters are supported for now.");
                    }
                }
            }
        }

        _writer.WriteUShort(1); // result format codes
        _writer.WriteUShort(1); // all binary for now
    }

    public void WriteDescribe(EncodedString name = default, bool portalName = true)
    {
        const byte portal = (byte)'P';
        const byte statement = (byte)'S';

        var nameBytes = name.AsNullTerminatedSpan(ClientEncoding);
        StartMessage(FrontendType.Describe, bodyLength:
            sizeof(byte) + // 'portal' or 'statement'
            nameBytes.Length // command/portal name
        );
        _writer.WriteByte(portalName ? portal : statement);
        _writer.WriteRaw(nameBytes);
    }

    public void WriteExecute(EncodedString portalName = default)
    {
        var portalNameBytes = portalName.AsNullTerminatedSpan(ClientEncoding);
        StartMessage(FrontendType.Execute, bodyLength:
            portalNameBytes.Length + // Null-terminated portal name (always empty for now)
            sizeof(int) // Max number of rows
        );
        _writer.WriteRaw(portalNameBytes);
        _writer.WriteUInt(0); // all rows
    }

    public void WriteSync()
    {
        StartMessage(FrontendType.Sync, bodyLength: 0);
    }

    public void WriteClose(EncodedString name = default, bool portalName = false)
    {
        const byte portal = (byte)'P';
        const byte statement = (byte)'S';

        var nameBytes = name.AsNullTerminatedSpan(ClientEncoding);
        StartMessage(FrontendType.Close, bodyLength:
            sizeof(byte) + // 'portal' or 'statement'
            nameBytes.Length // command/portal name
        );
        _writer.WriteByte(portalName ? portal : statement);
        _writer.WriteRaw(nameBytes);
    }

    static int GetStringWithNullTerminatorByteCount(string value, Encoding encoding)
        => encoding.GetByteCount(value) + sizeof(byte);

    internal void CopyStartupBuffer<TBuffer>(TBuffer buffer) where TBuffer : ICopyableBuffer<byte>
        => _writer.CopyFrom(buffer);

    internal void WriteStartupMD5Password(string username, string plainPassword, ReadOnlySpan<byte> salt, Encoding encoding)
    {
        var hashed = HashPassword(username, plainPassword, salt, encoding);

        var hashedPasswordLength = GetStringWithNullTerminatorByteCount(hashed, encoding);
        StartMessage(FrontendType.Authentication, bodyLength: hashedPasswordLength);
        _writer.WriteStringWithNullTerminator(hashed, encoding, hashedPasswordLength);

        static string HashPassword(string username, string plainPassword, ReadOnlySpan<byte> salt, Encoding encoding)
        {
            ArgumentNullException.ThrowIfNull(plainPassword);
            if (salt.Length != 4)
                throw new ArgumentException("4 byte salt was not provided");

            var plaintext = ArrayPool<byte>.Shared.Rent(encoding.GetByteCount(plainPassword) + encoding.GetByteCount(username));
            var passwordEncodedCount = encoding.GetBytes(plainPassword.AsSpan(), plaintext);
            var usernameEncodedCount = encoding.GetBytes(username.AsSpan(), plaintext.AsSpan(passwordEncodedCount));

            var pgHash = ArrayPool<byte>.Shared.Rent(MD5.HashSizeInBytes);
            if (MD5.HashData(plaintext.AsSpan(0, passwordEncodedCount + usernameEncodedCount), pgHash) != MD5.HashSizeInBytes)
                ThrowInvalidLength();
            ArrayPool<byte>.Shared.Return(plaintext, clearArray: true);
            var pgHexHash = Convert.ToHexString((ReadOnlySpan<byte>)pgHash).ToLowerInvariant();

            var plainChallenge = ArrayPool<byte>.Shared.Rent(encoding.GetByteCount(pgHexHash) + salt.Length);
            var hexHashEncodedCount = encoding.GetBytes(pgHexHash.AsSpan(), plainChallenge);
            salt.CopyTo(plainChallenge.AsSpan(hexHashEncodedCount));
            // We reuse pghash as the final output given md5 is always the same size.
            var challengeHash = pgHash;
            if (MD5.HashData(plainChallenge.AsSpan(0, hexHashEncodedCount + salt.Length), challengeHash) != MD5.HashSizeInBytes)
                ThrowInvalidLength();
            ArrayPool<byte>.Shared.Return(plainChallenge, clearArray: true);

            var result = string.Concat("md5", Convert.ToHexString((ReadOnlySpan<byte>)challengeHash).ToLowerInvariant());
            ArrayPool<byte>.Shared.Return(challengeHash, clearArray: true);
            return result;

            static void ThrowInvalidLength() => throw new InvalidOperationException("Dev error, md5 is not a variable size algo.");
        }
    }

    void StartMessage(FrontendType type, int bodyLength)
    {
        Span<byte> header = stackalloc byte[sizeof(byte) + sizeof(int)];
        header[0] = type.ToByte();
        // TODO actually prevent overflows like in npgsql, that approach wouldn't actually cost us any perf though.
        // Small sanity check, though we don't depend on it, we instead throw during flush or here if messages were started smaller than what is being sent.
        // It's the safest way to do streaming writes without needing to correctly find and handle every integer addition accumulating into message length.
        BinaryPrimitives.WriteUInt32BigEndian(header.Slice(1), checked(sizeof(uint) + (uint)bodyLength));
        _writer.WriteRaw(header);

        _executionControl.OnMessageWrite(type);
    }

    public void Flush()
    {
        _executionControl.ThrowIfCannotWrite();
        _writer.Flush();
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        _executionControl.ThrowIfCannotWrite();
        // When a flow pipelines a flush never gets followed by a read - in the first phase - so we can always delay flushes.
        if (_executionControl.IsPipelined)
            return new();

        return _writer.FlushAsync(cancellationToken);
    }

    public ValueTask FlushAuto(CancellationToken cancellationToken = default)
    {
        if (_executionControl.IsAsync)
            return FlushAsync(cancellationToken);

        Flush();
        return new();
    }
}
