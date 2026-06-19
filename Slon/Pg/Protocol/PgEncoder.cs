using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Slon.Buffers;
using Slon.Transport;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol;

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

    // Cached writable signal the flow parks in the transport TLS slot around a Resumable
    // call. Cached on the writer (per-connection) so each call reuses one instance, no
    // per-op allocation. Auto-reset on consumption keeps it ready for the next WouldBlock
    // cycle.
    public WritableSignal WritableSignal => _writer.WritableSignal;

    // Opens a scope that places the writer's cached signal in the transport's TLS slot for
    // the scope's lifetime, restoring on Dispose. Use this from a Resumable-driving caller
    // (flow body or sync wrapper) so the transport sees the signal underneath. Lets the
    // caller stay agnostic to the TLS plumbing.
    public ResumableScope BeginResumableScope() => new(_writer.WritableSignal);

    // Forwards to the underlying writer so the sync encoder variants and higher-composition
    // sync drivers can park and signal without reaching into the transport directly.
    void WaitWritable() => _writer.WaitWritable();
    void SignalWritable(Exception? exception = null) => _writer.SignalWritable(exception);
    Exception TranslateAbort(Exception ex) => _writer.TranslateAbort(ex);

    // Dispatches a pending Resumable's driver loop to a LongRunning thread. Caller is
    // expected to have already observed that the resumable isn't completed (so the shunt is
    // needed). The LongRunning delegate opens its own ResumableScope so the transport's TLS
    // slot stays populated through the resumption thread's lifetime, then runs the same
    // driver body the sync wrappers use inline
    // (while (!t.IsCompleted) { WaitWritable, Signal }, then GetResult).
    public ValueTask RunResumableTask(ValueTask resumable)
    {
        var encoder = this;
        return new ValueTask(Task.Factory.StartNew(static state =>
        {
            var (e, t) = ((PgEncoder, ValueTask))state!;
            using var _ = e.BeginResumableScope();
            while (!t.IsCompleted)
            {
                try
                {
                    e.WaitWritable();
                }
                catch (Exception ex)
                {
                    // A WaitWritable throw (deadline expiry, abort) would otherwise strand the parked
                    // write coroutine and leak the exception onto this side task. Route it through the
                    // signal's fault path so the coroutine unwinds and the abort-translated exception
                    // reaches the flow's execute path.
                    e.SignalWritable(e.TranslateAbort(ex));
                    break;
                }
                e.SignalWritable();
            }
            t.GetAwaiter().GetResult();
        }, (encoder, resumable), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default));
    }

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

    // Names the caller's intent: "I have a ResumableScope open, drive my returned task."
    // Body is just the Async variant. Transport reads the TLS signal and translates WouldBlock
    // into a pending ValueTask backed by it. Kept as a separate method so call sites stay
    // self-documenting and the Async / Resumable bodies can diverge later if serializer
    // auto-flush needs different scheduling.
    public ValueTask WriteQueryResumable(string commandText) => WriteQueryAsync(commandText);

    // Sync core. Back-pressure is handled at the transport layer via the TLS-armed Resumable
    // path, not at the encoder level, so this is just the buffer fill.
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

    // See WriteQueryResumable for the contract.
    public ValueTask WriteParseResumable(string commandText, EncodedString commandName = default, ParameterTypeList parameterTypes = default)
        => WriteParseAsync(commandText, commandName, parameterTypes);

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

    // See WriteQueryResumable for the contract.
    public ValueTask WriteBindResumable(EncodedString commandName = default, EncodedString portalName = default, ImmutableArray<Parameter> parameters = default)
        => WriteBindAsync(commandName, portalName, parameters);

    public void WriteBind(EncodedString commandName = default, EncodedString portalName = default, ImmutableArray<Parameter> parameters = default)
    {
        // The signature invites `default` for the no-parameters case; a default ImmutableArray
        // NREs on every member, so normalize before first use.
        if (parameters.IsDefault)
            parameters = ImmutableArray<Parameter>.Empty;

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

    // Recovery hook: pad a torn in-flight message to its declared length with zero bytes so the
    // server's framing reader exits the message at the declared boundary. Returns the byte count
    // written (0 = nothing in flight or message was already complete). Callers (ResyncRecoveryFlow)
    // pair this with a subsequent WriteSync + flush so the server discards the padded message
    // garbage as an ERROR and resyncs on the Sync's RFQ.
    internal int PadCurrentMessage() => _writer.CompleteCurrentMessageWithPadding();

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
        // Arm message-length tracking BEFORE writing the header so the prior message's
        // declared-vs-written check fires at this boundary (it reads UnflushedBytes, which the
        // header bytes about to be written would otherwise contaminate). totalLength is on-wire
        // size: 1 (type) + sizeof(uint) (length field including itself) + bodyLength.
        _writer.StartMessage(checked(sizeof(byte) + sizeof(uint) + bodyLength));

        Span<byte> header = stackalloc byte[sizeof(byte) + sizeof(int)];
        header[0] = type.ToByte();
        BinaryPrimitives.WriteUInt32BigEndian(header.Slice(1), checked(sizeof(uint) + (uint)bodyLength));
        _writer.WriteRaw(header);

        _executionControl.OnMessageWrite(type);
    }

    // Calls FlushAsync expecting the caller (flow level) to have opened an
    // EncoderResumableScope so the writer's signal is in the transport TLS slot. The
    // transport picks up the TLS signal, does sync non-blocking syscalls, and translates
    // WouldBlock into a pending ValueTask backed by that signal. No exception, just a
    // pending shape that propagates faithfully through SslStream, NetworkStream, and any
    // other async wrapper in between. The flow's driver (inline or shunted to LongRunning)
    // holds the signal reference and drives it externally via WaitWritable plus
    // signal.Signal. No try/catch at this layer, the transport is the coroutine, the flow
    // is the driver.

    // Async-path flush deferral. A pipelined async flush isn't followed by a read in the first phase,
    // so it can be delayed to batch with later writes - but only while the buffer stays under the
    // writer's flush threshold. Past it the flush must run to bound buffering and apply send-window
    // backpressure; the resulting (possibly parked) flush rides the trailing slot, drained by the
    // concurrent read. Sync flushes never defer (see Flush/FlushResumable).
    bool CanDelayFlush => _executionControl.IsPipelined && _writer.UnflushedBytes < PgProtocolDataWriter.UnflushedBytesFlushThreshold;

    // Sync flushes always run: a sync flow owns the executor for its duration, so a deferred flush
    // would never be picked up (the source never unwinds to the cross-item pre-flush) and the pipeline
    // would stall behind buffered, unsent bytes. Deferral becomes viable only once the sync executor
    // runs on its own thread.
    public ValueTask FlushResumable()
    {
        _executionControl.ThrowIfCannotWrite();
        return _writer.FlushAsync(default);
    }

    public void Flush()
    {
        _executionControl.ThrowIfCannotWrite();
        _writer.Flush();
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        _executionControl.ThrowIfCannotWrite();
        if (CanDelayFlush)
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
