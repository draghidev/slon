using System.Buffers;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Slon.Buffers;
using Slon.Text;
using Slon.Transport;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol;

readonly struct PgEncoder
{
    readonly PgClientFlow.ExecutionControl _executionControl;
    readonly ProtocolDataWriter _writer;

    internal PgEncoder(PgClientFlow.ExecutionControl executionControl, ProtocolDataWriter writer)
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
    public WriteResumeSignal ResumeSignal => _writer.ResumeSignal;

    // Opens a scope that places the writer's cached signal in the transport's TLS slot for
    // the scope's lifetime, restoring on Dispose. Use this from a Resumable-driving caller
    // (flow body or sync wrapper) so the transport sees the signal underneath. Lets the
    // caller stay agnostic to the TLS plumbing.
    public ResumableScope BeginResumableScope() => new(_writer.ResumeSignal);

    // Forwards to the underlying writer so the sync encoder variants and higher-composition
    // sync drivers can park and signal without reaching into the transport directly.
    void WaitWritable() => _writer.WaitWritable();
    void ResumeWrite(Exception? exception = null) => _writer.ResumeWrite(exception);
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
                    e.ResumeWrite(e.TranslateAbort(ex));
                    break;
                }
                e.ResumeWrite();
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
            return WriteBindAsync(commandName, portalName, parameters,
                cancellationToken: cancellationToken);
        WriteBind(commandName, portalName, parameters);
        return new();
    }

    public async ValueTask WriteBindAsync(EncodedString commandName = default, EncodedString portalName = default,
        ImmutableArray<Parameter> parameters = default, ImmutableArray<PgFormat> resultFormats = default,
        ParameterWriterStrategy? parameterWriterStrategy = null,
        CancellationToken cancellationToken = default)
    {
        NormalizeAndValidate(ref parameters, ref resultFormats);
        WriteBindPreamble(commandName, portalName, parameters, resultFormats);
        var strategy = parameterWriterStrategy ?? ParameterWriterStrategy.Raw;
        object? state = null;
        foreach (var parameter in parameters)
        {
            var size = parameter.GetSize();
            _writer.WriteInt(size);
            if (size < 0)
                continue;

            state ??= _writer.GetParameterWriterState(strategy);
            await strategy.WriteAsync(state, parameter, cancellationToken).ConfigureAwait(false);
        }
        WriteResultFormats(resultFormats);
    }

    // See WriteQueryResumable for the contract.
    public ValueTask WriteBindResumable(EncodedString commandName = default, EncodedString portalName = default,
        ImmutableArray<Parameter> parameters = default, ImmutableArray<PgFormat> resultFormats = default,
        ParameterWriterStrategy? parameterWriterStrategy = null)
        => WriteBindAsync(commandName, portalName, parameters, resultFormats, parameterWriterStrategy);

    public void WriteBind(EncodedString commandName)
    {
        var commandNameBytes = commandName.AsNullTerminatedSpan(ClientEncoding);
        StartMessage(FrontendType.Bind, bodyLength: commandNameBytes.Length + 1 + 4 * sizeof(ushort));
        _writer.WriteByte(0); // unnamed portal
        _writer.WriteRaw(commandNameBytes);
        _writer.WriteUShort(0); // parameter format codes
        _writer.WriteUShort(0); // parameters
        _writer.WriteUShort(1); // result format codes
        _writer.WriteUShort(1); // all binary
    }

    public void WriteBind(EncodedString commandName = default, EncodedString portalName = default,
        ImmutableArray<Parameter> parameters = default, ImmutableArray<PgFormat> resultFormats = default,
        ParameterWriterStrategy? parameterWriterStrategy = null)
    {
        NormalizeAndValidate(ref parameters, ref resultFormats);
        WriteBindPreamble(commandName, portalName, parameters, resultFormats);
        var strategy = parameterWriterStrategy ?? ParameterWriterStrategy.Raw;
        object? state = null;
        foreach (var parameter in parameters)
        {
            var size = parameter.GetSize();
            _writer.WriteInt(size);
            if (size < 0)
                continue;

            state ??= _writer.GetParameterWriterState(strategy);
            strategy.Write(state, in parameter);
        }
        WriteResultFormats(resultFormats);
    }

    void WriteBindPreamble(EncodedString commandName, EncodedString portalName,
        ImmutableArray<Parameter> parameters, ImmutableArray<PgFormat> resultFormats)
    {
        var encoding = ClientEncoding;
        var portalNameBytes = portalName.AsNullTerminatedSpan(encoding);
        var commandNameBytes = commandName.AsNullTerminatedSpan(encoding);
        var parameterCount = checked((ushort)parameters.Length);
        var parameterBytes = sizeof(ushort);
        foreach (var parameter in parameters)
        {
            var size = parameter.GetSize();
            parameterBytes = checked(parameterBytes + sizeof(int) + Math.Max(0, size));
        }
        var formatCodeBytes = parameterCount is 0 ? sizeof(ushort) : 2 * sizeof(ushort);

        StartMessage(FrontendType.Bind, bodyLength: checked(
            commandNameBytes.Length + portalNameBytes.Length + formatCodeBytes + parameterBytes
            + sizeof(ushort) + Math.Max(1, resultFormats.Length) * sizeof(ushort)));
        _writer.WriteRaw(portalNameBytes);
        _writer.WriteRaw(commandNameBytes);
        if (parameterCount is 0)
        {
            _writer.WriteUShort(0);
            _writer.WriteUShort(0);
        }
        else
        {
            _writer.WriteUShort(1);
            _writer.WriteUShort((ushort)PgFormat.Binary);
            _writer.WriteUShort(parameterCount);
        }
    }

    void WriteResultFormats(ImmutableArray<PgFormat> resultFormats)
    {
        if (resultFormats.Length is 0)
        {
            _writer.WriteUShort(1);
            _writer.WriteUShort((ushort)PgFormat.Binary);
            return;
        }

        _writer.WriteUShort((ushort)resultFormats.Length);
        foreach (var format in resultFormats)
            _writer.WriteUShort((ushort)format);
    }

    static void NormalizeAndValidate(ref ImmutableArray<Parameter> parameters,
        ref ImmutableArray<PgFormat> resultFormats)
    {
        if (parameters.IsDefault)
            parameters = ImmutableArray<Parameter>.Empty;
        if (resultFormats.IsDefault)
            resultFormats = ImmutableArray<PgFormat>.Empty;
        if (parameters.Length > ushort.MaxValue)
            throw new ArgumentException("Too many parameters.", nameof(parameters));
        if (resultFormats.Length > ushort.MaxValue)
            throw new ArgumentException("Too many result format codes.", nameof(resultFormats));
        foreach (var format in resultFormats)
            if (format is not PgFormat.Text and not PgFormat.Binary)
                throw new ArgumentOutOfRangeException(
                    nameof(resultFormats), format, "Unknown PostgreSQL result format code.");
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

    public void WriteExecute()
    {
        StartMessage(FrontendType.Execute, bodyLength: sizeof(byte) + sizeof(int));
        _writer.WriteByte(0); // unnamed portal
        _writer.WriteUInt(0); // all rows
    }

    public void WriteExecute(EncodedString portalName)
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
    internal int CurrentMessagePaddingLength => _writer.CurrentMessagePaddingLength;
    internal int PadCurrentMessage(int maxBytes = int.MaxValue)
        => _writer.CompleteCurrentMessageWithPadding(maxBytes);

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

    internal void CopyStartupBuffer(ReadOnlySpan<byte> buffer) => _writer.WriteRaw(buffer);

    internal void WritePasswordResponse(string response, Encoding encoding)
    {
        var responseLength = GetStringWithNullTerminatorByteCount(response, encoding);
        StartMessage(FrontendType.Authentication, responseLength);
        _writer.WriteStringWithNullTerminator(response, encoding, responseLength);
    }

    internal void WriteSaslInitialResponse(string mechanism, ReadOnlySpan<byte> response)
    {
        var mechanismLength = GetStringWithNullTerminatorByteCount(mechanism, Encoding.UTF8);
        StartMessage(FrontendType.Authentication, mechanismLength + sizeof(int) + response.Length);
        _writer.WriteStringWithNullTerminator(mechanism, Encoding.UTF8, mechanismLength);
        _writer.WriteInt(response.Length);
        _writer.WriteRaw(response);
    }

    internal void WriteAuthenticationResponse(ReadOnlySpan<byte> response)
    {
        StartMessage(FrontendType.Authentication, response.Length);
        _writer.WriteRaw(response);
    }

    void StartMessage(FrontendType type, int bodyLength)
    {
        // Arm message-length tracking BEFORE writing the header so the prior message's
        // declared-vs-written check fires at this boundary (it reads UnflushedBytes, which the
        // header bytes about to be written would otherwise contaminate). totalLength is on-wire
        // size: 1 (type) + sizeof(uint) (length field including itself) + bodyLength.
        _writer.StartMessage(type.ToByte(), bodyLength);

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
    // so it can be delayed when a successor is already queued to contribute another write. An inline
    // producer-driven turn flushes instead: reaching that successor requires returning through the
    // executor's suspension boundary, making the deferral counterproductive. The buffer threshold
    // still bounds accumulation and applies send-window backpressure. Sync flushes never defer.
    bool CanDeferFlush
        => _executionControl.SupportsDeferredFlush
            && _executionControl.HasQueuedFlow
            && !_executionControl.IsInlineDrive
            && _writer.UnflushedBytes < ProtocolDataWriter.UnflushedBytesFlushThreshold;

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        _executionControl.ThrowIfCannotWrite();
        if (CanDeferFlush)
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
