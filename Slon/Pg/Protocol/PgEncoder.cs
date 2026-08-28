using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Runtime;
using Slon.Text;
using Slon.Transport;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct PgEncoder
{
    public readonly ref struct ResumableWriteScope
    {
        readonly TransportConnection.ResumableWrite _previous;

        public ResumableWriteScope(ResumeSignal signal) : this(signal, default) { }

        public ResumableWriteScope(ResumeSignal signal, TimeSpan timeout)
        {
            _previous = TransportConnection.CurrentResumableWrite;
            TransportConnection.CurrentResumableWrite = new(signal, timeout);
        }

        public void Dispose() => TransportConnection.CurrentResumableWrite = _previous;
    }

    readonly PgClientFlow.ExecutionControl _executionControl;
    readonly ProtocolDataWriter _writer;

    internal PgEncoder(PgClientFlow.ExecutionControl executionControl, ProtocolDataWriter writer)
    {
        _executionControl = executionControl;
        _writer = writer;
    }

    internal Encoding ClientEncoding => _writer.ClientEncoding;

    public bool LastMessageInducesRfq => _executionControl.LastMessageInducesRfq;

    // Cached writable signal the flow parks in the transport TLS slot around a resumable-write
    // call. Cached on the writer (per-connection) so each call reuses one instance, no
    // per-op allocation. Auto-reset on consumption keeps it ready for the next WouldBlock
    // cycle.
    public ResumeSignal ResumeSignal => _writer.ResumeSignal;

    // Opens a scope that places the writer's cached signal in the transport's TLS slot for
    // the scope's lifetime, restoring on Dispose. Use this from a resumable-write caller
    // (flow body or sync wrapper) so the transport sees the signal underneath. Lets the
    // caller stay agnostic to the TLS plumbing.
    public ResumableWriteScope BeginResumableWriteScope() => new(_writer.ResumeSignal, _writer.WriteTimeout);

    // Forwards to the underlying writer so the sync encoder variants and higher-composition
    // sync drivers can park and signal without reaching into the transport directly.
    void WaitUntilWritable() => _writer.WaitUntilWritable(_writer.ResumeSignal.GetRemainingTimeout());
    void ResumeWrite(Exception? exception = null) => _writer.ResumeWrite(exception);
    Exception TranslateAbort(Exception ex) => _writer.TranslateAbort(ex);

    // Dispatches a pending resumable write's driver loop to a LongRunning thread. Caller is
    // expected to have already observed that the resumable isn't completed (so the shunt is
    // needed). The LongRunning delegate opens its own ResumableWriteScope so the transport's TLS
    // slot stays populated through the resumption thread's lifetime, then runs the same
    // driver body the sync wrappers use inline
    // (while (!t.IsCompleted) { WaitUntilWritable, Signal }, then GetResult).
    public ValueTask RunResumableTask(ValueTask resumable)
    {
        var encoder = this;
        return new ValueTask(Task.Factory.StartNew(static state =>
        {
            var (e, t) = ((PgEncoder, ValueTask))state!;
            using var _ = e.BeginResumableWriteScope();
            while (!t.IsCompleted)
            {
                try
                {
                    e.WaitUntilWritable();
                }
                catch (Exception ex)
                {
                    // A WaitUntilWritable throw (deadline expiry, abort) would otherwise strand the parked
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

    // Names the caller's intent: "I have a ResumableWriteScope open, drive my returned task."
    // Body is just the Async variant. Transport reads the TLS signal and translates WouldBlock
    // into a pending ValueTask backed by it. Kept as a separate method so call sites stay
    // self-documenting and the Async / Resumable bodies can diverge later if serializer
    // auto-flush needs different scheduling.
    public ValueTask WriteQueryResumable(string commandText) => WriteQueryAsync(commandText);

    // Sync core. Back-pressure is handled at the transport layer via the TLS-armed resumable-write
    // path, not at the encoder level, so this is just the buffer fill.
    public void WriteQuery(string commandText)
    {
        var encoding = ClientEncoding;
        var commandTextLength = GetStringWithNullTerminatorByteCount(commandText, encoding);
        StartMessage(FrontendType.Query, bodyLength: commandTextLength);
        _writer.WriteStringWithNullTerminator(commandText, encoding, commandTextLength);
    }

    public ValueTask WriteParseAuto(string commandText, EncodedCString commandName = default, ParameterTypeList parameterTypes = default, CancellationToken cancellationToken = default)
    {
        if (_executionControl.IsAsync)
            return WriteParseAsync(commandText, commandName, parameterTypes, cancellationToken);
        WriteParse(commandText, commandName, parameterTypes);
        return new();
    }

    public async ValueTask WriteParseAsync(string commandText, EncodedCString commandName = default, ParameterTypeList parameterTypes = default, CancellationToken cancellationToken = default)
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
    public ValueTask WriteParseResumable(string commandText, EncodedCString commandName = default, ParameterTypeList parameterTypes = default)
        => WriteParseAsync(commandText, commandName, parameterTypes);

    public void WriteParse(string commandText, EncodedCString commandName = default, ParameterTypeList parameterTypes = default)
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

    public async ValueTask WriteBindAsync(EncodedCString commandName = default, EncodedCString portalName = default,
        ParameterSource parameters = default, ImmutableArray<PgFormat> resultFormats = default,
        CancellationToken cancellationToken = default)
    {
        var parameterCount = parameters.Count;
        NormalizeAndValidate(parameterCount, ref resultFormats);
        if (parameters.State is null or Parameter[])
        {
            var directParameters = parameters.State as Parameter[] ?? [];
            WriteBindPreamble(commandName, portalName, parameterCount,
                GetDirectParameterBytes(directParameters), resultFormats);
            for (var i = 0; i < directParameters.Length; i++)
                await WriteDirectParameterAsync(directParameters[i], cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var writer = parameters.Writer
                ?? throw new InvalidOperationException("A deferred parameter source requires a parameter writer.");
            var source = parameters.State;
            using var lease = writer.BeginWriteCore(source, parameterCount);
            var writerState = BindParameters(lease, parameterCount, writer, out var parameterBytes);
            WriteBindPreamble(commandName, portalName, parameterCount,
                parameterBytes, resultFormats);
            for (var i = 0; i < parameterCount; i++)
            {
                var size = lease.GetSize(i);
                _writer.WriteInt(size);
                if (size >= 0)
                    await lease.WriteAsync(writerState!, i, cancellationToken)
                        .ConfigureAwait(false);
            }
        }
        WriteResultFormats(resultFormats);
    }

    // See WriteQueryResumable for the contract.
    public ValueTask WriteBindResumable(EncodedCString commandName = default, EncodedCString portalName = default,
        ParameterSource parameters = default, ImmutableArray<PgFormat> resultFormats = default)
        => WriteBindAsync(commandName, portalName, parameters, resultFormats);

    public void WriteBind(EncodedCString commandName)
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

    // Executes a prepared statement without parameters and with default result formats: Bind on the
    // unnamed portal, an optional portal Describe, an optional Execute, then syncCount Sync messages,
    // all written through one reserved span. A flow appends at most one Sync of its own beside the
    // command's, so syncCount is bounded to two.
    internal void WritePreparedExecution(EncodedCString commandName, bool describe, bool execute, int syncCount)
    {
        WritePreparedExecutionCore(_writer, ClientEncoding, commandName, describe, execute, syncCount);
        _executionControl.OnMessageWrite(FrontendType.Bind);
        if (describe)
            _executionControl.OnMessageWrite(FrontendType.Describe);
        if (execute)
            _executionControl.OnMessageWrite(FrontendType.Execute);
        for (var i = 0; i < syncCount; i++)
            _executionControl.OnMessageWrite(FrontendType.Sync);
    }

    // Each message is still armed and advanced on its own so the per-message declared-length check
    // holds. The reserved span stays valid across the advances because the buffering writer only
    // reallocates on a reservation it cannot satisfy.
    internal static void WritePreparedExecutionCore(ProtocolDataWriter writer, Encoding encoding,
        EncodedCString commandName, bool describe, bool execute, int syncCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(syncCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(syncCount, 2);

        const int header = sizeof(byte) + sizeof(uint);
        const int describeBody = sizeof(byte) + 1; // 'P' and the unnamed portal
        const int executeBody = sizeof(byte) + sizeof(int); // unnamed portal, all rows
        var commandNameBytes = commandName.AsNullTerminatedSpan(encoding);
        var bindBody = checked(1 + commandNameBytes.Length + 4 * sizeof(ushort));
        var total = checked(header + bindBody
            + (describe ? header + describeBody : 0)
            + (execute ? header + executeBody : 0)
            + syncCount * header);
        var span = writer.GetSpan(total);

        writer.StartMessage(header + bindBody);
        WriteHeader(span, FrontendType.Bind, bindBody);
        span[header] = 0; // unnamed portal
        commandNameBytes.CopyTo(span.Slice(header + 1));
        var formats = span.Slice(header + 1 + commandNameBytes.Length);
        BinaryPrimitives.WriteUInt16BigEndian(formats, 0); // parameter format codes
        BinaryPrimitives.WriteUInt16BigEndian(formats.Slice(2), 0); // parameters
        BinaryPrimitives.WriteUInt16BigEndian(formats.Slice(4), 1); // result format codes
        BinaryPrimitives.WriteUInt16BigEndian(formats.Slice(6), 1); // all binary
        writer.Advance(header + bindBody);
        span = span.Slice(header + bindBody);

        if (describe)
        {
            writer.StartMessage(header + describeBody);
            WriteHeader(span, FrontendType.Describe, describeBody);
            span[header] = (byte)'P';
            span[header + 1] = 0;
            writer.Advance(header + describeBody);
            span = span.Slice(header + describeBody);
        }

        if (execute)
        {
            writer.StartMessage(header + executeBody);
            WriteHeader(span, FrontendType.Execute, executeBody);
            span[header] = 0; // unnamed portal
            BinaryPrimitives.WriteUInt32BigEndian(span.Slice(header + 1), 0); // all rows
            writer.Advance(header + executeBody);
            span = span.Slice(header + executeBody);
        }

        for (var i = 0; i < syncCount; i++)
        {
            writer.StartMessage(header);
            WriteHeader(span, FrontendType.Sync, 0);
            writer.Advance(header);
            span = span.Slice(header);
        }

        static void WriteHeader(Span<byte> span, FrontendType type, int bodyLength)
        {
            span[0] = type.ToByte();
            BinaryPrimitives.WriteUInt32BigEndian(span.Slice(1), checked((uint)(sizeof(uint) + bodyLength)));
        }
    }

    public void WriteBind(EncodedCString commandName = default, EncodedCString portalName = default,
        ParameterSource parameters = default, ImmutableArray<PgFormat> resultFormats = default)
    {
        var parameterCount = parameters.Count;
        NormalizeAndValidate(parameterCount, ref resultFormats);
        if (parameters.State is null or Parameter[])
        {
            var directParameters = parameters.State as Parameter[] ?? [];
            WriteBindPreamble(commandName, portalName, parameterCount,
                GetDirectParameterBytes(directParameters), resultFormats);
            foreach (var parameter in directParameters)
                WriteDirectParameter(parameter);
        }
        else
        {
            var writer = parameters.Writer
                ?? throw new InvalidOperationException("A deferred parameter source requires a parameter writer.");
            var source = parameters.State;
            using var lease = writer.BeginWriteCore(source, parameterCount);
            var writerState = BindParameters(lease, parameterCount, writer, out var parameterBytes);
            WriteBindPreamble(commandName, portalName, parameterCount,
                parameterBytes, resultFormats);
            for (var i = 0; i < parameterCount; i++)
            {
                var size = lease.GetSize(i);
                _writer.WriteInt(size);
                if (size >= 0)
                    lease.Write(writerState!, i);
            }
        }
        WriteResultFormats(resultFormats);
    }

    object? BindParameters(ParameterWriter.WriteLease lease, int parameterCount,
        ParameterWriter writer, out int parameterBytes)
    {
        parameterBytes = sizeof(ushort);
        if (parameterCount is 0)
            return null;

        var writerState = _writer.GetParameterWriterState(writer);
        for (var parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++)
        {
            lease.Bind(writerState, parameterIndex);
            parameterBytes = checked(parameterBytes + sizeof(int)
                + Math.Max(0, lease.GetSize(parameterIndex)));
        }
        return writerState;
    }

    void WriteBindPreamble(EncodedCString commandName, EncodedCString portalName,
        int parameterCountValue, int parameterBytes, ImmutableArray<PgFormat> resultFormats)
    {
        var encoding = ClientEncoding;
        var portalNameBytes = portalName.AsNullTerminatedSpan(encoding);
        var commandNameBytes = commandName.AsNullTerminatedSpan(encoding);
        var parameterCount = checked((ushort)parameterCountValue);
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

    static int GetDirectParameterBytes(Parameter[] parameters)
    {
        var bytes = sizeof(ushort);
        foreach (var parameter in parameters)
            bytes = checked(bytes + sizeof(int) + Math.Max(0, parameter.Size));
        return bytes;
    }

    void WriteDirectParameter(in Parameter parameter)
    {
        var size = parameter.Size;
        _writer.WriteInt(size);
        if (size < 0)
            return;
        if (parameter.Value is byte[] bytes)
        {
            _writer.WriteRaw(bytes);
            return;
        }

        var stream = (Stream)parameter.Value!;
        var remaining = size;
        while (remaining > 0)
        {
            var span = _writer.GetSpan(1);
            var read = stream.Read(span[..Math.Min(span.Length, remaining)]);
            if (read is 0)
                throw new EndOfStreamException("Parameter stream ended before its declared length.");
            _writer.Advance(read);
            remaining -= read;
        }
    }

    async ValueTask WriteDirectParameterAsync(Parameter parameter, CancellationToken cancellationToken)
    {
        var size = parameter.Size;
        _writer.WriteInt(size);
        if (size < 0)
            return;
        if (parameter.Value is byte[] bytes)
        {
            _writer.WriteRaw(bytes);
            return;
        }

        var stream = (Stream)parameter.Value!;
        var remaining = size;
        while (remaining > 0)
        {
            var memory = _writer.GetMemory(1);
            var read = await stream.ReadAsync(memory[..Math.Min(memory.Length, remaining)], cancellationToken)
                .ConfigureAwait(false);
            if (read is 0)
                throw new EndOfStreamException("Parameter stream ended before its declared length.");
            _writer.Advance(read);
            remaining -= read;
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

    static void NormalizeAndValidate(int parameterCount, ref ImmutableArray<PgFormat> resultFormats)
    {
        if (resultFormats.IsDefault)
            resultFormats = ImmutableArray<PgFormat>.Empty;
        if (parameterCount > ushort.MaxValue)
            throw new ArgumentException("Too many parameters.", nameof(parameterCount));
        if (resultFormats.Length > ushort.MaxValue)
            throw new ArgumentException("Too many result format codes.", nameof(resultFormats));
        foreach (var format in resultFormats)
            if (format is not PgFormat.Text and not PgFormat.Binary)
                throw new ArgumentOutOfRangeException(
                    nameof(resultFormats), format, "Unknown PostgreSQL result format code.");
    }

    public void WriteDescribe(EncodedCString name = default, bool portalName = true)
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

    public void WriteExecute(EncodedCString portalName)
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

    public void WriteClose(EncodedCString name = default, bool portalName = false)
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
    // ResumableWriteScope so the writer's signal is in the transport TLS slot. The
    // transport picks up the TLS signal, does sync non-blocking syscalls, and translates
    // WouldBlock into a pending ValueTask backed by that signal. No exception, just a
    // pending shape that propagates faithfully through SslStream, NetworkStream, and any
    // other async wrapper in between. The flow's driver (inline or shunted to LongRunning)
    // holds the signal reference and drives it externally via WaitUntilWritable plus
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
