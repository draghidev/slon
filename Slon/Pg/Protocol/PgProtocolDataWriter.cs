using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Slon.Buffers;
using Slon.Buffers.Binary;
using Slon.Pg.Types;
using Slon.Transport;

namespace Slon.Pg.Protocol;

sealed class PgProtocolDataWriter(IOutputWriter<byte> writer, Encoding clientEncoding, Action waitWritable, CancellationToken abortToken, PgClientProtocol.Control control)
{
    BufferingWriter _bufferingWriter = new(writer);
    readonly CancellationToken _abortToken = abortToken;
    readonly PgClientProtocol.Control _control = control;
    CancellationTokenSource _cts = CancellationTokenSource.CreateLinkedTokenSource(abortToken);

    // Current message state (npgsql NpgsqlWriteBuffer.StartMessage / AdvanceMessageBytesFlushed
    // pattern, GHSA-x9vc-6hfv-hg8c). Validation is structural: per-write Write* methods are
    // unchecked, the check rolls up at Flush and at the next StartMessage. `_messageBytesFlushed`
    // is anchored at -unflushedAtStart so `unflushed + _messageBytesFlushed = bytesWrittenForCurrentMessage`
    // across mid-message flushes too - lets multiple messages stack in the buffer and still get
    // validated at message boundaries without forcing a flush per message.
    int? _messageLength;
    int _messageBytesFlushed;

    // Per-connection cached signal. The flow parks this in
    // TransportConnection.SyncNonBlockingSignal around each Resumable call, the transport
    // returns it on WouldBlock as the pending source, the flow's driver fires it via
    // SignalWritable. Reused across operations thanks to auto-reset on consumption.
    public WritableSignal WritableSignal { get; } = new();

    // Maps a thrown exception to a SocketError, or null when the exception isn't recognizable
    // as a transport-level socket error. Handles the .NET socket-stack path. NetworkStream and
    // SslStream wrap SocketException in IOException, raw Socket throws it directly.
    public SocketError? GetSocketError(Exception ex)
    {
        if (ex is SocketException se)
            return se.SocketErrorCode;
        if (ex is IOException && ex.InnerException is SocketException ise)
            return ise.SocketErrorCode;
        return null;
    }

    // Driver hooks for the sync wrappers and higher-composition sync drivers. SignalWritable
    // fires the cached writable signal, releasing any coroutine awaiter that captured it on
    // WouldBlock. WaitWritable forwards to the transport's wait callback (typically
    // Socket.Poll on a SelectMode.SelectWrite), parking the calling thread until writable.
    public void SignalWritable() => WritableSignal.Signal();
    public void WaitWritable() => waitWritable();

    internal void CopyFrom<TBuffer>(TBuffer buffer) where TBuffer : ICopyableBuffer<byte>
        => buffer.CopyTo(writer);

    internal Func<PgTypeId, Oid> OidLookup { get; } = static pgTypeId => PgTypeCatalog.Default.GetOid(pgTypeId);
    internal Encoding ClientEncoding { get; set; } = clientEncoding;

    public long UnflushedBytes => _bufferingWriter.UnflushedBytes;

    // Arms message-length tracking for a new message. `totalLength` is the on-wire size of the
    // full message (type byte + length field + body, e.g. 5 + bodyLength for normal frontend
    // messages). Validates the previous message wrote exactly its declared length before
    // accepting the new one - the check fires at the next StartMessage boundary so per-write
    // calls stay free. Mid-message flushes are handled by AdvanceMessageBytesFlushed maintaining
    // _messageBytesFlushed as the "bytes already pushed past the buffer" counter for the current
    // message, so the cross-message algebra holds without a flush per message.
    internal void StartMessage(int totalLength)
    {
        var unflushed = checked((int)_bufferingWriter.UnflushedBytes);
        // bytesWrittenForPrevious = unflushed + _messageBytesFlushed; the cross-flush case adds
        // _messageBytesFlushed (negative at start, positive after advances). The previous message
        // is fully written iff that equals _messageLength.
        if (_messageLength is { } prev && unflushed + _messageBytesFlushed != prev)
            ThrowUnderwritten(prev, unflushed + _messageBytesFlushed);
        _messageBytesFlushed = -unflushed;
        _messageLength = totalLength;
    }

    // Advances the message-bytes counter as buffered bytes leave the buffer (Flush). Catches
    // over-writes: a busted converter that emitted more bytes than its message declared trips
    // here before the bytes reach the wire. The buffer-level check is the CVE-2024-32655
    // defense-in-depth for converters that size incorrectly and silently write through.
    void AdvanceMessageBytesFlushed(int count)
    {
        if (_messageLength is null)
            return; // Pre-startup raw writes (e.g. StartupMessage's CopyStartupBuffer).
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count));
        if ((long)_messageBytesFlushed + count > _messageLength)
            ThrowOverwritten(_messageLength.Value, _messageBytesFlushed + (long)count);
        _messageBytesFlushed += count;
    }

    static void ThrowUnderwritten(int declared, int actual)
        => throw new InvalidOperationException($"Message wrote {actual} bytes, declared length was {declared}.");

    static void ThrowOverwritten(int declared, long projected)
        => throw new InvalidOperationException($"Message write would exceed declared length {declared} (projected {projected}).");

    // TODO make a cut-off from where we start streaming the string.
    public ValueTask WriteStringWithNullTerminatorAsync(string value, Encoding encoding, int? encodedLength = null, CancellationToken cancellationToken = default)
    {
        _bufferingWriter.WriteStringWithNullTerminator<BufferingWriter>(value, encoding, encodedLength);
        return new();
    }

    public void WriteStringWithNullTerminator(string value, Encoding encoding, int? encodedLength = null)
        => _bufferingWriter.WriteStringWithNullTerminator<BufferingWriter>(value, encoding, encodedLength);
    public void WriteRaw(ReadOnlySpan<byte> value) => _bufferingWriter.Write(value);
    public void WriteUShort(ushort value) => _bufferingWriter.WriteUInt16BigEndian<BufferingWriter>(value);
    public void WriteByte(byte value) => _bufferingWriter.WriteByte<BufferingWriter>(value);
    public void WriteUInt(uint value) => _bufferingWriter.WriteUInt32BigEndian<BufferingWriter>(value);
    public void WriteInt(int value) => _bufferingWriter.WriteInt32BigEndian<BufferingWriter>(value);

    public void Flush(TimeSpan timeout = default)
    {
        AdvanceMessageBytesFlushed(checked((int)_bufferingWriter.UnflushedBytes));
        _bufferingWriter.Flush(timeout);
    }

    /// Flow-owned escape hatch from a parked flush. Without it the only break-out is protocol
    /// abort. An uncaught firing triggers the protocol's recovery path, so prefer a
    /// coordination-boundary check in connection-preserving flows.
    public ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        AdvanceMessageBytesFlushed(checked((int)_bufferingWriter.UnflushedBytes));
        var task = _bufferingWriter.FlushAsync(_cts.Token);
        if (task.IsCompletedSuccessfully)
            return task;
        return Core(task, cancellationToken);

        async ValueTask Core(ValueTask task, CancellationToken cancellationToken)
        {
            var registration = cancellationToken.UnsafeRegister(static (state, _) => ((CancellationTokenSource)state!).Cancel(), _cts);
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException oce) when (oce.CancellationToken == _cts.Token)
            {
                if (_abortToken.IsCancellationRequested)
                    _control.ThrowIfClosed();
                if (cancellationToken.IsCancellationRequested)
                    throw new OperationCanceledException(cancellationToken);
                throw;
            }
            finally
            {
                registration.Dispose();
                // Recycle after a user-CT cancel so the next call has a fresh CTS. Abort is
                // terminal, leave the CTS cancelled.
                if (_cts.IsCancellationRequested && !_abortToken.IsCancellationRequested)
                    _cts = CancellationTokenSource.CreateLinkedTokenSource(_abortToken);
            }
        }
    }

    // Wrapper to shield callers from slow writers (e.g. PipeWriter).
    struct BufferingWriter(IOutputWriter<byte> writer) : IOutputWriter<byte>
    {
        Memory<byte> _memory;
        ArraySegment<byte> _memoryArray;
        int _remaining;

        int Consumed => _memory.Length - _remaining;

        public long UnflushedBytes => Consumed + writer.UnflushedBytes;

        public void Advance(int count)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(count, _remaining);
            _remaining -= count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            if (_remaining < sizeHint)
            {
                writer.Advance(Consumed);
                _memory = writer.GetMemory(sizeHint);
                MemoryMarshal.TryGetArray(_memory, out _memoryArray);
                _remaining = _memory.Length;
            }

            return _memory.Slice(Consumed);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            if (_remaining < sizeHint)
            {
                writer.Advance(Consumed);
                _memory = writer.GetMemory(sizeHint);
                MemoryMarshal.TryGetArray(_memory, out _memoryArray);
                _remaining = _memory.Length;
            }

            var consumed = Consumed;
            return _memoryArray.Array is not { } array
                ? _memory.Span.Slice(consumed)
                : array.AsSpan(_memoryArray.Offset + consumed, _memoryArray.Count - consumed);
        }

        public void Flush(TimeSpan timeout = default)
        {
            writer.Advance(Consumed);
            _memory = default;
            _memoryArray = default;
            _remaining = 0;
            writer.Flush(timeout);
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default)
        {
            writer.Advance(Consumed);
            _memory = default;
            _memoryArray = default;
            _remaining = 0;
            return writer.FlushAsync(cancellationToken);
        }
    }
}
