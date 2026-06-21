using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Slon.Buffers;
using Slon.Buffers.Binary;
using Slon.Pg.Types;
using Slon.Transport;

namespace Slon.Pg.Protocol;

// Shared per-protocol write-side wire state. One instance per protocol, behind any number of
// PgProtocolDataWriter shells (the base protocol shell plus a per-exclusive-scope shell). The
// single-pump invariant means only one shell ever drives this channel at a time, so the message-
// length tracking and BufferingWriter are safe to share. Token-bearing concerns (CTS, abort
// translation, the abort-catch flush wrappers) live in the shell, not here; the channel exposes
// only token-free flush primitives.
sealed class WriteChannel
{
    readonly IOutputWriter<byte> _writer;
    BufferingWriter _bufferingWriter;

    // Current message state (npgsql NpgsqlWriteBuffer.StartMessage / AdvanceMessageBytesFlushed
    // pattern, GHSA-x9vc-6hfv-hg8c). Validation is structural: per-write Write* methods are
    // unchecked, the check rolls up at Flush and at the next StartMessage. `_messageBytesFlushed`
    // is anchored at -unflushedAtStart so `unflushed + _messageBytesFlushed = bytesWrittenForCurrentMessage`
    // across mid-message flushes too - lets multiple messages stack in the buffer and still get
    // validated at message boundaries without forcing a flush per message.
    int? _messageLength;
    int _messageBytesFlushed;

    readonly Action _waitWritable;

    // Per-connection cached signal. The flow parks this in
    // TransportConnection.SyncNonBlockingSignal around each Resumable call, the transport
    // returns it on WouldBlock as the pending source, the flow's driver fires it via
    // Signal. Reused across operations thanks to auto-reset on consumption.
    public WritableSignal WritableSignal { get; } = new();

    public WriteChannel(IOutputWriter<byte> writer, Encoding clientEncoding, Action waitWritable)
    {
        _writer = writer;
        _bufferingWriter = new(writer);
        _waitWritable = waitWritable;
        ClientEncoding = clientEncoding;
    }

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

    // Driver hooks for the sync wrappers and higher-composition sync drivers. Signal
    // fires the cached writable signal, releasing any coroutine awaiter that captured it on
    // WouldBlock. WaitWritable forwards to the transport's wait callback (typically
    // Socket.Poll on a SelectMode.SelectWrite), parking the calling thread until writable.
    public void SignalWritable(Exception? exception = null) => WritableSignal.Signal(exception);
    public void WaitWritable() => _waitWritable();

    internal void CopyFrom<TBuffer>(TBuffer buffer) where TBuffer : ICopyableBuffer<byte>
        => buffer.CopyTo(_writer);

    internal Func<PgTypeId, Oid> OidLookup { get; } = static pgTypeId => PgTypeCatalog.Default.GetOid(pgTypeId);
    internal Encoding ClientEncoding { get; set; }

    public long UnflushedBytes => _bufferingWriter.UnflushedBytes;

    // Buffered bytes above which a flush is forced rather than deferred. Sized to ~one MTU so a forced
    // flush maps to roughly a single segment. Single source of truth: the encoder's in-flow deferral
    // (PgEncoder.CanDelayFlush) and the source's arm gate (PgClientFlowSource) both key off it.
    internal const long UnflushedBytesFlushThreshold = 1000;

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

    // Recovery: pad the current torn message to its declared length with zero bytes so the
    // server's framing reader exits the message at the declared boundary and resyncs on the
    // subsequent Sync. Returns the byte count written (0 if no message in flight, or the
    // declared length was already reached). After padding, the next StartMessage's validation
    // sees a complete message and the buffer is safe to continue / flush.
    //
    // INVARIANT (flow-level, not enforced here): a padded message is NEVER followed by its
    // own Execute - the zero-padded body can parse as a valid Bind whose corrupted parameter
    // would silently corrupt data. Recovery's job is to inject Sync and drain RFQs, not to
    // continue any pipelined work; the unexecuted portal dies at Sync.
    internal int CompleteCurrentMessageWithPadding()
    {
        if (_messageLength is null)
            return 0;
        var unflushed = checked((int)_bufferingWriter.UnflushedBytes);
        var remaining = _messageLength.Value - (unflushed + _messageBytesFlushed);
        if (remaining <= 0)
            return 0;
        var padded = remaining;

        Span<byte> zeros = stackalloc byte[256];
        zeros.Clear();
        while (remaining > 0)
        {
            var chunk = Math.Min(remaining, zeros.Length);
            _bufferingWriter.Write(zeros.Slice(0, chunk));
            remaining -= chunk;
        }
        return padded;
    }

    public void WriteStringWithNullTerminator(string value, Encoding encoding, int? encodedLength = null)
        => _bufferingWriter.WriteStringWithNullTerminator<BufferingWriter>(value, encoding, encodedLength);
    public void WriteRaw(ReadOnlySpan<byte> value) => _bufferingWriter.Write(value);
    public void WriteUShort(ushort value) => _bufferingWriter.WriteUInt16BigEndian<BufferingWriter>(value);
    public void WriteByte(byte value) => _bufferingWriter.WriteByte<BufferingWriter>(value);
    public void WriteUInt(uint value) => _bufferingWriter.WriteUInt32BigEndian<BufferingWriter>(value);
    public void WriteInt(int value) => _bufferingWriter.WriteInt32BigEndian<BufferingWriter>(value);

    // Token-free flush primitives. The shell wraps these with the abort-catch translation; the
    // message-bytes advance must run before the bytes leave the buffer so the over-write check fires.
    public void Flush(TimeSpan timeout = default)
    {
        AdvanceMessageBytesFlushed(checked((int)_bufferingWriter.UnflushedBytes));
        _bufferingWriter.Flush(timeout);
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        AdvanceMessageBytesFlushed(checked((int)_bufferingWriter.UnflushedBytes));
        return _bufferingWriter.FlushAsync(cancellationToken);
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
