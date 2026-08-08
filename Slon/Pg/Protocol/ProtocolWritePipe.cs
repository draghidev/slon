using System.Buffers.Binary;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Slon.Buffers;
using Slon.Buffers.Binary;
using Slon.Pg.Types;
using Slon.Transport;

namespace Slon.Pg.Protocol;

// Shared per-protocol write-side wire state. One instance per protocol, behind any number of
// ProtocolDataWriter shells (the base protocol shell plus a per-exclusive-scope shell). The
// single-pump invariant means only one shell ever drives this pipe at a time, so the message-
// length tracking and BufferingWriter are safe to share. Token-bearing concerns (CTS, abort
// translation and abort-aware flush wrappers) live in the shell, not here; the pipe exposes
// only token-free flush primitives.
sealed class ProtocolWritePipe(IOutputWriter writer, Encoding clientEncoding, Action waitWritable)
{
    BufferingWriter _bufferingWriter = new(writer);

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
    // Signal. Reused across operations thanks to auto-reset on consumption.
    public WriteResumeSignal ResumeSignal { get; } = new();

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
    public void ResumeWrite(Exception? exception = null) => ResumeSignal.Signal(exception);
    public void WaitWritable() => waitWritable();

    internal Func<PgTypeId, Oid> OidLookup { get; } = static pgTypeId => PgTypeCatalog.Default.GetOid(pgTypeId);
    internal Encoding ClientEncoding { get; set; } = clientEncoding;

    public long UnflushedBytes => _bufferingWriter.UnflushedBytes;

    // Buffered bytes above which a flush is forced rather than deferred. Sized to ~one MTU so a forced
    // flush maps to roughly a single segment. Single source of truth: the encoder's in-flow deferral
    // (PgEncoder.CanDeferFlush) and the source's arm gate (PgClientFlowSource) both key off it.
    internal const long UnflushedBytesFlushThreshold = 1000;

    internal Memory<byte> GetMemory(int sizeHint = 0) => _bufferingWriter.GetMemory(sizeHint);
    internal Span<byte> GetSpan(int sizeHint = 0) => _bufferingWriter.GetSpan(sizeHint);
    internal void Advance(int count) => _bufferingWriter.Advance(count);

    // Validates the previous message, arms length tracking for the new one, then writes its
    // five-byte header directly into the buffered span. Keeping these together avoids a second
    // shell traversal and a temporary header copy. Mid-message flushes are handled by
    // AdvanceMessageBytesFlushed maintaining _messageBytesFlushed as the bytes already pushed
    // past the buffer, so the cross-message algebra holds without a flush per message.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void StartMessage(byte type, int bodyLength)
    {
        var totalLength = checked(sizeof(byte) + sizeof(uint) + bodyLength);
        var unflushed = checked((int)_bufferingWriter.UnflushedBytes);
        // bytesWrittenForPrevious = unflushed + _messageBytesFlushed; the cross-flush case adds
        // _messageBytesFlushed (negative at start, positive after advances). The previous message
        // is fully written iff that equals _messageLength.
        if (_messageLength is { } prev && unflushed + _messageBytesFlushed != prev)
            ThrowUnderwritten(prev, unflushed + _messageBytesFlushed);
        _messageBytesFlushed = -unflushed;
        _messageLength = totalLength;

        var header = _bufferingWriter.GetSpan(sizeof(byte) + sizeof(uint));
        header[0] = type;
        BinaryPrimitives.WriteUInt32BigEndian(header.Slice(1), checked(sizeof(uint) + (uint)bodyLength));
        _bufferingWriter.Advance(sizeof(byte) + sizeof(uint));
    }

    // Raw budget seam used by recovery-focused tests and any preframed writer. Unlike the
    // frontend-message overload, this arms tracking without emitting bytes.
    internal void StartMessage(int totalLength)
    {
        var unflushed = checked((int)_bufferingWriter.UnflushedBytes);
        if (_messageLength is { } prev && unflushed + _messageBytesFlushed != prev)
            ThrowUnderwritten(prev, unflushed + _messageBytesFlushed);
        _messageBytesFlushed = -unflushed;
        _messageLength = totalLength;
    }

    // Validate the about-to-flush bytes against the declared length, WITHOUT mutating the counter.
    // Catches over-writes: a busted converter that emitted more bytes than its message declared trips
    // here BEFORE the bytes reach the wire. The buffer-level check is the CVE-2024-32655 defense-in-
    // depth for converters that size incorrectly and silently write through. The counter is committed
    // separately, only after the flush actually drained the bytes (CommitMessageBytesFlushed), so a
    // faulted flush leaves the counter untouched and the bytes still buffered - the next flush
    // re-validates and commits exactly the bytes that remain.
    void CheckMessageBytesFlushed(int count)
    {
        if (_messageLength is null)
            return; // Pre-startup raw writes (e.g. StartupMessage's CopyStartupBuffer).
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count));
        if ((long)_messageBytesFlushed + count > _messageLength)
            ThrowOverwritten(_messageLength.Value, _messageBytesFlushed + (long)count);
    }

    // Commit the counter for bytes that actually left the buffer (the drained delta: before-after
    // UnflushedBytes). Zero on a fault that drained nothing - a harmless no-op.
    void CommitMessageBytesFlushed(int drained)
    {
        if (_messageLength is null)
            return;
        _messageBytesFlushed += drained;
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
    // message-bytes CHECK runs BEFORE the bytes leave the buffer so the over-write check fires before
    // anything reaches the wire (CVE-2024-32655 defense). The counter then commits ONLY the bytes that
    // actually drained, measured as (before - UnflushedBytes-after): a clean flush drains all `count`,
    // a faulted or partial flush leaves the un-sent remainder in UnflushedBytes, so the commit is the
    // prefix that left and the remainder stays buffered for the next flush to re-check and commit. This
    // is correct whether the flush wrote nothing, wrote part-then-faulted, or wrote all - no
    // assumption about all-or-nothing. (The old advance-before-flush over-counted on the shutdown-drain
    // reflush of still-buffered bytes: spurious "exceed declared length".) Committed in finally so it
    // runs on the fault path too, before the exception propagates to the shell's abort translation.
    public void Flush(TimeSpan timeout = default)
    {
        var before = checked((int)_bufferingWriter.UnflushedBytes);
        CheckMessageBytesFlushed(before);
        try
        {
            _bufferingWriter.Flush(timeout);
        }
        finally
        {
            CommitMessageBytesFlushed(before - checked((int)_bufferingWriter.UnflushedBytes));
        }
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        var before = checked((int)_bufferingWriter.UnflushedBytes);
        CheckMessageBytesFlushed(before);
        var task = _bufferingWriter.FlushAsync(cancellationToken);
        if (task.IsCompletedSuccessfully)
        {
            task.GetAwaiter().GetResult();
            _bufferingWriter.RefreshUnflushedBytes();
            CommitMessageBytesFlushed(before - checked((int)_bufferingWriter.UnflushedBytes));
            return ValueTask.CompletedTask;
        }
        return Awaited(this, task, before);

        static async ValueTask Awaited(ProtocolWritePipe self, ValueTask task, int before)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            finally
            {
                self._bufferingWriter.RefreshUnflushedBytes();
                self.CommitMessageBytesFlushed(before - checked((int)self._bufferingWriter.UnflushedBytes));
            }
        }
    }

    // Wrapper to shield callers from slow writers (e.g. PipeWriter).
    struct BufferingWriter : IOutputWriter
    {
        readonly IOutputWriter _writer;
        Memory<byte> _memory;
        ArraySegment<byte> _memoryArray;
        int _remaining;
        long _unflushedBytes;

        public BufferingWriter(IOutputWriter writer)
        {
            _writer = writer;
            _unflushedBytes = writer.UnflushedBytes;
        }

        int Consumed => _memory.Length - _remaining;

        public long UnflushedBytes => _unflushedBytes;

        public void RefreshUnflushedBytes() => _unflushedBytes = _writer.UnflushedBytes;

        public void Advance(int count)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(count, _remaining);
            _remaining -= count;
            _unflushedBytes += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            if (_remaining == 0 || _remaining < sizeHint)
            {
                _writer.Advance(Consumed);
                _memory = _writer.GetMemory(sizeHint);
                MemoryMarshal.TryGetArray(_memory, out _memoryArray);
                _remaining = _memory.Length;
            }

            return _memory.Slice(Consumed);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            if (_remaining == 0 || _remaining < sizeHint)
            {
                _writer.Advance(Consumed);
                _memory = _writer.GetMemory(sizeHint);
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
            _writer.Advance(Consumed);
            _memory = default;
            _memoryArray = default;
            _remaining = 0;
            try
            {
                _writer.Flush(timeout);
            }
            finally
            {
                RefreshUnflushedBytes();
            }
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default)
        {
            _writer.Advance(Consumed);
            _memory = default;
            _memoryArray = default;
            _remaining = 0;
            return _writer.FlushAsync(cancellationToken);
        }
    }
}
