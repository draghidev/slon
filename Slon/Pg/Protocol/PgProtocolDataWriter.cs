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

    public void Flush(TimeSpan timeout = default) => _bufferingWriter.Flush(timeout);

    /// Flow-owned escape hatch from a parked flush. Without it the only break-out is protocol
    /// abort. An uncaught firing triggers the protocol's recovery path, so prefer a
    /// coordination-boundary check in connection-preserving flows.
    public ValueTask FlushAsync(CancellationToken cancellationToken)
    {
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
