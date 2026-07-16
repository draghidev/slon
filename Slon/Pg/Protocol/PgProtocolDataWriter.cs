using System.Net.Sockets;
using System.Text;
using Slon.Buffers;
using Slon.Pg.Types;
using Slon.Transport;

namespace Slon.Pg.Protocol;

// Thin, poolable write-side shell over a shared WriteChannel. Carries the token-bearing concerns:
// the scope/protocol abort token, its linked CTS (+ recycle), TranslateAbort, and the abort-catch
// wrappers around Flush/FlushAsync. The physical wire state (BufferingWriter, message-length
// tracking, Write*) lives in the channel; the shell delegates. Each exclusive scope gets its own
// shell with the SCOPE token over the one shared channel; the single-pump invariant keeps only one
// shell active at a time.
sealed class PgProtocolDataWriter
{
    readonly WriteChannel _channel;
    readonly CancellationToken _abortToken;
    readonly PgClientProtocol.Control _control;
    CancellationTokenSource _cts;

    PgProtocolDataWriter(WriteChannel channel, CancellationToken abortToken, PgClientProtocol.Control control)
    {
        _channel = channel;
        _abortToken = abortToken;
        _control = control;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(abortToken);
    }

    public PgProtocolDataWriter(IOutputWriter<byte> writer, Encoding clientEncoding, Action waitWritable, CancellationToken abortToken, PgClientProtocol.Control control)
        : this(new WriteChannel(writer, clientEncoding, waitWritable), abortToken, control)
    {
    }

    // Builds a scope-bound shell over an existing channel, carrying the scope's abort token.
    public static PgProtocolDataWriter CreateScopeShell(PgProtocolDataWriter baseShell, CancellationToken abortToken, PgClientProtocol.Control control)
        => new(baseShell._channel, abortToken, control);

    public WriteChannel Channel => _channel;

    public WritableSignal WritableSignal => _channel.WritableSignal;

    public SocketError? GetSocketError(Exception ex) => _channel.GetSocketError(ex);

    public void SignalWritable(Exception? exception = null) => _channel.SignalWritable(exception);
    public void WaitWritable() => _channel.WaitWritable();

    // Abort-to-typed-exception translation shared by the sync flush catch and the resumable driver:
    // the canonical closed exception once the abort token has fired, else the original. Mirrors the
    // async flush catch so every sync seam surfaces PgClientClosedException, not a bare deadline fault.
    // Keyed on the SHELL's token (the scope token for a scope shell), so a scope-only abort fires here.
    public Exception TranslateAbort(Exception ex)
        => _abortToken.IsCancellationRequested && _control.ClosedException is { } closed ? closed : ex;

    internal void CopyFrom<TBuffer>(TBuffer buffer) where TBuffer : ICopyableBuffer<byte>
        => _channel.CopyFrom(buffer);

    internal Func<PgTypeId, Oid> OidLookup => _channel.OidLookup;
    internal Encoding ClientEncoding
    {
        get => _channel.ClientEncoding;
        set => _channel.ClientEncoding = value;
    }

    public long UnflushedBytes => _channel.UnflushedBytes;

    internal const long UnflushedBytesFlushThreshold = WriteChannel.UnflushedBytesFlushThreshold;

    internal void StartMessage(byte type, int bodyLength) => _channel.StartMessage(type, bodyLength);
    internal void StartMessage(int totalLength) => _channel.StartMessage(totalLength);

    internal int CompleteCurrentMessageWithPadding() => _channel.CompleteCurrentMessageWithPadding();

    // TODO make a cut-off from where we start streaming the string.
    public ValueTask WriteStringWithNullTerminatorAsync(string value, Encoding encoding, int? encodedLength = null, CancellationToken cancellationToken = default)
    {
        _channel.WriteStringWithNullTerminator(value, encoding, encodedLength);
        return new();
    }

    public void WriteStringWithNullTerminator(string value, Encoding encoding, int? encodedLength = null)
        => _channel.WriteStringWithNullTerminator(value, encoding, encodedLength);
    public void WriteRaw(ReadOnlySpan<byte> value) => _channel.WriteRaw(value);
    public void WriteUShort(ushort value) => _channel.WriteUShort(value);
    public void WriteByte(byte value) => _channel.WriteByte(value);
    public void WriteUInt(uint value) => _channel.WriteUInt(value);
    public void WriteInt(int value) => _channel.WriteInt(value);

    public void Flush(TimeSpan timeout = default)
    {
        try
        {
            _channel.Flush(timeout);
        }
        catch (Exception ex) when (_abortToken.IsCancellationRequested)
        {
            // Sync writers park on the socket deadline, not the abort token, so an abort that
            // fires mid-flush only surfaces here as the timeout/socket fault once the deadline
            // expires. Translate to the typed closed exception so the late-waking thread sees
            // PgClientClosedException, not a bare TimeoutException.
            throw TranslateAbort(ex);
        }
    }

    /// Flow-owned escape hatch from a parked flush. Without it the only break-out is protocol
    /// abort. An uncaught firing triggers the protocol's recovery path, so prefer a
    /// coordination-boundary check in connection-preserving flows.
    public ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        var task = _channel.FlushAsync(_cts.Token);
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
            catch (Exception ex) when (_abortToken.IsCancellationRequested)
            {
                // Abort canceled the flush. .NET surfaces an entry cancel as an OCE but a mid-flight
                // one as a wrapped IOException/SocketException, so catch type-agnostically (mirrors the
                // sync seam above and the decoder's TranslateReadCancellation) and translate to the
                // closed exception. Keyed on the abort token, not the exception type.
                throw TranslateAbort(ex);
            }
            catch (OperationCanceledException oce) when (oce.CancellationToken == _cts.Token)
            {
                // Not abort (handled above): the flow's own escape-hatch CT fired. Surface as the caller's OCE.
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
}
