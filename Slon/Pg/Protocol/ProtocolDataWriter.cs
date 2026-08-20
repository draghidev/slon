using System.Buffers;
using System.Net.Sockets;
using System.Text;
using Slon.Buffers;
using Slon.Pg.Types;
using Slon.Transport;

namespace Slon.Pg.Protocol;

// Thin, poolable write-side shell over a shared ProtocolWritePipe. Carries the token-bearing concerns:
// the scope/protocol abort token, its linked CTS (+ recycle), TranslateAbort, and the abort-catch
// wrappers around Flush/FlushAsync. The physical wire state (BufferingWriter, message-length
// tracking and writes) lives in the pipe; the shell delegates. Each exclusive scope gets its own
// shell with the scope token over the shared pipe; the single-pump invariant keeps only one
// shell active at a time.
sealed class ProtocolDataWriter : IOutputWriter
{
    readonly ProtocolWritePipe _pipe;
    readonly CancellationToken _abortToken;
    readonly PgClientProtocol.Control _control;
    CancellationTokenSource _cts;
    ParameterWriterStrategy? _parameterWriterStrategy;
    object? _parameterWriterState;
    Encoding? _parameterWriterEncoding;

    ProtocolDataWriter(ProtocolWritePipe pipe, CancellationToken abortToken, PgClientProtocol.Control control)
    {
        _pipe = pipe;
        _abortToken = abortToken;
        _control = control;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(abortToken);
    }

    public ProtocolDataWriter(IOutputWriter writer, Encoding clientEncoding, Action waitWritable, CancellationToken abortToken, PgClientProtocol.Control control)
        : this(new ProtocolWritePipe(writer, clientEncoding, waitWritable), abortToken, control)
    {
    }

    // Builds a scope-bound shell over the shared pipe with the scope's abort token.
    public static ProtocolDataWriter CreateScopeShell(ProtocolDataWriter baseShell, CancellationToken abortToken, PgClientProtocol.Control control)
        => new(baseShell._pipe, abortToken, control);

    public ProtocolWritePipe Pipe => _pipe;

    public WriteResumeSignal ResumeSignal => _pipe.ResumeSignal;

    public SocketError? GetSocketError(Exception ex) => _pipe.GetSocketError(ex);

    public void ResumeWrite(Exception? exception = null) => _pipe.ResumeWrite(exception);
    public void WaitWritable() => _pipe.WaitWritable();

    // Abort-to-typed-exception translation shared by the sync flush catch and the resumable driver:
    // the flow-termination verdict once the abort token has fired, else the original. The protocol's
    // completion retains the canonical close; I/O driven for a flow receives its per-flow verdict.
    // Keyed on the SHELL's token (the scope token for a scope shell), so a scope-only abort fires here.
    public Exception TranslateAbort(Exception ex)
        => _abortToken.IsCancellationRequested && _control.ClosedException is not null
            ? _control.FlowTerminationException
            : ex;

    internal Func<PgTypeId, Oid> OidLookup => _pipe.OidLookup;
    internal Encoding ClientEncoding
    {
        get => _pipe.ClientEncoding;
        set => _pipe.ClientEncoding = value;
    }

    // The state may retain this token-bearing shell (PgWriter does), so cache it here rather
    // than on the shared pipe. A base protocol and each exclusive scope consequently retain
    // their own opaque serializer state while Bind remains allocation-free after first use.
    internal object GetParameterWriterState(ParameterWriterStrategy strategy)
    {
        var encoding = ClientEncoding;
        if (!ReferenceEquals(_parameterWriterStrategy, strategy)
            || !ReferenceEquals(_parameterWriterEncoding, encoding))
        {
            _parameterWriterState = strategy.CreateWriterState(this, encoding);
            _parameterWriterStrategy = strategy;
            _parameterWriterEncoding = encoding;
        }
        return _parameterWriterState!;
    }

    public long UnflushedBytes => _pipe.UnflushedBytes;

    public Memory<byte> GetMemory(int sizeHint = 0) => _pipe.GetMemory(sizeHint);
    public Span<byte> GetSpan(int sizeHint = 0) => _pipe.GetSpan(sizeHint);
    public void Advance(int count) => _pipe.Advance(count);

    internal const long UnflushedBytesFlushThreshold = ProtocolWritePipe.UnflushedBytesFlushThreshold;

    internal void StartMessage(byte type, int bodyLength) => _pipe.StartMessage(type, bodyLength);
    internal void StartMessage(int totalLength) => _pipe.StartMessage(totalLength);

    internal int CurrentMessagePaddingLength => _pipe.CurrentMessagePaddingLength;
    internal int CompleteCurrentMessageWithPadding(int maxBytes = int.MaxValue)
        => _pipe.CompleteCurrentMessageWithPadding(maxBytes);

    internal void WriteTerminate()
        => _pipe.StartMessage(PgTypes.FrontendType.Terminate.ToByte(), bodyLength: 0);

    // TODO make a cut-off from where we start streaming the string.
    public ValueTask WriteStringWithNullTerminatorAsync(string value, Encoding encoding, int? encodedLength = null, CancellationToken cancellationToken = default)
    {
        _pipe.WriteStringWithNullTerminator(value, encoding, encodedLength);
        return new();
    }

    public void WriteStringWithNullTerminator(string value, Encoding encoding, int? encodedLength = null)
        => _pipe.WriteStringWithNullTerminator(value, encoding, encodedLength);
    public void WriteRaw(ReadOnlySpan<byte> value) => _pipe.WriteRaw(value);
    public void WriteUShort(ushort value) => _pipe.WriteUShort(value);
    public void WriteByte(byte value) => _pipe.WriteByte(value);
    public void WriteUInt(uint value) => _pipe.WriteUInt(value);
    public void WriteInt(int value) => _pipe.WriteInt(value);

    public void Flush(TimeSpan timeout = default)
    {
        try
        {
            _pipe.Flush(timeout);
        }
        catch (Exception) when (_control.ClosedException is not null)
        {
            throw _control.FlowTerminationException;
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

    /// Flow-owned cancellation path for a parked flush. Without it the only break-out is protocol
    /// abort. An uncaught firing triggers the protocol's recovery path, so prefer a
    /// coordination-boundary check in connection-preserving flows.
    public ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        ValueTask task;
        try
        {
            task = _pipe.FlushAsync(_cts.Token);
        }
        catch (Exception ex)
        {
            // Async I/O reports operational failure through its awaitable even when the lower writer
            // discovers it synchronously. This keeps orchestration callers free of EH-only inline walls.
            task = ValueTask.FromException(
                _control.ClosedException is not null ? _control.FlowTerminationException : ex);
        }
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
            catch (Exception) when (_control.ClosedException is not null)
            {
                throw _control.FlowTerminationException;
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
