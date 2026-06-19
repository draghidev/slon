using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using Slon.Transport;

namespace Slon.Pg.Protocol;

// Postgres side-channel cancel request. A fresh TCP connection is opened to the server, the
// 16-byte CancelRequest is written, and the server responds by attempting to cancel the
// currently-running query on the backend matching (processId, secretKey) and then FINing the
// side connection. There is no in-band ack - the FIN is the only signal that the server
// received and acted on the request. Recontextualization back to "did the cancel land on the
// command we meant" happens via post-hoc attribution on the main protocol's read stream
// (cancel ERROR with SQLSTATE 57014 arrives or doesn't).
//
// The message format is unusual for Postgres frontend messages: no leading type byte. Same
// shape as StartupMessage - the framing is implied by it being the only thing the server
// expects on a fresh connection. So this helper writes raw bytes rather than going through
// PgEncoder.StartMessage.
static class CancelRequest
{
    const int MessageLength = sizeof(int) * 4; // length + code + processId + secretKey
    const int CancelRequestCode = (1234 << 16) | 5678; // 80877102 = 0x04D2162E, per protocol docs.

    // Sends the 16-byte cancel request on the provided transport and waits for the server's FIN.
    // Caller owns the transport lifecycle: opens the connection (with the same TLS / endpoint
    // policy as the main connection it's cancelling) before calling, releases it (abortive close +
    // endpoint completion - the connection has no Dispose surface) after. The cancel
    // request itself is fire-and-forget at the protocol level - no in-band ack - so this method
    // returns when the server closes its end (the only confirmation that the request was
    // received). Throws on transport faults.
    public static async ValueTask SendAsync(TransportConnection transport, int processId, int secretKey, CancellationToken cancellationToken = default)
    {
        var writer = transport.Writer;
        var span = writer.GetSpan(MessageLength);
        BinaryPrimitives.WriteInt32BigEndian(span, MessageLength);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(sizeof(int)), CancelRequestCode);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(sizeof(int) * 2), processId);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(sizeof(int) * 3), secretKey);
        writer.Advance(MessageLength);

        var flushResult = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        if (flushResult.IsCanceled)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new OperationCanceledException();
        }

        // Wait for the server's FIN. Reading until EOF is the canonical "cancel was processed"
        // signal - the server reads, attempts the cancel, then closes its end of the
        // connection. A non-empty response is unexpected and indicates the server is not in
        // the post-startup cancel-handling state (misconfiguration or hostile peer); we
        // surface that as the rare diagnostic case rather than treating it as a normal response.
        var reader = transport.Reader;
        while (true)
        {
            var readResult = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (readResult.IsCanceled)
            {
                cancellationToken.ThrowIfCancellationRequested();
                throw new OperationCanceledException();
            }
            reader.AdvanceTo(readResult.Buffer.End);
            if (readResult.IsCompleted)
                return; // FIN received - cancel delivered.
        }
    }
}
