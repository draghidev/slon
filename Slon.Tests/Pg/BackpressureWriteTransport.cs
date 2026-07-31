using System.Buffers;
using System.IO.Pipelines;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Write-direction analogue of GatedReplayTransport (StoppingTokenInMemoryTests): accept up to a
// bounded send window of client bytes, then stall the flush, so a real trailing write task /
// partial flush is produced. This is the only way to manufacture write backpressure, torn
// messages, and flush-failure - the cases recovery's wire-takeover model turns on. See
// docs/recovery-takeover.md.
//
// Reads (server -> client responses, RFQs) are released on demand via ReleaseSegment, exactly
// like GatedReplayTransport. Writes (client -> server) accumulate in a bounded pipe whose
// PauseWriterThreshold is the simulated send window; ReleaseWriteAsync drains capacity to
// resume a parked flush, KillWire faults the next flush (the dead-wire oracle).
sealed class BackpressureWriteTransport : TransportConnection
{
    readonly Pipe _toClient = new();
    readonly Pipe _toServer;

    public BackpressureWriteTransport(byte[] handshake, int sendWindow)
    {
        _toServer = new Pipe(new PipeOptions(
            pauseWriterThreshold: sendWindow,
            resumeWriterThreshold: Math.Max(1, sendWindow / 2),
            useSynchronizationContext: false));

        // Deliver the handshake so StartAsync completes, same as GatedReplayTransport.
        _toClient.Writer.WriteAsync(handshake).AsTask().GetAwaiter().GetResult();
    }

    public override PipeReader Reader => _toClient.Reader;
    public override PipeWriter Writer => _toServer.Writer;
    public override void WaitWritable() { }

    // Server -> client: release a pre-scripted response segment (the handshake is already out).
    public void ReleaseSegment(byte[] bytes)
    {
        try
        {
            _toClient.Writer.WriteAsync(bytes).AsTask().GetAwaiter().GetResult();
        }
        catch
        {
            // Reader completed during shutdown - the released bytes have no consumer.
        }
    }

    // Drain up to `count` of the bytes the client has written, freeing send-window capacity so a
    // parked FlushAsync resumes. Returns the number of bytes actually drained.
    public async Task<long> ReleaseWriteAsync(long count)
    {
        long drained = 0;
        while (drained < count)
        {
            if (!_toServer.Reader.TryRead(out var result))
                result = await _toServer.Reader.ReadAsync().ConfigureAwait(false);

            var take = Math.Min(count - drained, result.Buffer.Length);
            drained += take;
            _toServer.Reader.AdvanceTo(result.Buffer.Slice(0, take).End);

            if (result.IsCompleted && result.Buffer.Length == take)
                break;
        }
        return drained;
    }

    // Drain everything the client has currently buffered (non-blocking), freeing the window so a
    // parked flush resumes. Returns bytes drained. Use when you want to unblock without knowing
    // the exact pending count (e.g. resuming a parked startup).
    public long DrainAvailable()
    {
        long drained = 0;
        while (_toServer.Reader.TryRead(out var result))
        {
            var len = result.Buffer.Length;
            _toServer.Reader.AdvanceTo(result.Buffer.End);
            drained += len;
            if (result.IsCompleted || len == 0)
                break;
        }
        return drained;
    }

    // Drain the client's buffered writes up to and including the Mth Sync ('S') frontend message,
    // leaving the rest in the pipe (to be discarded by a following KillWire). Models a write
    // truncated after M independently-synced commands. Returns the number of Syncs drained.
    public int DrainUntilSyncs(int syncCount)
    {
        if (syncCount <= 0 || !_toServer.Reader.TryRead(out var result))
            return 0;

        var buffer = result.Buffer;
        var reader = new SequenceReader<byte>(buffer);
        var seen = 0;
        var consumeTo = buffer.Start;
        while (reader.Remaining >= 5)
        {
            reader.TryRead(out var type);
            reader.TryReadBigEndian(out int lenField); // includes its own 4 bytes, excludes the type
            var body = lenField - 4;
            if (body < 0 || reader.Remaining < body)
                break; // partial trailing message; stop at the last complete boundary
            reader.Advance(body);

            if (type == (byte)'S')
            {
                seen++;
                consumeTo = reader.Position;
                if (seen >= syncCount)
                    break;
            }
        }

        _toServer.Reader.AdvanceTo(consumeTo);
        return seen;
    }

    // Wait until the client's buffered writes contain `marker` (e.g. recovery's resync ROLLBACK).
    // Examines without consuming, so send-window accounting is untouched and later drains see the
    // full stream; each new flush re-triggers the scan. Do not run concurrently with the draining
    // methods above - both sides read _toServer.
    public async Task WaitForWrittenAsync(string marker)
    {
        var pattern = System.Text.Encoding.ASCII.GetBytes(marker);
        while (true)
        {
            var result = await _toServer.Reader.ReadAsync().ConfigureAwait(false);
            var buffer = result.Buffer;
            var found = Contains(buffer, pattern);
            _toServer.Reader.AdvanceTo(buffer.Start, buffer.End);
            if (found)
                return;
            if (result.IsCompleted)
                throw new InvalidOperationException($"client writer completed before '{marker}' was written");
        }

        static bool Contains(ReadOnlySequence<byte> buffer, ReadOnlySpan<byte> pattern)
        {
            if (buffer.IsSingleSegment)
                return buffer.FirstSpan.IndexOf(pattern) >= 0;
            var flat = new byte[buffer.Length];
            buffer.CopyTo(flat);
            return flat.AsSpan().IndexOf(pattern) >= 0;
        }
    }

    // Kill the wire: completing the reader with an exception makes the client's next FlushAsync
    // throw it - the dead-wire oracle case for recovery's flush WhenAny arm.
    public void KillWire(Exception exception) => _toServer.Reader.Complete(exception);
}
