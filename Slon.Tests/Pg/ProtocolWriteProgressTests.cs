using System.Buffers.Binary;
using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Write-side progress when ordinary socket tests cannot isolate the mechanism: a pending async flush
// must allow the read side to advance, and a fault in the synchronous write driver must reach its awaiter.
[TestClass]
public class ProtocolWriteProgressTests
{
    // Window << request, and the request exceeds the encoder's deferred-flush threshold, so the
    // command's FlushAsync actually runs and parks.
    const int SendWindow = 4096;
    const int RequestPadding = 16 * 1024;

    [TestMethod]
    public async Task AsyncFlow_BackpressuredWrite_ReadDrainsConcurrently_NoDeadlock()
    {
        var options = PgTestPool.NewOptions();
        var transport = new BackpressureWriteTransport(Handshake(), sendWindow: SendWindow);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);

        // Clear the startup bytes so the only thing that can fill the send window is the command.
        transport.DrainAvailable();

        // The server's response is already sitting in the receive buffer; the client just has to read it.
        // NoData + a row-less CommandComplete keeps the script minimal (no RowDescription/DataRow encoding).
        transport.ReleaseSegment(ParseComplete());
        transport.ReleaseSegment(BindComplete());
        transport.ReleaseSegment(NoData());
        transport.ReleaseSegment(CommandComplete());
        transport.ReleaseSegment(ReadyForQuery());

        // SQL padded (via a comment) past both the send window and the encoder's deferred-flush threshold,
        // so the command's flush genuinely back-pressures.
        var sql = "select 1 /* " + new string('x', RequestPadding) + " */";
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));

        // Model the TCP cycle: the server only drains the client's parked request AFTER the client has read
        // the server's response. Under the correct (trailing) design the read makes progress concurrently,
        // this fires, the window drains, and the parked flush resumes. Under an inline write-await the read
        // never starts, this never fires, and the test times out on the first MoveNextAsync.
        var readMadeProgress = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = Task.Run(async () =>
        {
            await readMadeProgress.Task.ConfigureAwait(false);
            long drained = 0;
            for (var i = 0; i < 100 && drained < RequestPadding; i++)
            {
                drained += transport.DrainAvailable();
                if (drained < RequestPadding)
                    await Task.Delay(5).ConfigureAwait(false);
            }
        });

        try
        {
            var e = flow.GetAsyncEnumerator();

            // The discriminating wait: completes only if the read drained the wire while the write was still
            // parked on the send window. A deadlock surfaces here as a TimeoutException, not a hung runner.
            var first = await e.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            readMadeProgress.TrySetResult();
            Assert.IsTrue(first, "first result must be delivered from the concurrently-drained read");

            while (await e.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10))) { }
            await e.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            readMadeProgress.TrySetResult();
            try { await protocol.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(5)); }
            catch { }
        }
    }

    static byte[] Handshake()
    {
        var b = new byte[64];
        var o = 0;
        b[o++] = (byte)'R'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 0); o += 4;
        b[o++] = (byte)'K'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 12); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 4321); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8765); o += 4;
        b[o++] = (byte)'Z'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 5); o += 4; b[o++] = (byte)'I';
        return b.AsSpan(0, o).ToArray();
    }

    static byte[] EmptyMessage(char type)
    {
        var msg = new byte[5];
        msg[0] = (byte)type;
        BinaryPrimitives.WriteInt32BigEndian(msg.AsSpan(1), 4);
        return msg;
    }

    static byte[] ParseComplete() => EmptyMessage('1');
    static byte[] BindComplete() => EmptyMessage('2');
    static byte[] NoData() => EmptyMessage('n');

    static byte[] CommandComplete()
    {
        ReadOnlySpan<byte> body = "SELECT 1 "u8;
        var msg = new byte[1 + 4 + body.Length];
        msg[0] = (byte)'C';
        BinaryPrimitives.WriteInt32BigEndian(msg.AsSpan(1), 4 + body.Length);
        body.CopyTo(msg.AsSpan(5));
        return msg;
    }

    static byte[] ReadyForQuery()
    {
        var msg = new byte[6];
        msg[0] = (byte)'Z';
        BinaryPrimitives.WriteInt32BigEndian(msg.AsSpan(1), 5);
        msg[5] = (byte)'I';
        return msg;
    }

    [TestMethod]
    public async Task RunResumableTask_WaitWritableThrowsUnderAbort_RoutesClosedToAwaiter()
    {
        // A real protocol, driven to closed, just to obtain the canonical ClosedException.
        var closedProtocol = await PgTestPool.NewIsolatedAsync();
        await closedProtocol.DisposeAsync();
        var control = new PgClientProtocol.Control(closedProtocol, poolFacing: true);
        Assert.IsNotNull(control.ClosedException, "protocol should be closed after DisposeAsync");

        // WaitWritable stands in for the parked sync write's deadline/abort fault. A pre-cancelled
        // abort token drives TranslateAbort to the closed exception (decoupled from the disposed CTS).
        Action waitWritable = () => throw new TimeoutException("simulated write-deadline expiry");
        var writer = new ProtocolDataWriter(
            new BufferOutputWriter(), Encoding.UTF8, waitWritable, new CancellationToken(canceled: true), control);
        var encoder = new PgEncoder(default, writer);

        // The "write coroutine": parks on the signal exactly as FlushResumable does on WouldBlock.
        async ValueTask Park() => await writer.ResumeSignal.Pending();
        var body = Park();
        Assert.IsFalse(body.IsCompleted, "the coroutine should be parked on the signal");

        var driver = encoder.RunResumableTask(body);

        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await driver);
    }
}
