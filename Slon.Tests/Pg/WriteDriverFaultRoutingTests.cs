using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Buffers;
using Slon.Pg.Protocol;

namespace Slon.Tests.Pg;

// RunResumableTask drives the sync write coroutine on a LongRunning thread via WaitWritable. The
// WritableSignal the body parks on had no fault field, so a WaitWritable throw (deadline / abort)
// stranded the coroutine and leaked onto the side task instead of reaching the flow. The fix routes
// the throw through WritableSignal.Signal(exception) (after TranslateAbort) so the coroutine
// unwinds with the translated exception.
//
// Isolated on purpose. A full sync CommandFlow always has a following read whose OWN abort
// translation yields the same PgClientClosedException, so a flow-level test passes with or without
// this fix - it can't attribute the result to the write path. Driving RunResumableTask directly
// removes the read, so the assertion pins the write-driver routing alone: without the fix this
// awaiter sees the raw TimeoutException (and the coroutine strands); with it, the closed exception.
[TestClass]
public class WriteDriverFaultRoutingTests
{
    [TestMethod]
    public async Task RunResumableTask_WaitWritableThrowsUnderAbort_RoutesClosedToAwaiter()
    {
        // A real protocol, driven to closed, just to obtain the canonical ClosedException.
        var closedProtocol = await PgTestPool.NewIsolatedAsync();
        await closedProtocol.DisposeAsync();
        var control = new PgClientProtocol.Control(closedProtocol);
        Assert.IsNotNull(control.ClosedException, "protocol should be closed after DisposeAsync");

        // WaitWritable stands in for the parked sync write's deadline/abort fault. A pre-cancelled
        // abort token drives TranslateAbort to the closed exception (decoupled from the disposed CTS).
        Action waitWritable = () => throw new TimeoutException("simulated write-deadline expiry");
        var writer = new PgProtocolDataWriter(
            new MemoryBufferWriter(), Encoding.UTF8, waitWritable, new CancellationToken(canceled: true), control);
        var encoder = new PgEncoder(default, writer);

        // The "write coroutine": parks on the signal exactly as FlushResumable does on WouldBlock.
        async ValueTask Park() => await writer.WritableSignal.Pending();
        var body = Park();
        Assert.IsFalse(body.IsCompleted, "the coroutine should be parked on the signal");

        var driver = encoder.RunResumableTask(body);

        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await driver);
    }
}
