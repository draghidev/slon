using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// A sync read is a blocking syscall no cancellation token reaches; only closing the socket unblocks
// it. Forceful DisposeAsync now aborts the transport (closes the socket) as part of firing
// AbortToken, BEFORE the drain awaits the in-flight flows - so a flow parked in a sync read faults,
// the read translation maps it to PgClientClosedException, the flow completes, and the drain finishes.
// Before the transport-disposal fix this hung (the socket was never closed, so the parked read never
// returned and the drain waited on it forever) and leaked the socket on every teardown.
[TestClass]
[DoNotParallelize]
public class SyncTeardownTests
{
    static readonly TimeSpan Cap = TimeSpan.FromSeconds(10);

    [TestMethod]
    public async Task SyncRead_ParkedAwaitingResponse_ForcefulDispose_SeesClosed()
    {
        var protocol = await PgTestPool.NewIsolatedAsync();

        Exception? observed = null;
        var bg = Task.Run(() =>
        {
            try
            {
                // pg_sleep withholds the response, so the sync read parks in the blocking syscall.
                var flow = new CommandFlow(async: false, Command.Create("select pg_sleep(30)"));
                Assert.IsTrue(protocol.TryQueue(flow));
                var e = flow.GetEnumerator();
                while (e.MoveNext()) { }
                e.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                observed = ex;
            }
        });

        // Let the query reach the wire and the read park on the (withheld) response. 100ms is ~10x the
        // real localhost park time; the test doesn't depend on the exact value, only that the read is
        // parked before DisposeAsync.
        await Task.Delay(100);

        // Forceful: fires AbortToken and aborts the transport (closes the socket), which is the only
        // thing that breaks the otherwise-uninterruptible blocking read.
        await protocol.DisposeAsync();

        await bg.WaitAsync(Cap);

        Assert.IsNotNull(observed, "the parked sync read should have surfaced an exception, not hung");
        Assert.IsInstanceOfType<PgClientClosedException>(Root(observed!),
            $"sync read surfaced {Root(observed!).GetType().Name}, expected PgClientClosedException");

        static Exception Root(Exception ex)
        {
            while (ex is not PgClientClosedException && ex.InnerException is not null)
                ex = ex.InnerException;
            return ex;
        }
    }
}
