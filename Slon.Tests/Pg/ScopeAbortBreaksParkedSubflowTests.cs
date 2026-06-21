using System.Buffers;
using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// A scope-only abort (the ADO connection-dispose lever) must break a subflow parked on a wire read
// while the pooled protocol SURVIVES (its own token never trips). The scope-bound decoder/writer shells
// over the shared Read/WriteChannel carry the scope's token, so AbortActiveScope reaches a parked
// subflow; the protocol's own base shells keep the protocol token, so the pool unit is unaffected.
[TestClass]
[DoNotParallelize]
public class ScopeAbortBreaksParkedSubflowTests
{
    static Task<PgClientProtocol> ConnectAsync() => PgTestPool.NewIsolatedAsync(o =>
    {
        // Default heartbeat is 1s; the cascade-driven activation/timeout paths key off it, so tighten it.
        o.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
    });

    static Exception GetRoot(Exception ex)
    {
        while (ex.InnerException is not null && ex is not PgClientClosedException)
            ex = ex.InnerException;
        return ex;
    }

    [TestMethod]
    public async Task ScopeAbort_BreaksSubflowParkedOnRead_ProtocolSurvives()
    {
        var protocol = await ConnectAsync();
        try
        {
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;

            // pg_sleep keeps the subflow's body parked on a wire read (awaiting the row/RFQ).
            var sub = scope.Queue(new CommandFlow(async: true, Command.Create("select pg_sleep(5)")));

            var runTask = Task.Run(async () =>
            {
                var e = sub.GetAsyncEnumerator();
                try
                {
                    while (await e.MoveNextAsync()) { }
                    await e.DisposeAsync();
                    return (Exception?)null;
                }
                catch (Exception ex)
                {
                    return ex;
                }
            });

            // Let the subflow reach its parked read before tripping the scope-only abort.
            await Task.Delay(50);
            protocol.AbortActiveScope();

            var observed = await runTask.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsNotNull(observed, "Scope-only abort should have broken the parked read.");
            Assert.IsInstanceOfType<PgClientClosedException>(GetRoot(observed));

            // The pooled protocol must NOT have tripped its own token.
            Assert.IsFalse(protocol.IsCompleted, "Protocol must survive a scope-only abort.");
        }
        finally
        {
            await protocol.DisposeAsync();
        }
    }

    // Write side, isolated like WriteDriverFaultRoutingTests. A real parked socket write needs full
    // TCP back-pressure (flaky), so this drives the scope writer shell directly: a sink whose FlushAsync
    // parks on the passed token stands in for the parked write. CreateScopeShell links the shell's CTS
    // to the SCOPE token, so tripping the scope abort cancels the flush, and TranslateAbort (keyed on the
    // scope token + the control's ClosedException) surfaces PgClientClosedException. Without the scope-
    // bound shell the writer would key on the protocol token and never break.
    [TestMethod]
    public async Task ScopeAbort_BreaksWriterParkedOnFlush_ViaScopeToken()
    {
        // A real closed protocol just to obtain the canonical ClosedException via a Control.
        var closedProtocol = await ConnectAsync();
        await closedProtocol.DisposeAsync();
        var control = new PgClientProtocol.Control(closedProtocol, poolFacing: true);
        Assert.IsNotNull(control.ClosedException, "protocol should be closed after DisposeAsync");

        using var scopeAbort = new CancellationTokenSource();
        var sink = new ParkOnFlushSink();
        var baseShell = new PgProtocolDataWriter(sink, Encoding.UTF8, static () => { }, default, control);
        var scopeShell = PgProtocolDataWriter.CreateScopeShell(baseShell, scopeAbort.Token, control);

        var flushTask = scopeShell.FlushAsync(CancellationToken.None);
        Assert.IsFalse(flushTask.IsCompleted, "the flush should be parked on the sink");

        scopeAbort.Cancel();

        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await flushTask);
    }

    // Sink whose async flush parks on the token the writer passes (its CTS, linked to the scope token).
    sealed class ParkOnFlushSink : IOutputWriter<byte>
    {
        byte[] _buffer = new byte[4096];
        public long UnflushedBytes => 0;
        public void Advance(int count) { }
        public Memory<byte> GetMemory(int sizeHint = 0) => _buffer;
        public Span<byte> GetSpan(int sizeHint = 0) => _buffer;
        public void Flush(TimeSpan timeout = default) { }
        public async ValueTask FlushAsync(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var reg = cancellationToken.Register(static s => ((TaskCompletionSource)s!).TrySetCanceled(), tcs);
            await tcs.Task.ConfigureAwait(false);
        }
    }
}
