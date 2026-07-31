using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Multi-waiter exclusive scopes: N BeginExclusiveScope waiters share one ExclusiveScopeState, serialized
// by the outer pipeline's ordering (the fair hand-out). Covers concurrent begins, fair FIFO turn order,
// consumer-detach (canceled BeginScopeAsync), done-before-executed fast retire, and the pre-turn cascade
// path (a waiter torn down by a protocol stop before it ever won its turn).
[TestClass]
public class ExclusiveScopeMultiWaiterTests
{
    static Task<PgClientProtocol> ConnectAsync() => PgTestPool.NewIsolatedAsync(o =>
    {
        o.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
    });

    static async Task DrainAsync(CommandFlow flow)
    {
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    // Two concurrent begins: B is enqueued while A holds the scope. B's turn must not land until A ends.
    [TestMethod]
    public async Task TwoConcurrentScopes_SerializeFairly()
    {
        var protocol = await ConnectAsync();
        try
        {
            var a = protocol.BeginExclusiveScope(async: true);
            await a.HandoffReady;

            // B begins while A still holds the wire - a second waiter on the outer pipeline.
            var b = protocol.BeginExclusiveScope(async: true);
            Assert.IsFalse(b.HandoffReady.IsCompleted, "B must not acquire while A holds the scope");

            await DrainAsync(a.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
            await a.CompleteScopeAsync();

            // Now B's turn lands.
            await b.HandoffReady.WaitAsync(TimeSpan.FromSeconds(10));
            await DrainAsync(b.Queue(new CommandFlow(async: true, Command.Create("select 2"))));
            await b.CompleteScopeAsync();
        }
        finally
        {
            await protocol.DisposeAsync();
        }
    }

    // Canceling a waiter's BeginScopeAsync before its turn DETACHES the consumer: the flow still takes its
    // turn (it is issued) and retires fast without holding the wire, so a later waiter runs cleanly and the
    // protocol survives.
    [TestMethod]
    public async Task CancelBeginScope_BeforeTurn_DetachesAndNextWaiterProceeds()
    {
        var protocol = await ConnectAsync();
        try
        {
            var a = protocol.BeginExclusiveScope(async: true);
            await a.HandoffReady;

            // B waits behind A; cancel B before A releases, so B never wins its turn with a consumer.
            var b = protocol.BeginExclusiveScope(async: true);
            using var cts = new CancellationTokenSource();
            var bWait = b.BeginScopeAsync(cts.Token);
            cts.Cancel();
            await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () => await bWait);

            // C waits behind B. Releasing A lets B retire fast (consumer-gone) and C acquire.
            var c = protocol.BeginExclusiveScope(async: true);
            await DrainAsync(a.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
            await a.CompleteScopeAsync();

            await c.HandoffReady.WaitAsync(TimeSpan.FromSeconds(10));
            await DrainAsync(c.Queue(new CommandFlow(async: true, Command.Create("select 3"))));
            await c.CompleteScopeAsync();

            Assert.IsFalse(protocol.IsCompleted, "protocol must survive a consumer-detach");
        }
        finally
        {
            await protocol.DisposeAsync();
        }
    }

    // Canceling a waiter's BeginScopeAsync AFTER it won the handoff is too late: it resolves normally and
    // the caller owns the scope.
    [TestMethod]
    public async Task CancelBeginScope_AfterHandoff_IsTooLate()
    {
        var protocol = await ConnectAsync();
        try
        {
            var a = protocol.BeginExclusiveScope(async: true);
            using var cts = new CancellationTokenSource();
            await a.BeginScopeAsync(cts.Token); // wins immediately (no contention)
            cts.Cancel(); // too late
            await DrainAsync(a.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
            await a.CompleteScopeAsync();
        }
        finally
        {
            await protocol.DisposeAsync();
        }
    }

    // Pre-turn waiter teardown: a waiter enqueued behind a holder is torn down by a protocol stop BEFORE
    // it ever won its turn (it never acquired; _acquired stays false). Its body must release cleanly and
    // the protocol must reach Completed - no hang. (Note: the cascade's pre-turn _acquired guard is a
    // robustness fix - without it a swallowed NRE in the heartbeat is backstopped by OnComplete - so this
    // test pins teardown CONVERGENCE for an unactivated waiter, not the guard itself.)
    [TestMethod]
    public async Task ProtocolStop_WhileWaiterPreTurn_ReleasesCleanly()
    {
        var protocol = await ConnectAsync();
        var a = protocol.BeginExclusiveScope(async: true);
        await a.HandoffReady;
        // B waits behind A, never activated.
        var b = protocol.BeginExclusiveScope(async: true);
        var bWait = b.HandoffReady;

        // Run A to a parked-idle state, then stop the protocol without ending either scope.
        await DrainAsync(a.Queue(new CommandFlow(async: true, Command.Create("select 1"))));

        await protocol.DisposeAsync();

        // B's pre-turn waiter must have been torn down (faulted), not left hanging.
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await bWait.WaitAsync(TimeSpan.FromSeconds(10)));
        // DisposeAsync is fire-and-forget; Completed is only guaranteed once the drain is joined
        // (a post-dispose CompleteAsync returns the same drain task).
        await protocol.CompleteAsync();
        Assert.IsTrue(protocol.IsCompleted, "protocol must reach Completed despite a pre-turn waiter");
    }
}
