using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Stress for ExclusiveAccessFlow: hammers wire-takeover + nested-pipeline + flyweight-reuse +
// recursive-handoff to shake the outer/inner decoder-rebind and reuse races against the quiet
// baseline. Override iteration count via SLON_STRESS_ITERATIONS (default 1000).
[TestClass]
[DoNotParallelize]
public class ExclusiveAccessFlowStressTests
{
    // Real exclusive-access flows per iteration (this base count is further divided per scenario).
    // Capped so a blanket high count can't mountain the suite; SLON_UNCAPPED=1 drives the raw value.
    static int Iterations => StressEnv.Iterations(fallback: 100, cap: 5_000);

    static readonly TimeSpan Cap = TimeSpan.FromSeconds(10);

    // Fail fast on a deadlock instead of hanging the whole suite. where carries the iteration so a
    // rare stress failure points at the attempt that wedged.
    static async Task Capped(Task work, string where)
    {
        try { await work.WaitAsync(Cap); }
        catch (TimeoutException) { Assert.Fail($"{where}: hung (deadlock under stress)."); }
    }

    static async Task DrainAsync(CommandFlow flow)
    {
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    static async Task RunScopeAsync(PgClientProtocol protocol)
    {
        var scope = protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;
        await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
        await scope.CompleteScopeAsync();
    }

    static async Task RunManyCommandsScopeAsync(PgClientProtocol protocol)
    {
        var scope = protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;
        for (int k = 0; k < 8; k++)
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
        await scope.CompleteScopeAsync();
    }

    static async Task RunSyncSubflowScopeAsync(PgClientProtocol protocol)
    {
        var scope = protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;
        var cmd = scope.Queue(new CommandFlow(async: false, Command.Create("select 1")));
        var e = cmd.GetEnumerator();
        while (e.MoveNext()) { }
        await e.DisposeAsync();
        await scope.CompleteScopeAsync();
    }

    // Many sequential scopes on one protocol: hammers flyweight reuse (re-Initialize) + the
    // outer->inner decoder rebind each scope.
    [TestMethod]
    public async Task Stress_RepeatedScopes_Reuse()
    {
        var protocol = await PgTestPool.GetProtocolAsync();
        for (int i = 0; i < Iterations; i++)
            await Capped(RunScopeAsync(protocol), $"RepeatedScopes iter {i}");
    }

    // Many commands within one scope, drained one-at-a-time (the consumer-driven read contract):
    // stresses the inner pipeline + decoder rebind across a long-lived scope.
    [TestMethod]
    public async Task Stress_ManyCommands_WithinScope()
    {
        var protocol = await PgTestPool.GetProtocolAsync();
        var iters = Math.Max(1, Iterations / 10);
        for (int i = 0; i < iters; i++)
            await Capped(RunManyCommandsScopeAsync(protocol), $"ManyCommands iter {i}");
    }

    // Sync subflow inside a scope: fires the RECURSIVE handoff (EnqueueSyncWithHandoff on the inner
    // source) so the caller's thread drives the INNER executor - the reason the source was unified.
    [TestMethod]
    public async Task Stress_SyncSubflow_RecursiveHandoff()
    {
        var protocol = await PgTestPool.GetProtocolAsync();
        var iters = Math.Max(1, Iterations / 2);
        for (int i = 0; i < iters; i++)
            await Capped(RunSyncSubflowScopeAsync(protocol), $"SyncSubflow iter {i}");
    }

    // N protocols each running scopes concurrently: per-protocol isolation + concurrent takeovers.
    [TestMethod]
    public async Task Stress_ConcurrentScopes_AcrossProtocols()
    {
        var concurrency = Math.Min(PgTestPool.MaxConnections, 8);
        var perThread = Math.Max(1, Iterations / concurrency);
        var protocols = new PgClientProtocol[concurrency];
        for (int i = 0; i < concurrency; i++)
            protocols[i] = await PgTestPool.GetProtocolAsync();
        var tasks = new Task[concurrency];
        for (int i = 0; i < concurrency; i++)
        {
            var protocol = protocols[i];
            var p = i;
            tasks[i] = Task.Run(async () =>
            {
                for (int j = 0; j < perThread; j++)
                    await Capped(RunScopeAsync(protocol), $"ConcurrentScopes p{p} iter {j}");
            });
        }
        await Task.WhenAll(tasks);
    }
}
