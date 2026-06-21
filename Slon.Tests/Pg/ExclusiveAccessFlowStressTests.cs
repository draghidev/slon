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
    static int Iterations
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("SLON_STRESS_ITERATIONS");
            return int.TryParse(raw, out var n) && n > 0 ? n : 1000;
        }
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

    // Many sequential scopes on one protocol: hammers flyweight reuse (re-Initialize) + the
    // outer->inner decoder rebind each scope.
    [TestMethod]
    public async Task Stress_RepeatedScopes_Reuse()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        for (int i = 0; i < Iterations; i++)
            await RunScopeAsync(lease.Protocol);
    }

    // Many commands within one scope, drained one-at-a-time (the consumer-driven read contract):
    // stresses the inner pipeline + decoder rebind across a long-lived scope.
    [TestMethod]
    public async Task Stress_ManyCommands_WithinScope()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var iters = Math.Max(1, Iterations / 10);
        for (int i = 0; i < iters; i++)
        {
            var scope = lease.Protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;
            for (int k = 0; k < 8; k++)
                await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
            await scope.CompleteScopeAsync();
        }
    }

    // Sync subflow inside a scope: fires the RECURSIVE handoff (EnqueueSyncWithHandoff on the inner
    // source) so the caller's thread drives the INNER executor - the reason the source was unified.
    [TestMethod]
    public async Task Stress_SyncSubflow_RecursiveHandoff()
    {
        await using var lease = await PgTestPool.LeaseAsync();
        var iters = Math.Max(1, Iterations / 2);
        for (int i = 0; i < iters; i++)
        {
            var scope = lease.Protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;
            var cmd = scope.Queue(new CommandFlow(async: false, Command.Create("select 1")));
            var e = cmd.GetEnumerator();
            while (e.MoveNext()) { }
            await e.DisposeAsync();
            await scope.CompleteScopeAsync();
        }
    }

    // N protocols each running scopes concurrently: per-protocol isolation + concurrent takeovers.
    [TestMethod]
    public async Task Stress_ConcurrentScopes_AcrossProtocols()
    {
        const int concurrency = 8;
        var perThread = Math.Max(1, Iterations / concurrency);
        var leases = new PgTestPool.Lease[concurrency];
        var leased = 0;
        try
        {
            for (int i = 0; i < concurrency; i++) { leases[i] = await PgTestPool.LeaseAsync(); leased++; }
            var tasks = new Task[concurrency];
            for (int i = 0; i < concurrency; i++)
            {
                var protocol = leases[i].Protocol;
                tasks[i] = Task.Run(async () =>
                {
                    for (int j = 0; j < perThread; j++)
                        await RunScopeAsync(protocol);
                });
            }
            await Task.WhenAll(tasks);
        }
        finally
        {
            for (int i = 0; i < leased; i++)
                await leases[i].DisposeAsync();
        }
    }
}
