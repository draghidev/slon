using Slon.Tests.Pg;

// Method-level parallelism by default. Classes that can't tolerate it
// (fast-heartbeat-tick contention, TP-count assertions) opt out with [DoNotParallelize].
[assembly: Parallelize(Workers = 0, Scope = ExecutionScope.MethodLevel)]

namespace Slon.Tests;

// MSTest permits exactly one [AssemblyCleanup] per assembly. All test-pool helpers route
// their drains through here. Future per-suite teardown (schema cleanup, etc.) hangs off
// DrainAsync as well.
[TestClass]
public static class TestAssemblyHooks
{
    [AssemblyInitialize]
    public static async Task WarmAsync(TestContext _)
        => await AdoTestPool.WarmAsync();

    [AssemblyCleanup]
    public static async Task DrainAsync()
    {
        await PgTestPool.DrainAsync();
        await AdoTestPool.DrainAsync();
    }
}
