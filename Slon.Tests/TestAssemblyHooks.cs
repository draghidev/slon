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
    public static async Task WarmAsync(TestContext context)
    {
        var certificateWarm = Task.Run(static () => TlsTestCertificate.Instance);
        await AdoTestPool.WarmAsync();
        _ = await certificateWarm;
    }

    [AssemblyCleanup]
    public static async Task DrainAsync()
    {
        await AdoTestPool.DrainAsync();
    }
}
