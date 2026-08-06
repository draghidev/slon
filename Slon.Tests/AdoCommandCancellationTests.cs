using Slon.Tests.Pg;

namespace Slon.Tests;

[TestClass]
public class AdoCommandCancellationTests : ConnectionCreatingTest
{
    [TestMethod]
    public async Task Cancel_TargetsItsActiveCommandFlow()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        // CancelRequest exposure can outlive the operation it targeted. This test deliberately
        // creates that exposure, so it must not publish the affected wire into the assembly pool.
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(
            options => options with { MaxPoolSize = 1 });
        await using var command = new SlonCommand(dataSource,
            $"select pg_advisory_xact_lock({blocker.Key})");
        var execution = command.ExecuteScalarAsync();

        await blocker.WaitUntilContendedAsync();
        command.Cancel();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await execution.WaitAsync(TestTimeout.Hang));
        await blocker.ReleaseAsync();
    }
}
