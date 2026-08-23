namespace Slon.Tests;

using Microsoft.Extensions.Time.Testing;

[TestClass]
public class LongRunningConnectionTests
{
    [ConnectionCreatingTestMethod]
    public async Task DataSourceCommand_IsNotScheduledBehindLongRunningConnection()
    {
        var time = new FakeTimeProvider();
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(
            options => options with { PoolSize = 1, TimeProvider = time });
        var connection = await dataSource.OpenConnectionAsync(SlonConnectionOptions.LongRunning);
        try
        {
            await using var dataSourceCommand = dataSource.CreateCommand("select 42");
            var pending = dataSourceCommand.ExecuteNonQueryAsync();
            await Task.Yield();
            Assert.IsFalse(pending.IsCompleted,
                "datasource work must remain outside the long-running connection");

            await connection.DisposeAsync();

            _ = await pending;
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }
}
