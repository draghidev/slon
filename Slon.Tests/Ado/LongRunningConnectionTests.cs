namespace Slon.Tests;

[TestClass]
public class LongRunningConnectionTests : ConnectionCreatingTest
{
    [TestMethod]
    public async Task DataSourceCommand_IsNotScheduledBehindLongRunningConnection()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(
            options => options with { PoolSize = 1 });
        var connection = await dataSource.OpenConnectionAsync(longRunning: true);
        try
        {
            await using (var ownCommand = connection.CreateCommand("select 1"))
                _ = await ownCommand.ExecuteNonQueryAsync();

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
