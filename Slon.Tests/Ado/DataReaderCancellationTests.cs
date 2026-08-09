using Slon.Tests.Pg;

namespace Slon.Tests.Ado;

[TestClass]
public class DataReaderCancellationTests : ConnectionCreatingTest
{
    [TestMethod]
    public async Task DisposeAsync_CancelsRemainingBatchWindowsAndReturnsUsableWire()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(o => o with { MaxPoolSize = 1 });
        await using var batch = dataSource.CreateBatch();
        batch.EnableErrorBarriers = true;
        batch.BatchCommands.Add(batch.CreateBatchCommand("select pg_backend_pid()"));
        batch.BatchCommands.Add(batch.CreateBatchCommand($"select pg_advisory_xact_lock({blocker.Key})"));
        batch.BatchCommands.Add(batch.CreateBatchCommand($"select pg_advisory_xact_lock({blocker.Key})"));

        var reader = await batch.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        var processId = reader.GetInt32(0);
        await blocker.WaitUntilContendedAsync(processId);

        await reader.DisposeAsync();

        await using var command = new SlonCommand(dataSource, "select 1");
        Assert.AreEqual(0, await command.ExecuteNonQueryAsync());
    }

    [TestMethod]
    public async Task Dispose_CancelsRemainingBatchWindowsAndReturnsUsableWire()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        using var dataSource = AdoTestPool.NewIsolatedDataSource(o => o with { MaxPoolSize = 1 });
        using var batch = dataSource.CreateBatch();
        batch.EnableErrorBarriers = true;
        batch.BatchCommands.Add(batch.CreateBatchCommand("select pg_backend_pid()"));
        batch.BatchCommands.Add(batch.CreateBatchCommand($"select pg_advisory_xact_lock({blocker.Key})"));
        batch.BatchCommands.Add(batch.CreateBatchCommand($"select pg_advisory_xact_lock({blocker.Key})"));

        var reader = batch.ExecuteReader();
        Assert.IsTrue(reader.Read());
        var processId = reader.GetInt32(0);
        await blocker.WaitUntilContendedAsync(processId);

        reader.Dispose();

        using var command = new SlonCommand(dataSource, "select 1");
        Assert.AreEqual(0, command.ExecuteNonQuery());
    }
}
