namespace Slon.Tests;

using System.Data;

[TestClass]
public class DataReaderTests
{
    [TestMethod]
    public async Task ConcurrentDataSourceBatches_ExposeEveryResult()
    {
        var tasks = Enumerable.Range(0, 16).Select(async operationId =>
        {
            await using var batch = AdoTestPool.CreateBatch();
            for (var i = 0; i < 3; i++)
                batch.BatchCommands.Add(batch.CreateBatchCommand($"select {operationId * 3 + i}"));

            await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
            for (var i = 0; i < 3; i++)
            {
                Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
                Assert.AreEqual(operationId * 3 + i, reader.GetInt32(0));
                Assert.IsFalse(await reader.ReadAsync(CancellationToken.None));
                Assert.AreEqual(i != 2, await reader.NextResultAsync(CancellationToken.None));
            }
        });

        await Task.WhenAll(tasks);
    }

    const int FirstLength = 128 * 1024;
    const int SecondLength = FirstLength + 1;
    static readonly string Query = $"SELECT repeat('x', {FirstLength}), 42 UNION ALL SELECT repeat('y', {SecondLength}), 43";

    [TestMethod]
    public async Task Async_LargeRowsContinueWithinTheirMessageBoundary()
    {
        await using var command = AdoTestPool.CreateCommand(Query);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);

        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        var first = await reader.GetFieldValueAsync<string>(0, CancellationToken.None);
        Assert.AreEqual(FirstLength, first.Length);
        Assert.AreEqual('x', first[0]);
        Assert.AreEqual(42, reader.GetInt32(1));

        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        var second = await reader.GetFieldValueAsync<string>(0, CancellationToken.None);
        Assert.AreEqual(SecondLength, second.Length);
        Assert.AreEqual('y', second[0]);
        Assert.AreEqual(43, reader.GetInt32(1));
        Assert.IsFalse(await reader.ReadAsync(CancellationToken.None));
    }

    [TestMethod]
    public void Sync_LargeRowContinuesWithinItsMessageBoundary()
    {
        using var command = AdoTestPool.CreateCommand(Query);
        using var reader = command.ExecuteReader();

        Assert.IsTrue(reader.Read());
        Assert.AreEqual(FirstLength, reader.GetString(0).Length);
        Assert.AreEqual(42, reader.GetInt32(1));
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(SecondLength, reader.GetString(0).Length);
        Assert.AreEqual(43, reader.GetInt32(1));
        Assert.IsFalse(reader.Read());
    }

    [TestMethod]
    public async Task SequentialAccess_EmitsPartialRowsThatCanStillUseBufferedAccess()
    {
        await using var command = AdoTestPool.CreateCommand(Query);
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, CancellationToken.None);

        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        Assert.AreEqual(FirstLength, reader.GetString(0).Length);
        Assert.AreEqual(42, reader.GetInt32(1));
        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        Assert.AreEqual(SecondLength, reader.GetString(0).Length);
        Assert.AreEqual(43, reader.GetInt32(1));
        Assert.IsFalse(await reader.ReadAsync(CancellationToken.None));
    }

    [TestMethod]
    public async Task ExecuteNonQuery_ReturnsRowsAffected_PerStatementType()
    {
        var t = "slon_ra_" + Guid.NewGuid().ToString("N");
        await AdoTestPool.ExecuteNonQueryAsync($"CREATE TABLE {t} (x int)");
        try
        {
            Assert.AreEqual(5, await AdoTestPool.ExecuteNonQueryAsync($"INSERT INTO {t} VALUES (1),(2),(3),(4),(5)"), "INSERT");
            Assert.AreEqual(5, await AdoTestPool.ExecuteNonQueryAsync($"UPDATE {t} SET x = x + 1"), "UPDATE");           // x -> 2..6
            Assert.AreEqual(2, await AdoTestPool.ExecuteNonQueryAsync($"DELETE FROM {t} WHERE x > 4"), "DELETE");        // 5,6
            Assert.AreEqual(0, await AdoTestPool.ExecuteNonQueryAsync($"UPDATE {t} SET x = 0 WHERE x > 100"), "UPDATE 0"); // affects none
        }
        finally
        {
            await AdoTestPool.ExecuteNonQueryAsync($"DROP TABLE {t}");
        }
    }

    [TestMethod]
    public async Task ExecuteNonQuery_NonDataModifying_IsZero()
    {
        // SELECT / DDL don't count toward RecordsAffected.
        Assert.AreEqual(0, await AdoTestPool.ExecuteNonQueryAsync("SELECT 1"), "SELECT");
        Assert.AreEqual(0, await AdoTestPool.ExecuteNonQueryAsync("SELECT generate_series(1, 10)"), "SELECT 10 rows");
    }

    [TestMethod]
    public async Task BatchExecuteNonQuery_SumsAllCommandResults()
    {
        var t = "slon_batch_ra_" + Guid.NewGuid().ToString("N");
        await using var connection = await AdoTestPool.OpenConnectionAsync();
        await using var setup = new SlonCommand(connection, $"CREATE TABLE {t} (x int)");
        await setup.ExecuteNonQueryAsync();
        try
        {
            await using var batch = connection.CreateBatch();
            batch.BatchCommands.Add(batch.CreateBatchCommand($"INSERT INTO {t} VALUES (1),(2),(3)"));
            batch.BatchCommands.Add(batch.CreateBatchCommand($"UPDATE {t} SET x = x + 1"));
            batch.BatchCommands.Add(batch.CreateBatchCommand("SELECT generate_series(1, 10)"));
            batch.BatchCommands.Add(batch.CreateBatchCommand($"DELETE FROM {t} WHERE x = 4"));

            Assert.AreEqual(7, await batch.ExecuteNonQueryAsync());
        }
        finally
        {
            setup.CommandText = $"DROP TABLE {t}";
            await setup.ExecuteNonQueryAsync();
        }
    }
}
