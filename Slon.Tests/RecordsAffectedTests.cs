namespace Slon.Tests;

// End-to-end: the parsed CommandComplete row count flows through CommandResult.RecordsAffected and is
// summed by ExecuteNonQuery. Stateless - a real (non-temp) table + no cross-command session state - so it
// runs on the MULTIPLEXED data-source command path (no connection lease / exclusive scope).
[TestClass]
[DoNotParallelize]
public class RecordsAffectedTests
{
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
