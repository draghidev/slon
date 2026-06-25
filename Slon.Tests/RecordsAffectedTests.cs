namespace Slon.Tests;

// End-to-end: the parsed CommandComplete row count flows through CommandResult.RecordsAffected and is
// summed by ExecuteNonQuery. A real (non-temp) table so the separate commands can land on different
// multiplexed wires without depending on session-local state.
[TestClass]
[DoNotParallelize]
public class RecordsAffectedTests
{
    static async Task<int> ExecNonQuery(SlonConnection conn, string sql)
    {
        await using var cmd = new SlonCommand(conn, sql);
        return await cmd.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [TestMethod]
    public async Task ExecuteNonQuery_ReturnsRowsAffected_PerStatementType()
    {
        await using var conn = await AdoTestPool.OpenConnectionAsync();
        var t = "slon_ra_" + Guid.NewGuid().ToString("N");
        await ExecNonQuery(conn, $"CREATE TABLE {t} (x int)");
        try
        {
            Assert.AreEqual(5, await ExecNonQuery(conn, $"INSERT INTO {t} VALUES (1),(2),(3),(4),(5)"), "INSERT");
            Assert.AreEqual(5, await ExecNonQuery(conn, $"UPDATE {t} SET x = x + 1"), "UPDATE");           // x -> 2..6
            Assert.AreEqual(2, await ExecNonQuery(conn, $"DELETE FROM {t} WHERE x > 4"), "DELETE");        // 5,6
            Assert.AreEqual(0, await ExecNonQuery(conn, $"UPDATE {t} SET x = 0 WHERE x > 100"), "UPDATE 0"); // affects none
        }
        finally
        {
            await ExecNonQuery(conn, $"DROP TABLE {t}");
        }
    }

    [TestMethod]
    public async Task ExecuteNonQuery_NonDataModifying_IsZero()
    {
        await using var conn = await AdoTestPool.OpenConnectionAsync();
        // SELECT / DDL don't count toward RecordsAffected.
        Assert.AreEqual(0, await ExecNonQuery(conn, "SELECT 1"), "SELECT");
        Assert.AreEqual(0, await ExecNonQuery(conn, "SELECT generate_series(1, 10)"), "SELECT 10 rows");
    }
}
