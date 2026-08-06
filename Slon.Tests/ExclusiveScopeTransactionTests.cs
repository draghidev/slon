using System.Data;

namespace Slon.Tests;

// Transaction SQL emission on the held exclusive scope: BEGIN is prepended to the first command while
// Commit/Rollback emit COMMIT/ROLLBACK, all as ordinary commands serial on the connection's held wire. Verified by effect - a
// TEMP table created OUTSIDE the tx (so it survives a rollback), an INSERT inside it, then a DELETE whose
// affected-row count reveals whether the INSERT persisted (commit) or was discarded (rollback / dispose).
[TestClass]
public class ExclusiveScopeTransactionTests : ConnectionCreatingTest
{
    static async Task<int> Exec(SlonConnection conn, string sql)
    {
        await using var cmd = new SlonCommand(conn, sql);
        return await cmd.ExecuteNonQueryAsync(CancellationToken.None);
    }

    static int ExecSync(SlonConnection conn, string sql)
    {
        using var cmd = new SlonCommand(conn, sql);
        return cmd.ExecuteNonQuery();
    }

    [TestMethod]
    public async Task Async_BeginIsDeferred_FirstReaderStartsAtUserResult()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        var begin = conn.BeginTransactionAsync();
        Assert.IsTrue(begin.IsCompletedSuccessfully, "BEGIN should be deferred to the first command.");
        await using var tx = await begin;

        await using (var cmd = new SlonCommand(conn, "select 42"))
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            Assert.IsTrue(await reader.ReadAsync());
            Assert.AreEqual(42, reader.GetInt32(0));
            Assert.IsFalse(await reader.NextResultAsync());
        }

        await tx.RollbackAsync();
    }

    [TestMethod]
    public async Task Async_Commit_PersistsChanges()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        var t = "slon_tx_" + Guid.NewGuid().ToString("N");
        await Exec(conn, $"CREATE TEMP TABLE {t} (x int)");
        await using (var tx = await conn.BeginTransactionAsync())
        {
            Assert.AreEqual(3, await Exec(conn, $"INSERT INTO {t} VALUES (1),(2),(3)"));
            await tx.CommitAsync();
        }
        Assert.AreEqual(3, await Exec(conn, $"DELETE FROM {t}"), "committed rows must persist");
    }

    [TestMethod]
    public async Task Async_Rollback_DiscardsChanges()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        var t = "slon_tx_" + Guid.NewGuid().ToString("N");
        await Exec(conn, $"CREATE TEMP TABLE {t} (x int)");
        await using (var tx = await conn.BeginTransactionAsync())
        {
            Assert.AreEqual(3, await Exec(conn, $"INSERT INTO {t} VALUES (1),(2),(3)"));
            await tx.RollbackAsync();
        }
        Assert.AreEqual(0, await Exec(conn, $"DELETE FROM {t}"), "rolled-back rows must be gone");
    }

    [TestMethod]
    public async Task Async_DisposeWithoutCommit_RollsBack()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        var t = "slon_tx_" + Guid.NewGuid().ToString("N");
        await Exec(conn, $"CREATE TEMP TABLE {t} (x int)");
        await using (var tx = await conn.BeginTransactionAsync())
            await Exec(conn, $"INSERT INTO {t} VALUES (1),(2),(3)");
        // tx disposed without Commit -> safety-net rollback.
        Assert.AreEqual(0, await Exec(conn, $"DELETE FROM {t}"), "an un-committed tx must roll back on dispose");
    }

    [TestMethod]
    public void Sync_Commit_PersistsChanges()
    {
        using var ds = AdoTestPool.NewIsolatedDataSource();
        using var conn = ds.OpenConnection();
        var t = "slon_tx_" + Guid.NewGuid().ToString("N");
        ExecSync(conn, $"CREATE TEMP TABLE {t} (x int)");
        using (var tx = conn.BeginTransaction())
        {
            Assert.AreEqual(3, ExecSync(conn, $"INSERT INTO {t} VALUES (1),(2),(3)"));
            tx.Commit();
        }
        Assert.AreEqual(3, ExecSync(conn, $"DELETE FROM {t}"), "committed rows must persist");
    }

    [TestMethod]
    public async Task Async_NestedBegin_Throws()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        await using var tx = await conn.BeginTransactionAsync();
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () => await conn.BeginTransactionAsync());
        await tx.RollbackAsync();
    }

    [TestMethod]
    public async Task Async_IsolationLevel_Serializable()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.Serializable);
        Assert.AreEqual(IsolationLevel.Serializable, tx.IsolationLevel);
        await Exec(conn, "SELECT 1");
        await tx.CommitAsync();
    }
}
