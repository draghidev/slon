namespace Slon.Tests;

// SlonConnection holds an exclusive scope for its whole lease, so session-local state set on the connection is
// visible to its later commands - the correctness property multiplexing would break. Validated with a TEMP
// table (session-local: an INSERT/UPDATE referencing it succeeds only if the earlier CREATE landed on the SAME
// held wire - on a multiplexed wire it would be "relation does not exist") and a session GUC read back stably.
[TestClass]
[DoNotParallelize]
public class ExclusiveScopeAdoTests
{
    static async Task<int> ExecNonQuery(SlonConnection conn, string sql)
    {
        await using var cmd = new SlonCommand(conn, sql);
        return await cmd.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [TestMethod]
    public async Task Async_SessionLocalTempTable_VisibleAcrossCommands()
    {
        await using var conn = await AdoTestPool.OpenConnectionAsync();
        await ExecNonQuery(conn, "CREATE TEMP TABLE slon_scope_probe (x int)");
        // The INSERT/UPDATE reference the session-local table: they succeed only because the prior CREATE ran on
        // THIS same held wire. On a multiplexed wire they'd land elsewhere and throw "relation does not exist".
        Assert.AreEqual(3, await ExecNonQuery(conn, "INSERT INTO slon_scope_probe VALUES (1),(2),(3)"));
        Assert.AreEqual(3, await ExecNonQuery(conn, "UPDATE slon_scope_probe SET x = x + 1"));
        await ExecNonQuery(conn, "DROP TABLE slon_scope_probe");
    }

    [TestMethod]
    public async Task Async_SessionGucStableAcrossCommands()
    {
        await using var conn = await AdoTestPool.OpenConnectionAsync();
        await ExecNonQuery(conn, "SET application_name = 'slon_scope_probe'");
        for (var i = 0; i < 8; i++)
        {
            await using var cmd = new SlonCommand(conn, "SHOW application_name");
            await using var reader = await cmd.ExecuteReaderAsync(CancellationToken.None);
            Assert.IsTrue(await reader.ReadAsync(CancellationToken.None), $"iteration {i}: no row");
            Assert.AreEqual("slon_scope_probe", reader.GetString(0), $"iteration {i}: GUC not stable - command left the held wire");
        }
    }

    // Closing a connection must release its scope so the wire returns to the pool. The shared pool is
    // MaxPoolSize=4, so leasing-and-closing well past that count only completes if every close releases:
    // a leaked scope would pin its wire and the 5th open would starve and hang the test.
    [TestMethod]
    public async Task Async_CloseReleasesScope_ReusableBeyondPoolCapacity()
    {
        for (var i = 0; i < 8; i++)
        {
            await using var conn = await AdoTestPool.OpenConnectionAsync();
            await ExecNonQuery(conn, "CREATE TEMP TABLE t (x int)");
            // INSERT references the just-created session-local table: it returns 1 only if the CREATE landed
            // on this same held wire - which requires this lease to have gotten a freshly-released scope.
            Assert.AreEqual(1, await ExecNonQuery(conn, "INSERT INTO t VALUES (1)"));
        }
    }

    // Saturate the pool with concurrent leases (each holding its own scope), close them all, then prove a
    // fresh batch still opens - if any concurrent close failed to release its scope the wire would be lost.
    [TestMethod]
    public async Task Async_ConcurrentLeases_AllReleaseCleanly()
    {
        async Task LeaseRunClose()
        {
            await using var conn = await AdoTestPool.OpenConnectionAsync();
            await ExecNonQuery(conn, "SELECT 1");
        }

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => LeaseRunClose()));
        // All scopes released above; a fresh saturating batch must still complete.
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => LeaseRunClose()));
    }

    // Closing then reopening the SAME connection object re-acquires a fresh scope. The reopened lease runs
    // its own session-local state (CREATE TEMP + INSERT) on the newly held wire - which only works if the
    // first close released the scope and the reopen acquired a new one.
    [TestMethod]
    public async Task Async_ReopenSameConnection_ReacquiresScope()
    {
        await using var conn = await AdoTestPool.OpenConnectionAsync();
        await ExecNonQuery(conn, "CREATE TEMP TABLE slon_reopen_probe (x int)");
        await conn.CloseAsync();

        await conn.OpenAsync(CancellationToken.None);
        await ExecNonQuery(conn, "CREATE TEMP TABLE slon_reopen_probe (x int)");
        Assert.AreEqual(2, await ExecNonQuery(conn, "INSERT INTO slon_reopen_probe VALUES (1),(2)"));
    }
}
