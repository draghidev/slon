namespace Slon.Tests;

// SlonConnection holds an exclusive scope for its whole lease, so session-local state set on the connection is
// visible to its later commands - the correctness property multiplexing would break. Validated with a TEMP
// table (session-local: an INSERT/UPDATE referencing it succeeds only if the earlier CREATE landed on the SAME
// held wire - on a multiplexed wire it would be "relation does not exist") and a session GUC read back stably.
//
// Each test uses an ISOLATED data source: these lease EXCLUSIVE connections, so sharing the small multiplexed
// test pool would starve it / let one lease's failure poison the others. Stateless command tests use the
// shared multiplexed path (AdoTestPool.ExecuteNonQueryAsync) instead.
[TestClass]
public class ExclusiveScopeAdoTests
{
    static async Task<int> ExecNonQuery(SlonConnection conn, string sql)
    {
        await using var cmd = new SlonCommand(conn, sql);
        return await cmd.ExecuteNonQueryAsync(CancellationToken.None);
    }

    static int ExecNonQuerySync(SlonConnection connection, string sql)
    {
        var callerThread = Environment.CurrentManagedThreadId;
        using var command = new SlonCommand(connection, sql);
        var result = command.ExecuteNonQuery();
        Assert.AreEqual(callerThread, Environment.CurrentManagedThreadId);
        return result;
    }

    // Prove the current lease holds a real wire and runs session-local state on it: a uniquely-named TEMP
    // table whose INSERT succeeds only if the CREATE landed on this same held wire. Unique name + DROP keeps
    // the assertion local to this lease rather than relying on release-time session reset.
    static async Task ProveHeldWire(SlonConnection conn)
    {
        var t = "slon_held_" + Guid.NewGuid().ToString("N");
        await ExecNonQuery(conn, $"CREATE TEMP TABLE {t} (x int)");
        Assert.AreEqual(1, await ExecNonQuery(conn, $"INSERT INTO {t} VALUES (1)"));
        await ExecNonQuery(conn, $"DROP TABLE {t}");
    }

    [TestMethod]
    public async Task Async_SessionLocalTempTable_VisibleAcrossCommands()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        var t = "slon_scope_" + Guid.NewGuid().ToString("N");
        await ExecNonQuery(conn, $"CREATE TEMP TABLE {t} (x int)");
        // The INSERT/UPDATE reference the session-local table: they succeed only because the prior CREATE ran on
        // THIS same held wire. On a multiplexed wire they'd land elsewhere and throw "relation does not exist".
        Assert.AreEqual(3, await ExecNonQuery(conn, $"INSERT INTO {t} VALUES (1),(2),(3)"));
        Assert.AreEqual(3, await ExecNonQuery(conn, $"UPDATE {t} SET x = x + 1"));
        await ExecNonQuery(conn, $"DROP TABLE {t}");
    }

    [TestMethod]
    public async Task Async_SessionGucStableAcrossCommands()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        await ExecNonQuery(conn, "SET application_name = 'slon_scope_probe'");
        for (var i = 0; i < 8; i++)
        {
            await using var cmd = new SlonCommand(conn, "SHOW application_name");
            await using var reader = await cmd.ExecuteReaderAsync(CancellationToken.None);
            Assert.IsTrue(await reader.ReadAsync(CancellationToken.None), $"iteration {i}: no row");
            Assert.AreEqual("slon_scope_probe", reader.GetString(0), $"iteration {i}: GUC not stable - command left the held wire");
        }
    }

    // Closing a connection must release its scope so the wire returns to the pool. The isolated source is
    // MaxPoolSize=4, so leasing-and-closing well past that count only completes if every close releases:
    // a leaked scope would pin its wire and the 5th open would starve and hang the test.
    [TestMethod]
    public async Task Async_CloseReleasesScope_ReusableBeyondPoolCapacity()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        for (var i = 0; i < 8; i++)
        {
            await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
            await ProveHeldWire(conn);
        }
    }

    // Saturate the pool with concurrent leases (each holding its own scope), close them all, then prove a
    // fresh batch still opens - if any concurrent close failed to release its scope the wire would be lost.
    [TestMethod]
    public async Task Async_ConcurrentLeases_AllReleaseCleanly()
    {
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        async Task LeaseRunClose()
        {
            await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
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
        await using var ds = AdoTestPool.NewIsolatedDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        await ProveHeldWire(conn);
        await conn.CloseAsync();

        await conn.OpenAsync(CancellationToken.None);
        // The reopened lease must hold a freshly-acquired scope and run its own session-local state.
        await ProveHeldWire(conn);
    }

    [TestMethod]
    public void Sync_SessionLocalTempTable_StaysOnCallerThread()
    {
        var callerThread = Environment.CurrentManagedThreadId;
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var connection = dataSource.OpenConnection();
        Assert.AreEqual(callerThread, Environment.CurrentManagedThreadId);

        var table = "slon_sync_" + Guid.NewGuid().ToString("N");
        ExecNonQuerySync(connection, $"CREATE TEMP TABLE {table} (x int)");
        Assert.AreEqual(1, ExecNonQuerySync(connection, $"INSERT INTO {table} VALUES (1)"));
        ExecNonQuerySync(connection, $"DROP TABLE {table}");

        connection.Close();
        Assert.AreEqual(callerThread, Environment.CurrentManagedThreadId);
    }

    [TestMethod]
    public void Sync_Reader_StaysOnCallerThread()
    {
        var callerThread = Environment.CurrentManagedThreadId;
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var connection = dataSource.OpenConnection();
        ExecNonQuerySync(connection, "SET application_name = 'slon_sync_probe'");

        for (var i = 0; i < 4; i++)
        {
            using var command = new SlonCommand(connection, "SHOW application_name");
            using var reader = command.ExecuteReader();
            Assert.IsTrue(reader.Read(), $"iteration {i}: no row");
            Assert.AreEqual("slon_sync_probe", reader.GetString(0));
            Assert.AreEqual(callerThread, Environment.CurrentManagedThreadId);
        }
    }
}
