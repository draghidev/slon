namespace Slon.Tests;

// Sync command path through a held exclusive scope. A sync ExecuteNonQuery/ExecuteReader on a sync-opened
// SlonConnection runs as a sync SUBFLOW that the caller's own thread drives end-to-end - the nested sync
// handoff one level down (the same EnqueueSyncWithHandoff rendezvous the protocol-level SyncFlowHandoffTests
// exercise, but inside the scope's inner pipeline). The thread checks assert the caller's thread stays put
// across open / execute / close: a path that trampolined onto a TP thread would return on a different one,
// and one that sync-over-async'd would deadlock under TP starvation (the starvation test below).
[TestClass]
[DoNotParallelize]
public class ExclusiveScopeSyncAdoTests
{
    // Sync ExecuteNonQuery with the caller-thread-stays-put check baked in (the established
    // SyncFlowHandoffTests pattern, applied to the ADO sync command path).
    static int ExecNonQuery(SlonConnection conn, string sql)
    {
        var before = Environment.CurrentManagedThreadId;
        using var cmd = new SlonCommand(conn, sql);
        var n = cmd.ExecuteNonQuery();
        Assert.AreEqual(before, Environment.CurrentManagedThreadId, $"sync ExecuteNonQuery returned on a different thread: {sql}");
        return n;
    }

    [TestMethod]
    public void Sync_SessionLocalTempTable_StaysOnCallerThread()
    {
        var caller = Environment.CurrentManagedThreadId;
        using var ds = AdoTestPool.NewIsolatedDataSource();
        using var conn = ds.OpenConnection();
        Assert.AreEqual(caller, Environment.CurrentManagedThreadId, "sync Open returned on a different thread");

        var t = "slon_sync_" + Guid.NewGuid().ToString("N");
        ExecNonQuery(conn, $"CREATE TEMP TABLE {t} (x int)");
        // INSERT references the session-local table: succeeds only if the CREATE landed on this held wire.
        Assert.AreEqual(1, ExecNonQuery(conn, $"INSERT INTO {t} VALUES (1)"));
        ExecNonQuery(conn, $"DROP TABLE {t}");

        conn.Close();
        Assert.AreEqual(caller, Environment.CurrentManagedThreadId, "sync Close returned on a different thread");
    }

    [TestMethod]
    public void Sync_Reader_StaysOnCallerThread()
    {
        var caller = Environment.CurrentManagedThreadId;
        using var ds = AdoTestPool.NewIsolatedDataSource();
        using var conn = ds.OpenConnection();
        ExecNonQuery(conn, "SET application_name = 'slon_sync_probe'");
        for (var i = 0; i < 4; i++)
        {
            using var cmd = new SlonCommand(conn, "SHOW application_name");
            using var reader = cmd.ExecuteReader();
            Assert.IsTrue(reader.Read(), $"iteration {i}: no row");
            Assert.AreEqual("slon_sync_probe", reader.GetString(0), $"iteration {i}: GUC not stable on the held wire");
            Assert.AreEqual(caller, Environment.CurrentManagedThreadId, $"iteration {i}: sync reader returned on a different thread");
        }
    }
}
