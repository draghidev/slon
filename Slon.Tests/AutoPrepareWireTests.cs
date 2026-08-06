using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Text;

namespace Slon.Tests;

// Wire-level smoke tests for auto-prepare. Each test drives commands against real PostgreSQL,
// then inspects Slon's substrate state (PgConnection presence, CommandTracker registry, etc.)
// via InternalsVisibleTo to verify the state machine transitioned correctly.
//
// Assumes a local PostgreSQL accessible at 127.0.0.1:5432 with user "postgres" password "postgres123".
[TestClass]
public class AutoPrepareWireTests
{
    // Each test customizes auto-prepare thresholds and inspects CommandTracker state, so the
    // data source must be isolated per test (sharing would cross-pollute prepared-statement
    // names and admission counters). Routes through AdoTestPool so endpoint and credentials
    // stay centralized; the test-specific knobs ride the configure delegate.
    static SlonDataSource CreateDataSource(
        int maxAutoPreparations = 10,
        int autoMinimumUses = 5,
        int maxPoolSize = 4,
        TimeSpan? heartbeatInterval = null,
        TimeSpan? maintenanceInterval = null)
        => AdoTestPool.NewIsolatedDataSource(o => o with
        {
            MaxPoolSize = maxPoolSize,
            MaxActiveAutoPreparations = maxAutoPreparations,
            AutoPreparationMinimumUses = autoMinimumUses,
            HeartbeatInterval = heartbeatInterval ?? TimeSpan.FromSeconds(1),
            MaintenanceInterval = maintenanceInterval ?? TimeSpan.FromSeconds(1),
        });

    [TestMethod]
    public async Task AutoPrepares_After_Threshold()
    {
        await using var ds = CreateDataSource(maxAutoPreparations: 10, autoMinimumUses: 5);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        const string sql = "select 1";
        const int runs = 6;
        for (var i = 0; i < runs; i++)
        {
            await using var cmd = new SlonCommand(conn, sql);
            _ = await cmd.ExecuteNonQueryAsync(CancellationToken.None);
        }

        var pg = conn.UnderlyingPgConnection;
        Assert.IsNotNull(pg);
        var entry = pg.TrackedEntries.FirstOrDefault(e => e.Command.CommandText == sql);
        Assert.IsNotNull(entry.Command, $"Expected presence entry for \"{sql}\".");
        Assert.AreEqual(TrackedStatus.Tracked, entry.Status);
    }

    [TestMethod]
    public async Task SameSqlBatch_RidesSingleInBatchPreparation()
    {
        await using var ds = CreateDataSource(maxAutoPreparations: 10, autoMinimumUses: 5);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        await using var batch = conn.CreateBatch();
        const int commandCount = 6;
        for (var i = 0; i < commandCount; i++)
            batch.BatchCommands.Add(batch.CreateBatchCommand("select 1"));

        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
        var results = 0;
        var rows = 0;
        do
        {
            results++;
            while (await reader.ReadAsync(CancellationToken.None))
                rows++;
        } while (await reader.NextResultAsync(CancellationToken.None));

        Assert.AreEqual(commandCount, results);
        Assert.AreEqual(commandCount, rows);
    }

    [TestMethod]
    public async Task DoesNotPrepare_Below_Threshold()
    {
        await using var ds = CreateDataSource(maxAutoPreparations: 10, autoMinimumUses: 5);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        // Below threshold: command should NOT be tracked. Admission counter bumps but never
        // crosses, so no TrackedCommand gets minted and no presence entry exists.
        const string sql = "select 2";
        for (var i = 0; i < 3; i++)
        {
            await using var cmd = new SlonCommand(conn, sql);
            _ = await cmd.ExecuteNonQueryAsync(CancellationToken.None);
        }

        var pg = conn.UnderlyingPgConnection;
        Assert.IsNotNull(pg);
        Assert.IsFalse(pg.TrackedEntries.Any(e => e.Command.CommandText == sql),
            $"Did not expect presence entry for \"{sql}\" with only 3 uses (threshold=5).");
    }

    [TestMethod]
    public async Task EvictsLeastRecentlyUsed_When_AtCapacity()
    {
        // Cap of 2 means after admitting a third distinct SQL, the LRU must be evicted.
        await using var ds = CreateDataSource(maxAutoPreparations: 2, autoMinimumUses: 3);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        const string sqlA = "select 100 as evict_a";
        const string sqlB = "select 101 as evict_b";
        const string sqlC = "select 102 as evict_c";

        // Push each past threshold so all three get admitted. Order matters for LRU, A oldest.
        await RunN(conn, sqlA, 4);
        await RunN(conn, sqlB, 4);
        await RunN(conn, sqlC, 4);

        // Workload-scope tracker: at most 2 live auto entries (cap was 2). The third admission
        // forced eviction of the LRU.
        var tracker = conn.UnderlyingProxy?.Tracker;
        Assert.IsNotNull(tracker);
        Assert.IsTrue(tracker.LiveAutoCount <= 2,
            $"Expected ≤2 live auto entries after eviction; got {tracker.LiveAutoCount}.");

        // Fan-out: eviction's onEvict callback walks the registered connections and pushes
        // EvictDeallocate for any conn where the victim is Tracked. Our conn had sqlA Tracked
        // before the eviction, so it should now hold an EvictDeallocate in its maintenance queue
        // (or already-drained if the heartbeat ticked in the meantime).
        var pg = conn.UnderlyingPgConnection!;
        var pending = pg.PeekMaintenance();
        var evictedFromPresence = !pg.TrackedEntries.Any(e => e.Command.CommandText == sqlA);

        Assert.IsTrue(
            pending.OfType<EvictDeallocate>().Any() || evictedFromPresence,
            "Expected either a queued EvictDeallocate for the LRU victim OR presence already cleared by a drained MaintenanceFlow.");

        // The evicted TrackedCommand should be Invalidated either way (TryEvictLruLocked calls
        // Invalidate before fanning out).
        var sqlAEntry = pg.TrackedEntries.FirstOrDefault(e => e.Command.CommandText == sqlA);
        if (sqlAEntry.Command is not null)
            Assert.IsTrue(sqlAEntry.Command.IsInvalid, "Evicted TrackedCommand must be Invalidated.");
    }

    [TestMethod]
    public async Task MultipleConnections_Each_TrackPresence()
    {
        // Two connections from the same pool, each independently preparing the same SQL.
        // Since names are workload-scope (shared TrackedCommand), both PgConnections should end
        // up with the same TrackedCommand in their presence map, each at status Tracked.
        await using var ds = CreateDataSource(maxAutoPreparations: 5, autoMinimumUses: 3, maxPoolSize: 4);

        await using var conn1 = await ds.OpenConnectionAsync(CancellationToken.None);
        await using var conn2 = await ds.OpenConnectionAsync(CancellationToken.None);

        const string sql = "select 200";
        await RunN(conn1, sql, 4);
        await RunN(conn2, sql, 4);

        var pg1 = conn1.UnderlyingPgConnection;
        var pg2 = conn2.UnderlyingPgConnection;
        Assert.IsNotNull(pg1);
        Assert.IsNotNull(pg2);

        var entry1 = pg1.TrackedEntries.FirstOrDefault(e => e.Command.CommandText == sql);
        var entry2 = pg2.TrackedEntries.FirstOrDefault(e => e.Command.CommandText == sql);
        Assert.IsNotNull(entry1.Command, "conn1 missing presence entry");
        Assert.IsNotNull(entry2.Command, "conn2 missing presence entry");
        Assert.AreEqual(TrackedStatus.Tracked, entry1.Status);
        Assert.AreEqual(TrackedStatus.Tracked, entry2.Status);

        // Workload-scope sharing: both PgConnections should reference the SAME TrackedCommand
        // object (the workload tracker mints one).
        if (ReferenceEquals(pg1, pg2))
        {
            // Same PgConnection (multiplex pool returned same one to both leases). Trivially same TC.
        }
        else
        {
            Assert.AreSame(entry1.Command, entry2.Command,
                "Expected workload-scope TrackedCommand to be shared across PgConnections.");
        }
    }

    [TestMethod]
    public async Task MultiplexedCommands_PrepareOnSelectedWire()
    {
        await using var ds = CreateDataSource(autoMinimumUses: 2, maxPoolSize: 1);
        const string sql = "select 201 as multiplexed_prepared";

        for (var i = 0; i < 4; i++)
        {
            await using var command = new SlonCommand(ds, sql);
            await command.ExecuteNonQueryAsync(CancellationToken.None);
        }

        await using var connection = await ds.OpenConnectionAsync(CancellationToken.None);
        var entry = connection.UnderlyingPgConnection!.TrackedEntries.Single(
            e => e.Command.CommandText == sql);
        Assert.AreEqual(TrackedStatus.Tracked, entry.Status);
    }

    [TestMethod]
    public async Task MultiplexedSyncCommands_PrepareOnSelectedWire()
    {
        await using var ds = CreateDataSource(autoMinimumUses: 2, maxPoolSize: 1);
        const string sql = "select 203 as multiplexed_sync_prepared";

        for (var i = 0; i < 4; i++)
        {
            using var command = new SlonCommand(ds, sql);
            command.ExecuteNonQuery();
        }

        await using var connection = await ds.OpenConnectionAsync(CancellationToken.None);
        Assert.AreEqual(TrackedStatus.Tracked,
            connection.UnderlyingPgConnection!.TrackedEntries.Single(e => e.Command.CommandText == sql).Status);
    }

    [TestMethod]
    public async Task MultiplexedBatch_RidesSelectedWirePreparation()
    {
        await using var ds = CreateDataSource(autoMinimumUses: 2, maxPoolSize: 1);
        const string sql = "select 202 as multiplexed_batch_prepared";
        await using var batch = ds.CreateBatch();
        for (var i = 0; i < 8; i++)
            batch.BatchCommands.Add(batch.CreateBatchCommand(sql));

        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
        var results = 0;
        do
        {
            while (await reader.ReadAsync(CancellationToken.None)) { }
            results++;
        } while (await reader.NextResultAsync(CancellationToken.None));

        Assert.AreEqual(8, results);
        await using var connection = await ds.OpenConnectionAsync(CancellationToken.None);
        Assert.AreEqual(TrackedStatus.Tracked,
            connection.UnderlyingPgConnection!.TrackedEntries.Single(e => e.Command.CommandText == sql).Status);
    }

    // Eviction queues an EvictDeallocate for the LRU victim; draining it must clear the victim from
    // the connection's presence map. Two things made the old version flake:
    //
    //  1. The drain rode a heartbeat tick. Waiting on that real-time tick starved under suite TP
    //     load (a FakeTimeProvider didn't help either, since the continuation responding to the tick
    //     is itself a ThreadPool work item). We instead push a probe carrying a completion TCS:
    //     PushMaintenance force-arms a MaintenanceFlow immediately onto the protocol executor (the
    //     reliable path commands use), and FIFO ordering means the probe's completion fires only
    //     after the EvictDeallocate ahead of it has drained. A Close against a nonexistent statement
    //     is a no-op per protocol (CloseComplete), so the probe touches no real server state.
    //
    //  2. The LRU stamp is Environment.TickCount64 ("statistical only"), so sqlA and sqlB used
    //     back-to-back could tie and either become the approximate-LRU victim. Waiting for the actual
    //     tick transition after sqlA puts it in an older tick than sqlB - the only thing that matters: sqlC
    //     crossing the cap evicts the LRU of the existing pair {sqlA, sqlB}, so sqlC (the new
    //     entrant) is never a candidate and needs no gap of its own.
    [TestMethod]
    public async Task Eviction_Drains_MaintenanceQueue_AndClearsPresence()
    {
        await using var ds = CreateDataSource(maxAutoPreparations: 2, autoMinimumUses: 3, maxPoolSize: 1);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        const string sqlA = "select 300 as drain_a";
        const string sqlB = "select 301 as drain_b";
        const string sqlC = "select 302 as drain_c";

        // Separate sqlA's stamp from sqlB's so sqlA is unambiguously the older eviction candidate,
        // without paying a fixed delay larger than the platform's TickCount64 resolution.
        await RunN(conn, sqlA, 4);
        var stamp = Environment.TickCount64;
        while (Environment.TickCount64 == stamp)
            await Task.Delay(1);
        await RunN(conn, sqlB, 4);
        await RunN(conn, sqlC, 4);

        var pg = conn.UnderlyingPgConnection!;

        // sqlA was evicted synchronously under the admission lock when sqlC crossed the cap: it is
        // invalidated in presence with an EvictDeallocate queued, both before sqlC's run returned.
        var sqlAEntry = pg.TrackedEntries.FirstOrDefault(e => e.Command.CommandText == sqlA);
        Assert.IsNotNull(sqlAEntry.Command, "Expected sqlA still present (invalidated) before the drain.");
        Assert.IsTrue(sqlAEntry.Command.IsInvalid, "Expected sqlA to be the invalidated LRU victim.");
        var sqlAName = sqlAEntry.Command.StoredCommandName;
        Assert.IsTrue(
            pg.PeekMaintenance().OfType<EvictDeallocate>().Any(e => e.Tracked.CommandText == sqlA),
            "Expected a queued EvictDeallocate for the evicted sqlA.");

        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        pg.PushMaintenance(new CloseStatement("slon_test_drain_probe") { Completion = drained });
        // Maintenance runs on the protocol's OUTER pipeline, which the connection's held exclusive scope
        // owns for the whole lease - so the armed MaintenanceFlow is queued behind the scope and can't
        // drain. Release the scope (close the connection); the PgConnection survives the lease (pool unit),
        // so we keep inspecting it. Maintenance is a protocol concern, never scope work.
        await conn.CloseAsync();
        await drained.Task;

        // The completion fires from the flow's cleanup walk, after RemoveTracked for sqlA and before
        // CommitMaintenanceRange. Presence is the meaningful invariant and is settled here; the queue
        // may still be transiently linked, so we don't assert on it.
        Assert.IsFalse(
            pg.TrackedEntries.Any(e => e.Command.CommandText == sqlA),
            "Expected the MaintenanceFlow to have drained the EvictDeallocate and removed sqlA from presence.");

        await using var reacquired = await ds.OpenConnectionAsync(CancellationToken.None);
        Assert.AreSame(pg, reacquired.UnderlyingPgConnection,
            "The single-connection pool must reacquire the session whose maintenance was drained.");
        // Ground truth first: server-side presence discriminates a maintenance delivery hole (row
        // still present, the Close never took effect) from a client-side error loss (row gone, so a
        // non-throwing deallocate below means the 26000 was swallowed on the way up). Row existence
        // only, no value decode.
        await using var probe = new SlonCommand(reacquired,
            $"select 1 from pg_prepared_statements where name = '{sqlAName}'");
        await using (var probeReader = await probe.ExecuteReaderAsync(CancellationToken.None))
        {
            Assert.IsFalse(await probeReader.ReadAsync(CancellationToken.None),
                $"pg_prepared_statements still holds {sqlAName} after the drained maintenance Close: the Close never took effect server-side.");
        }
        await using var deallocate = new SlonCommand(reacquired, $"deallocate {sqlAName}");
        var error = await Assert.ThrowsExactlyAsync<PgErrorException>(async () =>
            await deallocate.ExecuteNonQueryAsync(CancellationToken.None));
        Assert.AreEqual(PgErrorCodes.InvalidSqlStatementName, error.SqlState,
            "The maintenance Close must remove the named statement from PostgreSQL, not just client presence.");
    }

    [TestMethod]
    public async Task EvictionMaintenanceBatch_ClosesEveryServerStatement()
    {
        const int evictions = 4;
        await using var ds = CreateDataSource(maxAutoPreparations: 1, autoMinimumUses: 2, maxPoolSize: 1);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        var pg = conn.UnderlyingPgConnection!;
        var evictedNames = new List<EncodedString>(evictions);

        // The held scope keeps maintenance behind this lease, accumulating every Close into one
        // extended-protocol window. Each new admission evicts the preceding prepared statement.
        for (var i = 0; i <= evictions; i++)
        {
            var sql = $"select {i} as maintenance_batch_{i}";
            await RunN(conn, sql, 3);
            var tracked = pg.TrackedEntries.Single(e => e.Command.CommandText == sql).Command;
            if (i < evictions)
                evictedNames.Add(tracked.StoredCommandName);
        }

        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        pg.PushMaintenance(new CloseStatement("slon_maintenance_batch_probe") { Completion = drained });
        await conn.CloseAsync();
        await drained.Task;

        await using var reacquired = await ds.OpenConnectionAsync(CancellationToken.None);
        Assert.AreSame(pg, reacquired.UnderlyingPgConnection);
        foreach (var name in evictedNames)
        {
            await using var probe = new SlonCommand(reacquired,
                $"select 1 from pg_prepared_statements where name = '{name}'");
            await using var reader = await probe.ExecuteReaderAsync(CancellationToken.None);
            Assert.IsFalse(await reader.ReadAsync(CancellationToken.None),
                $"Maintenance reported completion while PostgreSQL still held evicted statement {name}.");
        }
    }

    [TestMethod]
    public async Task MissingStatementErrors_AreNeverSuppressedByExecuteNonQuery()
    {
        await using var ds = CreateDataSource(maxAutoPreparations: 1, autoMinimumUses: 2, maxPoolSize: 1);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        // Exercise consecutive error-recovery windows here; process-level stress supplies the
        // larger exposure multiplier.
        for (var i = 0; i < 8; i++)
        {
            var name = $"slon_missing_{i}";
            var error = await Deallocate(name);
            if (error is null)
            {
                // Ground truth before the retry. A real statement list discriminates a genuine
                // server-side statement from a shifted read window; the probe itself failing on a
                // shifted window is equally a verdict.
                string prepared;
                try
                {
                    await using var probe = new SlonCommand(conn,
                        "select coalesce(string_agg(name, ','), '<none>') from pg_prepared_statements");
                    await using var probeReader = await probe.ExecuteReaderAsync(CancellationToken.None);
                    prepared = await probeReader.ReadAsync(CancellationToken.None)
                        ? probeReader.GetString(0)
                        : "<no row>";
                }
                catch (Exception probeEx)
                {
                    prepared = $"<probe failed: {probeEx.GetType().Name}: {probeEx.Message}>";
                }
                var repeated = await Deallocate(name);
                Assert.Fail($"Iteration {i} returned normally for a missing prepared statement; " +
                    (repeated is null
                        ? "the identical retry also returned normally, so error delivery was lost."
                        : $"the identical retry produced {repeated.SqlState}, so the first command found a server-side statement.") +
                    $" Server prepared statements at failure: [{prepared}]");
            }
            Assert.AreEqual(PgErrorCodes.InvalidSqlStatementName, error.SqlState, $"Iteration {i}");
        }

        async Task<PgErrorException?> Deallocate(string name)
        {
            await using var command = new SlonCommand(conn, $"deallocate {name}");
            try
            {
                await command.ExecuteNonQueryAsync(CancellationToken.None);
                return null;
            }
            catch (PgErrorException error)
            {
                return error;
            }
        }
    }

    [TestMethod]
    public async Task WorkloadTracker_Registers_PgConnection()
    {
        // Sanity: opening a connection registers its PgConnection with the workload tracker so
        // eviction fanout can find it.
        await using var ds = CreateDataSource();
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);

        var tracker = conn.UnderlyingProxy?.Tracker;
        Assert.IsNotNull(tracker);
        Assert.IsTrue(tracker.RegisteredConnectionCount >= 1,
            $"Expected ≥1 registered connection; got {tracker.RegisteredConnectionCount}.");
    }

    [TestMethod]
    public async Task MissingPreparedStatement_ClearsWirePresence()
    {
        await using var ds = CreateDataSource(autoMinimumUses: 2);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        const string sql = "select 401 as missing_prepared";

        await RunN(conn, sql, 3);
        var pg = conn.UnderlyingPgConnection!;
        var tracked = pg.TrackedEntries.Single(e => e.Command.CommandText == sql);
        Assert.AreEqual(TrackedStatus.Tracked, tracked.Status);

        await using (var deallocate = new SlonCommand(conn, $"deallocate {tracked.Command.CommandName}"))
            await deallocate.ExecuteNonQueryAsync(CancellationToken.None);

        await using (var command = new SlonCommand(conn, sql))
        {
            var error = await Assert.ThrowsExactlyAsync<PgErrorException>(async () =>
                await command.ExecuteNonQueryAsync(CancellationToken.None));
            Assert.AreEqual(PgErrorCodes.InvalidSqlStatementName, error.SqlState);
        }

        Assert.AreEqual(0, pg.TrackedCount);
        await RunN(conn, sql, 1);
        Assert.IsTrue(pg.TrackedEntries.Any(e => e.Command.CommandText == sql));
    }

    [TestMethod]
    public async Task ChangedPreparedResultType_InvalidatesTrackedCommand()
    {
        await using var ds = CreateDataSource(autoMinimumUses: 2);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        const string table = "slon_changed_prepared_result";
        const string sql = $"select value from {table}";

        await Execute(conn, $"create temporary table {table} as select 1::integer as value");

        await RunN(conn, sql, 3);
        var pg = conn.UnderlyingPgConnection!;
        var tracked = pg.TrackedEntries.Single(e => e.Command.CommandText == sql).Command;

        await using (var alter = new SlonCommand(conn,
            $"alter table {table} alter column value type text using value::text"))
            await alter.ExecuteNonQueryAsync(CancellationToken.None);

        await using (var command = new SlonCommand(conn, sql))
        {
            var error = await Assert.ThrowsExactlyAsync<PgErrorException>(async () =>
                await command.ExecuteNonQueryAsync(CancellationToken.None));
            Assert.AreEqual(PgErrorCodes.FeatureNotSupported, error.SqlState);
        }

        Assert.IsTrue(tracked.IsInvalid);
        Assert.IsFalse(pg.TrackedEntries.Any(e => ReferenceEquals(e.Command, tracked)));
        await RunN(conn, sql, 1);
    }

    [TestMethod]
    public async Task InvalidatedPreparation_RemovesPresenceAndRetainsCleanupName()
    {
        await using var ds = CreateDataSource(autoMinimumUses: 2);
        await using var conn = await ds.OpenConnectionAsync(CancellationToken.None);
        const string sql = "select 402 as invalidated_preparation";

        await RunN(conn, sql, 3);
        var pg = conn.UnderlyingPgConnection!;
        var tracked = pg.TrackedEntries.Single(e => e.Command.CommandText == sql).Command;
        var name = tracked.StoredCommandName;

        pg.RemoveTracked(tracked);
        Assert.IsTrue(pg.TryBeginPreparing(tracked));
        Assert.IsTrue(tracked.Invalidate());

        pg.CompletePreparing(tracked,
            CommandDescriptor.CreatePrepared(name, tracked.ParameterTypes, tracked.RowDescription));

        Assert.IsFalse(pg.TrackedEntries.Any(e => ReferenceEquals(e.Command, tracked)));
        Assert.IsTrue(pg.PeekMaintenance().OfType<CloseStatement>().Any(e => e.Name == name));
    }

    static async Task Execute(SlonConnection conn, string sql)
    {
        await using var command = new SlonCommand(conn, sql);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    static async Task RunN(SlonConnection conn, string sql, int n)
    {
        for (var i = 0; i < n; i++)
        {
            await using var cmd = new SlonCommand(conn, sql);
            _ = await cmd.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }
}
