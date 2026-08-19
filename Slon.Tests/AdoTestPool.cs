using Slon.Tests.Pg;

namespace Slon.Tests;

// Assembly-scoped SlonDataSource for ADO-layer tests that complete cleanly with the default
// configuration. Mirrors PgTestPool's split: clean tests share, anything that destroys the
// data source / pool or needs custom config (auto-prepare with non-default thresholds,
// heartbeat-tick tweaks, max-pool changes) constructs its own via NewIsolatedDataSource.
static class AdoTestPool
{
    static readonly SlonDataSource _shared = new(NewOptions());

    // Match the low-level test pool so worker and connection pressure stay consistent across both
    // surfaces. PG_TEST_POOL_MAX can tighten both pools for deliberate multiplexing stress.
    internal static SlonDataSourceOptions NewOptions() => new()
    {
        EndPoint = TestEndPoint.Default,
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
        MaxPoolSize = PgTestPool.MaxConnections,
        MaintenanceInterval = TimeSpan.FromSeconds(1),
    };

    // Lease a SlonConnection from the shared SlonDataSource. SlonConnection's DisposeAsync
    // already returns it to the pool, so callers just `await using` the result.
    internal static ValueTask<SlonConnection> OpenConnectionAsync(CancellationToken ct = default)
        => _shared.OpenConnectionAsync(ct);

    // Sync sibling: opens synchronously (sync exclusive-scope acquire) so the whole lease - acquire,
    // commands, release - drives end-to-end on the caller's thread via the nested sync handoff.
    internal static SlonConnection OpenConnection() => _shared.OpenConnection();

    // MULTIPLEXED command path: a data-source-bound command runs on a pool-picked wire without leasing a
    // connection / exclusive scope. The right tool for stateless one-off commands - keeps the small shared
    // pool exercising multiplexing instead of starving on exclusive leases. Use OpenConnection* only when
    // the test genuinely needs session state / transactions across commands (and prefer an isolated source
    // for those, so an exclusive lease can't poison the shared pool).
    internal static async ValueTask<int> ExecuteNonQueryAsync(string sql, CancellationToken ct = default)
    {
        await using var cmd = new SlonCommand(_shared, sql);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    internal static int ExecuteNonQuery(string sql)
    {
        using var cmd = new SlonCommand(_shared, sql);
        return cmd.ExecuteNonQuery();
    }

    internal static async ValueTask ExecuteBatchNonQueryAsync(params string[] commands)
    {
        await using var batch = _shared.CreateBatch();
        foreach (var command in commands)
            batch.BatchCommands.Add(batch.CreateBatchCommand(command));
        _ = await batch.ExecuteNonQueryAsync(CancellationToken.None);
    }

    // A data-source-bound (MULTIPLEXED) command, for tests that need to drive a specific execute method
    // themselves (e.g. assert ExecuteScalar throws). Runs on a pool-picked wire - no connection lease.
    internal static SlonCommand CreateCommand(string sql) => _shared.CreateCommand(sql);
    internal static SlonBatch CreateBatch() => _shared.CreateBatch();

    internal static async Task WarmAsync()
        => Assert.AreEqual(0, await ExecuteNonQueryAsync("SELECT 1"));

    // Construct a fresh, non-pooled SlonDataSource the caller owns end to end. Use in tests
    // that need non-default configuration (auto-prepare thresholds, tight heartbeat ticks,
    // alternate pool sizing) or that intentionally fault the wire / break the pool's state.
    // SlonDataSourceOptions is a record with init-only properties, so callers transform via
    // `o => o with { ... }`.
    internal static SlonDataSource NewIsolatedDataSource(Func<SlonDataSourceOptions, SlonDataSourceOptions>? transform = null)
    {
        var options = NewOptions();
        if (transform is not null) options = transform(options);
        return new SlonDataSource(options);
    }

    // Disposes the shared SlonDataSource. Called from TestAssemblyHooks so the assembly's
    // single permitted [AssemblyCleanup] sweeps every helper pool.
    internal static ValueTask DrainAsync() => _shared.DisposeAsync();
}
