namespace Slon.Tests;

// Assembly-scoped SlonDataSource for ADO-layer tests that complete cleanly with the default
// configuration. Mirrors PgTestPool's split: clean tests share, anything that destroys the
// data source / pool or needs custom config (auto-prepare with non-default thresholds,
// heartbeat-tick tweaks, max-pool changes) constructs its own via NewIsolatedDataSource.
static class AdoTestPool
{
    static readonly SlonDataSource _shared = new(NewOptions());

    // MaxPoolSize deliberately small so concurrent test methods compete for wires and exercise
    // the multiplexing / pipelining machinery under contention. Larger pool would dilute the
    // pressure into one-wire-per-method and hide real concurrent-dispatch bugs.
    internal static SlonDataSourceOptions NewOptions() => new()
    {
        EndPoint = TestEndPoint.Default,
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
        MaxPoolSize = 4,
        HeartbeatInterval = TimeSpan.FromSeconds(1),
        MaintenanceInterval = TimeSpan.FromSeconds(1),
    };

    // Lease a SlonConnection from the shared SlonDataSource. SlonConnection's DisposeAsync
    // already returns it to the pool, so callers just `await using` the result.
    internal static ValueTask<SlonConnection> OpenConnectionAsync(CancellationToken ct = default)
        => _shared.OpenConnectionAsync(ct);

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
