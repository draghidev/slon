using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

sealed class PgAdvisoryLock : IAsyncDisposable
{
    readonly PgClientProtocol _owner;
    bool _held = true;

    PgAdvisoryLock(PgClientProtocol owner, long key)
    {
        _owner = owner;
        Key = key;
    }

    public long Key { get; }

    public Command WaitCommand => Command.Create($"select pg_advisory_xact_lock({Key})");

    public static async Task<PgAdvisoryLock> AcquireAsync()
    {
        // Random keys: a PID-plus-counter scheme collides across process lifetimes (PID reuse
        // regenerated a key an orphaned session still held, wedging fixture setup forever).
        var key = Random.Shared.NextInt64();
        var owner = await PgTestPool.NewIsolatedAsync();
        try
        {
            // Session-scoped, set once: bounds every later blocking acquire on this owner,
            // including HoldAsync re-acquires. Separate statement, the extended protocol is
            // single-statement per command.
            await PgTestPool.RunAsync(owner, "set lock_timeout = '10s'");
            await AcquireBoundedAsync(owner, key);
            return new(owner, key);
        }
        catch
        {
            await owner.DisposeAsync();
            throw;
        }
    }

    public async Task HoldAsync()
    {
        if (_held)
            return;

        await AcquireBoundedAsync(_owner, Key);
        _held = true;
    }

    // Bounded acquisition: pg_advisory_lock waits forever on a leaked holder, turning a fixture
    // collision into a wedged run. lock_timeout bounds the wait server-side with no client
    // polling (advisory waits honor it, and blocking-acquire latency stays sub-millisecond on
    // the uncontended path); on expiry, name the advisory holders so the failure is a
    // diagnosis, not a hang.
    static async Task AcquireBoundedAsync(PgClientProtocol owner, long key)
    {
        try
        {
            await PgTestPool.RunAsync(owner, $"select pg_advisory_lock({key})");
        }
        catch (PgErrorException ex) when (ex.SqlState == "55P03")
        {
            var holders = await ReadSingleValueAsync(owner,
                "select coalesce(string_agg(a.pid || ' age=' || (now() - a.backend_start)::text || ' ' || coalesce(left(a.query, 40), ''), '; '), '<none>') " +
                "from pg_locks l join pg_stat_activity a on a.pid = l.pid where l.locktype = 'advisory'");
            throw new InvalidOperationException(
                $"Advisory lock {key} could not be acquired within 10s. Advisory holders: [{holders}]", ex);
        }
    }

    // Runs a single-command flow and returns the first row's first column as text, or null when
    // the result has no rows. Avoids ExecuteScalar (ADO layer) on purpose: this fixture drives
    // the raw protocol.
    static async Task<string?> ReadSingleValueAsync(PgClientProtocol owner, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        if (!owner.TryQueue(flow))
            throw new InvalidOperationException("The advisory-lock owner protocol rejected a fixture command.");
        string? value = null;
        var results = flow.GetAsyncEnumerator();
        while (await results.MoveNextAsync())
        {
            var rows = results.Current.GetAsyncEnumerator();
            while (await rows.MoveNextAsync())
                value ??= rows.Current.GetValue<string>(0);
            await rows.DisposeAsync();
            results.Current.GetCommandComplete();
        }
        await results.DisposeAsync();
        return value;
    }

    public async Task ReleaseAsync()
    {
        if (!_held)
            return;

        await PgTestPool.RunAsync(_owner, $"select pg_advisory_unlock({Key})");
        _held = false;
    }

    // Both polls are BOUNDED: PL/pgSQL observes cancels, not client death, so an unbounded loop
    // abandoned by test teardown spins server-side forever, pinning this session's advisory lock
    // and starving every later acquirer (observed as ten 8-hour spinner backends wedging a run).
    // The raise ends the query; an orphaned session then exits at its next client read, releasing
    // its locks. 20s is far beyond any test's contention window.
    public Task WaitUntilContendedAsync(int backendProcessId)
        => PgTestPool.RunAsync(_owner, $$"""
            do $$
            declare i int := 0;
            begin
                while not exists (
                    select 1 from pg_stat_activity
                    where pid = {{backendProcessId}} and wait_event = 'advisory')
                loop
                    perform pg_stat_clear_snapshot();
                    perform pg_sleep(0.001);
                    i := i + 1;
                    if i > 20000 then
                        raise exception 'advisory contention poll expired';
                    end if;
                end loop;
            end $$
            """);

    public async Task<bool> IsContendedAsync(int backendProcessId)
        => (await ReadSingleValueAsync(_owner, $$"""
            select case when exists (
                select 1 from pg_stat_activity
                where pid = {{backendProcessId}} and wait_event = 'advisory')
                then 'yes' else 'no' end
            """)) is "yes";

    public Task WaitUntilContendedAsync()
        => PgTestPool.RunAsync(_owner, $$"""
            do $$
            declare i int := 0;
            begin
                while not exists (
                    select 1 from pg_stat_activity
                    where query like '%pg_advisory_xact_lock({{Key}})%' and wait_event = 'advisory')
                loop
                    perform pg_stat_clear_snapshot();
                    perform pg_sleep(0.001);
                    i := i + 1;
                    if i > 20000 then
                        raise exception 'advisory contention poll expired';
                    end if;
                end loop;
            end $$
            """);

    public Task WaitUntilBackendGoneAsync(int backendProcessId)
        => PgTestPool.RunAsync(_owner, $$"""
            do $$
            declare i int := 0;
            begin
                while exists (select 1 from pg_stat_activity where pid = {{backendProcessId}})
                loop
                    perform pg_stat_clear_snapshot();
                    perform pg_sleep(0.001);
                    i := i + 1;
                    if i > 20000 then
                        raise exception 'backend termination poll expired';
                    end if;
                end loop;
            end $$
            """);

    public async ValueTask DisposeAsync()
    {
        try
        {
            await ReleaseAsync();
        }
        finally
        {
            await _owner.CompleteAsync();
        }
    }
}
