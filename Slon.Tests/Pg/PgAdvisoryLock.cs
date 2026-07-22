using Slon.Pg;
using Slon.Pg.Protocol;

namespace Slon.Tests.Pg;

sealed class PgAdvisoryLock : IAsyncDisposable
{
    static int s_nextKey;
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
        var key = ((long)(uint)Environment.ProcessId << 32) | (uint)Interlocked.Increment(ref s_nextKey);
        var owner = await PgTestPool.NewIsolatedAsync();
        try
        {
            await PgTestPool.RunAsync(owner, $"select pg_advisory_lock({key})");
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

        await PgTestPool.RunAsync(_owner, $"select pg_advisory_lock({Key})");
        _held = true;
    }

    public async Task ReleaseAsync()
    {
        if (!_held)
            return;

        await PgTestPool.RunAsync(_owner, $"select pg_advisory_unlock({Key})");
        _held = false;
    }

    public Task WaitUntilContendedAsync(int backendProcessId)
        => PgTestPool.RunAsync(_owner, $$"""
            do $$
            begin
                while not exists (
                    select 1 from pg_stat_activity
                    where pid = {{backendProcessId}} and wait_event = 'advisory')
                loop
                    perform pg_stat_clear_snapshot();
                    perform pg_sleep(0.001);
                end loop;
            end $$
            """);

    public Task WaitUntilContendedAsync()
        => PgTestPool.RunAsync(_owner, $$"""
            do $$
            begin
                while not exists (
                    select 1 from pg_stat_activity
                    where query like '%pg_advisory_xact_lock({{Key}})%' and wait_event = 'advisory')
                loop
                    perform pg_stat_clear_snapshot();
                    perform pg_sleep(0.001);
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
            await _owner.DisposeAsync();
        }
    }
}
