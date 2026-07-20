using Slon.Pg;
using Slon.Pg.Protocol;

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

    public Command WaitCommand => Command.Create($"select pg_advisory_lock({Key})");

    public static async Task<PgAdvisoryLock> AcquireAsync()
    {
        var key = Random.Shared.NextInt64(1, long.MaxValue);
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
