namespace Slon.Pools;

sealed class PooledLinkedSource(Action<PooledLinkedSource> returnAction) : CancellationTokenSource, IDisposable, IAsyncDisposable
{
    CancellationTokenRegistration _registration;

    public CancellationToken LinkedToken => _registration.Token;

    internal void Initialize(CancellationTokenRegistration registration)
    {
        _registration = registration;
    }

    internal new bool TryReset()
    {
        if (base.TryReset())
        {
            _registration = default;
            return true;
        }

        return false;
    }

    public new void Dispose()
    {
        _registration.Dispose();
        returnAction(this);
    }

    public ValueTask DisposeAsync()
    {
        var task = _registration.DisposeAsync();
        if (task.IsCompleted)
        {
            task.GetAwaiter().GetResult();
            returnAction(this);
            return new();
        }

        return Core(task);

        async ValueTask Core(ValueTask task)
        {
            await task.ConfigureAwait(false);
            returnAction(this);
        }
    }
}
