namespace Slon.Pools;

sealed class PooledLinkedSource : CancellationTokenSource, IDisposable, IAsyncDisposable
{
    readonly Action<PooledLinkedSource>? _returnAction;
    CancellationTokenRegistration _registration;

    internal PooledLinkedSource(Action<PooledLinkedSource> returnAction)
        => _returnAction = returnAction;

    internal PooledLinkedSource(TimeSpan timeout, TimeProvider timeProvider)
        : base(timeout, timeProvider)
    {
    }

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
        ReturnOrDispose();
    }

    public ValueTask DisposeAsync()
    {
        var task = _registration.DisposeAsync();
        if (task.IsCompleted)
        {
            task.GetAwaiter().GetResult();
            ReturnOrDispose();
            return new();
        }

        return Core(task);

        async ValueTask Core(ValueTask task)
        {
            await task.ConfigureAwait(false);
            ReturnOrDispose();
        }
    }

    void ReturnOrDispose()
    {
        if (_returnAction is { } returnAction)
            returnAction(this);
        else
            base.Dispose();
    }
}
