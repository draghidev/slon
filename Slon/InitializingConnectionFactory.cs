namespace Slon.Pools;

sealed class InitializingConnectionFactory<T>(
    IPoolConnectionFactory<T> factory, Action<T, TimeSpan>? initializer = null, Func<T, CancellationToken, ValueTask>? asyncInitializer = null)
    : IPoolConnectionFactory<T> where T : class, IPoolConnection<T>
{
    public T Create(ConnectionPoolContext<T> poolContext, TimeSpan timeout = default)
    {
        var deadline = new Deadline(timeout);
        var connection = factory.Create(poolContext, deadline.GetRemaining());
        initializer?.Invoke(connection, deadline.GetRemaining());
        return connection;
    }

    public async ValueTask<T> CreateAsync(ConnectionPoolContext<T> poolContext, CancellationToken cancellationToken = default)
    {
        var connection = await factory.CreateAsync(poolContext, cancellationToken).ConfigureAwait(false);
        if (asyncInitializer is not null)
            await asyncInitializer(connection, cancellationToken).ConfigureAwait(false);
        return connection;
    }
}
