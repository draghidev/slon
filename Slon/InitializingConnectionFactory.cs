namespace Slon.Pools;

sealed class InitializingConnectionFactory<T>(
    IPoolConnectionFactory<T> factory, Action<T, TimeSpan>? initializer = null, Func<T, CancellationToken, ValueTask>? asyncInitializer = null)
    : IPoolConnectionFactory<T> where T : class, IPoolConnection<T>
{
    public T Create(ConnectionPoolContext<T> poolContext, TimeSpan timeout = default)
    {
        var deadline = new Deadline(timeout);
        var connection = factory.Create(poolContext, deadline.GetRemaining());
        try
        {
            initializer?.Invoke(connection, deadline.GetRemaining());
            return connection;
        }
        catch (Exception ex)
        {
            connection.CompleteAsync(ex).GetAwaiter().GetResult();
            throw;
        }
    }

    public async ValueTask<T> CreateAsync(ConnectionPoolContext<T> poolContext, CancellationToken cancellationToken = default)
    {
        var connection = await factory.CreateAsync(poolContext, cancellationToken).ConfigureAwait(false);
        try
        {
            if (asyncInitializer is not null)
                await asyncInitializer(connection, cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch (Exception ex)
        {
            await connection.CompleteAsync(ex).ConfigureAwait(false);
            throw;
        }
    }
}
