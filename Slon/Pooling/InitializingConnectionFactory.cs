using Slon.Runtime;

namespace Slon.Pooling;

/// Composes a connection factory with initialization before pool admission, closing connections whose initialization fails.
sealed class InitializingConnectionFactory<T>(
    IPoolConnectionFactory<T> factory,
    Action<T, TimeSpan>? initializer = null,
    Func<T, CancellationToken, ValueTask>? asyncInitializer = null)
    : IPoolConnectionFactory<T> where T : class, IPoolConnection<T>
{
    readonly IPoolConnectionFactory<T> _factory = Validate(factory, initializer, asyncInitializer);

    static IPoolConnectionFactory<T> Validate(IPoolConnectionFactory<T> factory,
        Action<T, TimeSpan>? initializer, Func<T, CancellationToken, ValueTask>? asyncInitializer)
    {
        ArgumentNullException.ThrowIfNull(factory);
        if ((initializer is null) != (asyncInitializer is null))
            throw new ArgumentException("Synchronous and asynchronous connection initializers must be configured together.");
        return factory;
    }

    public T Create(ConnectionPoolContext<T> poolContext, TimeSpan timeout = default)
    {
        var deadline = new Deadline(timeout);
        var connection = _factory.Create(poolContext, deadline.GetRemaining());
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
        var connection = await _factory.CreateAsync(poolContext, cancellationToken).ConfigureAwait(false);
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
