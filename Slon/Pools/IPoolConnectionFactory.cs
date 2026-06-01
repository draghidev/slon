namespace Slon.Pools;

public interface IPoolConnectionFactory<T>
    where T : class, IPoolConnection<T>
{
    T Create(ConnectionPoolContext<T> poolContext, TimeSpan timeout = default);
    ValueTask<T> CreateAsync(ConnectionPoolContext<T> poolContext, CancellationToken cancellationToken = default);
}
