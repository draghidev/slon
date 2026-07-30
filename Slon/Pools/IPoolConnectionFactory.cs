namespace Slon.Pools;

public interface IPoolConnectionFactory<T>
    where T : class, IPoolConnection<T>
{
    /// Must observe <paramref name="timeout"/>. Pool disposal waits for an in-progress create.
    T Create(ConnectionPoolContext<T> poolContext, TimeSpan timeout = default);
    /// Must observe <paramref name="cancellationToken"/>. Pool disposal waits for an in-progress create.
    ValueTask<T> CreateAsync(ConnectionPoolContext<T> poolContext, CancellationToken cancellationToken = default);
}
