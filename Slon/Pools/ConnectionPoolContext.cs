namespace Slon.Pools;

public sealed class ConnectionPoolContext<T>(Action<T> signalIdle, Func<T, Func<T, TimeSpan, ValueTask>, IDisposable> onHeartbeat)
    where T : class, IPoolConnection<T>
{
    public Action CreateConnectionIdleSignal(T connection) => () => signalIdle(connection);

    public IDisposable OnHeartbeat(Func<T, TimeSpan, ValueTask> action, T connection)
        => onHeartbeat(connection, action);
}
