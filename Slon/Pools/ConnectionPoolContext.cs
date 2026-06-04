namespace Slon.Pools;

public sealed class ConnectionPoolContext<T>(Action<T> signalIdle, Action<T, Func<T, TimeSpan, ValueTask>> onHeartbeat)
    where T : class, IPoolConnection<T>
{
    public Action CreateConnectionIdleSignal(T connection) => () => signalIdle(connection);

    public void OnHeartbeat(Func<T, TimeSpan, ValueTask> action, T connection)
        => onHeartbeat(connection, action);
}
