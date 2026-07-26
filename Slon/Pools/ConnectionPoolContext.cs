namespace Slon.Pools;

public sealed class ConnectionPoolContext<T>(Action<T, bool> signalAvailability,
    Func<T, Func<T, TimeSpan, ValueTask>, IDisposable> onHeartbeat)
    where T : class, IPoolConnection<T>
{
    /// Signals that pool placement may now succeed. An idle signal also publishes the
    /// connection's idle ownership token.
    public void SignalAvailability(T connection, bool isIdle)
        => signalAvailability(connection, isIdle);

    public IDisposable OnHeartbeat(Func<T, TimeSpan, ValueTask> action, T connection)
        => onHeartbeat(connection, action);
}
