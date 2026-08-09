namespace Slon.Pools;

sealed class ConnectionPoolContext<T>(ConnectionPool<T>? pool, Action<T, bool> signalAvailability,
    Func<T, Func<T, TimeSpan, ValueTask>, IDisposable> onHeartbeat)
    where T : class, IPoolConnection<T>
{
    internal ConnectionPoolContext(Action<T, bool> signalAvailability,
        Func<T, Func<T, TimeSpan, ValueTask>, IDisposable> onHeartbeat)
        : this(null, signalAvailability, onHeartbeat) { }

    /// Signals that pool placement may now succeed. An idle signal also publishes the
    /// connection's idle ownership token.
    public void SignalAvailability(T connection, bool isIdle)
        => signalAvailability(connection, isIdle);

    public IDisposable OnHeartbeat(Func<T, TimeSpan, ValueTask> action, T connection)
        => onHeartbeat(connection, action);

    public ValueTask<T> GetAsync<TState>(Func<ConnectionCandidate<T>, TState, bool> schedule,
        TState state, TimeSpan timeout, CancellationToken cancellationToken = default)
        => (pool ?? throw new InvalidOperationException("This pool context cannot schedule connections."))
            .GetAsync(schedule, state, timeout, cancellationToken);

    internal void TrackDetached(Task task)
        => (pool ?? throw new InvalidOperationException("This pool context cannot track detached work."))
            .TrackDetached(task);
}
