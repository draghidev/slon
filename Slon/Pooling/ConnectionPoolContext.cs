namespace Slon.Pooling;

[Experimental(ExperimentalDiagnostics.Pooling)]
public sealed class ConnectionPoolContext<T>
    where T : class, IPoolConnection<T>
{
    readonly ConnectionPool<T>? _pool;
    readonly Func<T, Func<T, TimeSpan, ValueTask>, IDisposable> _onHeartbeat;

    internal ConnectionPoolContext(ConnectionPool<T>? pool,
        Func<T, Func<T, TimeSpan, ValueTask>, IDisposable> onHeartbeat)
        => (_pool, _onHeartbeat) = (pool, onHeartbeat);

    internal ConnectionPoolContext(Func<T, Func<T, TimeSpan, ValueTask>, IDisposable> onHeartbeat)
        : this(null, onHeartbeat) { }

    public IDisposable OnHeartbeat(Func<T, TimeSpan, ValueTask> action, T connection)
        => _onHeartbeat(connection, action);

    public ValueTask<T> GetAsync<TState>(Func<ConnectionCandidate<T>, TState, bool> schedule,
        TState state, TimeSpan timeout, CancellationToken cancellationToken = default)
        => (_pool ?? throw new InvalidOperationException("This pool context cannot schedule connections."))
            .GetAsync(schedule, state, timeout, cancellationToken);

    internal void TrackBackgroundOperation(Func<Task> start)
        => (_pool ?? throw new InvalidOperationException("This pool context cannot track background operations."))
            .TrackBackgroundOperation(start);
}
