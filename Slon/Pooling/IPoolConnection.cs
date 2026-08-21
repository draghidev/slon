namespace Slon.Pooling;

/// Used for pools to identify idle protocols and compare the load of busy protocols.
interface IPoolConnection<TSelf>
    where TSelf : class, IPoolConnection<TSelf>
{
    /// Initiates terminal completion and completes only after the connection is fully quiet.
    /// Pool disposal waits for this task.
    Task CompleteAsync(Exception? exception = null);

    /// Advisory structural-idle observation. The definitive decision remains the scheduling callback.
    /// This lock-free observation may be stale.
    bool IsIdle { get; }
    /// Advisory scheduling predicate. False while new work must not be placed: during shutdown,
    /// terminal completion, or a transient recovery that has taken continuity away. A successful
    /// recovery may make it true again. The definitive decision remains the scheduling callback.
    bool IsSchedulable { get; }
    /// Passive terminal observation. Must be the same fully-quiet completion represented by
    /// <see cref="CompleteAsync"/>, without initiating completion itself.
    Task Completion { get; }
    int CompareTo(TSelf? other);

    /// Called by the pool exactly once after the fully-created connection has been installed in
    /// its pool slot, and before any initial work is scheduled. The registration is the capability
    /// through which later availability changes are signalled.
    void Start(ConnectionPool<TSelf>.Registration registration);
}

interface IPoolConnectionFactory<T>
    where T : class, IPoolConnection<T>
{
    /// Must observe <paramref name="timeout"/>. Pool disposal waits for an in-progress create.
    T Create(ConnectionPoolContext<T> poolContext, TimeSpan timeout = default);
    /// Must observe <paramref name="cancellationToken"/>. Pool disposal waits for an in-progress create.
    ValueTask<T> CreateAsync(ConnectionPoolContext<T> poolContext, CancellationToken cancellationToken = default);
}
