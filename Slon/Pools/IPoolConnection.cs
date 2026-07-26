namespace Slon.Pools;

/// Used for pools to determine whether a protocol is idle or to determine which protocol is the most idle.
public interface IPoolConnection<TSelf>
    where TSelf : class, IPoolConnection<TSelf>
{
    Task CompleteAsync(Exception? exception = null);

    bool IsIdle { get; }
    /// Advisory scheduling predicate. False while new work must not be placed: during shutdown,
    /// terminal completion, or a transient recovery that has taken continuity away. A successful
    /// recovery may make it true again. The definitive decision remains the scheduling callback.
    bool IsSchedulable { get; }
    /// Passive terminal observation. Must be the same fully-quiet completion represented by
    /// <see cref="CompleteAsync"/>, without initiating completion itself.
    Task Completion { get; }
    int CompareTo(TSelf? other);

    /// Atomically takes an idle connection out of scheduling rotation for pool pruning.
    /// Implementations may refuse because the connection is no longer idle or owns retained
    /// session state. A successful claim must prevent all later scheduling.
    bool TryBeginPruning();

    /// Called by the pool exactly once after the fully-created connection has been installed in
    /// its pool slot, and before any initial work is scheduled. Implementations should use this
    /// admission boundary to enable idle publication.
    void Start();
}
