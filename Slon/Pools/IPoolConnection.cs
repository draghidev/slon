namespace Slon.Pools;

/// Used for pools to determine whether a protocol is idle or to determine which protocol is the most idle.
public interface IPoolConnection<TSelf>
    where TSelf : class, IPoolConnection<TSelf>
{
    ValueTask CompleteAsync(Exception? exception = null);

    bool IsIdle { get; }
    bool IsCompleted { get; }
    int CompareTo(TSelf? other);

    /// Called by the pool exactly once, after the create-path has committed the lease for this
    /// connection. Implementations should use this to unblock any idle-channel publication that
    /// was suppressed during initial startup. Without the gate, depth-to-zero transitions
    /// during startup would publish the connection as available before the pool's
    /// future.Complete had assigned it to its first lessee.
    void Start();
}
