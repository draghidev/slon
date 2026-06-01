namespace Slon.Pools;

/// Used for pools to determine whether a protocol is idle or to determine which protocol is the most idle.
public interface IPoolConnection<TSelf>
    where TSelf : class, IPoolConnection<TSelf>
{
    ValueTask CompleteAsync(Exception? exception = null);

    bool IsIdle { get; }
    bool IsCompleted { get; }
    int CompareTo(TSelf? other);
}
