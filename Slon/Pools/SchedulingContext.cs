namespace Slon.Pools;

public readonly struct SchedulingContext<T>
{
    public SchedulingContext(T connection, CancellationToken userCancellationToken, bool idle = true)
    {
        Connection = connection;
        UserCancellationToken = userCancellationToken;
        Idle = idle;
    }

    public T Connection { get; }

    /// The cancellation token passed to the original Get call. Schedule predicates can use this
    /// when their scheduling work needs to honor caller cancellation.
    public CancellationToken UserCancellationToken { get; }

    /// This connection was idle when selected.
    /// <returns>True when idle. False when busy, there may be delays before new work gets processed.</returns>
    /// <remarks>
    /// Schedulers should not add work to connections that became idle when they weren't when this scheduling context was created.
    /// <br/><br/>
    /// Failure to do so can cause e.g. synchronous callers expecting an idle connection to block on async work due to the connection actually being busy.
    /// Degradation in connection selection performance might also occur when connections marked as being idle are often not idle.
    /// </remarks>
    public bool Idle { get; }
}