namespace Slon.Pools;

public readonly struct SchedulingContext<T>
{
    public SchedulingContext(T connection, CancellationToken userCancellationToken, bool idle = true)
    {
        Connection = connection;
        Idle = idle;
    }

    public T Connection { get; }

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