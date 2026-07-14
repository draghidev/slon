namespace Slon.Pools;

public readonly struct ConnectionCandidate<T>(T connection, CancellationToken cancellationToken, bool wasIdle = true)
{
    public T Connection { get; } = connection;

    /// The cancellation token passed to the original Get call. Schedule predicates can use this
    /// when their scheduling work needs to honor caller cancellation.
    public CancellationToken CancellationToken { get; } = cancellationToken;

    /// This connection was idle when selected.
    /// <returns>True when idle. False when busy, there may be delays before new work gets processed.</returns>
    /// <remarks>
    /// Schedulers should not add work to connections that became idle when they weren't when this scheduling context was created.
    /// <br/><br/>
    /// Failure to do so can cause e.g. synchronous callers expecting an idle connection to block on async work due to the connection actually being busy.
    /// Degradation in connection selection performance might also occur when connections marked as being idle are often not idle.
    /// </remarks>
    public bool WasIdle { get; } = wasIdle;
}
