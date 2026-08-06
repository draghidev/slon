namespace Slon.Pools;

readonly struct ConnectionCandidate<T>(T connection, CancellationToken cancellationToken, bool isIdleCandidate = true)
{
    public T Connection { get; } = connection;

    /// The cancellation token passed to the original Get call. Schedule predicates can use this
    /// when their scheduling work needs to honor caller cancellation.
    public CancellationToken CancellationToken { get; } = cancellationToken;

    /// Whether the pool selected this connection through the idle or newly-opened path.
    /// <remarks>
    /// A non-idle candidate may only accept work if it remains busy. This prevents a
    /// pipelining attempt from claiming a connection after its busy-to-idle transition.
    /// </remarks>
    public bool IsIdleCandidate { get; } = isIdleCandidate;
}
