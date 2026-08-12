namespace Slon.Pools;

struct IdleTokenTenure
{
    const long StateMask = 3;
    const long None = 0;
    const long Queued = 1;
    const long Claimed = 2;
    const long ClaimedWithPendingPublication = 3;

    long _word;

    /// Assigns the coordinator generation before the connection-side idle
    /// level can become visible. This does not create or defer a token.
    internal void PreparePublication(long generation)
    {
        while (true)
        {
            var word = Volatile.Read(ref _word);
            if (Interlocked.CompareExchange(ref _word,
                    generation | (word & StateMask), word) == word)
                return;
        }
    }

    /// Records an idle edge at the generation assigned by the pool coordinator.
    /// Returns true only when the caller must enqueue the identity token.
    internal bool CommitPublication(long generation)
    {
        while (true)
        {
            var word = Volatile.Read(ref _word);
            if ((word & ~StateMask) != generation)
                ThrowInvalidTransition(nameof(CommitPublication), word);
            var state = word & StateMask;
            long next;
            switch (state)
            {
                case None:
                    next = generation | Queued;
                    break;
                case Queued:
                    next = generation | Queued;
                    break;
                case ClaimedWithPendingPublication:
                    next = generation | ClaimedWithPendingPublication;
                    break;
                case Claimed:
                    next = generation | ClaimedWithPendingPublication;
                    break;
                default:
                    System.Diagnostics.Debug.Fail("Invalid idle-token state.");
                    return false;
            }

            if (Interlocked.CompareExchange(ref _word, next, word) == word)
                return state is None;
        }
    }

    internal long Generation => Volatile.Read(ref _word) & ~StateMask;

    /// Claims the queued token and returns its latest publication generation.
    internal long Claim()
    {
        while (true)
        {
            var word = Volatile.Read(ref _word);
            if ((word & StateMask) is not Queued)
                ThrowInvalidTransition(nameof(Claim), word);
            if (Interlocked.CompareExchange(ref _word,
                    (word & ~StateMask) | Claimed, word) == word)
                return word & ~StateMask;
        }
    }

    /// Returns the token. True means a publication raced the claim and the
    /// caller must make token visibility precede another coordinator bell.
    internal bool Return()
    {
        while (true)
        {
            var word = Volatile.Read(ref _word);
            var state = word & StateMask;
            if (state is not (Claimed or ClaimedWithPendingPublication))
                ThrowInvalidTransition(nameof(Return), word);
            if (Interlocked.CompareExchange(ref _word,
                    (word & ~StateMask) | Queued, word) == word)
                return state is ClaimedWithPendingPublication;
        }
    }

    internal bool Consume()
    {
        while (true)
        {
            var word = Volatile.Read(ref _word);
            var state = word & StateMask;
            if (state is not (Claimed or ClaimedWithPendingPublication))
                ThrowInvalidTransition(nameof(Consume), word);
            if (Interlocked.CompareExchange(ref _word,
                    word & ~StateMask, word) == word)
                return state is ClaimedWithPendingPublication;
        }
    }

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    static void ThrowInvalidTransition(string operation, long word)
        => throw new InvalidOperationException(
            $"Idle-token operation {operation} is invalid in state {word & StateMask}.");
}

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
