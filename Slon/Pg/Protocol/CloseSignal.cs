namespace Slon.Pg.Protocol;

// Owns the close reason and the two-phase cancellation (stopping then abort) as one object, so the
// "materialize the reason before tripping any token" invariant is structural rather than held by text
// ordering across methods. Every trip calls Materialize as its first statement, so a consumer that
// wakes on a fired token (the decoder/writer abort-translation sites, the flow teardown hooks) always
// reads a non-null Reason.
//
// Reason is the PgClientClosedException the consumers throw directly (the wrapper, not the raw cause);
// Materialize constructs it once. A trip with a null cause still produces a valid PgClientClosedException
// (wrapping null), so set-once works without a separate "tripped but no reason" sentinel.
//
// Linking: a child created via CreateLinked tracks the parent's tokens (its CTSes are linked to the
// parent's) and chains Reason to the parent, so a parent trip cascades into the child and the child
// resolves the parent's reason. A child can also be tripped independently (scope-only teardown) without
// touching the parent. The child disposes only its own CTSes, never the parent's.
sealed class CloseSignal : IDisposable
{
    PgClientClosedException? _reason;
    readonly CancellationTokenSource _stoppingCts;
    readonly CancellationTokenSource _abortCts;
    readonly CloseSignal? _parent;

    CloseSignal(CancellationTokenSource stoppingCts, CancellationTokenSource abortCts, CloseSignal? parent)
    {
        _stoppingCts = stoppingCts;
        _abortCts = abortCts;
        _parent = parent;
    }

    // Root: both CTSes on the time provider so a FakeTimeProvider drives the abort escalation
    // deterministically in tests.
    public static CloseSignal CreateRoot(TimeProvider timeProvider)
        => new(new CancellationTokenSource(Timeout.InfiniteTimeSpan, timeProvider),
               new CancellationTokenSource(Timeout.InfiniteTimeSpan, timeProvider),
               parent: null);

    // Linked child: its tokens fire when the parent's do (and can be tripped independently). Chains
    // Reason to the parent.
    public static CloseSignal CreateLinked(CloseSignal parent, TimeProvider timeProvider)
        => new(CancellationTokenSource.CreateLinkedTokenSource(parent.StoppingToken),
               CancellationTokenSource.CreateLinkedTokenSource(parent.AbortToken),
               parent);

    /// The close reason consumers throw. Falls through to the parent's so a child cascaded from a parent
    /// trip resolves the parent's reason. Null until the first trip materializes it.
    public PgClientClosedException? Reason => Volatile.Read(ref _reason) ?? _parent?.Reason;

    public CancellationToken StoppingToken => _stoppingCts.Token;
    public CancellationToken AbortToken => _abortCts.Token;

    // Set the reason once (first writer wins), wrapping the cause in the exception consumers throw.
    // Volatile fast-path so repeat trips are alloc-free and don't CAS. A null cause still produces a
    // valid wrapper, so set-once holds.
    void Materialize(Exception? cause)
    {
        if (Volatile.Read(ref _reason) is not null)
            return;
        Interlocked.CompareExchange(ref _reason, new PgClientClosedException(cause), null);
    }

    /// Materialize a specific reason without tripping a token (the Shutdown gate publishes the reason
    /// under the protocol lock before the stop/abort escalation runs).
    public void MaterializeReason(Exception? cause) => Materialize(cause);

    /// Graceful stop: materialize, then fire the stopping token. The wire keeps running so it can reach
    /// a clean state; the abort token is armed separately (ArmAbortTimeout) to escalate.
    public Task StopAsync()
    {
        Materialize(null);
        return _stoppingCts.CancelAsync();
    }

    /// Forceful abort: materialize, then fire the abort token.
    public void Abort()
    {
        Materialize(null);
        _abortCts.Cancel();
    }

    public void ArmAbortTimeout(TimeSpan delay)
    {
        Materialize(null);
        _abortCts.CancelAfter(delay);
    }

    public void DisarmAbortTimeout() => _abortCts.CancelAfter(Timeout.InfiniteTimeSpan);

    // Disposes only its own CTSes (a linked child's CTSes hold a registration on the parent's tokens;
    // disposing them releases that registration). Never disposes the parent.
    public void Dispose()
    {
        _stoppingCts.Dispose();
        _abortCts.Dispose();
    }
}
