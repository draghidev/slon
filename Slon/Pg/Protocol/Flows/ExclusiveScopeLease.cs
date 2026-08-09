namespace Slon.Pg.Protocol.Flows;

// A caller-owned handle for one tenure of the reusable exclusive-access flow. Keeping the tenure
// identity here prevents a retained handle from operating a later scope after the hosting flow is reset.
sealed class ExclusiveScopeLease
{
    readonly ExclusiveAccessFlow _flow;
    readonly long _tenure;
    int _released;

    internal ExclusiveScopeLease(ExclusiveAccessFlow flow, long tenure)
    {
        _flow = flow;
        _tenure = tenure;
        HandoffReady = flow.HandoffReady;
    }

    public Task HandoffReady { get; }

    internal Task WaitForHandoffAsync(CancellationToken cancellationToken)
    {
        EnsureActive();
        return _flow.WaitForHandoffAsync(_tenure, cancellationToken);
    }

    public T Queue<T>(T subflow, CancellationToken cancellationToken = default) where T : PgClientFlow
    {
        EnsureActive();
        return _flow.Queue(_tenure, subflow, cancellationToken);
    }

    public ValueTask CompleteScopeAsync()
    {
        if (Interlocked.Exchange(ref _released, 1) is not 0)
            throw new InvalidOperationException("The exclusive-scope lease has already been released.");
        return _flow.CompleteScopeAsync(_tenure);
    }

    void EnsureActive()
    {
        if (Volatile.Read(ref _released) is not 0)
            throw new InvalidOperationException("The exclusive-scope lease has already been released.");
    }
}
