using System.Collections.Immutable;

namespace Slon.Pg.Protocol;

// Single-pump mutable state over immutable snapshots. Startup establishes the connection's base.
// Later ParameterStatus traffic is accumulated for the active flow, then published at flow
// retirement. Consumers therefore only read immutable state and returning to the base is
// allocation-free.
sealed class PgServerParameterState
{
    Dictionary<string, string>? _startup = new(StringComparer.Ordinal);
    Dictionary<string, string>? _delta;
    Dictionary<string, string>? _pending;
    ImmutableDictionary<string, string>? _base;
    ImmutableDictionary<string, string>? _current;
    int _revision;

    public int Revision => Volatile.Read(ref _revision);
    public ImmutableDictionary<string, string> BaseSnapshot => _base
        ?? throw new InvalidOperationException("PostgreSQL startup has not completed yet.");

    public ImmutableDictionary<string, string> CurrentSnapshot
        => Volatile.Read(ref _current) ?? BaseSnapshot;

    public void Set(string name, string value)
    {
        if (_startup is { } startup)
        {
            startup[name] = value;
            return;
        }

        (_pending ??= new(StringComparer.Ordinal))[name] = value;
    }

    // Called by the protocol's universal flow-retirement path. PostgreSQL reports GUC changes at
    // query boundaries; retaining the last value per name makes a multi-command flow one atomic
    // publication and naturally collapses changes that return to their prior value.
    public void CommitFlow()
    {
        if (_pending is not { Count: > 0 } pending)
            return;

        var baseSnapshot = BaseSnapshot;
        var delta = _delta ??= new(StringComparer.Ordinal);
        var changed = false;
        foreach (var (name, value) in pending)
        {
            var hasCurrent = delta.TryGetValue(name, out var current)
                || baseSnapshot.TryGetValue(name, out current);
            if (hasCurrent && StringComparer.Ordinal.Equals(current, value))
                continue;

            changed = true;
            if (baseSnapshot.TryGetValue(name, out var baseValue)
                && StringComparer.Ordinal.Equals(baseValue, value))
                delta.Remove(name);
            else
                delta[name] = value;
        }
        pending.Clear();

        if (!changed)
            return;

        var snapshot = delta.Count is 0
            ? baseSnapshot
            : baseSnapshot.SetItems(delta);

        Volatile.Write(ref _current, snapshot);
        Interlocked.Increment(ref _revision);
    }

    public ImmutableDictionary<string, string> CompleteStartup()
    {
        if (_startup is null)
            throw new InvalidOperationException("PostgreSQL startup has already completed.");

        _base = _current = _startup.ToImmutableDictionary(StringComparer.Ordinal);
        _startup = null;
        return _base;
    }
}
