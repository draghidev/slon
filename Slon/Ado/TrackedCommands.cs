using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

using Slon.Pg;

namespace Slon;

sealed class TrackedCommands(int maxAuto, int autoMinimumUses, Action<TrackedCommand>? onEvict = null)
{
    readonly ConcurrentDictionary<string, TrackedCommand?[]> _commands = new(concurrencyLevel: 1, capacity: 1);
    readonly AutoPrepareCandidates? _candidates = maxAuto is 0 ? null : new();
    // Admission/eviction is serialized. Readers (Find) stay lockless.
    readonly Lock _admissionLock = new();
    int _nameCounter; // monotonic, names never collide
    int _autoCount;   // currently-live auto TrackedCommands, gated against maxAuto

    public ICollection<TrackedCommand?[]> Commands => _commands.Values;

    // Test/diagnostic. Count of currently-live (non-evicted, non-invalidated) Auto entries.
    internal int LiveAutoCount
    {
        get
        {
            var count = 0;
            foreach (var variants in _commands.Values)
                foreach (var v in variants)
                    if (v is { Kind: TrackedCommandKind.Auto, IsInvalid: false })
                        count++;
            return count;
        }
    }

    public bool TryGet(in CommandDescriptor descriptor, [NotNullWhen(true)]out TrackedCommand? tracked)
    {
        if (maxAuto is 0)
        {
            tracked = null;
            return false;
        }

        if (Find(TrackedCommandKind.Auto, descriptor) is { } t)
        {
            tracked = t;
            return true;
        }

        return Core(descriptor, out tracked);

        [MethodImpl(MethodImplOptions.NoInlining)]
        bool Core(CommandDescriptor descriptor, [NotNullWhen(true)]out TrackedCommand? tracked)
        {
            var commandText = descriptor.UnpreparedCommandText;
            var candidate = _candidates!.Observe(commandText);

            if (candidate.Uses < autoMinimumUses)
            {
                tracked = null;
                return false;
            }

            lock (_admissionLock)
            {
                // Re-check under lock. A peer admission may have just landed for this descriptor.
                if (Find(TrackedCommandKind.Auto, descriptor) is { } existing)
                {
                    tracked = existing;
                    _candidates.Remove(commandText, candidate);
                    return true;
                }

                if (_autoCount >= maxAuto && !TryEvictLruLocked())
                {
                    tracked = null;
                    return false;
                }

                var name = ++_nameCounter;
                var newTracked = new TrackedCommand(TrackedCommandKind.Auto, descriptor with { CommandName = $"_ap{name}" });
                var added = TryAdd(newTracked);
                Debug.Assert(added, "Re-check under lock should have caught a peer admission.");
                _autoCount++;
                _candidates.Remove(commandText, candidate);
                tracked = newTracked;
                return true;
            }
        }
    }

    sealed class AutoPrepareCandidates
    {
        // Candidate evidence is deliberately stronger than the caller's SQL string. Short-lived
        // equal strings should accumulate uses rather than lose their entry to a Gen0 collection.
        static readonly TimeSpan NormalRetention = TimeSpan.FromMinutes(1);

        CandidateSet _set = new();

        internal AutoPrepareCandidates() => _ = new Gen2GcCallback(this);

        internal Candidate Observe(string commandText)
        {
            var now = Stopwatch.GetTimestamp();
            var set = Volatile.Read(ref _set);
            var candidate = set.Entries.GetOrAdd(
                commandText, static (_, timestamp) => new Candidate(timestamp), now);
            Volatile.Write(ref candidate.LastObservedTimestamp, now);
            if (Interlocked.Increment(ref candidate.Uses) is 1)
                UpdateMaximum(ref set.MaximumCount, set.Entries.Count);
            return candidate;
        }

        void Trim(TimeSpan retention)
        {
            var now = Stopwatch.GetTimestamp();
            var set = Volatile.Read(ref _set);
            foreach (var entry in set.Entries)
            {
                if (Stopwatch.GetElapsedTime(Volatile.Read(ref entry.Value.LastObservedTimestamp), now)
                    >= retention)
                    set.Entries.TryRemove(entry);
            }

            var count = set.Entries.Count;
            if ((long)count * 2 >= Volatile.Read(ref set.MaximumCount))
                return;

            var replacement = new CandidateSet();
            foreach (var entry in set.Entries)
                replacement.Entries.TryAdd(entry.Key, entry.Value);
            replacement.MaximumCount = replacement.Entries.Count;
            Interlocked.CompareExchange(ref _set, replacement, set);
        }

        internal bool Remove(string commandText, Candidate candidate)
            => Volatile.Read(ref _set).Entries.TryRemove(KeyValuePair.Create(commandText, candidate));

        static void UpdateMaximum(ref int maximum, int value)
        {
            var current = Volatile.Read(ref maximum);
            while (value > current)
            {
                var observed = Interlocked.CompareExchange(ref maximum, value, current);
                if (observed == current)
                    return;
                current = observed;
            }
        }

        internal sealed class CandidateSet
        {
            public readonly ConcurrentDictionary<string, Candidate> Entries = new(concurrencyLevel: 1, capacity: 1);
            public int MaximumCount;
        }

        internal sealed class Candidate(long lastObservedTimestamp)
        {
            public long LastObservedTimestamp = lastObservedTimestamp;
            public int Uses;
        }

        sealed class Gen2GcCallback
        {
            readonly WeakReference<AutoPrepareCandidates> _owner;
            int _gen2Count = GC.CollectionCount(2);
            long _lastGen2Timestamp;

            internal Gen2GcCallback(AutoPrepareCandidates owner) => _owner = new(owner);

            ~Gen2GcCallback()
            {
                if (!_owner.TryGetTarget(out var owner))
                    return;

                var gen2Count = GC.CollectionCount(2);
                if (gen2Count != _gen2Count)
                {
                    _gen2Count = gen2Count;
                    var now = Stopwatch.GetTimestamp();
                    var retention = _lastGen2Timestamp is 0
                        ? NormalRetention
                        : Stopwatch.GetElapsedTime(_lastGen2Timestamp, now);
                    if (retention > NormalRetention)
                        retention = NormalRetention;
                    _lastGen2Timestamp = now;
                    owner.Trim(retention);
                }
                GC.ReRegisterForFinalize(this);
            }
        }
    }

    // Must be called with _admissionLock held.
    bool TryEvictLruLocked()
    {
        TrackedCommand? lru = null;
        long lruTicks = long.MaxValue;
        foreach (var variants in _commands.Values)
        {
            for (var i = 0; i < variants.Length; i++)
            {
                var variant = variants[i];
                if (variant is null || variant.Kind != TrackedCommandKind.Auto || variant.IsInvalid)
                    continue;
                var ticks = variant.LastAccessedTicks;
                if (ticks < lruTicks)
                {
                    lruTicks = ticks;
                    lru = variant;
                }
            }
        }

        if (lru is null)
            return false;

        // Concurrent Invalidate (e.g. from a connection-side drift path) may have raced us.
        if (!lru.Invalidate())
            return false;

        Remove(lru);
        _autoCount--;
        // Fan out a maintenance signal to the registry so each PgConnection that holds this
        // name can DEALLOCATE it. Runs while we hold _admissionLock, keep the callback cheap
        // (push-to-queue + arm flag, the real wire work happens off-thread on each connection).
        onEvict?.Invoke(lru);
        return true;
    }

    bool Remove(TrackedCommand tracked)
    {
        if (!_commands.TryGetValue(tracked.CommandText, out var variants))
            return false;

        for (var i = 0; i < variants.Length; i++)
        {
            if (ReferenceEquals(variants[i], tracked))
            {
                variants[i] = null;
                return true;
            }
        }

        return false;
    }

    public bool InvalidateAuto(TrackedCommand tracked)
    {
        lock (_admissionLock)
        {
            if (tracked.Kind is not TrackedCommandKind.Auto || !tracked.Invalidate() || !Remove(tracked))
                return false;

            _autoCount--;
            return true;
        }
    }

    public TrackedCommand GetOrAdd(TrackedCommand tracked)
        => TryAdd(tracked, out var existingTracked) ? tracked : existingTracked;

    public bool TryAdd(TrackedCommand tracked)
        => TryAdd(tracked, out _);

    bool TryAdd(TrackedCommand tracked, [NotNullWhen(false)]out TrackedCommand? existingTracked)
    {
        while (true)
        {
            var variants = _commands.GetOrAdd(tracked.CommandText, static (_, tracked) => [tracked], tracked);
            if (ReferenceEquals(variants[0], tracked))
            {
                existingTracked = null;
                return true;
            }

            var freeIndex = -1;
            do
            {
                for (var i = 0; i < variants.Length; i++)
                {
                    var variant = variants[i];
                    if (variant is null && freeIndex is -1)
                        freeIndex = i;

                    if (tracked.Kind == variant?.Kind && variant.ParameterTypes.DeepEquals(tracked.ParameterTypes))
                    {
                        existingTracked = variant;
                        return false;
                    }
                }
            } while (freeIndex is not -1 && Interlocked.CompareExchange(ref variants[freeIndex], tracked, null) is null);

            var newVariants = variants;
            Array.Resize(ref newVariants, variants.Length * 2);
            newVariants[variants.Length] = tracked;

            if (_commands.TryUpdate(tracked.CommandText, variants, newVariants))
            {
                existingTracked = null;
                return true;
            }
        }
    }

    public bool Complete(TrackedCommandKind kind, CommandDescriptor descriptor)
    {
        var tracked = Find(kind, descriptor);
        tracked?.Complete(descriptor);
        return tracked is not null;
    }

    public TrackedCommand? Find(TrackedCommandKind kind, in CommandDescriptor descriptor)
    {
        if (!_commands.TryGetValue(descriptor.UnpreparedCommandText, out var variants))
            return null;

        foreach (var variant in variants)
        {
            Debug.Assert(variant is null || variant.CommandText == descriptor.UnpreparedCommandText);
            if (kind == variant?.Kind && variant.ParameterTypes.DeepEquals(descriptor.ParameterTypes))
                return variant;
        }

        return null;
    }
}
