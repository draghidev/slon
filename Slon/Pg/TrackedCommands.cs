using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg;
using Slon.Runtime.CompilerServices;

namespace Slon;

sealed class TrackedCommands(int maxAuto, int autoMinimumUses, Action<TrackedCommand>? onEvict = null)
{
    readonly ConcurrentDictionary<string, TrackedCommand?[]> _commands = new(concurrencyLevel: 1, capacity: 1);
    // Candidate set keyed by SQL text content (so callers aggregate) with weak-key lifetime
    // (so dropped SQL strings clean themselves up).
    readonly WeakKeyedTable<string, StrongBox<int>> _candidates = new();
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
            var box = _candidates.GetOrAdd(commandText, static _ => new StrongBox<int>());

            if (Interlocked.Increment(ref box.Value) < autoMinimumUses)
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
                    _candidates.Remove(commandText);
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
                _candidates.Remove(commandText);
                tracked = newTracked;
                return true;
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
