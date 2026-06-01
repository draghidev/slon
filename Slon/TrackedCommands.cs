using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg;

namespace Slon;

sealed class TrackedCommands(int maxAuto, int autoMinimumUses)
{
    readonly ConcurrentDictionary<string, TrackedCommand?[]> _commands = new(concurrencyLevel: 1, capacity: 1);
    int _autoCount;
    ConditionalWeakTable<string, TrackedCandidate>? _weakCandidates;
    string?[]? _candidates;

    public ICollection<TrackedCommand?[]> Commands => _commands.Values;

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
            _weakCandidates ??= new();
            _candidates ??= new string?[maxAuto];
            TrackedCandidate? candidate;
            while (!_weakCandidates.TryGetValue(commandText, out candidate))
            {
                // TODO maybe we use a hashset + count check instead?
                // Use IndexOf so we get vectorized search.
                var auto = _candidates.AsSpan();
                int i;
                while ((i = auto.IndexOf((string?)null)) is not -1)
                {
                    if (Interlocked.CompareExchange(ref auto[i], commandText, null) is null)
                        break;
                    auto = auto[i..];
                }

                // No more room for new auto candidates.
                if (i is -1)
                {
                    tracked = null;
                    return false;
                }

                if (_weakCandidates.TryAdd(commandText, candidate = new TrackedCandidate(autoMinimumUses)))
                    break;

                Volatile.Write(ref auto[i], null);
            }

            if (!candidate.TryPromote())
            {
                tracked = null;
                return false;
            }

            var autoCount = Interlocked.Increment(ref _autoCount);
            if (autoCount <= maxAuto)
            {
                tracked = GetOrAdd(new TrackedCommand(TrackedCommandKind.Auto, descriptor with { CommandName = $"_ap{autoCount}" }));
                return true;
            }
            Interlocked.Decrement(ref _autoCount);
            tracked = null;
            return false;
        }
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

    sealed class TrackedCandidate(int minimumUses)
    {
        int _trackedUses;
        public int TrackedUses => _trackedUses;
        public bool TryPromote() => Interlocked.Increment(ref _trackedUses) >= minimumUses;
    }
}
