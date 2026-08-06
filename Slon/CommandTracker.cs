using System.Diagnostics;
using System.Runtime.CompilerServices;
using Slon.Pg;
using Slon.Text;

namespace Slon;

readonly struct TrackerResult(TrackedCommand? tracked)
{
    public TrackedCommand? Tracked => tracked;
    public CommandDescriptor GetDescriptor(string commandText, ParameterTypeList parameterTypes)
    {
        return tracked?.TryGetPreparedDescriptor(out var descriptor) == true
            ? descriptor
            : CommandDescriptor.Create(commandText, parameterTypes, tracked?.CommandName ?? default);
    }
}

sealed class CommandTracker : IDisposable, IAsyncDisposable
{
    readonly TrackedCommands? _tracked;
    List<EncodedString>? _leakedCommandNames;
    ConditionalWeakTable<object, OwnedCommands>? _owned;
    // Registered PgConnections whose presence map may hold entries from _tracked. Used to fan out
    // eviction-driven DEALLOCATE pushes. Walked under _registryLock so concurrent register/deregister
    // doesn't race the eviction callback.
    List<PgConnection>? _registeredConnections;
    readonly Lock _registryLock = new();
    bool _disposed;

    readonly CommandTracker? _parent;

    public CommandTracker(int maxAuto, int autoMinimumUses, CommandTracker? parent = null)
    {
        _parent = parent;
        if (maxAuto > 0)
            _tracked = new(maxAuto, autoMinimumUses, onEvict: OnEvict);
    }

    void OnEvict(TrackedCommand tracked)
    {
        // Snapshot under lock then push outside, PushMaintenance is cheap but we don't want to
        // hold the registry lock across queue+arm logic in case it expands later.
        PgConnection[] snapshot;
        lock (_registryLock)
        {
            if (_registeredConnections is null || _registeredConnections.Count is 0)
                return;
            snapshot = _registeredConnections.ToArray();
        }
        foreach (var connection in snapshot)
        {
            // Only fan out to connections where the name is actually Tracked. Preparing entries
            // have a Parse in flight. DEALLOCATE-ing now either races the Parse or wastes work
            // (we'd just re-Parse on next use). The next eviction round picks them up if still due.
            if (connection.GetTrackedStatus(tracked) is TrackedStatus.Tracked)
                connection.PushMaintenance(new EvictDeallocate(tracked));
        }
    }

    internal void InvalidateAuto(TrackedCommand tracked)
    {
        if (_tracked?.InvalidateAuto(tracked) == true)
            OnEvict(tracked);
    }

    public void Register(PgConnection connection)
    {
        lock (_registryLock)
            (_registeredConnections ??= new()).Add(connection);
    }

    public void Deregister(PgConnection connection)
    {
        lock (_registryLock)
            _registeredConnections?.Remove(connection);
    }

    public bool HasParent => _parent is not null;

    // Test/diagnostic. Exposed via InternalsVisibleTo so tests can verify admission/eviction
    // semantics directly without round-tripping through the protocol.
    internal int RegisteredConnectionCount
    {
        get
        {
            lock (_registryLock)
                return _registeredConnections?.Count ?? 0;
        }
    }
    internal int LiveAutoCount => _tracked?.LiveAutoCount ?? 0;

    // Track is admission/identity only. Completion (transitioning Tracked status, presence updates,
    // etc.) is the caller's concern. The proxy's delegate-baked path attaches a winner closure
    // that calls tracked.Complete + protocol.SetTracked when Parse lands. For the explicit-prepare
    // path, `nameSource` mints `_ep{N}` names, per-session so successive SlonConnections sharing
    // the same PgConnection through the pool can't collide on names.
    public TrackerResult Track(in CommandDescriptor descriptor, TrackedCommand? tracked = null, object? owningInstance = null, PgConnection? nameSource = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (descriptor.IsPrepared)
            ThrowHelper.ThrowArgumentException(nameof(descriptor), "Descriptor is a prepared descriptor.");

        if (tracked?.IsCompleted == true)
            return new(tracked);

        if (owningInstance is null)
        {
            // Auto-prepare path.
            if (_tracked is null)
                return _parent?.Track(descriptor, tracked) ?? default;
            if (_tracked.TryGet(descriptor, out tracked) && !tracked.IsInvalid)
                return new(tracked);
            return default;
        }

        if (nameSource is null)
            ThrowHelper.ThrowArgumentException(nameof(nameSource), "Explicit-prepare path requires a name source.");
        return Core(owningInstance, descriptor, nameSource);

        [MethodImpl(MethodImplOptions.NoInlining)]
        TrackerResult Core(object owningInstance, in CommandDescriptor descriptor, PgConnection nameSource)
        {
            // Explicit-prepare path.
            _owned ??= new();
            if (!_owned.TryGetValue(owningInstance, out var ownedTracker))
                ownedTracker = CreateOwnedTracker(owningInstance, descriptor, nameSource);

            TrackedCommand? tc;
            while ((tc = ownedTracker.Find(descriptor)) is null)
            {
                tc = new(TrackedCommandKind.Command, descriptor with { CommandName = nameSource.MintExplicitPrepareName() });
                if (ownedTracker.TryAdd(tc))
                    break;
            }

            return tc.IsInvalid ? default : new(tc);
        }

        OwnedCommands CreateOwnedTracker(object owningInstance, in CommandDescriptor descriptor, PgConnection nameSource)
        {
            var tc = new TrackedCommand(TrackedCommandKind.Command, descriptor with { CommandName = nameSource.MintExplicitPrepareName() });
            var tracker = new OwnedCommands(_leakedCommandNames ??= new());
            var success = tracker.TryAdd(tc);
            Debug.Assert(success);

            // We were raced by a concurrent explicit prepare, that shouldn't normally happen but it's easy to handle.
            while (!_owned!.TryAdd(owningInstance, tracker))
            {
                if (_owned.TryGetValue(owningInstance, out var existing))
                {
                    tracker = existing;
                    break;
                }
            }
            return tracker;
        }
    }

    // Atomically take + clear the accumulated leaked-names list. Used by SlonConnection.Dispose
    // (and similar lifecycle hooks) to push CloseStatement maintenance items onto the current
    // PgConnection. Leaked names come from OwnedCommands finalizers that fired while this tracker
    // was alive. By the time we drain, the owning user object is gone and we just need the wire
    // DEALLOCATE.
    public List<EncodedString>? DrainLeakedNames()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Interlocked.Exchange(ref _leakedCommandNames, null);
    }

    // Walk every owned tracker and collect the live TrackedCommands.
    // Used by SlonConnection.UnprepareAll to build the DEALLOCATE batch before disposing.
    public TrackedCommand[] CollectOwned()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_owned is null)
            return [];

        var sink = new List<TrackedCommand>();
        foreach (var (_, ownedTracker) in _owned)
            ownedTracker.CollectInto(sink);
        return sink.Count is 0 ? [] : sink.ToArray();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, true))
            return;
        if (_owned is not null)
        {
            // TODO aggregate and queue a close flow.
            foreach (var (_, ownedTracker) in _owned)
                ownedTracker.Dispose();
        }
        // _parent is the workload-scope tracker owned by SlonDataSource, not ours to dispose.
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, true))
            return default;
        if (_owned is not null)
        {
            // TODO aggregate and queue a close flow.
            foreach (var (_, ownedTracker) in _owned)
                ownedTracker.Dispose();
        }
        // _parent is the workload-scope tracker owned by SlonDataSource, not ours to dispose.
        return default;
    }

    // We don't want to pass our AdoConnectionProxy as these instances would then have a path back to the CWT.
    // If proxy doesn't get disposed we don't clear the table which keeps table cycles alive until all keys are collected.
    // That would mean we leak memory until we reach that moment, see: https://github.com/dotnet/runtime/issues/12255
    sealed class OwnedCommands(List<EncodedString> leakedCommandNames) : IDisposable
    {
        readonly TrackedCommands _tracked = new(0, 0);
        bool _disposed;

        public bool TryAdd(TrackedCommand tracked)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            return _tracked.TryAdd(tracked);
        }

        public TrackedCommand? Find(CommandDescriptor descriptor)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            return _tracked.Find(TrackedCommandKind.Command, descriptor);
        }

        public bool Complete(CommandDescriptor descriptor)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            return _tracked.Complete(TrackedCommandKind.Command, descriptor);
        }

        public void CollectInto(List<TrackedCommand> sink)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            foreach (var variants in _tracked.Commands)
            {
                foreach (var command in variants)
                    if (command is not null && !command.IsInvalid)
                        sink.Add(command);
            }
        }

        public void Dispose() => Dispose(disposing: true);

        void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (!Interlocked.Exchange(ref _disposed, true))
                    GC.SuppressFinalize(this);
            }
            else
            {
                foreach (var variants in _tracked.Commands)
                {
                    foreach (var command in variants)
                        if (command?.CommandName is { } name)
                            leakedCommandNames.Add(name);
                }
            }
        }

        ~OwnedCommands() => Dispose(disposing: false);
    }
}
