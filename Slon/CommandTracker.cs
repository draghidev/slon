using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Slon.Pg;

namespace Slon;

readonly struct TrackerResult(TrackedCommand? tracked, Action<CommandResult, object?>? completeTrackedAction = null)
{
    public Action<CommandResult, object?>? CompleteTrackedAsObjectAction => completeTrackedAction;
    public Action<CommandResult, TrackedCommand>? CompleteTrackedAction => completeTrackedAction;
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
    readonly ConcurrentDictionary<TrackedCommand, bool> _preparingCommands = new(concurrencyLevel: 1, capacity: 0);
    readonly TrackedCommands? _tracked;
    List<EncodedString>? _leakedCommandNames;
    ConditionalWeakTable<object, OwnedCommands>? _owned;
    Action<CommandResult, object?>? _completeTrackedAction;
    bool _disposed;

    int _counter;
    readonly CommandTracker? _parent;

    public CommandTracker(int maxAuto, int autoMinimumUses, CommandTracker? parent = null)
    {
        _parent = parent;
        if (maxAuto > 0)
            _tracked = new(maxAuto, autoMinimumUses);
    }

    // TODO have to make sure our explicit names won't clash across trackes (need some shared sequence).

    string GetNextExplicitlyPreparedName() => $"_ep{Interlocked.Increment(ref _counter)}";

    public bool HasParent => _parent is not null;

    // public TrackerResult TrackParentOwned(object owningInstance, in CommandDescriptor descriptor, TrackedCommand? tracked = null)
    // {
    //     if (parent is null)
    //         ThrowHelper.ThrowInvalidOperation("Parent tracker is not set.");
    //     return parent.Track(descriptor, tracked, owningInstance);
    // }

    public TrackerResult Track(in CommandDescriptor descriptor, TrackedCommand? tracked = null, object? owningInstance = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (descriptor.IsPrepared)
            ThrowHelper.ThrowArgumentException(nameof(descriptor), "Descriptor is a prepared descriptor.");

        if (tracked?.IsCompleted == true)
            return new(tracked, tracked.RowDescription is null ? _completeTrackedAction ??= CompleteTracked : null);

        // This is an auto prepare.
        if (owningInstance is null)
        {
            if (_tracked is null)
                return _parent?.Track(descriptor, tracked) ?? default;

            if (_tracked.TryGet(descriptor, out tracked))
            {
                if (tracked.IsCompleted)
                    return new(tracked, tracked.RowDescription is null ? _completeTrackedAction ??= CompleteTracked : null);

                return !tracked.IsInvalid && _preparingCommands.TryAdd(tracked, true)
                    ? new(tracked, _completeTrackedAction ??= CompleteTracked)
                    // TODO if we wanted to return in progress preparations to tag along on
                    // we must know in which way this new command is connected to the tracked one.
                    // If it's in its own sync batch we can't share the preparation as the original preparation might error.
                    // If it's in the same batch we can, as the previous error will make sure we won't end up executing it.
                    : default;
            }

            return default;
        }

        return Core(owningInstance, descriptor, tracked);

        [MethodImpl(MethodImplOptions.NoInlining)]
        TrackerResult Core(object owningInstance, in CommandDescriptor descriptor, TrackedCommand? tracked = null)
        {
            // This is an explicit prepare.
            _owned ??= new();
            if (!_owned.TryGetValue(owningInstance, out var ownedTracker))
                ownedTracker = CreateOwnedTracker(owningInstance, descriptor, tracked);

            while ((tracked = ownedTracker.Find(descriptor)) is null)
            {
                tracked = new(TrackedCommandKind.Command, descriptor with { CommandName = GetNextExplicitlyPreparedName() });
                if (ownedTracker.TryAdd(tracked))
                    break;
            }

            // TODO there's probably a race here where the tracked command turns invalid just after we have added it.
            // If no flow is already preparing it, return the command name.
            if (tracked.IsCompleted)
                return new(tracked);

            return !tracked.IsInvalid && _preparingCommands.TryAdd(tracked, true)
                ? new(tracked, _completeTrackedAction ??= CompleteTracked)
                : new(tracked);
        }

        void CompleteTracked(CommandResult result, object? state)
        {
            var tracked = (TrackedCommand)state!;
            var metadata = result.GetMetadata();
            try
            {
                // Here we would make RowDescription portable (once we need it).
                if (metadata.IsPrepared)
                {
                    // We were raced, which would be unexpected, or it got invalidated...
                    if (!tracked.Complete(metadata.ToPreparedDescriptor()))
                    {
                        if (!tracked.IsInvalid)
                            ThrowHelper.ThrowInvalidOperation("Command was completed by another caller, this should not happen.");
                        // If it's invalid we must add it to leaked commands for cleanup.
                        // TODO if we were disposed we have to do something else.
                        Debug.Assert(_leakedCommandNames is not null, "Any owned command complete shouldn't find a null leaked list.");
                        _leakedCommandNames.Add(tracked.CommandName);
                    }
                }
                // If !metadata.IsPrepared the Parse failed (or was skipped). We leave tracked at
                // Initialized so a future caller can re-attempt the preparation; the finally below
                // releases the in-flight marker so they aren't blocked from doing so.
            }
            finally
            {
                _preparingCommands.TryRemove(tracked, out _);
            }
        }

        OwnedCommands CreateOwnedTracker(object owningInstance, in CommandDescriptor descriptor, TrackedCommand? tracked)
        {
            if (tracked is not null)
                ThrowHelper.ThrowArgumentException(nameof(tracked), "Instance was not found but prepared name was given?");

            tracked = new TrackedCommand(TrackedCommandKind.Command, descriptor with { CommandName = GetNextExplicitlyPreparedName() });
            var tracker = new OwnedCommands(_leakedCommandNames ??= new());
            var success = tracker.TryAdd(tracked);
            Debug.Assert(success);

            // We were raced by a concurrent explicit prepare, that shouldn't normally happen but it's easy to handle.
            while (!_owned.TryAdd(owningInstance, tracker!))
            {
                if (_owned.TryGetValue(owningInstance, out tracker))
                    break;
            }

            Debug.Assert(tracker is not null);
            return tracker;
        }
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
