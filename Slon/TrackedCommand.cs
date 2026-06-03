using System.Runtime.CompilerServices;
using Slon.Pg;

namespace Slon;

enum TrackedCommandKind
{
    /// An explicitly prepared command, will be prepared for each connection it runs on.
    Command,
    /// An automatically prepared command based on usage statistics.
    Auto,
}

sealed class TrackedCommand
{
    State _state;
    long _lastAccessedTicks;

    public TrackedCommand(TrackedCommandKind kind, CommandDescriptor descriptor)
    {
        if (descriptor.CommandName.IsDefault)
            ThrowHelper.ThrowArgumentException(nameof(descriptor), "Command name must be provided.");
        if (descriptor.IsPrepared)
            ThrowHelper.ThrowArgumentException(nameof(descriptor), "Descriptor is already prepared.");

        // Preserve types to avoid rooting parameters.
        _state = new State(Status.Initialized, CommandDescriptor.Create(descriptor.UnpreparedCommandText, descriptor.ParameterTypes.Preserve(), descriptor.CommandName));
        CommandText = descriptor.UnpreparedCommandText;
        Kind = kind;
    }

    public string CommandText { get; }
    public TrackedCommandKind Kind { get; }

    public EncodedString CommandName
    {
        get
        {
            var state = Volatile.Read(ref _state);
            return state.Status is Status.Invalid ? default : state.Descriptor.CommandName;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetPreparedDescriptor(out CommandDescriptor descriptor)
    {
        var state = Volatile.Read(ref _state);
        if (state.Status is not (Status.CompletedIndeterminate or Status.CompletedSuccesfully))
        {
            descriptor = default;
            return false;
        }

        // LRU access stamp, statistical only, tearing protection is the only requirement.
        Volatile.Write(ref _lastAccessedTicks, Environment.TickCount64);
        descriptor = state.Descriptor;
        return true;
    }

    // Whether the command has gone through all the required operations to be used to run matching commands in a prepared fashion.
    public bool IsCompleted => Volatile.Read(ref _state).Status is Status.CompletedIndeterminate or Status.CompletedSuccesfully;
    public bool IsCompletedSuccessfully => Volatile.Read(ref _state).Status is Status.CompletedSuccesfully;
    public bool IsInvalid => Volatile.Read(ref _state).Status is Status.Invalid;

    internal long LastAccessedTicks => Volatile.Read(ref _lastAccessedTicks);

    // Returns true if this call transitioned to Invalid. False if already Invalid.
    public bool Invalidate()
    {
        while (true)
        {
            var current = Volatile.Read(ref _state);
            if (current.Status is Status.Invalid)
                return false;
            var next = new State(Status.Invalid, current.Descriptor);
            if (Interlocked.CompareExchange(ref _state, next, current) == current)
                return true;
        }
    }

    public bool Complete(CommandDescriptor descriptor)
    {
        if (!descriptor.IsPrepared)
            ThrowHelper.ThrowArgumentException(nameof(descriptor), "Descriptor is not prepared.");

        var prepared = CommandDescriptor.CreatePrepared(descriptor.CommandName, descriptor.ParameterTypes.Preserve(), descriptor.PreparedRowDescription?.Preserve());
        var newStatus = descriptor.PreparedRowDescription is null ? Status.CompletedIndeterminate : Status.CompletedSuccesfully;
        // (status, descriptor) is loop-invariant. Hoist the allocation so contention does not amplify it.
        var next = new State(newStatus, prepared);

        while (true)
        {
            var current = Volatile.Read(ref _state);
            // Allowed transitions:
            //   Initialized             -> any prepared status
            //   CompletedIndeterminate  -> CompletedIndeterminate (refresh) or CompletedSuccesfully (upgrade)
            //   CompletedSuccesfully / Invalid -> terminal
            var canTransition = current.Status switch
            {
                Status.Initialized => true,
                Status.CompletedIndeterminate => true,
                _ => false
            };
            if (!canTransition)
                return false;

            if (Interlocked.CompareExchange(ref _state, next, current) == current)
                return true;
        }
    }

    public RowDescription? RowDescription
    {
        get
        {
            var descriptor = Volatile.Read(ref _state).Descriptor;
            return descriptor.IsPrepared ? descriptor.PreparedRowDescription : null;
        }
    }

    public ParameterTypeList ParameterTypes => Volatile.Read(ref _state).Descriptor.ParameterTypes;

    enum Status
    {
        Initialized,
        Invalid,
        CompletedIndeterminate,
        CompletedSuccesfully,
    }

    sealed class State(Status status, CommandDescriptor descriptor)
    {
        public readonly Status Status = status;
        public readonly CommandDescriptor Descriptor = descriptor;
    }
}
