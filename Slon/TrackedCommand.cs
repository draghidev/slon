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
    Status _status;
    StrongBox<CommandDescriptor> _descriptorBox;
    long _lastAccessedTicks;

    public TrackedCommand(TrackedCommandKind kind, CommandDescriptor descriptor)
    {
        if (descriptor.CommandName.IsDefault)
            ThrowHelper.ThrowArgumentException(nameof(descriptor), "Command name must be provided.");
        if (descriptor.IsPrepared)
            ThrowHelper.ThrowArgumentException(nameof(descriptor), "Descriptor is already prepared.");

        // We must preserve the types to prevent rooting parameters.
        _descriptorBox = new(CommandDescriptor.Create(descriptor.UnpreparedCommandText, descriptor.ParameterTypes.Preserve(), descriptor.CommandName));
        CommandText = descriptor.UnpreparedCommandText;
        Kind = kind;
    }

    Status GetStatus() => (Status)Volatile.Read(ref Unsafe.As<Status, int>(ref _status));

    public EncodedString CommandName => IsInvalid ? default : _descriptorBox.Value.CommandName;
    public string CommandText { get; }
    public TrackedCommandKind Kind { get; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetPreparedDescriptor(out CommandDescriptor descriptor)
    {
        if (!IsCompleted)
        {
            descriptor = default;
            return false;
        }

        _lastAccessedTicks = Environment.TickCount64;
        descriptor = _descriptorBox.Value;
        return true;
    }

    // Whether the command has gone through all the required operations to be used to run matching commands in a prepared fashion.
    public bool IsCompleted => GetStatus() is Status.CompletedIndeterminate or Status.CompletedSuccesfully;
    public bool IsCompletedSuccessfully => GetStatus() is Status.CompletedSuccesfully;
    public bool IsInvalid => GetStatus() is Status.Invalid;

    public void Invalidate() => Interlocked.Exchange(ref _status, Status.Invalid);

    public bool Complete(CommandDescriptor descriptor)
    {
        if (!descriptor.IsPrepared)
            ThrowHelper.ThrowArgumentException(nameof(descriptor), "Descriptor is not prepared.");

        var comparand = _descriptorBox.Value.IsPrepared && _descriptorBox.Value.PreparedRowDescription is null ? Status.CompletedIndeterminate : Status.Initialized;
        var value = descriptor.PreparedRowDescription is null ? Status.CompletedIndeterminate : Status.CompletedSuccesfully;
        _descriptorBox = new(CommandDescriptor.CreatePrepared(descriptor.CommandName, descriptor.ParameterTypes.Preserve(), descriptor.PreparedRowDescription?.Preserve()));
        return Interlocked.CompareExchange(ref _status, value, comparand) == comparand;
    }

    public RowDescription? RowDescription
    {
        get
        {
            var descriptorBox = _descriptorBox;
            return descriptorBox.Value.IsPrepared ? descriptorBox.Value.PreparedRowDescription : null;
        }
    }

    public ParameterTypeList ParameterTypes => _descriptorBox.Value.ParameterTypes;

    enum Status
    {
        Initialized,
        Invalid,
        CompletedIndeterminate,
        CompletedSuccesfully,
    }
}
