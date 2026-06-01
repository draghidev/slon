using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;

namespace Slon.Pg;

readonly struct Command()
{
    // Whether the command only describes itself, without execution, also redescribes prepared commands.
    public bool DescribeOnly { get; init; } = false;
    public bool WithSync { get; init; } = false;
    public bool PreferSimple { get; init; } = false;
    public CommandDescriptor Descriptor { get; init; } = default;
    public TimeSpan Timeout { get; init; } = default;
    public ImmutableArray<Parameter> Parameters { get; init; } = [];

    public static Command Create(string commandText, ParameterTypeList parameterTypes = default, EncodedString commandName = default)
        => new() { Descriptor = CommandDescriptor.Create(commandText, parameterTypes, commandName) };

    public static Command Create(CommandDescriptor descriptor)
        => new() { Descriptor = descriptor };
}

readonly struct CommandList
{
    readonly Command _command;
    readonly ImmutableArray<Command> _commands;

    public CommandList(ImmutableArray<Command> commands)
        => _commands = commands;

    public CommandList(params ReadOnlySpan<Command> commands)
    {
        switch (commands.Length)
        {
            case 1:
                _command = commands[0];
                break;
            default:
                _commands = [..commands];
                break;
        }
    }

    public int Count => _commands.IsDefault ? 1 : _commands.Length;

    public Command this[int i] => ItemRef(i);

    [UnscopedRef]
    public ref readonly Command ItemRef(int index)
    {
        if (_commands.IsDefault)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(index, 0);
            return ref _command;
        }

        return ref _commands.ItemRef(index);
    }

    [UnscopedRef]
    public ReadOnlySpan<Command>.Enumerator GetEnumerator() => AsSpan().GetEnumerator();

    [UnscopedRef]
    ReadOnlySpan<Command> AsSpan()
        => _commands.IsDefault ? new(in _command) : _commands.AsSpan();
}

readonly struct CommandMetadata
{
    // Which original command this result belongs to, important for prepared commands and multi result simple protocol commands.
    public int CommandIndex { get; init; }
    public EncodedString CommandName { get; init; }
    public ParameterTypeList ParameterTypes { get; init; }
    public RowDescription? RowDescription { get; init; }
    public bool IsPrepared { get; init; }

    public CommandDescriptor ToPreparedDescriptor()
    {
        if (!IsPrepared)
            ThrowHelper.ThrowInvalidOperation("Command is not prepared.");

        return CommandDescriptor.CreatePrepared(CommandName, ParameterTypes.Preserve(), RowDescription?.Preserve());
    }
}
