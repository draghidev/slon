using System.Buffers;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Slon.Text;

namespace Slon.Pg;

readonly struct Command()
{
    // Whether the command only describes itself, without execution, also redescribes prepared commands.
    public bool DescribeOnly { get; init; } = false;
    // Process the complete response, but omit it from the flow enumerator.
    public bool SuppressEnumeration { get; init; } = false;
    public bool WithSync { get; init; } = false;
    public bool PreferSimple { get; init; } = false;
    public CommandDescriptor Descriptor { get; init; } = default;
    public TimeSpan Timeout { get; init; } = default;
    public ImmutableArray<Parameter> Parameters { get; init; } = [];
    // Empty means Slon's default (all binary). A single entry applies to every result column;
    // otherwise the count must match the returned RowDescription.
    public ImmutableArray<PgFormat> ResultFormats { get; init; } = [];

    public static Command Create(string commandText, ParameterTypeList parameterTypes = default, EncodedString commandName = default)
        => new() { Descriptor = CommandDescriptor.Create(commandText, parameterTypes, commandName) };

    public static Command Create(CommandDescriptor descriptor)
        => new() { Descriptor = descriptor };
}

enum PgFormat : short
{
    Text = 0,
    Binary = 1
}

readonly struct CommandList
{
    readonly Command _command;
    readonly Command[]? _commands;
    readonly int _count;
    readonly bool _isPooled;

    internal CommandList(Command[] commands, int count, bool isPooled)
    {
        _commands = commands;
        _count = count;
        _isPooled = isPooled;
    }

    public CommandList(ImmutableArray<Command> commands)
    {
        _commands = ImmutableCollectionsMarshal.AsArray(commands) ?? [];
        _count = commands.IsDefault ? 0 : commands.Length;
    }

    public CommandList(params ReadOnlySpan<Command> commands)
    {
        switch (commands.Length)
        {
            case 1:
                _command = commands[0];
                break;
            default:
                _commands = commands.ToArray();
                _count = commands.Length;
                break;
        }
    }

    public int Count => _commands is null ? 1 : _count;

    public int VisibleCount
    {
        get
        {
            var count = 0;
            foreach (ref readonly var command in AsSpan())
            {
                if (!command.SuppressEnumeration)
                    count++;
            }
            return count;
        }
    }

    public Command this[int i] => ItemRef(i);

    [UnscopedRef]
    public ref readonly Command ItemRef(int index)
    {
        if (_commands is null)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(index, 0);
            return ref _command;
        }

        return ref _commands[index];
    }

    [UnscopedRef]
    public ReadOnlySpan<Command>.Enumerator GetEnumerator() => AsSpan().GetEnumerator();

    [UnscopedRef]
    ReadOnlySpan<Command> AsSpan()
    {
        if (_commands is null)
            return new(in _command);
        return _commands.AsSpan(0, _count);
    }

    internal void Return()
    {
        if (_isPooled)
            ArrayPool<Command>.Shared.Return(_commands!, clearArray: true);
    }
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
