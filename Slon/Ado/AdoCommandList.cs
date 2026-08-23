using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Slon;

struct AdoCommandList<TCommand> where TCommand : IAdoCommand
{
    TCommand _command;
    bool _commandHasValue;
    List<TCommand>? _commands;

    public AdoCommandList()
    {
        _command = default!;
    }

    public AdoCommandList(int initialCapacity)
    {
        _command = default!;
        _commands = initialCapacity > 0 ? new List<TCommand>(initialCapacity) : null;
    }

    public int Count => _commandHasValue ? 1 : _commands?.Count ?? 0;

    public TCommand this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, Count);
            return _commandHasValue ? _command : _commands![index];
        }
    }

    public bool Contains(TCommand item) => IndexOf(item) != -1;
    public int IndexOf(TCommand item)
    {
        if (_commandHasValue && EqualityComparer<TCommand>.Default.Equals(_command, item))
            return 0;

        return _commands?.IndexOf(item) ?? -1;
    }

    public void Add(TCommand command)
    {
        if (!_commandHasValue && _commands is null)
        {
            _commandHasValue = true;
            _command = command;
            return;
        }
        if (_commands is null)
        {
            _commands = new List<TCommand>();
            if (_command is not null)
            {
                _commands.Add(_command);
                _command = default!;
                _commandHasValue = false;
            }
        }
        _commands.Add(command);
    }

    public void Insert(int index, TCommand command)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(index, Count);
        if (index == Count)
        {
            Add(command);
            return;
        }
        if (_commands is not null)
        {
            _commands.Insert(index, command);
            return;
        }

        Debug.Assert(_commandHasValue && index == 0);
        _commands = [command, _command];
        _command = default!;
        _commandHasValue = false;
    }

    public void RemoveAt(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, Count);
        if (_commandHasValue)
        {
            Debug.Assert(index == 0);
            _command = default!;
            _commandHasValue = false;
        }
        else
        {
            _commands!.RemoveAt(index);
        }
    }

    public bool Remove(TCommand item)
    {
        if (_commandHasValue && EqualityComparer<TCommand>.Default.Equals(_command, item))
        {
            _command = default!;
            _commandHasValue = false;
            return true;
        }

        return _commands?.Remove(item) ?? false;
    }

    public void CopyTo(TCommand[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        if (_commandHasValue)
        {
            array[arrayIndex] = _command;
            return;
        }

        _commands?.CopyTo(array, arrayIndex);
    }

    public void Clear()
    {
        _command = default!;
        _commandHasValue = false;
        _commands?.Clear();
    }

    [UnscopedRef]
    public ReadOnlySpan<TCommand>.Enumerator GetEnumerator()
        => ((ReadOnlySpan<TCommand>)AsSpan()).GetEnumerator();

    [UnscopedRef]
    public Span<TCommand> AsSpan()
        => _commandHasValue ? new Span<TCommand>(ref _command) : CollectionsMarshal.AsSpan(_commands);
}
