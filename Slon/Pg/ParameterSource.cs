using System.Collections.Immutable;

namespace Slon.Pg;

readonly struct ParameterSource
{
    readonly object? _state;
    readonly ParameterWriter? _writer;
    readonly int _count;

    public ParameterSource(object state, ParameterWriter writer)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(writer);
        if (state is Parameter[])
            ThrowHelper.ThrowArgumentException(nameof(state),
                $"A {nameof(Parameter)} array must be supplied as an {nameof(ImmutableArray<Parameter>)}.");
        _state = state;
        _writer = writer;
    }

    public ParameterSource(ImmutableArray<Parameter> parameters)
    {
        if (parameters.IsDefaultOrEmpty)
            return;
        _state = System.Runtime.InteropServices.ImmutableCollectionsMarshal.AsArray(parameters)!;
        _count = parameters.Length;
    }

    public int Count => _writer is null ? _count : _writer.GetParameterCountCore(_state!);
    public object? State => _state;
    public ParameterWriter? Writer => _writer;
}
