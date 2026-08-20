using System.Collections.Immutable;

namespace Slon.Pg;

readonly struct ParameterSource
{
    readonly object? _state;
    readonly int _count;

    public ParameterSource(object state, int count)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (state is Parameter[])
            ThrowHelper.ThrowArgumentException(nameof(state),
                $"A {nameof(Parameter)} array must be supplied as an {nameof(ImmutableArray<Parameter>)}.");
        _state = state;
        _count = count;
    }

    public ParameterSource(ImmutableArray<Parameter> parameters)
    {
        if (parameters.IsDefaultOrEmpty)
            return;
        _state = System.Runtime.InteropServices.ImmutableCollectionsMarshal.AsArray(parameters)!;
        _count = parameters.Length;
    }

    public int Count => _count;
    public object? State => _state;
}
