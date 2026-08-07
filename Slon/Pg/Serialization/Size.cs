using System.Diagnostics;

namespace Slon.Pg.Serialization;

public enum SizeKind
{
    Unknown,
    Exact,
    UpperBound
}

[DebuggerDisplay("{ToString(),nq}")]
public readonly struct Size : IEquatable<Size>
{
    readonly int _value;

    Size(SizeKind kind, int value)
    {
        Kind = kind;
        _value = value;
    }

    public SizeKind Kind { get; }
    public int Value => Kind is SizeKind.Unknown
        ? throw new InvalidOperationException("Cannot get a value from an unknown size.")
        : _value;

    internal int GetValueOrDefault() => _value;

    public static Size Create(int byteCount) => new(SizeKind.Exact, byteCount);
    public static Size CreateUpperBound(int byteCount) => new(SizeKind.UpperBound, byteCount);
    public static Size Unknown { get; } = new(SizeKind.Unknown, 0);
    public static Size Zero { get; } = new(SizeKind.Exact, 0);

    public Size Combine(Size other)
    {
        if (Kind is SizeKind.Unknown || other.Kind is SizeKind.Unknown)
            return Unknown;

        if (Kind is SizeKind.UpperBound || other.Kind is SizeKind.UpperBound)
        {
            var sum = (long)_value + other._value;
            return sum > int.MaxValue ? Unknown : CreateUpperBound((int)sum);
        }

        return Create(checked(_value + other._value));
    }

    public static implicit operator Size(int value) => Create(value);

    public bool Equals(Size other) => _value == other._value && Kind == other.Kind;
    public override bool Equals(object? obj) => obj is Size other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(_value, (int)Kind);
    public static bool operator ==(Size left, Size right) => left.Equals(right);
    public static bool operator !=(Size left, Size right) => !left.Equals(right);

    public override string ToString() => Kind switch
    {
        SizeKind.Exact or SizeKind.UpperBound => $"{_value} ({Kind})",
        SizeKind.Unknown => nameof(SizeKind.Unknown),
        _ => throw new ArgumentOutOfRangeException()
    };
}
