using System.Runtime.CompilerServices;

namespace Slon.Pg.Serialization;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct BufferRequirements : IEquatable<BufferRequirements>
{
    readonly Size _read;
    readonly Size _write;
    readonly bool _optionalBind;

    BufferRequirements(Size read, Size write, bool optionalBind)
    {
        _read = read;
        _write = write;
        _optionalBind = optionalBind;
    }

    public Size Read => _read;
    public Size Write => _write;
    public bool IsBindFixedSize => _write.Kind is SizeKind.Exact;
    public bool IsBindOptional => _optionalBind;

    public static BufferRequirements Streaming => new(Size.Unknown, Size.Unknown, optionalBind: false);
    public static BufferRequirements Value => new(
        Size.CreateUpperBound(int.MaxValue), Size.CreateUpperBound(int.MaxValue), optionalBind: false);
    public static BufferRequirements CreateFixedSize(int byteCount)
        => new(byteCount, byteCount, optionalBind: true);
    public static BufferRequirements CreateFixedSize(int byteCount, bool optionalBind)
        => new(byteCount, byteCount, optionalBind);
    public static BufferRequirements Create(Size value)
        => new(value, value, optionalBind: value.Kind is SizeKind.Exact);
    public static BufferRequirements Create(Size read, Size write)
        => new(read, write, optionalBind: write.Kind is SizeKind.Exact);
    public static BufferRequirements Create(Size read, Size write, bool optionalBind)
        => new(read, write, optionalBind);

    public BufferRequirements Combine(BufferRequirements other)
    {
        var read = CombineOrNull(_read, other._read) ?? Size.Unknown;
        var write = CombineOrNull(_write, other._write);
        return new(read, write ?? Size.Unknown,
            optionalBind: write is not null && _optionalBind && other._optionalBind);

        static Size? CombineOrNull(Size left, Size right)
        {
            try { return left.Combine(right); }
            catch (OverflowException) { return null; }
        }
    }

    public BufferRequirements Combine(int byteCount) => Combine(CreateFixedSize(byteCount));

    public bool Equals(BufferRequirements other)
        => _read == other._read && _write == other._write && _optionalBind == other._optionalBind;
    public override bool Equals(object? obj) => obj is BufferRequirements other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(_read, _write, _optionalBind);
    public static bool operator ==(BufferRequirements left, BufferRequirements right) => left.Equals(right);
    public static bool operator !=(BufferRequirements left, BufferRequirements right) => !left.Equals(right);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetMinimumBufferByteCount(Size requirement, int valueSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(valueSize);
        var byteCount = requirement.GetValueOrDefault();
        return requirement.Kind switch
        {
            SizeKind.Exact when byteCount != valueSize =>
                throw new ArgumentOutOfRangeException(nameof(requirement),
                    $"Exact buffer requirement ({byteCount} bytes) does not match value size ({valueSize} bytes)."),
            SizeKind.Exact or SizeKind.UpperBound => Math.Min(valueSize, byteCount),
            _ => byteCount
        };
    }
}
