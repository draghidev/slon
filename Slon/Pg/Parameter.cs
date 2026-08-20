using Slon.Pg.Types;

namespace Slon.Pg;

// A complete protocol value used directly without a deferred parameter writer.
readonly struct Parameter
{
    readonly object? _value;
    readonly Oid _oid;
    readonly int _size;

    public Oid Oid => _oid;
    public object? Value => _value;

    Parameter(object? value, Oid oid, int size)
    {
        _value = value;
        _oid = oid;
        _size = size;
    }

    public static Parameter Create(byte[] value, Oid oid)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new(value, oid, value.Length);
    }

    public static Parameter Create(Stream value, Oid oid)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (!value.CanSeek)
            throw new NotSupportedException("A non-seekable stream requires an explicit length.");
        return new(value, oid, GetStreamSize(value));
    }

    public static Parameter Create(Stream value, int length, Oid oid)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        return new(value, oid, length);
    }

    public static Parameter CreateNull(Oid oid) => new(null, oid, -1);

    public int Size => _value is null ? -1 : _size;

    static int GetStreamSize(Stream stream)
    {
        var length = stream.Length - stream.Position;
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        return checked((int)length);
    }
}
