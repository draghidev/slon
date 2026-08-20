using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Slon.Pg.Types;

// Primitive field decoding used while the PostgreSQL type catalog—and therefore the serializer
// graph needed by normal field readers—is still being constructed.
static class BootstrapFieldDecoder
{
    public static T Read<T>(ReadOnlySpan<byte> field, Encoding? textEncoding = null)
    {
        if (typeof(T) == typeof(int) || typeof(T) == typeof(uint))
        {
            if (field.Length == sizeof(int))
            {
                var value = BinaryPrimitives.ReadInt32BigEndian(field);
                return typeof(T) == typeof(int)
                    ? (T)(object)value
                    : (T)(object)unchecked((uint)value);
            }
            ThrowHelper.ThrowInvalidOperation();
        }

        if (typeof(T) == typeof(bool) && field.Length is 1)
            return (T)(object)(field[0] != 0);
        if (typeof(T) == typeof(byte[]))
            return (T)(object)field.ToArray();
        if (typeof(T) == typeof(string))
            return (T)(object)(textEncoding ?? Encoding.UTF8).GetString(field);

        ThrowHelper.ThrowInvalidOperation();
        return default!;
    }

    public static T Read<T>(in ReadOnlySequence<byte> field, Encoding? textEncoding = null)
    {
        if (field.IsSingleSegment)
            return Read<T>(field.FirstSpan, textEncoding);

        if (typeof(T) == typeof(int) || typeof(T) == typeof(uint))
        {
            var reader = new SequenceReader<byte>(field);
            if (field.Length == sizeof(int) && reader.TryReadBigEndian(out int value))
                return typeof(T) == typeof(int)
                    ? (T)(object)value
                    : (T)(object)unchecked((uint)value);
            ThrowHelper.ThrowInvalidOperation();
        }
        if (typeof(T) == typeof(bool) && field.Length is 1)
        {
            var reader = new SequenceReader<byte>(field);
            if (reader.TryRead(out var value))
                return (T)(object)(value != 0);
            ThrowHelper.ThrowInvalidOperation();
        }
        if (typeof(T) == typeof(byte[]))
            return (T)(object)field.ToArray();
        if (typeof(T) == typeof(string))
            return (T)(object)(textEncoding ?? Encoding.UTF8).GetString(field);

        ThrowHelper.ThrowInvalidOperation();
        return default!;
    }
}
