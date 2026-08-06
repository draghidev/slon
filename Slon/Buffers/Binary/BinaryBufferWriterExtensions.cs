using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Slon.Buffers.Binary;

static class BinaryBufferWriterExtensions
{
    public static void WriteByte<TWriter>(this ref TWriter writer, byte value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        var span = writer.GetSpan(1);
        span[0] = value;
        writer.Advance(sizeof(byte));
    }

    public static void WriteByte(this IBufferWriter<byte> writer, byte value)
    {
        var span = writer.GetSpan(1);
        span[0] = value;
        writer.Advance(sizeof(byte));
    }

    public static void WriteInt64LittleEndian<TWriter>(this ref TWriter writer, long value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteInt64LittleEndian(writer.GetSpan(sizeof(long)), value);
        writer.Advance(sizeof(long));
    }

    public static void WriteInt64LittleEndian(this IBufferWriter<byte> writer, long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(writer.GetSpan(sizeof(long)), value);
        writer.Advance(sizeof(long));
    }

    public static void WriteInt32LittleEndian<TWriter>(this ref TWriter writer, int value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteInt32LittleEndian(writer.GetSpan(sizeof(int)), value);
        writer.Advance(sizeof(int));
    }

    public static void WriteInt32LittleEndian(this IBufferWriter<byte> writer, int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(writer.GetSpan(sizeof(int)), value);
        writer.Advance(sizeof(int));
    }

    public static void WriteInt16LittleEndian<TWriter>(this ref TWriter writer, short value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteInt16LittleEndian(writer.GetSpan(sizeof(short)), value);
        writer.Advance(sizeof(short));
    }

    public static void WriteInt16LittleEndian(this IBufferWriter<byte> writer, short value)
    {
        BinaryPrimitives.WriteInt16LittleEndian(writer.GetSpan(sizeof(short)), value);
        writer.Advance(sizeof(short));
    }

    public static void WriteUInt64LittleEndian<TWriter>(this ref TWriter writer, ulong value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteUInt64LittleEndian(writer.GetSpan(sizeof(ulong)), value);
        writer.Advance(sizeof(ulong));
    }

    public static void WriteUInt64LittleEndian(this IBufferWriter<byte> writer, ulong value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(writer.GetSpan(sizeof(ulong)), value);
        writer.Advance(sizeof(ulong));
    }

    public static void WriteUInt32LittleEndian<TWriter>(this ref TWriter writer, uint value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteUInt32LittleEndian(writer.GetSpan(sizeof(uint)), value);
        writer.Advance(sizeof(uint));
    }

    public static void WriteUInt32LittleEndian(this IBufferWriter<byte> writer, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(writer.GetSpan(sizeof(uint)), value);
        writer.Advance(sizeof(uint));
    }

    public static void WriteUInt16LittleEndian<TWriter>(this ref TWriter writer, ushort value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteUInt16LittleEndian(writer.GetSpan(sizeof(ushort)), value);
        writer.Advance(sizeof(ushort));
    }

    public static void WriteUInt16LittleEndian(this IBufferWriter<byte> writer, ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(writer.GetSpan(sizeof(ushort)), value);
        writer.Advance(sizeof(ushort));
    }

    public static void WriteDoubleLittleEndian<TWriter>(this ref TWriter writer, double value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteDoubleLittleEndian(writer.GetSpan(sizeof(double)), value);
        writer.Advance(sizeof(double));
    }

    public static void WriteDoubleLittleEndian(this IBufferWriter<byte> writer, double value)
    {
        BinaryPrimitives.WriteDoubleLittleEndian(writer.GetSpan(sizeof(double)), value);
        writer.Advance(sizeof(double));
    }

    public static void WriteSingleLittleEndian<TWriter>(this ref TWriter writer, float value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteSingleLittleEndian(writer.GetSpan(sizeof(float)), value);
        writer.Advance(sizeof(float));
    }

    public static void WriteSingleLittleEndian(this IBufferWriter<byte> writer, float value)
    {
        BinaryPrimitives.WriteSingleLittleEndian(writer.GetSpan(sizeof(float)), value);
        writer.Advance(sizeof(float));
    }

    public static void WriteHalfLittleEndian<TWriter>(this ref TWriter writer, Half value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteHalfLittleEndian(writer.GetSpan(Unsafe.SizeOf<Half>()), value);
        writer.Advance(Unsafe.SizeOf<Half>());
    }

    public static void WriteHalfLittleEndian<TWriter>(this IBufferWriter<byte> writer, Half value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteHalfLittleEndian(writer.GetSpan(Unsafe.SizeOf<Half>()), value);
        writer.Advance(Unsafe.SizeOf<Half>());
    }

        public static void WriteInt64BigEndian<TWriter>(this ref TWriter writer, long value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteInt64BigEndian(writer.GetSpan(sizeof(long)), value);
        writer.Advance(sizeof(long));
    }

    public static void WriteInt64BigEndian(this IBufferWriter<byte> writer, long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(writer.GetSpan(sizeof(long)), value);
        writer.Advance(sizeof(long));
    }

    public static void WriteInt32BigEndian<TWriter>(this ref TWriter writer, int value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteInt32BigEndian(writer.GetSpan(sizeof(int)), value);
        writer.Advance(sizeof(int));
    }

    public static void WriteInt32BigEndian(this IBufferWriter<byte> writer, int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(writer.GetSpan(sizeof(int)), value);
        writer.Advance(sizeof(int));
    }

    public static void WriteInt16BigEndian<TWriter>(this ref TWriter writer, short value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteInt16BigEndian(writer.GetSpan(sizeof(short)), value);
        writer.Advance(sizeof(short));
    }

    public static void WriteInt16BigEndian(this IBufferWriter<byte> writer, short value)
    {
        BinaryPrimitives.WriteInt16BigEndian(writer.GetSpan(sizeof(short)), value);
        writer.Advance(sizeof(short));
    }

    public static void WriteUInt64BigEndian<TWriter>(this ref TWriter writer, ulong value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteUInt64BigEndian(writer.GetSpan(sizeof(ulong)), value);
        writer.Advance(sizeof(ulong));
    }

    public static void WriteUInt64BigEndian(this IBufferWriter<byte> writer, ulong value)
    {
        BinaryPrimitives.WriteUInt64BigEndian(writer.GetSpan(sizeof(ulong)), value);
        writer.Advance(sizeof(ulong));
    }

    public static void WriteUInt32BigEndian<TWriter>(this ref TWriter writer, uint value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteUInt32BigEndian(writer.GetSpan(sizeof(uint)), value);
        writer.Advance(sizeof(uint));
    }

    public static void WriteUInt32BigEndian(this IBufferWriter<byte> writer, uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(writer.GetSpan(sizeof(uint)), value);
        writer.Advance(sizeof(uint));
    }

    public static void WriteUInt16BigEndian<TWriter>(this ref TWriter writer, ushort value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteUInt16BigEndian(writer.GetSpan(sizeof(ushort)), value);
        writer.Advance(sizeof(ushort));
    }

    public static void WriteUInt16BigEndian(this IBufferWriter<byte> writer, ushort value)
    {
        BinaryPrimitives.WriteUInt16BigEndian(writer.GetSpan(sizeof(ushort)), value);
        writer.Advance(sizeof(ushort));
    }

    public static void WriteDoubleBigEndian<TWriter>(this ref TWriter writer, double value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteDoubleBigEndian(writer.GetSpan(sizeof(double)), value);
        writer.Advance(sizeof(double));
    }

    public static void WriteDoubleBigEndian(this IBufferWriter<byte> writer, double value)
    {
        BinaryPrimitives.WriteDoubleBigEndian(writer.GetSpan(sizeof(double)), value);
        writer.Advance(sizeof(double));
    }

    public static void WriteSingleBigEndian<TWriter>(this ref TWriter writer, float value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteSingleBigEndian(writer.GetSpan(sizeof(float)), value);
        writer.Advance(sizeof(float));
    }

    public static void WriteSingleBigEndian(this IBufferWriter<byte> writer, float value)
    {
        BinaryPrimitives.WriteSingleBigEndian(writer.GetSpan(sizeof(float)), value);
        writer.Advance(sizeof(float));
    }

    public static void WriteHalfBigEndian<TWriter>(this ref TWriter writer, Half value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteHalfBigEndian(writer.GetSpan(Unsafe.SizeOf<Half>()), value);
        writer.Advance(Unsafe.SizeOf<Half>());
    }

    public static void WriteHalfBigEndian<TWriter>(this IBufferWriter<byte> writer, Half value) where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        BinaryPrimitives.WriteHalfBigEndian(writer.GetSpan(Unsafe.SizeOf<Half>()), value);
        writer.Advance(Unsafe.SizeOf<Half>());
    }
}
