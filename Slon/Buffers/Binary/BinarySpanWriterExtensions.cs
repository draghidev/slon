using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace Slon.Buffers.Binary;

public static class BinarySpanWriterExtensions
{
    public static void WriteStringWithNullTerminator<TWriter>(this ref TWriter writer, string value, Encoding encoding, int? encodedLength = null)
        where TWriter : struct, IBufferWriter<byte>, allows ref struct
        => writer.WriteStringWithNullTerminator(value.AsSpan(), encoding, encodedLength);

    public static void WriteStringWithNullTerminator(this IBufferWriter<byte> writer, string value, Encoding encoding, int? encodedLength = null)
        => writer.WriteStringWithNullTerminator(value.AsSpan(), encoding, encodedLength);

    public static void WriteStringWithNullTerminator<TWriter>(this ref TWriter writer, ReadOnlySpan<char> value, Encoding encoding, int? encodedLength = null)
        where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        writer.WriteString(value, encoding, encodedLength - 1);
        writer.WriteByte(0);
    }

    public static void WriteStringWithNullTerminator(this IBufferWriter<byte> writer, ReadOnlySpan<char> value, Encoding encoding, int? encodedLength = null)
    {
        writer.WriteString(value, encoding, encodedLength - 1);
        writer.WriteByte(0);
    }

    public static void WriteString<TWriter>(this ref TWriter writer, string value, Encoding encoding, int? encodedLength = null)
        where TWriter : struct, IBufferWriter<byte>, allows ref struct
        => writer.WriteString(value.AsSpan(), encoding, encodedLength);

    public static void WriteString(this IBufferWriter<byte> writer, string value, Encoding encoding, int? encodedLength = null)
        => writer.WriteString(value.AsSpan(), encoding, encodedLength);

    public static void WriteString<TWriter>(this ref TWriter writer, ReadOnlySpan<char> value, Encoding encoding, int? encodedLength = null)
        where TWriter : struct, IBufferWriter<byte>, allows ref struct
    {
        if (value.IsEmpty)
            return;

        var dest = writer.GetSpan();
        var sourceLength = encodedLength ?? encoding.GetByteCount(value);

        if (dest.Length < sourceLength)
        {
            WriteChunked(ref writer, value, sourceLength, encoding);
            return;
        }

        encoding.GetBytes(value, dest);
        writer.Advance(sourceLength);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void WriteChunked(ref TWriter writer, ReadOnlySpan<char> data, int encodedLength, Encoding encoding)
        {
            var source = data;
            var totalBytesUsed = 0;
            var encoder = encoding.GetEncoder();
            var minBufferSize = encoding.GetMaxByteCount(1);
            var bytes = writer.GetSpan(minBufferSize);
            var completed = false;

            // This may be an underlying problem but encoder.Convert returns completed = true for UTF7 too early.
            // Therefore, we check encodedLength - totalBytesUsed too.
            while (!completed || encodedLength - totalBytesUsed != 0)
            {
                // Zero length spans are possible, though unlikely.
                // encoding.Convert and .Advance will both handle them so we won't special case for them.
                encoder.Convert(source, bytes, flush: true, out var charsUsed, out var bytesUsed, out completed);
                writer.Advance(bytesUsed);

                totalBytesUsed += bytesUsed;
                if (totalBytesUsed >= encodedLength)
                {
                    Debug.Assert(totalBytesUsed == encodedLength);
                    // Encoded everything
                    break;
                }

                source = source.Slice(charsUsed);

                // Get new span, more to encode.
                bytes = writer.GetSpan(minBufferSize);
            }
        }
    }

    public static void WriteString(this IBufferWriter<byte> writer, ReadOnlySpan<char> value, Encoding encoding, int? encodedLength = null)
    {
        if (value.IsEmpty)
            return;

        var dest = writer.GetSpan();
        var sourceLength = encodedLength ?? encoding.GetByteCount(value);

        if (dest.Length < sourceLength)
        {
            WriteChunked(writer, value, sourceLength, encoding);
            return;
        }

        encoding.GetBytes(value, dest);
        writer.Advance(sourceLength);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void WriteChunked(IBufferWriter<byte> writer, ReadOnlySpan<char> data, int encodedLength, Encoding encoding)
        {
            var source = data;
            var totalBytesUsed = 0;
            var encoder = encoding.GetEncoder();
            var minBufferSize = encoding.GetMaxByteCount(1);
            var bytes = writer.GetSpan(minBufferSize);
            var completed = false;

            // This may be an underlying problem but encoder.Convert returns completed = true for UTF7 too early.
            // Therefore, we check encodedLength - totalBytesUsed too.
            while (!completed || encodedLength - totalBytesUsed != 0)
            {
                // Zero length spans are possible, though unlikely.
                // encoding.Convert and .Advance will both handle them so we won't special case for them.
                encoder.Convert(source, bytes, flush: true, out var charsUsed, out var bytesUsed, out completed);
                writer.Advance(bytesUsed);

                totalBytesUsed += bytesUsed;
                if (totalBytesUsed >= encodedLength)
                {
                    Debug.Assert(totalBytesUsed == encodedLength);
                    // Encoded everything
                    break;
                }

                source = source.Slice(charsUsed);

                // Get new span, more to encode.
                bytes = writer.GetSpan(minBufferSize);
            }
        }
    }
}
