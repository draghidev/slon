using System.Buffers;
using System.Runtime.CompilerServices;

namespace Slon.Buffers;

static class BufferWriterExtensions
{
    /// <summary>
    /// Copies the caller's buffer into this writer and calls <see cref="IBufferWriter{T}.Advance(int)"/> with the length of the source buffer.
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="source">The buffer to copy in.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Write<TWriter, T>(this ref TWriter writer, scoped ReadOnlySpan<T> source) where TWriter : struct, IBufferWriter<T>, allows ref struct
    {
        var span = writer.GetSpan();
        if (span.Length < source.Length)
        {
            WriteChunked(ref writer, source);
            return;
        }

        source.CopyTo(span);
        writer.Advance(source.Length);

        static void WriteChunked(ref TWriter writer, scoped ReadOnlySpan<T> source)
        {
            while (source.Length > 0)
            {
                var span = writer.GetSpan(sizeHint: 1);
                var writable = Math.Min(source.Length, span.Length);
                source.Slice(0, writable).CopyTo(span);
                source = source.Slice(writable);
                writer.Advance(writable);
            }
        }
    }
}
