// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Slon.Buffers;

// Peeking (non advancing variants), cheaper than rewinding afterwards.
static class SequenceReaderExtensions
{
    /// <summary>
    /// Try to peek the given type out of the buffer if possible. Warning: this is dangerous to use with arbitrary
    /// structs- see remarks for full details.
    /// </summary>
    /// <remarks>
    /// IMPORTANT: The peek is a straight copy of bits. If a struct depends on specific state of it's members to
    /// behave correctly this can lead to exceptions, etc. If reading endian specific integers, use the explicit
    /// overloads such as <see cref="TryPeekLittleEndian(ref SequenceReader{byte}, out short)"/>
    /// </remarks>
    /// <returns>
    /// True if successful. <paramref name="value"/> will be default if failed (due to lack of space).
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static unsafe bool TryPeek<T>(ref this SequenceReader<byte> reader, out T value) where T : unmanaged
    {
        ReadOnlySpan<byte> span = reader.UnreadSpan;
        if (span.Length < sizeof(T))
            return TryPeekMultisegment(ref reader, out value);

        value = Unsafe.ReadUnaligned<T>(ref MemoryMarshal.GetReference(span));
        return true;
    }

    static unsafe bool TryPeekMultisegment<T>(ref SequenceReader<byte> reader, out T value) where T : unmanaged
    {
        Debug.Assert(reader.UnreadSpan.Length < sizeof(T));

        // Not enough data in the current segment, try to peek for the data we need.
        T buffer = default;
        Span<byte> tempSpan = new Span<byte>(&buffer, sizeof(T));

        if (!reader.TryCopyTo(tempSpan))
        {
            value = default;
            return false;
        }

        value = Unsafe.ReadUnaligned<T>(ref MemoryMarshal.GetReference(tempSpan));
        return true;
    }

    /// <summary>
    /// Peeks an <see cref="short"/> as little endian.
    /// </summary>
    /// <returns>False if there wasn't enough data for an <see cref="short"/>.</returns>
    public static bool TryPeekLittleEndian(ref this SequenceReader<byte> reader, out short value)
    {
        if (BitConverter.IsLittleEndian)
        {
            return reader.TryPeek(out value);
        }

        return TryPeekReverseEndianness(ref reader, out value);
    }

    /// <summary>
    /// Peeks an <see cref="short"/> as big endian.
    /// </summary>
    /// <returns>False if there wasn't enough data for an <see cref="short"/>.</returns>
    public static bool TryPeekBigEndian(ref this SequenceReader<byte> reader, out short value)
    {
        if (!BitConverter.IsLittleEndian)
        {
            return reader.TryPeek(out value);
        }

        return TryPeekReverseEndianness(ref reader, out value);
    }

    static bool TryPeekReverseEndianness(ref SequenceReader<byte> reader, out short value)
    {
        if (reader.TryPeek(out value))
        {
            value = BinaryPrimitives.ReverseEndianness(value);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Peeks an <see cref="int"/> as little endian.
    /// </summary>
    /// <returns>False if there wasn't enough data for an <see cref="int"/>.</returns>
    public static bool TryPeekLittleEndian(ref this SequenceReader<byte> reader, out int value)
    {
        if (BitConverter.IsLittleEndian)
        {
            return reader.TryPeek(out value);
        }

        return TryPeekReverseEndianness(ref reader, out value);
    }

    /// <summary>
    /// Peeks an <see cref="int"/> as big endian.
    /// </summary>
    /// <returns>False if there wasn't enough data for an <see cref="int"/>.</returns>
    public static bool TryPeekBigEndian(ref this SequenceReader<byte> reader, out int value)
    {
        if (!BitConverter.IsLittleEndian)
        {
            return reader.TryPeek(out value);
        }

        return TryPeekReverseEndianness(ref reader, out value);
    }

    static bool TryPeekReverseEndianness(ref SequenceReader<byte> reader, out int value)
    {
        if (reader.TryPeek(out value))
        {
            value = BinaryPrimitives.ReverseEndianness(value);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Peeks a <see cref="long"/> as little endian.
    /// </summary>
    /// <returns>False if there wasn't enough data for a <see cref="long"/>.</returns>
    public static bool TryPeekLittleEndian(ref this SequenceReader<byte> reader, out long value)
    {
        if (BitConverter.IsLittleEndian)
        {
            return reader.TryPeek(out value);
        }

        return TryPeekReverseEndianness(ref reader, out value);
    }

    /// <summary>
    /// Peeks a <see cref="long"/> as big endian.
    /// </summary>
    /// <returns>False if there wasn't enough data for a <see cref="long"/>.</returns>
    public static bool TryPeekBigEndian(ref this SequenceReader<byte> reader, out long value)
    {
        if (!BitConverter.IsLittleEndian)
        {
            return reader.TryPeek(out value);
        }

        return TryPeekReverseEndianness(ref reader, out value);
    }

    static bool TryPeekReverseEndianness(ref SequenceReader<byte> reader, out long value)
    {
        if (reader.TryPeek(out value))
        {
            value = BinaryPrimitives.ReverseEndianness(value);
            return true;
        }

        return false;
    }
}
