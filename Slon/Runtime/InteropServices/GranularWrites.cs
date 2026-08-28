using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Slon.Runtime.InteropServices;

/// Field-granular stores for BCL memory structs. A full struct assignment re-stores every reference
/// field through a write barrier; on the per-message and per-row paths those references rarely
/// change, so only changed references are stored. The mirrors follow the BCL's sequential field
/// order and are verified by a test.
static class GranularWrites
{
    struct SequenceLayout
    {
        public object? StartObject;
        public object? EndObject;
        public int StartInteger;
        public int EndInteger;
    }

    struct MemoryLayout
    {
        public object? Object;
        public int Index;
        public int Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Write(ref ReadOnlySequence<byte> destination, in ReadOnlySequence<byte> value)
    {
        Debug.Assert(Unsafe.SizeOf<ReadOnlySequence<byte>>() == Unsafe.SizeOf<SequenceLayout>());
        ref var target = ref Unsafe.As<ReadOnlySequence<byte>, SequenceLayout>(ref destination);
        ref readonly var source = ref Unsafe.As<ReadOnlySequence<byte>, SequenceLayout>(ref Unsafe.AsRef(in value));
        if (!ReferenceEquals(target.StartObject, source.StartObject))
            target.StartObject = source.StartObject;
        if (!ReferenceEquals(target.EndObject, source.EndObject))
            target.EndObject = source.EndObject;
        target.StartInteger = source.StartInteger;
        target.EndInteger = source.EndInteger;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Write(ref ReadOnlyMemory<byte> destination, in ReadOnlyMemory<byte> value)
    {
        Debug.Assert(Unsafe.SizeOf<ReadOnlyMemory<byte>>() == Unsafe.SizeOf<MemoryLayout>());
        ref var target = ref Unsafe.As<ReadOnlyMemory<byte>, MemoryLayout>(ref destination);
        ref readonly var source = ref Unsafe.As<ReadOnlyMemory<byte>, MemoryLayout>(ref Unsafe.AsRef(in value));
        if (!ReferenceEquals(target.Object, source.Object))
            target.Object = source.Object;
        target.Index = source.Index;
        target.Length = source.Length;
    }

    // Layout probes for the verifying test: the mirrored reference fields must be the buffers the
    // public API reports.
    internal static (object? Start, object? End) SequenceObjects(in ReadOnlySequence<byte> value)
    {
        ref readonly var layout = ref Unsafe.As<ReadOnlySequence<byte>, SequenceLayout>(ref Unsafe.AsRef(in value));
        return (layout.StartObject, layout.EndObject);
    }

    internal static (object? Object, int Index, int Length) MemoryFields(in ReadOnlyMemory<byte> value)
    {
        ref readonly var layout = ref Unsafe.As<ReadOnlyMemory<byte>, MemoryLayout>(ref Unsafe.AsRef(in value));
        return (layout.Object, layout.Index, layout.Length);
    }
}
