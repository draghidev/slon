using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Slon.Pools;

static class Padded
{
    // It's 64 bytes for x64 but we can't easily load architecture specific IL, so it is what it is.
    const int CacheLineSize = 128;

    [StructLayout(LayoutKind.Explicit, Size = CacheLineSize)]
    public struct Object
    {

        [FieldOffset(0)]
        public object? Value;
    }

    [StructLayout(LayoutKind.Explicit, Size = CacheLineSize)]
    public struct Int
    {
        [FieldOffset(0)]
        public int Value;
    }
}

static class ProcessorIdHelper
{
    static readonly bool NativeSupport = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || Thread.GetCurrentProcessorId() != Thread.CurrentThread.ManagedThreadId;

    static int ProcessorIndex = -1;

    [ThreadStatic]
    static int ProcessorId;

    // Also guaranteed to start at 0
    public static int GetProcessorId()
    {
        if (NativeSupport)
            return Thread.GetCurrentProcessorId() % Environment.ProcessorCount;

        if (ProcessorId == default)
            ProcessorId = (Interlocked.Increment(ref ProcessorIndex) % Environment.ProcessorCount) + 1;

        return ProcessorId - 1;
    }
}

readonly struct StripedRef<T>(int count, int length)
{
    readonly Padded.Object[] _items = new Padded.Object[count];

    public int Length => count;
    public int LengthPerStripe => length;

    public int CurrentIndex => ProcessorIdHelper.GetProcessorId() % count;
    public ref T[] Current
    {
        get
        {
            ref var current = ref this[CurrentIndex];
            return ref current!;
        }
    }

    public ref T[] this[int index]
    {
        get
        {
            ref var core = ref _items[index];
            ref var v = ref Unsafe.As<object?, T[]?>(ref core.Value);
            v ??= new T[length];
            return ref v!;
        }
    }
}

readonly struct StripedInt(int count)
{
    readonly Padded.Int[] _items = new Padded.Int[count];

    public int Length => count;

    public int CurrentIndex => ProcessorIdHelper.GetProcessorId() % count;
    public ref int Current => ref this[CurrentIndex];

    public ref int this[int index]
    {
        get
        {
            ref var core = ref _items[index];
            return ref core.Value;
        }
    }
}
