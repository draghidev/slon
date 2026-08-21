using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;

namespace Slon.Benchmark;

/// <summary>
/// Isolates the cost of each primitive used on the Pipeline hot path.
/// </summary>
[IterationCount(15)]
[MemoryDiagnoser]
public class PrimitiveBenchmarks
{
    Lock _lock = null!;
    object _item = null!;
    CancellationTokenSource _linkedCts = null!;
    CancellationToken _linkedToken;
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _slonMrvtsc;
    System.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _bclMrvtsc;
    int _intField;
    bool _boolField;

    [GlobalSetup]
    public void Setup()
    {
        _lock = new Lock();
        _item = new object();
        _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        _linkedToken = _linkedCts.Token;
        _slonMrvtsc = new();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _linkedCts.Dispose();
    }

    /// <summary>Lock acquire + release, uncontended.</summary>
    [Benchmark]
    public void LockRoundTrip()
    {
        lock (_lock) { }
    }

    /// <summary>Two uncontended lock round-trips (Enqueue + RetireItem).</summary>
    [Benchmark]
    public void LockRoundTripX2()
    {
        lock (_lock) { }
        lock (_lock) { }
    }

    /// <summary>Three uncontended lock round-trips (Enqueue + RetireItem + WaitForNext).</summary>
    [Benchmark]
    public void LockRoundTripX3()
    {
        lock (_lock) { }
        lock (_lock) { }
        lock (_lock) { }
    }

    /// <summary>Volatile.Read of an int field.</summary>
    [Benchmark]
    public int VolatileReadInt()
    {
        return Volatile.Read(ref _intField);
    }

    /// <summary>Interlocked.Exchange on a bool.</summary>
    [Benchmark]
    public bool InterlockedExchangeBool()
    {
        return Interlocked.Exchange(ref _boolField, true);
    }

    /// <summary>Interlocked.Exchange on an int.</summary>
    [Benchmark]
    public int InterlockedExchangeInt()
    {
        return Interlocked.Exchange(ref _intField, 1);
    }

    /// <summary>CancellationToken.IsCancellationRequested on a linked CTS token.</summary>
    [Benchmark]
    public bool LinkedCtsIsCancellationRequested()
    {
        return _linkedToken.IsCancellationRequested;
    }

    /// <summary>CancellationToken.IsCancellationRequested on default token.</summary>
    [Benchmark]
    public bool DefaultCtsIsCancellationRequested()
    {
        return default(CancellationToken).IsCancellationRequested;
    }

    /// <summary>Slon ManualResetValueTaskSourceCore reset + set + get cycle.</summary>
    [Benchmark]
    public bool SlonMrvtscResetSetGet()
    {
        _slonMrvtsc.Reset();
        var version = _slonMrvtsc.Version;
        _slonMrvtsc.SetResult(true);
        return _slonMrvtsc.GetResult(version);
    }

    /// <summary>BCL ManualResetValueTaskSourceCore reset + set + get cycle.</summary>
    [Benchmark]
    public bool BclMrvtscResetSetGet()
    {
        _bclMrvtsc.Reset();
        var version = _bclMrvtsc.Version;
        _bclMrvtsc.SetResult(true);
        return _bclMrvtsc.GetResult(version);
    }

    /// <summary>ValueTask completion check for a synchronously completed ValueTask.</summary>
    [Benchmark]
    public bool CompletedValueTaskCheck()
    {
        var vt = ValueTask.CompletedTask;
        return vt.IsCompletedSuccessfully;
    }

    /// <summary>Await a synchronously completed ValueTask (measures async state machine overhead).</summary>
    [Benchmark]
    public async ValueTask AwaitCompletedValueTask()
    {
        await ValueTask.CompletedTask;
    }

    /// <summary>Await a synchronously completed ValueTask{T} (measures async state machine overhead).</summary>
    [Benchmark]
    public async ValueTask<bool> AwaitCompletedValueTaskT()
    {
        return await new ValueTask<bool>(true);
    }

    /// <summary>Check + skip await for synchronously completed ValueTask (our optimization).</summary>
    [Benchmark]
    public ValueTask SkipAwaitCompletedValueTask()
    {
        var vt = ValueTask.CompletedTask;
        if (!vt.IsCompletedSuccessfully)
            return AwaitSlow(vt);
        return default;

        static async ValueTask AwaitSlow(ValueTask vt) => await vt.ConfigureAwait(false);
    }

    /// <summary>Raw int spinlock acquire + release (our implementation).</summary>
    [Benchmark]
    public void RawSpinLockRoundTrip()
    {
        while (Interlocked.Exchange(ref _rawSpinLock, 1) != 0) { }
        Volatile.Write(ref _rawSpinLock, 0);
    }

    /// <summary>BCL SpinLock acquire + release (no thread tracking).</summary>
    [Benchmark]
    public void BclSpinLockRoundTrip()
    {
        var taken = false;
        _bclSpinLock.Enter(ref taken);
        _bclSpinLock.Exit(false);
    }

    int _rawSpinLock;
    SpinLock _bclSpinLock = new(false);

    /// <summary>Unbounded channel write + read round-trip.</summary>
    [Benchmark]
    public void ChannelWriteRead()
    {
        _channel.Writer.TryWrite(true);
        _channel.Reader.TryRead(out _);
    }

    /// <summary>Bounded(1) channel write + read round-trip.</summary>
    [Benchmark]
    public void BoundedChannelWriteRead()
    {
        _boundedChannel.Writer.TryWrite(true);
        _boundedChannel.Reader.TryRead(out _);
    }

    readonly System.Threading.Channels.Channel<bool> _channel = System.Threading.Channels.Channel.CreateUnbounded<bool>(new System.Threading.Channels.UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    readonly System.Threading.Channels.Channel<bool> _boundedChannel = System.Threading.Channels.Channel.CreateBounded<bool>(new System.Threading.Channels.BoundedChannelOptions(1) { SingleReader = true, SingleWriter = true });

    /// <summary>3-ref struct copy, individual write barriers, no GC poll.</summary>
    [Benchmark]
    public void ThreeRefStructCopy() => _threeRefDst = _threeRefSrc;

    /// <summary>4-ref struct copy, triggers BulkMoveWithWriteBarrier (with GC poll).</summary>
    [Benchmark]
    public void FourRefStructCopy() => _fourRefDst = _fourRefSrc;

    /// <summary>5-ref struct copy, triggers BulkMoveWithWriteBarrier (with GC poll).</summary>
    [Benchmark]
    public void FiveRefStructCopy() => _fiveRefDst = _fiveRefSrc;

    ThreeRefStruct _threeRefSrc = new() { a = new object(), b = new object(), c = new object() };
    ThreeRefStruct _threeRefDst;
    FourRefStruct _fourRefSrc = new() { a = new object(), b = new object(), c = new object(), d = new object() };
    FourRefStruct _fourRefDst;
    FiveRefStruct _fiveRefSrc = new() { a = new object(), b = new object(), c = new object(), d = new object(), e = new object() };
    FiveRefStruct _fiveRefDst;

    /// <summary>4-ref struct copy via BulkMoveWithWriteBarrierInternal directly (no GC poll).</summary>
    [Benchmark]
    public void FourRefStructCopyNoPoll()
    {
        BulkMoveWithWriteBarrierInternal(null,
            ref Unsafe.As<FourRefStruct, byte>(ref _fourRefDst),
            ref Unsafe.As<FourRefStruct, byte>(ref _fourRefSrc),
            (nuint)Unsafe.SizeOf<FourRefStruct>());
    }

    struct ThreeRefStruct { public object? a, b, c; }
    struct FourRefStruct { public object? a, b, c, d; }
    struct FiveRefStruct { public object? a, b, c, d, e; }

    [UnsafeAccessor(UnsafeAccessorKind.StaticMethod, Name = "BulkMoveWithWriteBarrierInternal")]
    static extern void BulkMoveWithWriteBarrierInternal(
        [UnsafeAccessorType("System.Buffer, System.Private.CoreLib")] object? bufferType,
        ref byte destination, ref byte source, nuint byteCount);
}
