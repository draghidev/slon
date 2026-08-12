using BenchmarkDotNet.Attributes;

namespace Slon.Benchmark;

// Isolates the slot-layout tradeoff in ConnectionPool. Hot is the optimistic steady-state
// case; Displaced rotates through enough separately allocated entries to exceed ordinary CPU
// caches and expose the dependent entry -> connection load.
[MemoryDiagnoser]
[WarmupCount(2)]
[IterationCount(5)]
public class ConnectionSlotLayoutBenchmark
{
    const int DisplacedBytes = 32 * 1024 * 1024;

    [Params(16, 200)]
    public int PoolSize;

    [Params(WorkingSet.Hot, WorkingSet.Displaced)]
    public WorkingSet CacheState;

    DirectSlot[][] _direct = null!;
    Entry?[][] _indirect = null!;
    CombinedEntry?[][] _combined = null!;
    int _directIndex;
    int _indirectIndex;
    int _combinedIndex;

    [GlobalSetup]
    public void Setup()
    {
        // Approximate the two heap objects plus slot storage. Exact object size is less
        // important than keeping the displaced case comfortably beyond private caches.
        var entryCount = CacheState is WorkingSet.Hot ? PoolSize : Math.Max(PoolSize, DisplacedBytes / 80);
        var setCount = Math.Max(1, entryCount / PoolSize);
        _direct = new DirectSlot[setCount][];
        _indirect = new Entry?[setCount][];
        _combined = new CombinedEntry?[setCount][];
        for (var set = 0; set < setCount; set++)
        {
            var direct = _direct[set] = new DirectSlot[PoolSize];
            var indirect = _indirect[set] = new Entry?[PoolSize];
            var combined = _combined[set] = new CombinedEntry?[PoolSize];
            for (var i = 0; i < PoolSize; i++)
            {
                var directConnection = new Connection(i + 1);
                direct[i] = new(directConnection, new(directConnection));

                var indirectConnection = new Connection(i + 1);
                indirect[i] = new(indirectConnection);

                var combinedConnection = new Connection(i + 1);
                combined[i] = new(combinedConnection);
            }
        }
    }

    [Benchmark(Baseline = true)]
    public int DirectConnection()
    {
        var slots = _direct[Next(ref _directIndex, _direct.Length)];
        var sum = 0;
        for (var i = 0; i < slots.Length; i++)
        {
            ref var slot = ref slots[i];
            var item = Volatile.Read(ref slot.Item);
            if (item is not Connection connection)
                continue;

            sum += connection.Load;
        }
        return sum;
    }

    [Benchmark]
    public int CompletedFuture()
    {
        var slots = _indirect[Next(ref _indirectIndex, _indirect.Length)];
        var sum = 0;
        for (var i = 0; i < slots.Length; i++)
        {
            var entry = Volatile.Read(ref slots[i]);
            if (entry is null || !entry.IsCompleted || entry.Result is not { } connection)
                continue;
            sum += connection.Load;
        }
        return sum;
    }

    [Benchmark]
    public int CombinedPublicationFuture()
    {
        var slots = _combined[Next(ref _combinedIndex, _combined.Length)];
        var sum = 0;
        for (var i = 0; i < slots.Length; i++)
        {
            var entry = Volatile.Read(ref slots[i]);
            if (entry?.Result is not { } connection)
                continue;
            sum += connection.Load;
        }
        return sum;
    }

    static int Next(ref int index, int length)
    {
        var value = index++;
        return (value & int.MaxValue) % length;
    }

    public enum WorkingSet : byte
    {
        Hot,
        Displaced
    }

    struct DirectSlot(Connection connection, Entry registration)
    {
        public object? Item = connection;
        public Entry? Registration = registration;
    }

    sealed class Entry(Connection connection)
    {
        Connection? _connection = connection;
        int _published = 1;
        long _idleTokenTenure;

        public bool IsCompleted => Volatile.Read(ref _published) != 0;
        public Connection? Result => Volatile.Read(ref _connection);

        // Keep approximately the state and layout of the real completed future.
        public ref long IdleTokenTenure => ref _idleTokenTenure;
    }

    sealed class Connection(int load)
    {
        int _load = load;
        public int Load => Volatile.Read(ref _load);
    }

    sealed class CombinedEntry(Connection connection)
    {
        object? _result = connection;
        long _idleTokenTenure;

        public Connection? Result => Volatile.Read(ref _result) as Connection;
        public ref long IdleTokenTenure => ref _idleTokenTenure;
    }
}
