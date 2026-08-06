using System.Diagnostics;

namespace Slon.Tests;

// A bounded parallel lane for tests that create private PostgreSQL connections. Shared-pool and
// in-memory tests remain fully parallel; private-connection tests retain their own internal
// concurrency without collectively producing an unbounded connection spike.
public abstract class ConnectionCreatingTest
{
    static readonly SemaphoreSlim Gate = new(initialCount: 4);
    static int _holders;
    IDisposable? _lease;

    public TestContext TestContext { get; set; } = null!;

    [TestInitialize]
    public async Task EnterConnectionCreatingLane()
    {
        var started = Stopwatch.GetTimestamp();
        if (!await Gate.WaitAsync(TestTimeout.Hang))
            throw new TimeoutException(
                $"Timed out waiting for the private-connection test lane " +
                $"(holders={Volatile.Read(ref _holders)}, capacity=4).");

        Interlocked.Increment(ref _holders);
        _lease = new Lease();
        var elapsed = Stopwatch.GetElapsedTime(started);
        if (elapsed >= TimeSpan.FromMilliseconds(1))
            TestContext.WriteLine($"Private-connection lane wait: {elapsed.TotalMilliseconds:F1} ms");
    }

    [TestCleanup]
    public void ExitConnectionCreatingLane()
        => Interlocked.Exchange(ref _lease, null)?.Dispose();

    sealed class Lease : IDisposable
    {
        int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            Interlocked.Decrement(ref _holders);
            Gate.Release();
        }
    }
}
