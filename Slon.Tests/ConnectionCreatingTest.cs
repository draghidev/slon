using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Slon.Tests;

// A bounded parallel lane for tests that create private PostgreSQL connections. Shared-pool and
// in-memory tests remain fully parallel; private-connection tests retain their own internal
// concurrency without collectively producing an unbounded connection spike.
public abstract class ConnectionCreatingTest
{
    IDisposable? _lease;

    public TestContext TestContext { get; set; } = null!;

    [TestInitialize]
    public async Task EnterConnectionCreatingLane()
    {
        var started = Stopwatch.GetTimestamp();
        _lease = await ConnectionCreatingLane.AcquireAsync();
        var elapsed = Stopwatch.GetElapsedTime(started);
        if (elapsed >= TimeSpan.FromMilliseconds(1))
            TestContext.WriteLine($"Private-connection lane wait: {elapsed.TotalMilliseconds:F1} ms");
    }

    [TestCleanup]
    public void ExitConnectionCreatingLane()
        => Interlocked.Exchange(ref _lease, null)?.Dispose();

}

static class ConnectionCreatingLane
{
    const int Capacity = 6;
    static readonly SemaphoreSlim Permits = new(Capacity);
    static readonly SemaphoreSlim Acquisition = new(1);

    internal static async ValueTask<IDisposable> AcquireAsync(int count = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, Capacity);

        await Acquisition.WaitAsync().ConfigureAwait(false);
        try
        {
            for (var i = 0; i < count; i++)
                await Permits.WaitAsync().ConfigureAwait(false);
        }
        finally
        {
            Acquisition.Release();
        }
        return new Lease(count);
    }

    sealed class Lease(int count) : IDisposable
    {
        int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            Permits.Release(count);
        }
    }
}

[AttributeUsage(AttributeTargets.Method)]
sealed class ConnectionCreatingTestMethodAttribute : TestMethodAttribute
{
    readonly int _connections;

    public ConnectionCreatingTestMethodAttribute(int connections = 1,
        [CallerFilePath] string filePath = "", [CallerLineNumber] int lineNumber = 0)
        : base(filePath, lineNumber)
        => _connections = connections;

    public override async Task<TestResult[]> ExecuteAsync(ITestMethod testMethod)
    {
        using var lease = await ConnectionCreatingLane.AcquireAsync(_connections).ConfigureAwait(false);
        return await base.ExecuteAsync(testMethod).ConfigureAwait(false);
    }
}
