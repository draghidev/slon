using System.Collections.Concurrent;
using Microsoft.Testing.Platform.Builder;
using Microsoft.Testing.Platform.Extensions;
using Microsoft.Testing.Platform.Extensions.Messages;

namespace Slon.Tests;

// Keeps active test names in memory so an exception on detached test-owned work can still be tied to
// the tests which were running. Successful processes write nothing.
static class LastChanceTestLedgerHook
{
    public static void AddExtensions(ITestApplicationBuilder builder, string[] args)
        => builder.TestHost.AddDataConsumer(static _ => LastChanceTestLedger.Instance);
}

sealed class LastChanceTestLedger : IDataConsumer
{
    readonly ConcurrentDictionary<string, string> _active = new();
    int _writingFailure;

    LastChanceTestLedger()
        => AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;

    public static LastChanceTestLedger Instance { get; } = new();
    public Type[] DataTypesConsumed { get; } = [typeof(TestNodeUpdateMessage)];
    public string Uid => nameof(LastChanceTestLedger);
    public string Version => "1.0.0";
    public string DisplayName => "Slon last-chance test ledger";
    public string Description => "Records active tests only when an unhandled exception terminates the process.";
    public Task<bool> IsEnabledAsync() => Task.FromResult(true);

    public Task ConsumeAsync(IDataProducer dataProducer, IData value, CancellationToken cancellationToken)
    {
        if (value is not TestNodeUpdateMessage update)
            return Task.CompletedTask;

        var node = update.TestNode;
        var key = node.Uid.Value;
        if (node.Properties.Any<InProgressTestNodeStateProperty>())
            _active[key] = node.DisplayName;
        else if (node.Properties.SingleOrDefault<TestNodeStateProperty>() is not null)
            _active.TryRemove(key, out _);
        return Task.CompletedTask;
    }

    void OnUnhandledException(object sender, UnhandledExceptionEventArgs args)
    {
        if (Interlocked.Exchange(ref _writingFailure, 1) != 0)
            return;

        try
        {
            var configuredPath = Environment.GetEnvironmentVariable("SLON_TEST_LAST_CHANCE_PATH");
            var path = string.IsNullOrWhiteSpace(configuredPath)
                ? Path.Combine(Path.GetTempPath(), $"slon-tests-last-chance-{Environment.ProcessId}.log")
                : configuredPath;
            using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
            using var writer = new StreamWriter(stream);
            writer.WriteLine($"utc={DateTimeOffset.UtcNow:O}");
            writer.WriteLine($"pid={Environment.ProcessId}");
            writer.WriteLine($"terminating={args.IsTerminating}");
            writer.WriteLine("active-tests:");
            foreach (var test in _active.OrderBy(static pair => pair.Value, StringComparer.Ordinal))
                writer.WriteLine($"  {test.Value} [{test.Key}]");
            writer.WriteLine("exception:");
            writer.WriteLine(args.ExceptionObject);
            writer.Flush();
            stream.Flush(flushToDisk: true);
        }
        catch
        {
            // Last-chance diagnostics must never replace or delay the terminating exception.
        }
    }
}
