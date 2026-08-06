using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Slon;

readonly record struct PoolMetricsSnapshot(int Open, int Idle, int Max, int Waiters);

interface IPoolMetricsSource
{
    PoolMetricsSnapshot GetMetricsSnapshot();
}

sealed class PoolMetricsReporter : IDisposable
{
    internal readonly IPoolMetricsSource Source;
    internal readonly KeyValuePair<string, object?> PoolNameTag;
    readonly TagList _immediateAdmissionTags;
    readonly TagList _waitedAdmissionTags;
    int _disposed;

    internal PoolMetricsReporter(IPoolMetricsSource source, string poolName)
    {
        Source = source;
        PoolNameTag = new("db.client.connection.pool.name", poolName);
        _immediateAdmissionTags = new() { PoolNameTag, { "slon.pool.admission.type", "immediate" } };
        _waitedAdmissionTags = new() { PoolNameTag, { "slon.pool.admission.type", "waited" } };
    }

    public bool AdmissionsEnabled => SlonMetrics.AdmissionsEnabled;
    public bool AdmissionTimeoutsEnabled => SlonMetrics.AdmissionTimeoutsEnabled;

    public void ReportAdmission(bool waited)
        => SlonMetrics.ReportAdmission(waited ? _waitedAdmissionTags : _immediateAdmissionTags);
    public void ReportAdmissionTimeout() => SlonMetrics.ReportAdmissionTimeout(PoolNameTag);

    public long StartConnectionCreate() => SlonMetrics.StartConnectionCreate();
    public void ReportConnectionCreated(long started)
        => SlonMetrics.ReportConnectionCreated(started, PoolNameTag);
    public void ReportConnectionCreateFailed()
        => SlonMetrics.ReportConnectionCreateFailed(PoolNameTag);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            SlonMetrics.Unregister(this);
    }
}

static class SlonMetrics
{
    const string Version = "0.1.0";
    static readonly Meter Meter = new("Slon", Version);
    static readonly InstrumentAdvice<double> ShortHistogramAdvice = new()
    {
        HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10]
    };

    static readonly Counter<long> Admissions = Meter.CreateCounter<long>(
        "slon.pool.admissions", unit: "{admission}",
        description: "The number of successful pool admissions.");
    static readonly Counter<long> AdmissionTimeouts = Meter.CreateCounter<long>(
        "db.client.connection.timeouts", unit: "{timeout}",
        description: "The number of connection-pool admission timeouts.");
    static readonly Histogram<double> ConnectionCreateDuration = Meter.CreateHistogram(
        "db.client.connection.create_time", unit: "s",
        description: "The time required to create a physical connection.",
        advice: ShortHistogramAdvice);
    static readonly Counter<long> ConnectionCreateFailures = Meter.CreateCounter<long>(
        "slon.pool.connection.create.failures", unit: "{failure}",
        description: "The number of physical connection creations that failed.");

    static PoolMetricsReporter[] _reporters = [];
    static readonly Lock ReportersLock = new();

    static SlonMetrics()
    {
        Meter.CreateObservableUpDownCounter("db.client.connection.count", ObserveConnectionCounts,
            unit: "{connection}", description: "The number of connections in each pool state.");
        Meter.CreateObservableUpDownCounter("db.client.connection.max", ObserveMaxConnections,
            unit: "{connection}", description: "The configured maximum number of open connections.");
        Meter.CreateObservableUpDownCounter("db.client.connection.pending_requests", ObservePendingRequests,
            unit: "{request}", description: "The number of requests waiting for pool admission.");
    }

    public static PoolMetricsReporter Register(IPoolMetricsSource source, string poolName)
    {
        var reporter = new PoolMetricsReporter(source, poolName);
        lock (ReportersLock)
        {
            var current = _reporters;
            var next = new PoolMetricsReporter[current.Length + 1];
            current.CopyTo(next, 0);
            next[^1] = reporter;
            Volatile.Write(ref _reporters, next);
        }
        return reporter;
    }

    internal static bool AdmissionsEnabled => Admissions.Enabled;
    internal static bool AdmissionTimeoutsEnabled => AdmissionTimeouts.Enabled;
    internal static void ReportAdmission(in TagList tags) => Admissions.Add(1, tags);
    internal static void ReportAdmissionTimeout(KeyValuePair<string, object?> poolNameTag)
        => AdmissionTimeouts.Add(1, poolNameTag);

    internal static long StartConnectionCreate()
        => ConnectionCreateDuration.Enabled ? Stopwatch.GetTimestamp() : 0;
    internal static void ReportConnectionCreated(long started, KeyValuePair<string, object?> poolNameTag)
    {
        if (started != 0)
            ConnectionCreateDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, poolNameTag);
    }
    internal static void ReportConnectionCreateFailed(KeyValuePair<string, object?> poolNameTag)
    {
        if (ConnectionCreateFailures.Enabled)
            ConnectionCreateFailures.Add(1, poolNameTag);
    }

    static IEnumerable<Measurement<int>> ObserveConnectionCounts()
    {
        var reporters = Volatile.Read(ref _reporters);
        var measurements = new Measurement<int>[reporters.Length * 2];
        for (var i = 0; i < reporters.Length; i++)
        {
            var reporter = reporters[i];
            var snapshot = reporter.Source.GetMetricsSnapshot();
            var offset = i * 2;
            measurements[offset] = new(snapshot.Idle, reporter.PoolNameTag,
                new("db.client.connection.state", "idle"));
            measurements[offset + 1] = new(snapshot.Open - snapshot.Idle, reporter.PoolNameTag,
                new("db.client.connection.state", "used"));
        }
        return measurements;
    }

    static IEnumerable<Measurement<int>> ObserveMaxConnections()
    {
        var reporters = Volatile.Read(ref _reporters);
        var measurements = new Measurement<int>[reporters.Length];
        for (var i = 0; i < reporters.Length; i++)
        {
            var reporter = reporters[i];
            measurements[i] = new(reporter.Source.GetMetricsSnapshot().Max, reporter.PoolNameTag);
        }
        return measurements;
    }

    static IEnumerable<Measurement<int>> ObservePendingRequests()
    {
        var reporters = Volatile.Read(ref _reporters);
        var measurements = new Measurement<int>[reporters.Length];
        for (var i = 0; i < reporters.Length; i++)
        {
            var reporter = reporters[i];
            measurements[i] = new(reporter.Source.GetMetricsSnapshot().Waiters, reporter.PoolNameTag);
        }
        return measurements;
    }

    internal static void Unregister(PoolMetricsReporter reporter)
    {
        lock (ReportersLock)
        {
            var current = _reporters;
            var index = Array.IndexOf(current, reporter);
            if (index < 0)
                return;
            if (current.Length == 1)
            {
                Volatile.Write(ref _reporters, []);
                return;
            }

            var next = new PoolMetricsReporter[current.Length - 1];
            if (index != 0)
                Array.Copy(current, 0, next, 0, index);
            if (index != next.Length)
                Array.Copy(current, index + 1, next, index, next.Length - index);
            Volatile.Write(ref _reporters, next);
        }
    }
}
