using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Time.Testing;
using Slon.Pooling;

namespace Slon.Tests;

[TestClass]
public sealed class PoolMetricsTests
{
    [TestMethod]
    public async Task PoolMetrics_ReportAdmissionPressureCapacityAndCreation()
    {
        using var listener = new RecordingMeterListener();
        var poolName = $"metrics-test-{Guid.NewGuid():N}";
        var time = new FakeTimeProvider();
        var factory = new MetricsConnectionFactory();
        var pool = new ConnectionPool<MetricsConnection>(factory, new()
        {
            MaxConnections = 1,
            MetricsName = poolName,
            TimeProvider = time
        });

        var first = await pool.GetAsync(static (candidate, _) => candidate.Connection.TryMakeBusy(),
            (object?)null, default);
        var timeout = pool.GetAsync(static (candidate, _) => candidate.Connection.TryMakeBusy(),
            (object?)null, TimeSpan.FromSeconds(1)).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1);

        listener.RecordObservableInstruments();
        Assert.AreEqual(0, listener.Last("db.client.connection.count", poolName,
            "db.client.connection.state", "idle"));
        Assert.AreEqual(1, listener.Last("db.client.connection.count", poolName,
            "db.client.connection.state", "used"));
        Assert.AreEqual(1, listener.Last("db.client.connection.max", poolName));
        Assert.AreEqual(1, listener.Last("db.client.connection.pending_requests", poolName));

        time.Advance(TimeSpan.FromSeconds(1));
        await Assert.ThrowsExactlyAsync<TimeoutException>(() => timeout);

        var waited = pool.GetAsync(static (candidate, _) => candidate.Connection.TryMakeBusy(),
            (object?)null, default).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1);
        first.MakeIdle();
        await waited;

        Assert.AreEqual(1, listener.Sum("slon.pool.admissions", poolName,
            "slon.pool.admission.type", "immediate"));
        Assert.AreEqual(1, listener.Sum("slon.pool.admissions", poolName,
            "slon.pool.admission.type", "waited"));
        Assert.AreEqual(1, listener.Sum("db.client.connection.timeouts", poolName));
        Assert.AreEqual(1, listener.Count("db.client.connection.create_time", poolName));

        await using (var failingPool = new ConnectionPool<MetricsConnection>(new FailingMetricsConnectionFactory(), new()
        {
            MaxConnections = 1,
            MetricsName = poolName
        }))
        {
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => failingPool.GetUnqualifiedAsync(default).AsTask());
        }
        Assert.AreEqual(1, listener.Sum("slon.pool.connection.create.failures", poolName));

        await pool.DisposeAsync();
        listener.Clear();
        listener.RecordObservableInstruments();
        Assert.IsFalse(listener.HasMeasurement("db.client.connection.max", poolName));
    }

    static async Task WaitUntilAsync(Func<bool> condition)
    {
        for (var i = 0; i < 1000; i++)
        {
            if (condition())
                return;
            await Task.Yield();
        }
        Assert.Fail("Condition was not reached.");
    }

    sealed class MetricsConnectionFactory : IPoolConnectionFactory<MetricsConnection>
    {
        public MetricsConnection Create(ConnectionPoolContext<MetricsConnection> context, TimeSpan timeout = default)
            => new();

        public ValueTask<MetricsConnection> CreateAsync(ConnectionPoolContext<MetricsConnection> context,
            CancellationToken cancellationToken = default)
            => new(new MetricsConnection());
    }

    sealed class FailingMetricsConnectionFactory : IPoolConnectionFactory<MetricsConnection>
    {
        public MetricsConnection Create(ConnectionPoolContext<MetricsConnection> context, TimeSpan timeout = default)
            => throw new InvalidOperationException("creation failed");

        public ValueTask<MetricsConnection> CreateAsync(ConnectionPoolContext<MetricsConnection> context,
            CancellationToken cancellationToken = default)
            => ValueTask.FromException<MetricsConnection>(new InvalidOperationException("creation failed"));
    }

    sealed class MetricsConnection : IPoolConnection<MetricsConnection>
    {
        readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int _idle = 1;
        ConnectionPool<MetricsConnection>.Registration _poolRegistration;

        public bool IsIdle => Volatile.Read(ref _idle) != 0;
        public bool IsSchedulable => !_completion.Task.IsCompleted;
        public Task Completion => _completion.Task;
        public void Start(ConnectionPool<MetricsConnection>.Registration registration)
            => _poolRegistration = registration;
        public int CompareTo(MetricsConnection? other) => 0;
        public bool TryMakeBusy() => Interlocked.Exchange(ref _idle, 0) != 0;
        public void MakeIdle()
        {
            Volatile.Write(ref _idle, 1);
            _poolRegistration.SignalAvailability(isIdle: true);
        }
        public Task CompleteAsync(Exception? exception = null)
        {
            _completion.TrySetResult();
            return _completion.Task;
        }
    }

    sealed class RecordingMeterListener : IDisposable
    {
        readonly MeterListener _listener = new();
        readonly ConcurrentQueue<MeasurementRecord> _records = new();

        public RecordingMeterListener()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == "Slon")
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<int>(Record);
            _listener.SetMeasurementEventCallback<long>(Record);
            _listener.SetMeasurementEventCallback<double>(Record);
            _listener.Start();
        }

        public void RecordObservableInstruments() => _listener.RecordObservableInstruments();
        public long Last(string instrument, string poolName, string? tagName = null, object? tagValue = null)
            => Convert.ToInt64(Filter(instrument, poolName, tagName, tagValue).Last().Value);
        public int Count(string instrument, string poolName) => Filter(instrument, poolName).Count();
        public long Sum(string instrument, string poolName, string? tagName = null, object? tagValue = null)
            => Filter(instrument, poolName, tagName, tagValue)
                .Sum(record => Convert.ToInt64(record.Value));
        public bool HasMeasurement(string instrument, string poolName)
            => Filter(instrument, poolName).Any();
        public void Clear() => _records.Clear();

        IEnumerable<MeasurementRecord> Filter(string instrument, string poolName,
            string? tagName = null, object? tagValue = null)
            => _records.Where(record => record.Instrument == instrument &&
                record.Tags.Any(tag => tag.Key == "db.client.connection.pool.name" && Equals(tag.Value, poolName)) &&
                (tagName is null || record.Tags.Any(tag => tag.Key == tagName && Equals(tag.Value, tagValue))));

        void Record<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state) where T : struct
            => _records.Enqueue(new(instrument.Name, measurement, tags.ToArray()));

        public void Dispose() => _listener.Dispose();

        readonly record struct MeasurementRecord(string Instrument, object Value,
            KeyValuePair<string, object?>[] Tags);
    }
}
