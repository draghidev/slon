using System.Collections.Concurrent;
using System.Diagnostics;
using Slon.Pg.Protocol;

namespace Slon.Tests;

[TestClass]
public sealed class AdoTracingTests
{
    [TestMethod]
    public async Task AdoExecution_EmitsLogicalDatabaseActivities()
    {
        using var listener = new RecordingActivityListener();
        await using var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            Name = $"trace-test-{Guid.NewGuid():N}"
        });
        using var parent = new Activity($"trace-test-parent-{Guid.NewGuid():N}").Start();

        await using (var batch = dataSource.CreateBatch())
        {
            batch.BatchCommands.Add(new SlonBatchCommand { CommandText = "select 1" });
            batch.BatchCommands.Add(new SlonBatchCommand { CommandText = "select 2" });
            await batch.ExecuteNonQueryAsync();
        }

        await using (var command = dataSource.CreateCommand("select * from slon_missing_trace_table"))
            await Assert.ThrowsExactlyAsync<PgErrorException>(() => command.ExecuteNonQueryAsync());

        var activities = listener.Stopped.Where(activity => activity.ParentSpanId == parent.SpanId).ToArray();
        Assert.AreEqual(2, activities.Length);

        var batchActivity = activities[0];
        Assert.AreEqual(ActivityKind.Client, batchActivity.Kind);
        Assert.AreEqual("postgresql", batchActivity.GetTagItem("db.system.name"));
        Assert.AreEqual(dataSource.Database, batchActivity.GetTagItem("db.namespace"));
        Assert.AreEqual($"BATCH {dataSource.Database}", batchActivity.DisplayName);
        Assert.AreEqual("BATCH", batchActivity.GetTagItem("db.operation.name"));
        Assert.AreEqual(2, batchActivity.GetTagItem("db.operation.batch.size"));
        Assert.AreEqual(ActivityStatusCode.Unset, batchActivity.Status);

        var failedActivity = activities[1];
        Assert.AreEqual(dataSource.Database, failedActivity.DisplayName);
        Assert.IsNull(failedActivity.GetTagItem("db.operation.name"));
        Assert.AreEqual(ActivityStatusCode.Error, failedActivity.Status);
        Assert.AreEqual("42P01", failedActivity.GetTagItem("db.response.status_code"));
        Assert.AreEqual("42P01", failedActivity.GetTagItem("error.type"));
    }

    sealed class RecordingActivityListener : IDisposable
    {
        readonly ActivityListener _listener;
        public ConcurrentQueue<Activity> Stopped { get; } = new();

        public RecordingActivityListener()
        {
            _listener = new()
            {
                ShouldListenTo = static source => source.Name == "Slon",
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => Stopped.Enqueue(activity)
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public void Dispose() => _listener.Dispose();
    }
}
