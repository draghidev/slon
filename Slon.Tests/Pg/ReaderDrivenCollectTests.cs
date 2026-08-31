using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Types;
using Slon.Text;

namespace Slon.Tests.Pg;

// One-await collection over the reader-driven flow: the caller's frame owns the decoder from
// activation to RFQ, every row is handed to a synchronous collector, and every terminal the flow
// can reach leaves the wire reusable and delivers the right outcome to the single await.
[TestClass]
public class ReaderDrivenCollectTests
{
    static async Task<CommandDescriptor> Prepare(PgClientProtocol protocol, string sql, EncodedCString name)
    {
        var results = protocol.Queue(new CommandFlow(async: true,
            Command.Create(sql, commandName: name) with { DescribeOnly = true })).GetAsyncEnumerator();
        CommandDescriptor descriptor = default;
        while (await results.MoveNextAsync())
            descriptor = results.Current.GetMetadata().ToPreparedDescriptor();
        await results.DisposeAsync();
        return descriptor;
    }

    static Task<PgClientProtocol> NewCancelableProtocolAsync()
        => PgTestPool.NewIsolatedAsync(options =>
            options.CancelSender = PgTestPool.CreateCancelSender(PgTestPool.NewOptions()));

    static ReaderDrivenCommandFlow QueuePrepared(PgClientProtocol protocol, in CommandDescriptor descriptor)
        => protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(descriptor)));

    static void CollectInt(object? state, Row row) => ((List<int>)state!).Add(row.GetValue<int>(0));

    static async Task<List<int>> CollectInts(ReaderDrivenCommandFlow flow, CancellationToken cancellationToken = default)
    {
        var values = new List<int>();
        await flow.CollectAsync(values, CollectInt, cancellationToken);
        return values;
    }

    static async Task<List<int>> EnumerateInts(ReaderDrivenCommandFlow flow)
    {
        var values = new List<int>();
        var results = flow.GetAsyncEnumerator();
        while (await results.MoveNextAsync())
        {
            var rows = results.Current.GetAsyncEnumerator();
            while (await rows.MoveNextAsync())
                values.Add(rows.Current.GetValue<int>(0));
            await rows.DisposeAsync();
        }
        await results.DisposeAsync();
        return values;
    }

    static async Task<int> ReadBackendPid(PgClientProtocol protocol)
    {
        var values = new List<int>();
        await protocol.Queue(new ReaderDrivenCommandFlow(Command.Create("select pg_backend_pid()")))
            .CollectAsync(values, CollectInt);
        return values[0];
    }

    [ConnectionCreatingTestMethod]
    [DataRow(0)]
    [DataRow(1)]
    [DataRow(5000)]
    public async Task Prepared_CollectsEveryRow(int rowCount)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, $"select generate_series(1, {rowCount})", $"collect_{rowCount}");

        var values = await CollectInts(QueuePrepared(protocol, descriptor));

        CollectionAssert.AreEqual(Enumerable.Range(1, rowCount).ToList(), values);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task ContiguousFieldMemory_RemainsValidAcrossExtendedBatches()
    {
        const int rowCount = 2000;
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol,
            $"select i, i::text || repeat('x', 96) from generate_series(1, {rowCount}) as i",
            "retained_result_memory");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();
        var values = new List<(int Id, ReadOnlyMemory<byte> Message)>(rowCount);

        try
        {
            Assert.IsTrue(await results.MoveNextAsync());
            var rows = results.Current.GetAsyncEnumerator();
            while (await rows.MoveNextAsync())
            {
                var reader = rows.Current.GetReader();
                values.Add((reader.Read<int>(), reader.ReadMemory()));
            }
            await rows.DisposeAsync();

            Assert.AreEqual(rowCount, values.Count);
            foreach (var (id, message) in values)
                Assert.AreEqual(id + new string('x', 96), Encoding.UTF8.GetString(message.Span));
        }
        finally
        {
            await results.DisposeAsync();
        }

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task ContiguousFieldMemory_BuffersStreamingRowsIntoTheRetainedBatch()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol,
            "select i, i::text || repeat('x', 20000) from generate_series(1, 3) as i",
            "retained_streaming_memory");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();
        var values = new List<ReadOnlyMemory<byte>>();

        try
        {
            Assert.IsTrue(await results.MoveNextAsync());
            var rows = results.Current.GetAsyncEnumerator();
            while (await rows.MoveNextAsync())
                values.Add(rows.Current.GetFieldMemory(1));
            await rows.DisposeAsync();

            Assert.AreEqual(3, values.Count);
            for (var i = 0; i < values.Count; i++)
                Assert.AreEqual((i + 1) + new string('x', 20000),
                    Encoding.UTF8.GetString(values[i].Span));
        }
        finally
        {
            await results.DisposeAsync();
        }

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task Unprepared_CollectsThroughTheGenericPrelude()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();

        var values = await CollectInts(protocol.Queue(
            new ReaderDrivenCommandFlow(Command.Create("select generate_series(1, 3)"))));

        CollectionAssert.AreEqual(new[] { 1, 2, 3 }, values);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task SharedOptions_PipelinedCollectorsCarryTheirOwnParameters()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var prepare = protocol.Queue(new CommandFlow(async: true,
            Command.Create("select $1::int",
                new ParameterTypeList(ImmutableArray.Create(Parameter.CreateNull(Oid.Unspecified))),
                "collect_shared") with { DescribeOnly = true, DescribeForPreparation = true, WithSync = true }));
        var prepared = prepare.GetAsyncEnumerator();
        Assert.IsTrue(await prepared.MoveNextAsync());
        var descriptor = prepared.Current.GetMetadata().ToPreparedDescriptor();
        await prepared.DisposeAsync();
        var options = new ReaderDrivenCommandOptions(Command.Create(descriptor));

        var first = protocol.Queue(new ReaderDrivenCommandFlow(options, Int4Parameter(7)));
        var second = protocol.Queue(new ReaderDrivenCommandFlow(options, Int4Parameter(11)));
        var third = protocol.Queue(new ReaderDrivenCommandFlow(options, Int4Parameter(13)));

        CollectionAssert.AreEqual(new[] { 7 }, await CollectInts(first));
        CollectionAssert.AreEqual(new[] { 11 }, await CollectInts(second));
        CollectionAssert.AreEqual(new[] { 13 }, await CollectInts(third));
        await PgTestPool.RunAsync(protocol, "select 1");

        static ParameterSource Int4Parameter(int value)
        {
            var bytes = new byte[sizeof(int)];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            return new(ImmutableArray.Create(Parameter.Create(bytes, (Oid)23u)));
        }
    }

    [ConnectionCreatingTestMethod]
    public async Task CommandError_IsDeliveredAndKeepsWire()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 1 / 0", "collect_error");

        await Assert.ThrowsExactlyAsync<PgErrorException>(
            () => CollectInts(QueuePrepared(protocol, descriptor)));

        var good = await Prepare(protocol, "select 42", "collect_error_successor");
        CollectionAssert.AreEqual(new[] { 42 }, await CollectInts(QueuePrepared(protocol, good)));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task PreCancelledToken_ReleasesCallerAndDrains()
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 1000)", "collect_precancel");
        var flow = QueuePrepared(protocol, descriptor);
        var token = new CancellationToken(canceled: true);

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => CollectInts(flow, token));
        Assert.AreEqual(token, exception.CancellationToken);
        await flow.GetAsyncEnumerator().DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task Cancellation_DuringInitialRead_DeliversAfterRfq()
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol, "select pg_sleep(30)", "collect_cancel_pending");
        using var cancellation = new CancellationTokenSource();

        var pending = CollectInts(QueuePrepared(protocol, descriptor), cancellation.Token);
        Assert.IsFalse(pending.IsCompleted);
        cancellation.CancelAfter(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => pending);

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task SameQueueAndConsumerToken_CancelsAndKeepsWire()
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(
            protocol, "select pg_sleep(30)", "collect_shared_cancellation");
        using var cancellation = new CancellationTokenSource();
        var flow = protocol.Queue(
            new ReaderDrivenCommandFlow(Command.Create(descriptor)),
            cancellation.Token);

        var pending = CollectInts(flow, cancellation.Token);
        cancellation.CancelAfter(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => pending);

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task Cancellation_AfterRows_DrainsThenDelivers()
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol,
            "select i from generate_series(1, 200000) as i, pg_sleep(case when i = 50 then 0.5 else 0 end)",
            "collect_cancel_rows");
        using var cancellation = new CancellationTokenSource();
        var seen = 0;

        var pending = QueuePrepared(protocol, descriptor).CollectAsync(cancellation,
            (state, _) =>
            {
                if (++seen == 10)
                    ((CancellationTokenSource)state!).Cancel();
            }, cancellation.Token).AsTask();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => pending);
        Assert.IsGreaterThanOrEqualTo(10, seen);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DataRow(1)]
    [DataRow(3)]
    public async Task CollectorException_IsDeliveredAfterDrainAndKeepsWire(int failingRow)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 5000)", $"collect_fault_{failingRow}");
        var seen = new List<int>();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await QueuePrepared(protocol, descriptor).CollectAsync(seen, (state, row) =>
            {
                var values = (List<int>)state!;
                values.Add(row.GetValue<int>(0));
                if (values.Count == failingRow)
                    throw new InvalidOperationException("collector");
            }));

        Assert.AreEqual("collector", exception.Message);
        Assert.AreEqual(failingRow, seen.Count, "no callback after the collector faulted");
        CollectionAssert.AreEqual(new[] { 42 },
            await CollectInts(protocol.Queue(new ReaderDrivenCommandFlow(Command.Create("select 42")))));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task GracefulClose_WhileCollecting_FaultsCollectorAndDrains()
    {
        var protocol = await PgTestPool.NewIsolatedAsync(options =>
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(20));
        var descriptor = await Prepare(protocol, "select pg_sleep(2), 1", "collect_graceful");

        var pending = CollectInts(QueuePrepared(protocol, descriptor));
        Assert.IsFalse(pending.IsCompleted);
        var completion = protocol.CompleteAsync();

        // The command completes on the wire; the close is delivered at the terminal.
        await Assert.ThrowsAsync<PgClientClosedException>(() => pending);
        await completion.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [ConnectionCreatingTestMethod(connections: 2)]
    public async Task BackendTermination_WhileCollecting_FaultsCollector()
    {
        await using var protocols = await PgTestPool.NewIsolatedProtocolsAsync(2);
        var killer = protocols.Items[1];
        await using var victim = await PgTestPool.NewIsolatedAsync();
        var pid = await ReadBackendPid(victim);
        var descriptor = await Prepare(victim, "select pg_sleep(10)", "collect_terminate");

        var pending = CollectInts(QueuePrepared(victim, descriptor));
        Assert.IsFalse(pending.IsCompleted);
        await PgTestPool.RunAsync(killer, $"select pg_terminate_backend({pid})");

        var exception = await Assert.ThrowsAsync<Exception>(() => pending);
        Assert.IsInstanceOfType<PgCollateralException>(exception);
        await victim.Completion;
    }

    [ConnectionCreatingTestMethod]
    public async Task Pipelined_Collectors_CompleteInFifoOrder()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 64)", "collect_fifo");
        var order = new List<int>();

        var flows = new ReaderDrivenCommandFlow[16];
        for (var i = 0; i < flows.Length; i++)
            flows[i] = QueuePrepared(protocol, descriptor);
        var pending = new Task[flows.Length];
        for (var i = 0; i < flows.Length; i++)
        {
            var index = i;
            pending[i] = flows[i].CollectAsync(order, (state, row) =>
            {
                if (row.GetValue<int>(0) == 1)
                    ((List<int>)state!).Add(index);
            }).AsTask();
        }
        await Task.WhenAll(pending);

        CollectionAssert.AreEqual(Enumerable.Range(0, flows.Length).ToList(), order);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task Collector_BesideOrdinaryFlows_AcrossIdleEdges()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 42", "collect_idle_edges");

        CollectionAssert.AreEqual(new[] { 42 }, await CollectInts(QueuePrepared(protocol, descriptor)));
        var ordinary = protocol.Queue(new CommandFlow(async: true, Command.Create("select 43"))).GetAsyncEnumerator();
        Assert.IsTrue(await ordinary.MoveNextAsync());
        var rows = ordinary.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        Assert.AreEqual(43, rows.Current.GetValue<int>(0));
        Assert.IsFalse(await rows.MoveNextAsync());
        await rows.DisposeAsync();
        Assert.IsFalse(await ordinary.MoveNextAsync());
        await ordinary.DisposeAsync();
        CollectionAssert.AreEqual(new[] { 42 }, await CollectInts(QueuePrepared(protocol, descriptor)));
        CollectionAssert.AreEqual(new[] { 42 }, await EnumerateInts(QueuePrepared(protocol, descriptor)));
        CollectionAssert.AreEqual(new[] { 42 }, await CollectInts(QueuePrepared(protocol, descriptor)));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task CollectAfterEnumeration_IsRejected()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 42", "collect_after_enumeration");
        var flow = QueuePrepared(protocol, descriptor);
        CollectionAssert.AreEqual(new[] { 42 }, await EnumerateInts(flow));

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => flow.CollectAsync(null, static (_, _) => { }).AsTask());
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task NoCallbackAfterCompletion_AndStateIsReleased()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 3)", "collect_release");
        var flow = QueuePrepared(protocol, descriptor);
        var (weakState, calls) = await CollectWithTrackedState(flow);

        await PgTestPool.RunAsync(protocol, "select 1");
        Assert.AreEqual(3, calls.Value);
        for (var attempt = 0; attempt < 10 && weakState.IsAlive; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        Assert.IsFalse(weakState.IsAlive, "the collector state must not be retained after release");
        Assert.AreEqual(3, calls.Value, "no callback after terminal completion");

        static async Task<(WeakReference State, StrongBox<int> Calls)> CollectWithTrackedState(ReaderDrivenCommandFlow flow)
        {
            var calls = new StrongBox<int>();
            var state = new object[] { calls };
            await flow.CollectAsync(state, static (state, _) => ((StrongBox<int>)((object[])state!)[0]).Value++);
            return (new WeakReference(state), calls);
        }
    }
}
