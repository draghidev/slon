using System.Buffers.Binary;
using System.Collections.Immutable;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Serialization;
using Slon.Pg.Types;
using Slon.Text;

namespace Slon.Tests.Pg;

// Ownership and lifecycle of ReaderDrivenCommandFlow: the consumer owns the decoder from activation
// to RFQ, abandonment transfers it exactly once, and every terminal leaves the wire reusable.
[TestClass]
public class ReaderDrivenCommandFlowTests
{
    static async Task<CommandDescriptor> Prepare(PgClientProtocol protocol, string sql, EncodedCString name)
    {
        var flow = protocol.Queue(new CommandFlow(async: true,
            Command.Create(sql, commandName: name) with { DescribeOnly = true }));
        var results = flow.GetAsyncEnumerator();
        CommandDescriptor descriptor = default;
        while (await results.MoveNextAsync())
            descriptor = results.Current.GetMetadata().ToPreparedDescriptor();
        await results.DisposeAsync();
        return descriptor;
    }

    static ReaderDrivenCommandFlow QueuePrepared(PgClientProtocol protocol, in CommandDescriptor descriptor,
        CancellationToken cancellationToken = default)
        => protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(descriptor)), cancellationToken);

    static async Task<int> CountRows(ReaderDrivenCommandFlow flow)
    {
        var results = flow.GetAsyncEnumerator();
        var count = 0;
        while (await results.MoveNextAsync())
        {
            var rows = results.Current.GetAsyncEnumerator();
            while (await rows.MoveNextAsync())
                count++;
            await rows.DisposeAsync();
        }
        await results.DisposeAsync();
        return count;
    }

    [ConnectionCreatingTestMethod]
    public async Task OneCommand_ZeroRows_NaturalExhaustion()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 1 where false", "rd_zero");

        Assert.AreEqual(0, await CountRows(QueuePrepared(protocol, descriptor)));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task OneCommand_SeveralRows_NaturalExhaustion()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 3)", "rd_rows");

        Assert.AreEqual(3, await CountRows(QueuePrepared(protocol, descriptor)));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task SharedOptions_PipelinedFlowsCarryTheirOwnParameters()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var prepare = protocol.Queue(new CommandFlow(async: true,
            Command.Create("select $1::int",
                new ParameterTypeList(ImmutableArray.Create(Parameter.CreateNull(Oid.Unspecified))),
                "rd_shared_options") with { DescribeOnly = true, DescribeForPreparation = true, WithSync = true }));
        var prepared = prepare.GetAsyncEnumerator();
        Assert.IsTrue(await prepared.MoveNextAsync());
        var descriptor = prepared.Current.GetMetadata().ToPreparedDescriptor();
        await prepared.DisposeAsync();
        var options = new ReaderDrivenCommandOptions(Command.Create(descriptor));
        var first = protocol.Queue(new ReaderDrivenCommandFlow(options, Int4Parameter(7)));
        var second = protocol.Queue(new ReaderDrivenCommandFlow(options, Int4Parameter(11)));

        Assert.AreEqual(7, await ReadSingleInt(first));
        Assert.AreEqual(11, await ReadSingleInt(second));
        await PgTestPool.RunAsync(protocol, "select 1");

        static ParameterSource Int4Parameter(int value)
        {
            var bytes = new byte[sizeof(int)];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            return new(ImmutableArray.Create(Parameter.Create(bytes, (Oid)23u)));
        }

        static async Task<int> ReadSingleInt(ReaderDrivenCommandFlow flow)
        {
            var results = flow.GetAsyncEnumerator();
            Assert.IsTrue(await results.MoveNextAsync());
            var rows = results.Current.GetAsyncEnumerator();
            Assert.IsTrue(await rows.MoveNextAsync());
            var value = rows.Current.GetValue<int>(0);
            Assert.IsFalse(await rows.MoveNextAsync());
            await rows.DisposeAsync();
            Assert.IsFalse(await results.MoveNextAsync());
            await results.DisposeAsync();
            return value;
        }
    }

    [TestMethod]
    public void SharedOptions_RejectParametersOnTheTemplate()
    {
        var template = Command.Create("select $1::int") with
        {
            Parameters = new ParameterSource(ImmutableArray.Create(Parameter.CreateNull(Oid.Unspecified)))
        };
        Assert.ThrowsExactly<ArgumentException>(() => new ReaderDrivenCommandOptions(template));
    }

    [ConnectionCreatingTestMethod]
    public async Task UnpreparedSimpleCommand_UsesGenericPrelude()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(new ReaderDrivenCommandFlow(Command.Create("select generate_series(1, 2)")));
        Assert.AreEqual(2, await CountRows(flow));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task EarlyDispose_BeforeFirstRow()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 100)", "rd_early");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task EarlyDispose_BeforeAnyRead()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 100)", "rd_unread");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task EarlyDispose_AfterOneRow_LargeResult()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 200000)", "rd_large");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync());
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task SyncDispose_AfterOneRow_DrainsToRfq()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 200000)", "rd_sync_dispose");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync());
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        results.Dispose();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task CommandError_BeforeRowPublication_KeepsWire()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 1 / 0", "rd_error");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync());
        var failed = results.Current;
        var rows = failed.GetAsyncEnumerator();
        Assert.IsFalse(await rows.MoveNextAsync());
        await rows.DisposeAsync();
        Assert.ThrowsExactly<PgErrorException>(() => failed.GetCommandComplete());
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    // A command error observed only by the drain surfaces from the waiting disposal.
    [ConnectionCreatingTestMethod]
    public async Task CommandError_ObservedByDrain_SurfacesFromDispose()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 1 / 0", "rd_drain_error");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        await Assert.ThrowsExactlyAsync<PgErrorException>(async () => await results.DisposeAsync());

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task ConcurrentDispose_WhileMoveNextPending_Throws()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select pg_sleep(0.5)", "rd_concurrent");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        var pending = results.MoveNextAsync();
        Assert.IsFalse(pending.IsCompleted);
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () => await results.DisposeAsync());
        Assert.IsTrue(await pending);
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod(connections: 2)]
    public async Task BackendTermination_DuringRead_MatchesCommandFlow()
    {
        await using var protocols = await PgTestPool.NewIsolatedProtocolsAsync(2);
        var victim = protocols.Items[0];
        var killer = protocols.Items[1];
        var descriptor = await Prepare(victim, "select pg_sleep(10)", "rd_terminate");
        var pid = await ReadBackendPid(victim);

        var results = QueuePrepared(victim, descriptor).GetAsyncEnumerator();
        var pending = results.MoveNextAsync();
        Assert.IsFalse(pending.IsCompleted);
        await PgTestPool.RunAsync(killer, $"select pg_terminate_backend({pid})");

        var readerDriven = await Assert.ThrowsAsync<Exception>(async () => await pending);
        await Assert.ThrowsAsync<Exception>(async () => await results.DisposeAsync());
        await victim.Completion;

        // The ordinary flow classifies the same event identically.
        await using var ordinaryVictim = await PgTestPool.NewIsolatedAsync();
        var ordinaryPid = await ReadBackendPid(ordinaryVictim);
        var ordinary = ordinaryVictim.Queue(new CommandFlow(async: true, Command.Create("select pg_sleep(10)")))
            .GetAsyncEnumerator();
        var ordinaryPending = ordinary.MoveNextAsync();
        await PgTestPool.RunAsync(killer, $"select pg_terminate_backend({ordinaryPid})");
        var ordinaryFault = await Assert.ThrowsAsync<Exception>(async () => await ordinaryPending);
        Assert.AreEqual(ordinaryFault.GetType(), readerDriven.GetType());
    }

    static async Task<int> ReadBackendPid(PgClientProtocol protocol)
    {
        var flow = protocol.Queue(new CommandFlow(async: true, Command.Create("select pg_backend_pid()")));
        var results = flow.GetAsyncEnumerator();
        var pid = 0;
        while (await results.MoveNextAsync())
        {
            var rows = results.Current.GetAsyncEnumerator();
            while (await rows.MoveNextAsync())
                pid = rows.Current.GetReader().Read<int>();
            await rows.DisposeAsync();
        }
        await results.DisposeAsync();
        return pid;
    }

    static Task<PgClientProtocol> NewCancelableProtocolAsync()
        => PgTestPool.NewIsolatedAsync(options =>
            options.CancelSender = PgTestPool.CreateCancelSender(PgTestPool.NewOptions()));

    [ConnectionCreatingTestMethod]
    public async Task Cancellation_WhileMoveNextPending_DeliversAfterRfq()
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol, "select pg_sleep(30)", "rd_cancel_pending");
        using var cts = new CancellationTokenSource();
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        var pending = results.MoveNextAsync(cts.Token);
        Assert.IsFalse(pending.IsCompleted);
        cts.CancelAfter(TimeSpan.FromMilliseconds(200));
        var canceled = await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () => await pending);
        Assert.AreEqual(cts.Token, canceled.CancellationToken);
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () => await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task Cancellation_AfterRowAvailable_DrainsThenDelivers()
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 200000)", "rd_cancel_ready");
        using var cts = new CancellationTokenSource();
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        Assert.IsTrue(await results.MoveNextAsync(cts.Token));
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        cts.Cancel();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () => await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task PreCancelledToken_ReleasesCallerAndDrains()
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 1000)", "rd_precancel");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();

        var canceled = new CancellationToken(canceled: true);
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () => await results.MoveNextAsync(canceled));
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () => await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task ConsumerToken_CancelledBeforeActivation_ReplaysOnActivation()
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 1000)", "rd_flow_token");
        using var cts = new CancellationTokenSource();
        // A predecessor holds the wire so the second flow waits for activation with a cancelled token.
        var blocker = protocol.Queue(new CommandFlow(async: true, Command.Create("select pg_sleep(0.3)")))
            .GetAsyncEnumerator();
        Assert.IsTrue(await blocker.MoveNextAsync());
        var results = QueuePrepared(protocol, descriptor, cts.Token).GetAsyncEnumerator(cts.Token);
        cts.Cancel();
        Assert.IsFalse(await blocker.MoveNextAsync());
        await blocker.DisposeAsync();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () => await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task ConsumerAttachment_EndsSubmissionTokenTenure()
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 3)", "rd_token_handoff");
        using var submissionCancellation = new CancellationTokenSource();
        // Keep the flow behind a predecessor so cancellation happens before activation.
        var blocker = protocol.Queue(new CommandFlow(async: true, Command.Create("select pg_sleep(0.3)")))
            .GetAsyncEnumerator();
        Assert.IsTrue(await blocker.MoveNextAsync());
        var results = QueuePrepared(protocol, descriptor, submissionCancellation.Token)
            .GetAsyncEnumerator();

        submissionCancellation.Cancel();
        Assert.IsFalse(await blocker.MoveNextAsync());
        await blocker.DisposeAsync();

        Assert.IsTrue(await results.MoveNextAsync());
        var rows = results.Current.GetAsyncEnumerator();
        var count = 0;
        while (await rows.MoveNextAsync())
            count++;
        await rows.DisposeAsync();
        Assert.AreEqual(3, count);
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task SuccessorAdmitted_WhenConsumerAbandons()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 200000)", "rd_successor");
        var first = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();
        var second = QueuePrepared(protocol, descriptor);

        Assert.IsTrue(await first.MoveNextAsync());
        await first.DisposeAsync();
        Assert.AreEqual(200000, await CountRows(second));

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task SyncFlow_QueuedBeside_ReaderDrivenFlow()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select generate_series(1, 3)", "rd_beside_sync");
        var readerDriven = QueuePrepared(protocol, descriptor);
        var sync = Task.Run(() => PgTestPool.RunSync(protocol, "select 2"));

        Assert.AreEqual(3, await CountRows(readerDriven));
        await sync;
        await PgTestPool.RunSync(protocol, "select 3");
        Assert.AreEqual(3, await CountRows(QueuePrepared(protocol, descriptor)));
    }

    [ConnectionCreatingTestMethod]
    public async Task Pipelined_SixteenFlows_ConsumedFifo()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 1", "rd_pipelined");
        var flows = Enumerable.Range(0, 16).Select(_ => QueuePrepared(protocol, descriptor)).ToArray();

        foreach (var flow in flows)
            Assert.AreEqual(1, await CountRows(flow));

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task Sequential_ManyIterations_MixedWithOrdinaryFlows()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 1", "rd_sequential");
        for (var i = 0; i < 500; i++)
        {
            Assert.AreEqual(1, await CountRows(QueuePrepared(protocol, descriptor)));
            if (i % 50 == 49)
                await PgTestPool.RunAsync(protocol, "select 2");
        }
    }

    // The trailing write faults while the consumer's read is pending. Recovery must sequence against
    // the outstanding read, the consumer must observe a failure, and the wire must come back clean.
    [ConnectionCreatingTestMethod]
    public async Task TornTrailingWrite_WithPendingRead_RecoversWire()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var results = protocol.Queue(new ReaderDrivenCommandFlow(TornStreamedBind())).GetAsyncEnumerator();

        Exception? observed = null;
        try
        {
            while (await results.MoveNextAsync())
            {
                var rows = results.Current.GetAsyncEnumerator();
                while (await rows.MoveNextAsync()) { }
                await rows.DisposeAsync();
                results.Current.GetCommandComplete();
            }
        }
        catch (Exception ex)
        {
            observed = ex;
        }
        try
        {
            await results.DisposeAsync();
        }
        catch (Exception ex)
        {
            observed ??= ex;
        }
        Assert.IsNotNull(observed, "the consumer must observe the torn write");

        await PgTestPool.RunAsync(protocol, "select 42::int4");
    }

    static Command TornStreamedBind()
    {
        var serializerOptions = new PgSerializerOptions(PgTypeCatalog.Default);
        var value = new SlonParameter<Stream>(new ThrowingReadStream(256 * 1024, 64 * 1024));
        var parameters = new SlonParameters { value };
        parameters.GetOrResolveTypeInfo(0, serializerOptions, preparedTypeId: null, allowUnspecified: false);
        var parameterSource = new ParameterSource(parameters, SerializerParameterWriter.Instance);
        return Command.Create("select octet_length($1::bytea)", new ParameterTypeList(parameterSource)) with
        {
            Parameters = parameterSource
        };
    }

    sealed class ThrowingReadStream(int length, int throwAfter) : Stream
    {
        int _position;

        public override int Read(Span<byte> buffer)
        {
            if (_position >= throwAfter)
                throw new IOException("Synthetic parameter read failure.");
            var count = Math.Min(buffer.Length, throwAfter - _position);
            buffer.Slice(0, count).Clear();
            _position += count;
            return count;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => new(Read(buffer.Span));
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => length;
        public override long Position { get => _position; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
    }

    // Graceful completion while the consumer holds a result: the heartbeat delivers stopping, the flow
    // drains itself so the protocol can finish without its consumer, and the consumer observes the
    // close on its next move.
    [ConnectionCreatingTestMethod]
    public async Task GracefulComplete_WhileConsumerHoldsResult_DrainsAndFaultsConsumer()
    {
        var protocol = await PgTestPool.NewIsolatedAsync(options =>
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(20));
        var descriptor = await Prepare(protocol, "select generate_series(1, 1000)", "rd_graceful");
        var results = QueuePrepared(protocol, descriptor).GetAsyncEnumerator();
        Assert.IsTrue(await results.MoveNextAsync());

        var completion = protocol.CompleteAsync();
        // The drain is autonomous: completion must not wait for the consumer's next move.
        await completion.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<PgClientClosedException>(async () => await results.MoveNextAsync());
        await results.DisposeAsync();
    }
}
