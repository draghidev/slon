using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Serialization;
using Slon.Pg.Types;
using Slon.Text;

namespace Slon.Tests.Pg.FlowAuthoring;

// Command-result semantics shared by consumer-driven PostgreSQL flows.
[TestClass]
public class ConsumerDrivenFlowSemanticContractTests
{
    public static IEnumerable<object[]> Implementations
        => ConsumerDrivenFlowAuthoringTests.Implementations;

    public static IEnumerable<object[]> ReusableImplementations
        => ConsumerDrivenFlowAuthoringTests.ReusableImplementations;

    public static IEnumerable<object[]> PreparedCases
    {
        get
        {
            foreach (var implementation in Implementations)
            {
                yield return [implementation[0], 0];
                yield return [implementation[0], 3];
            }
        }
    }
    static async Task<CommandDescriptor> Prepare(
        IConsumerDrivenFlowContract contract,
        PgClientProtocol protocol, string sql, EncodedCString name)
    {
        var results = Queue(contract, protocol,
            Command.Create(sql, commandName: name) with { DescribeOnly = true });
        CommandDescriptor descriptor = default;
        while (await results.MoveNextAsync())
            descriptor = results.Current.GetMetadata().ToPreparedDescriptor();
        await results.DisposeAsync();
        return descriptor;
    }

    static Results Queue(IConsumerDrivenFlowContract contract,
        PgClientProtocol protocol, in Command command,
        CancellationToken cancellationToken = default)
    {
        var flow = protocol.Queue(contract.Create(async: true, command), cancellationToken);
        return new(contract, contract.GetAsyncEnumerator(flow));
    }

    static async Task<int> CountRows(Results results)
    {
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
    [DynamicData(nameof(PreparedCases))]
    public async Task Prepared_NaturalExhaustion(
        IConsumerDrivenFlowContract contract, int rowCount)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(contract, protocol,
            $"select generate_series(1, {rowCount})", $"contract_rows_{rowCount}");

        Assert.AreEqual(rowCount,
            await CountRows(Queue(contract, protocol, Command.Create(descriptor))));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task Unprepared_NaturalExhaustion(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();

        Assert.AreEqual(2, await CountRows(Queue(contract, protocol,
            Command.Create("select generate_series(1, 2)"))));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task DisposeBeforeAnyRead_DrainsAndKeepsWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(contract, protocol,
            "select generate_series(1, 1000)", "contract_unread");
        var results = Queue(contract, protocol, Command.Create(descriptor));

        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task DisposeAfterOneRow_DrainsAndKeepsWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(contract, protocol,
            "select generate_series(1, 20000)", "contract_partial");
        var results = Queue(contract, protocol, Command.Create(descriptor));

        Assert.IsTrue(await results.MoveNextAsync());
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task CommandError_IsResultAndKeepsWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(contract, protocol, "select 1 / 0", "contract_error");
        var results = Queue(contract, protocol, Command.Create(descriptor));

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

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task PreparedMetadataAndCompletion_Agree(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(contract, protocol, "select 42::int4", "contract_metadata");
        var results = Queue(contract, protocol, Command.Create(descriptor));

        Assert.IsTrue(await results.MoveNextAsync());
        var result = results.Current;
        var metadata = result.GetMetadata();
        Assert.IsTrue(metadata.IsPrepared);
        Assert.AreEqual(descriptor.CommandName, metadata.CommandName);
        Assert.IsNotNull(metadata.RowDescription);
        var rows = result.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        Assert.AreEqual(42, rows.Current.GetReader().Read<int>());
        Assert.IsFalse(await rows.MoveNextAsync());
        await rows.DisposeAsync();
        Assert.IsTrue(result.IsComplete);
        Assert.AreEqual(StatementType.Select, result.GetCommandComplete().StatementType);
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task CancellationWhileReadPending_DeliversTokenAndKeepsWire(IConsumerDrivenFlowContract contract)
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        await using var protocol = await NewCancelableProtocolAsync();
        using var cancellation = new CancellationTokenSource();
        var results = Queue(contract, protocol, blocker.WaitCommand);

        var pending = results.MoveNextAsync(cancellation.Token);
        Assert.IsFalse(pending.IsCompleted);
        await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
        cancellation.Cancel();
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await pending);
        Assert.AreEqual(cancellation.Token, exception.CancellationToken);
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await results.MoveNextAsync());
        await results.DisposeAsync();
        await blocker.ReleaseAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task CancellationAfterRow_DrainsAndKeepsWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(contract, protocol,
            "select generate_series(1, 20000)", "contract_cancel_row");
        using var cancellation = new CancellationTokenSource();
        var results = Queue(contract, protocol, Command.Create(descriptor));

        Assert.IsTrue(await results.MoveNextAsync(cancellation.Token));
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        cancellation.Cancel();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await results.MoveNextAsync(cancellation.Token));
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task PreCancelledRead_ReleasesCallerAndKeepsWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(contract, protocol,
            "select generate_series(1, 1000)", "contract_precancel");
        var results = Queue(contract, protocol, Command.Create(descriptor));
        var cancellationToken = new CancellationToken(canceled: true);

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await results.MoveNextAsync(cancellationToken));
        Assert.AreEqual(cancellationToken, exception.CancellationToken);
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await results.MoveNextAsync(cancellationToken));
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task SuccessorProgressesAfterAbandonment(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(contract, protocol,
            "select generate_series(1, 20000)", "contract_successor");
        var first = Queue(contract, protocol, Command.Create(descriptor));
        var second = Queue(contract, protocol, Command.Create(descriptor));

        Assert.IsTrue(await first.MoveNextAsync());
        await first.DisposeAsync();
        Assert.AreEqual(20000, await CountRows(second));

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task GracefulStopDrainsHeldResultAndFaultsConsumer(IConsumerDrivenFlowContract contract)
    {
        var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(contract, protocol,
            "select generate_series(1, 1000)", "contract_graceful");
        var results = Queue(contract, protocol, Command.Create(descriptor));
        Assert.IsTrue(await results.MoveNextAsync());

        var complete = protocol.CompleteAsync();
        await protocol.Heartbeat(TimeSpan.Zero);
        await complete;
        await Assert.ThrowsAsync<PgClientClosedException>(
            async () => await results.MoveNextAsync());
        await results.DisposeAsync();
    }

    [ConnectionCreatingTestMethod(connections: 2)]
    [DynamicData(nameof(Implementations))]
    public async Task BackendTermination_IsCollateral(IConsumerDrivenFlowContract contract)
    {
        await using var protocols = await PgTestPool.NewIsolatedProtocolsAsync(2);
        var killer = protocols.Items[1];

        var exception = await Terminate();
        Assert.IsInstanceOfType<PgCollateralException>(exception);

        async Task<Exception> Terminate()
        {
            await using var victim = await PgTestPool.NewIsolatedAsync();
            var pid = await ReadBackendPid(contract, victim);
            var descriptor = await Prepare(contract, victim, "select pg_sleep(10)",
                "contract_terminate_command");
            var results = Queue(contract, victim, Command.Create(descriptor));
            var pending = results.MoveNextAsync();
            Assert.IsFalse(pending.IsCompleted);
            await PgTestPool.RunAsync(killer, $"select pg_terminate_backend({pid})");
            var exception = await Assert.ThrowsAsync<Exception>(async () => await pending);
            await Assert.ThrowsAsync<Exception>(async () => await results.DisposeAsync());
            await victim.Completion;
            return exception;
        }
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task TornTrailingWrite_RecoversWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var results = Queue(contract, protocol, TornStreamedBind());

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
        catch (Exception exception)
        {
            observed = exception;
        }
        try
        {
            await results.DisposeAsync();
        }
        catch (Exception exception)
        {
            observed ??= exception;
        }
        Assert.IsNotNull(observed);

        await PgTestPool.RunAsync(protocol, "select 42::int4");
    }

    [ConnectionCreatingTestMethod]
    [DynamicData(nameof(ReusableImplementations))]
    public async Task ResetAfterCompletion_StartsIndependentTenure(
        IReusableConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = contract.CreateReusable(async: true, Command.Create("select 1"));

        for (var tenure = 0; tenure < 2; tenure++)
        {
            if (tenure > 0)
                contract.Reset(flow, async: true, Command.Create("select 2"));
            protocol.Queue(flow);
            var results = contract.GetAsyncEnumerator(flow);
            Assert.IsTrue(await results.MoveNextAsync(),
                $"tenure {tenure} must publish its result");
            await results.Current.DisposeAsync();
            Assert.IsFalse(await results.MoveNextAsync());
            await results.DisposeAsync();
            await flow.WaitForComplete();
        }
    }

    static Task<PgClientProtocol> NewCancelableProtocolAsync()
        => PgTestPool.NewIsolatedAsync(options =>
            options.CancelSender = PgTestPool.CreateCancelSender(PgTestPool.NewOptions()));

    static async Task<int> ReadBackendPid(
        IConsumerDrivenFlowContract contract, PgClientProtocol protocol)
    {
        var results = Queue(contract, protocol, Command.Create("select pg_backend_pid()"));
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

    internal static Command TornStreamedBind()
    {
        var serializerOptions = new PgSerializerOptions(PgTypeCatalog.Default);
        var value = new SlonParameter<Stream>(new ThrowingReadStream(256 * 1024, 64 * 1024));
        var parameters = new SlonParameters { value };
        parameters.GetOrResolveTypeInfo(
            0, serializerOptions, preparedTypeId: null, allowUnspecified: false);
        var parameterSource = new ParameterSource(parameters, SerializerParameterWriter.Instance);
        return Command.Create(
            "select octet_length($1::bytea)", new ParameterTypeList(parameterSource)) with
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
            buffer[..count].Clear();
            _position += count;
            return count;
        }

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer, CancellationToken cancellationToken = default)
            => new(Read(buffer.Span));
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => length;
        public override long Position { get => _position; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count)
            => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count)
            => Read(buffer.AsSpan(offset, count));
    }

    readonly struct Results(
        IConsumerDrivenFlowContract contract,
        IAsyncEnumerator<CommandResult> inner) : IAsyncDisposable
    {
        internal CommandResult Current => inner.Current;
        internal ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
            => contract.MoveNextAsync(inner, cancellationToken);
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

}

