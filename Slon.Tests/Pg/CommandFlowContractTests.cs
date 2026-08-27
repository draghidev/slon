using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Serialization;
using Slon.Pg.Types;
using Slon.Text;

namespace Slon.Tests.Pg;

// The behavioral intersection shared by the general command flow and the async single-command
// specialization. Specialization-only ownership races remain in ReaderDrivenCommandFlowTests.
[TestClass]
public class CommandFlowContractTests
{
    static async Task<CommandDescriptor> Prepare(
        PgClientProtocol protocol, string sql, EncodedCString name)
    {
        var results = protocol.Queue(new CommandFlow(async: true,
            Command.Create(sql, commandName: name) with { DescribeOnly = true })).GetAsyncEnumerator();
        CommandDescriptor descriptor = default;
        while (await results.MoveNextAsync())
            descriptor = results.Current.GetMetadata().ToPreparedDescriptor();
        await results.DisposeAsync();
        return descriptor;
    }

    static Results Queue(PgClientProtocol protocol, bool readerDriven, in Command command,
        CancellationToken cancellationToken = default)
    {
        if (readerDriven)
        {
            var flow = protocol.Queue(new ReaderDrivenCommandFlow(command), cancellationToken);
            return new(flow.GetAsyncEnumerator());
        }
        else
        {
            var flow = protocol.Queue(new CommandFlow(async: true, command), cancellationToken);
            return new(flow.GetAsyncEnumerator());
        }
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
    [DataRow(false, 0)]
    [DataRow(true, 0)]
    [DataRow(false, 3)]
    [DataRow(true, 3)]
    public async Task Prepared_NaturalExhaustion(bool readerDriven, int rowCount)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol,
            $"select generate_series(1, {rowCount})", $"contract_rows_{rowCount}");

        Assert.AreEqual(rowCount,
            await CountRows(Queue(protocol, readerDriven, Command.Create(descriptor))));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task Unprepared_NaturalExhaustion(bool readerDriven)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();

        Assert.AreEqual(2, await CountRows(Queue(protocol, readerDriven,
            Command.Create("select generate_series(1, 2)"))));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task DisposeBeforeAnyRead_DrainsAndKeepsWire(bool readerDriven)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol,
            "select generate_series(1, 1000)", "contract_unread");
        var results = Queue(protocol, readerDriven, Command.Create(descriptor));

        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task DisposeAfterOneRow_DrainsAndKeepsWire(bool readerDriven)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol,
            "select generate_series(1, 20000)", "contract_partial");
        var results = Queue(protocol, readerDriven, Command.Create(descriptor));

        Assert.IsTrue(await results.MoveNextAsync());
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task CommandError_IsResultAndKeepsWire(bool readerDriven)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 1 / 0", "contract_error");
        var results = Queue(protocol, readerDriven, Command.Create(descriptor));

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
    [DataRow(false)]
    [DataRow(true)]
    public async Task PreparedMetadataAndCompletion_Agree(bool readerDriven)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 42::int4", "contract_metadata");
        var results = Queue(protocol, readerDriven, Command.Create(descriptor));

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
    [DataRow(false)]
    [DataRow(true)]
    public async Task CancellationWhileReadPending_DeliversTokenAndKeepsWire(bool readerDriven)
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol, "select pg_sleep(30)", "contract_cancel_pending");
        using var cancellation = new CancellationTokenSource();
        var results = Queue(protocol, readerDriven, Command.Create(descriptor));

        var pending = results.MoveNextAsync(cancellation.Token);
        Assert.IsFalse(pending.IsCompleted);
        cancellation.CancelAfter(TimeSpan.FromMilliseconds(200));
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await pending);
        Assert.AreEqual(cancellation.Token, exception.CancellationToken);
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await results.MoveNextAsync());
        await results.DisposeAsync();

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task CancellationAfterRow_DrainsAndKeepsWire(bool readerDriven)
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol,
            "select generate_series(1, 20000)", "contract_cancel_row");
        using var cancellation = new CancellationTokenSource();
        var results = Queue(protocol, readerDriven, Command.Create(descriptor));

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
    [DataRow(false)]
    [DataRow(true)]
    public async Task PreCancelledRead_ReleasesCallerAndKeepsWire(bool readerDriven)
    {
        await using var protocol = await NewCancelableProtocolAsync();
        var descriptor = await Prepare(protocol,
            "select generate_series(1, 1000)", "contract_precancel");
        var results = Queue(protocol, readerDriven, Command.Create(descriptor));
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
    [DataRow(false)]
    [DataRow(true)]
    public async Task SuccessorProgressesAfterAbandonment(bool readerDriven)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol,
            "select generate_series(1, 20000)", "contract_successor");
        var first = Queue(protocol, readerDriven, Command.Create(descriptor));
        var second = Queue(protocol, readerDriven, Command.Create(descriptor));

        Assert.IsTrue(await first.MoveNextAsync());
        await first.DisposeAsync();
        Assert.AreEqual(20000, await CountRows(second));

        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task GracefulStopDrainsHeldResultAndFaultsConsumer(bool readerDriven)
    {
        var protocol = await PgTestPool.NewIsolatedAsync(options =>
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(20));
        var descriptor = await Prepare(protocol,
            "select generate_series(1, 1000)", "contract_graceful");
        var results = Queue(protocol, readerDriven, Command.Create(descriptor));
        Assert.IsTrue(await results.MoveNextAsync());

        await protocol.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<PgClientClosedException>(
            async () => await results.MoveNextAsync());
        await results.DisposeAsync();
    }

    [ConnectionCreatingTestMethod(connections: 2)]
    public async Task BackendTermination_IsClassifiedEqually()
    {
        await using var protocols = await PgTestPool.NewIsolatedProtocolsAsync(2);
        var killer = protocols.Items[1];

        var ordinary = await Terminate(false);
        var readerDriven = await Terminate(true);
        Assert.AreEqual(ordinary.GetType(), readerDriven.GetType());
        Assert.IsInstanceOfType<PgCollateralException>(readerDriven);

        async Task<Exception> Terminate(bool useReaderDriven)
        {
            await using var victim = await PgTestPool.NewIsolatedAsync();
            var pid = await ReadBackendPid(victim);
            var descriptor = await Prepare(victim, "select pg_sleep(10)",
                useReaderDriven ? "contract_terminate_reader" : "contract_terminate_command");
            var results = Queue(victim, useReaderDriven, Command.Create(descriptor));
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
    [DataRow(false)]
    [DataRow(true)]
    public async Task TornTrailingWrite_RecoversWire(bool readerDriven)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var results = Queue(protocol, readerDriven, TornStreamedBind());

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

    static Task<PgClientProtocol> NewCancelableProtocolAsync()
        => PgTestPool.NewIsolatedAsync(options =>
            options.CancelSender = PgTestPool.CreateCancelSender(PgTestPool.NewOptions()));

    static async Task<int> ReadBackendPid(PgClientProtocol protocol)
    {
        var results = Queue(protocol, readerDriven: false,
            Command.Create("select pg_backend_pid()"));
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

    static Command TornStreamedBind()
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

    readonly struct Results : IAsyncDisposable
    {
        readonly CommandFlow.Enumerator _command;
        readonly ReaderDrivenCommandFlow.Enumerator _readerDriven;
        readonly bool _isReaderDriven;

        internal Results(CommandFlow.Enumerator command)
            => (_command, _readerDriven, _isReaderDriven) = (command, default, false);

        internal Results(ReaderDrivenCommandFlow.Enumerator readerDriven)
            => (_command, _readerDriven, _isReaderDriven) = (default, readerDriven, true);

        internal CommandResult Current
            => _isReaderDriven ? _readerDriven.Current : _command.Current;

        internal ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
            => _isReaderDriven
                ? _readerDriven.MoveNextAsync(cancellationToken)
                : _command.MoveNextAsync(cancellationToken);

        public ValueTask DisposeAsync()
            => _isReaderDriven ? _readerDriven.DisposeAsync() : _command.DisposeAsync();
    }
}
