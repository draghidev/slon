using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Text;

namespace Slon.Tests.Pg;

// A reader-driven flow resets the protocol-static read objects before it completes, so the idle
// edge keeps them for its successor instead of replacing them. Every terminal a reader-driven flow
// can reach must leave those objects usable by the next flow of either kind, and a handle retained
// past the terminal must be inert.
[TestClass]
public class ReaderDrivenReadStateTests
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

    static async Task<List<int>> ReadInts(IAsyncEnumerator<CommandResult> results)
    {
        var values = new List<int>();
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

    static Task<List<int>> ReadInts(ReaderDrivenCommandFlow flow) => ReadInts(flow.GetAsyncEnumerator());
    static Task<List<int>> ReadInts(CommandFlow flow) => ReadInts(flow.GetAsyncEnumerator());

    [ConnectionCreatingTestMethod]
    public async Task ReaderDrivenThenCommandFlow_AcrossIdleEdges()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 42", "rd_state_reader_first");

        CollectionAssert.AreEqual(new[] { 42 },
            await ReadInts(protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(descriptor)))));
        CollectionAssert.AreEqual(new[] { 43 },
            await ReadInts(protocol.Queue(new CommandFlow(async: true, Command.Create("select 43")))));
        CollectionAssert.AreEqual(new[] { 42 },
            await ReadInts(protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(descriptor)))));
    }

    [ConnectionCreatingTestMethod]
    public async Task CommandFlowThenReaderDriven_AcrossIdleEdges()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 42", "rd_state_command_first");

        CollectionAssert.AreEqual(new[] { 43 },
            await ReadInts(protocol.Queue(new CommandFlow(async: true, Command.Create("select 43")))));
        CollectionAssert.AreEqual(new[] { 42 },
            await ReadInts(protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(descriptor)))));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task RetainedExhaustedResult_IsInertWhileSuccessorRuns()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var first = await Prepare(protocol, "select 42", "rd_state_retained_first");
        var second = await Prepare(protocol, "select 43, 44", "rd_state_retained_second");

        var results = protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(first))).GetAsyncEnumerator();
        Assert.IsTrue(await results.MoveNextAsync());
        var retained = results.Current;
        var rows = retained.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        Assert.AreEqual(42, rows.Current.GetValue<int>(0));
        Assert.IsFalse(await rows.MoveNextAsync());
        await rows.DisposeAsync();
        Assert.IsFalse(await results.MoveNextAsync());
        await results.DisposeAsync();

        var successor = protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(second))).GetAsyncEnumerator();
        Assert.IsTrue(await successor.MoveNextAsync());
        Assert.AreEqual(2, successor.Current.FieldCount);
        var successorRows = successor.Current.GetAsyncEnumerator();
        Assert.IsTrue(await successorRows.MoveNextAsync());
        Assert.AreEqual(43, successorRows.Current.GetValue<int>(0));
        Assert.AreEqual(44, successorRows.Current.GetValue<int>(1));
        Assert.IsFalse(await successorRows.MoveNextAsync());
        await successorRows.DisposeAsync();
        Assert.IsFalse(await successor.MoveNextAsync());
        await successor.DisposeAsync();

        // The handle is not valid past its flow's terminal: it is the shared result object, so it
        // reflects whatever flow last initialized it. Retaining it must neither fault nor disturb the
        // successor, which is what the assertions above pin.
        Assert.IsTrue(retained.IsComplete);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task ReaderFailureAndRecovery_ThenSuccessor()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var descriptor = await Prepare(protocol, "select 42", "rd_state_recovery");

        var torn = protocol.Queue(new ReaderDrivenCommandFlow(CommandFlowContractTests.TornStreamedBind()))
            .GetAsyncEnumerator();
        Exception? observed = null;
        try
        {
            while (await torn.MoveNextAsync())
            {
                var rows = torn.Current.GetAsyncEnumerator();
                while (await rows.MoveNextAsync()) { }
                await rows.DisposeAsync();
                torn.Current.GetCommandComplete();
            }
        }
        catch (Exception exception)
        {
            observed = exception;
        }
        try
        {
            await torn.DisposeAsync();
        }
        catch (Exception exception)
        {
            observed ??= exception;
        }
        Assert.IsNotNull(observed);

        CollectionAssert.AreEqual(new[] { 42 },
            await ReadInts(protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(descriptor)))));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task CancellationDrain_ThenSuccessor()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync(options =>
            options.CancelSender = PgTestPool.CreateCancelSender(PgTestPool.NewOptions()));
        var large = await Prepare(protocol, "select generate_series(1, 20000)", "rd_state_cancel_large");
        var small = await Prepare(protocol, "select 42", "rd_state_cancel_small");

        using var cancellation = new CancellationTokenSource();
        var results = protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(large))).GetAsyncEnumerator();
        Assert.IsTrue(await results.MoveNextAsync(cancellation.Token));
        var rows = results.Current.GetAsyncEnumerator();
        Assert.IsTrue(await rows.MoveNextAsync());
        cancellation.Cancel();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await results.MoveNextAsync(cancellation.Token));
        await results.DisposeAsync();

        CollectionAssert.AreEqual(new[] { 42 },
            await ReadInts(protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(small)))));
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task AutonomousDrainAfterEarlyDispose_ThenSuccessor()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var large = await Prepare(protocol, "select generate_series(1, 20000)", "rd_state_drain_large");
        var small = await Prepare(protocol, "select 42", "rd_state_drain_small");

        var abandoned = protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(large))).GetAsyncEnumerator();
        await abandoned.DisposeAsync();

        CollectionAssert.AreEqual(new[] { 42 },
            await ReadInts(protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(small)))));
        await PgTestPool.RunAsync(protocol, "select 1");
    }
}
