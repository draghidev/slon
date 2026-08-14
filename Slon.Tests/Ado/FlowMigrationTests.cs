namespace Slon.Tests;

using Microsoft.Extensions.Time.Testing;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Tests.Pg;

[TestClass]
public class FlowMigrationTests : ConnectionCreatingTest
{
    [TestMethod]
    public async Task DataSourceCommand_MigratesFromFailedWireBeforeDispatch()
        => await RunMigration(async: true);

    [TestMethod]
    public async Task SynchronousDataSourceCommand_MigratesWhileWaitingForHandoff()
        => await RunMigration(async: false);

    static async Task RunMigration(bool async)
    {
        var time = new FakeTimeProvider();
        var scheduler = new PausableScheduler();
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(
            options => options with
            {
                PoolSize = 1,
                ExecutionScheduler = scheduler,
                TimeProvider = time
            });
        var connection = await dataSource.OpenConnectionAsync(CancellationToken.None);
        try
        {
            var protocol = connection.UnderlyingPgConnection!.Protocol;
            await scheduler.PauseAsync();
            await using var command = dataSource.CreateCommand("select 42");
            Task<int> result = async
                ? command.ExecuteNonQueryAsync()
                : Task.Run(command.ExecuteNonQuery);
            while (protocol.Backlog == 0)
                await Task.Yield();

            var shutdown = protocol.CompleteAsync(new IOException("retire the assigned wire"));
            scheduler.Resume();
            await shutdown;

            Assert.AreEqual(0, await result);
        }
        finally
        {
            scheduler.Resume();
            try { await connection.DisposeAsync(); }
            catch (Slon.Pg.Protocol.PgClientClosedException) { }
        }
    }

    [TestMethod]
    public async Task MigratedTwice_BindsOnceToFinalWire()
    {
        var time = new FakeTimeProvider();
        var firstScheduler = new PausableScheduler();
        var secondScheduler = new PausableScheduler();
        static void Configure(PgClientProtocolOptions options, FakeTimeProvider time,
            PausableScheduler? scheduler = null)
        {
            options.TimeProvider = time;
            options.FlowActivationTimeout = TimeSpan.FromSeconds(10);
            options.ExecutionScheduler = scheduler;
        }
        await using var first = await PgTestPool.NewIsolatedAsync(
            options => Configure(options, time, firstScheduler));
        await using var second = await PgTestPool.NewIsolatedAsync(
            options => Configure(options, time, secondScheduler));
        await using var final = await PgTestPool.NewIsolatedAsync(options => Configure(options, time));
        first.SetFlowBindingContext(new BindingProbeContext("first"));
        second.SetFlowBindingContext(new BindingProbeContext("second"));
        final.SetFlowBindingContext(new BindingProbeContext("final"));

        var firstScope = first.QueueExclusiveScope(async: true);
        var secondScope = second.QueueExclusiveScope(async: true);
        await firstScope.HandoffReady;
        await secondScope.HandoffReady;
        await firstScheduler.PauseAsync();
        await secondScheduler.PauseAsync();

        first.SetFlowMigration(migration =>
        {
            Assert.AreEqual(TimeSpan.FromSeconds(10), migration.GetRemainingTimeout());
            time.Advance(TimeSpan.FromSeconds(3));
            return MoveTo(second, migration);
        });
        second.SetFlowMigration(migration =>
        {
            Assert.AreEqual(TimeSpan.FromSeconds(7), migration.GetRemainingTimeout());
            time.Advance(TimeSpan.FromSeconds(4));
            Assert.AreEqual(TimeSpan.FromSeconds(3), migration.GetRemainingTimeout());
            return MoveTo(final, migration);
        });

        var strategy = new BindingProbeStrategy();
        var flow = new CommandFlow(async: true, new CommandFlowBinding { Strategy = strategy });
        Assert.IsTrue(first.TryQueue(flow,
            FlowEnqueueOptions.AllowMigration | FlowEnqueueOptions.RequireExistingPipeline));
        var drain = DrainAsync(flow);

        var firstShutdown = first.CompleteAsync(new IOException("retire first wire"));
        firstScheduler.Resume();
        await firstShutdown;
        var secondShutdown = second.CompleteAsync(new IOException("retire second wire"));
        secondScheduler.Resume();
        await secondShutdown;
        await drain;

        Assert.AreEqual(1, strategy.BindCount);
        Assert.AreEqual("final", strategy.ContextName);

        static bool MoveTo(PgClientProtocol protocol, FlowMigration migration)
        {
            var options = FlowEnqueueOptions.AllowMigration |
                (protocol.Outstanding is 0 ? FlowEnqueueOptions.None : FlowEnqueueOptions.RequireExistingPipeline);
            return migration.CompletePlacement(protocol.TryQueue(migration.PreparePlacement(), options));
        }
    }

    [TestMethod]
    public async Task WireAffineFlow_DoesNotInvokeMigration()
    {
        var scheduler = new PausableScheduler();
        await using var protocol = await PgTestPool.NewIsolatedAsync(
            options => options.ExecutionScheduler = scheduler);
        var scope = protocol.QueueExclusiveScope(async: true);
        await scope.HandoffReady;
        await scheduler.PauseAsync();
        var migrationCalls = 0;
        protocol.SetFlowMigration(_ =>
        {
            Interlocked.Increment(ref migrationCalls);
            return true;
        });
        var flow = protocol.Queue(new CommandFlow(async: true, Command.Create("select 1")));
        var drain = DrainAsync(flow);

        var shutdown = protocol.CompleteAsync(new IOException("retire wire"));
        scheduler.Resume();
        await shutdown;
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(() => drain);
        Assert.AreEqual(0, migrationCalls);
    }

    [TestMethod]
    public async Task RejectedReplacement_FaultsFlowOnce()
    {
        var scheduler = new PausableScheduler();
        await using var protocol = await PgTestPool.NewIsolatedAsync(
            options => options.ExecutionScheduler = scheduler);
        var scope = protocol.QueueExclusiveScope(async: true);
        await scope.HandoffReady;
        await scheduler.PauseAsync();
        var migrationCalls = 0;
        var placementError = new InvalidOperationException("replacement unavailable");
        protocol.SetFlowMigration(migration =>
        {
            Interlocked.Increment(ref migrationCalls);
            migration.Fail(placementError);
            return true;
        });
        var flow = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(flow,
            FlowEnqueueOptions.AllowMigration | FlowEnqueueOptions.RequireExistingPipeline));
        var drain = DrainAsync(flow);

        var shutdown = protocol.CompleteAsync(new IOException("retire wire"));
        scheduler.Resume();
        await shutdown;
        var thrown = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => drain);
        Assert.AreSame(placementError, thrown);
        Assert.AreEqual(1, migrationCalls);
    }

    static async Task DrainAsync(CommandFlow flow)
    {
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

}
