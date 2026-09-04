using Draghi.Pipelining;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg.FlowAuthoring;

public interface IConsumerDrivenFlowContract
{
    string Name { get; }
    PgClientFlow Create(bool async, params Command[] commands);
    IEnumerator<CommandResult> GetEnumerator(PgClientFlow flow);
    IAsyncEnumerator<CommandResult> GetAsyncEnumerator(PgClientFlow flow);
    ValueTask<bool> MoveNextAsync(
        IAsyncEnumerator<CommandResult> results, CancellationToken cancellationToken);
}

public interface IReusableConsumerDrivenFlowContract : IConsumerDrivenFlowContract
{
    PgClientFlow CreateReusable(bool async, params Command[] commands);
    void Reset(PgClientFlow flow, bool async, params Command[] commands);
}

sealed class CommandFlowContract : IReusableConsumerDrivenFlowContract
{
    public static CommandFlowContract Instance { get; } = new();
    public string Name => nameof(CommandFlow);

    public PgClientFlow Create(bool async, params Command[] commands)
        => new CommandFlow(async, commands);

    public IEnumerator<CommandResult> GetEnumerator(PgClientFlow flow)
        => ((CommandFlow)flow).GetEnumerator();

    public IAsyncEnumerator<CommandResult> GetAsyncEnumerator(PgClientFlow flow)
        => ((CommandFlow)flow).GetAsyncEnumerator();

    public ValueTask<bool> MoveNextAsync(
        IAsyncEnumerator<CommandResult> results, CancellationToken cancellationToken)
        => ((CommandFlow.Enumerator)results).MoveNextAsync(cancellationToken);

    public PgClientFlow CreateReusable(bool async, params Command[] commands)
        => new CommandFlow(async, enableActivationTimeout: false, commands);

    public void Reset(PgClientFlow flow, bool async, params Command[] commands)
    {
        flow.Reset();
        ((CommandFlow)flow).Initialize(async, commands);
    }

    public override string ToString() => Name;
}

[TestClass]
public class ConsumerDrivenFlowAuthoringTests : ConnectionCreatingTest
{
    public static IEnumerable<object[]> Implementations
    {
        get
        {
            yield return [CommandFlowContract.Instance];
        }
    }

    public static IEnumerable<object[]> ReusableImplementations
    {
        get
        {
            yield return [CommandFlowContract.Instance];
        }
    }

    [TestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task AsyncNaturalExhaustion_ReleasesWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(contract.Create(async: true,
            Command.Create("select 1"), Command.Create("select 2")));

        var results = contract.GetAsyncEnumerator(flow);
        while (await results.MoveNextAsync())
            await results.Current.DisposeAsync();
        await results.DisposeAsync();

        await flow.WaitForComplete();
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [TestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task SyncNaturalExhaustion_ReleasesWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(contract.Create(async: false,
            Command.Create("select 1"), Command.Create("select 2")));

        using (var results = contract.GetEnumerator(flow))
        {
            while (results.MoveNext())
                results.Current.Dispose();
        }

        await flow.WaitForComplete();
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [TestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task AsyncAbandonBeforeRead_DrainsAndReleasesWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(contract.Create(async: true,
            Command.Create("select generate_series(1, 20)"), Command.Create("select 2")));

        await contract.GetAsyncEnumerator(flow).DisposeAsync();

        await flow.WaitForComplete();
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [TestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task AsyncAbandonAfterPublication_DrainsAndReleasesWire(IConsumerDrivenFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(contract.Create(async: true,
            Command.Create("select generate_series(1, 20)"), Command.Create("select 2")));
        var results = contract.GetAsyncEnumerator(flow);

        Assert.IsTrue(await results.MoveNextAsync());
        await results.DisposeAsync();

        await flow.WaitForComplete();
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [TestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task AbortWithoutConsumer_CompletesFlow(IConsumerDrivenFlowContract contract)
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(contract.Create(async: true, blocker.WaitCommand));
        try
        {
            await blocker.WaitUntilContendedAsync(protocol.FlowControl.BackendProcessId);
            await protocol.CompleteAsync(new IOException("contract abort"));
            await Assert.ThrowsExactlyAsync<PgClientClosedException>(
                async () => await flow.WaitForComplete());
        }
        finally
        {
            await blocker.ReleaseAsync();
            await protocol.DisposeAsync();
        }
    }

    [TestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task MixedSyncAndAsync_NeverRunsConsumerOnExecutor(
        IConsumerDrivenFlowContract contract)
    {
        var executionScheduler = new TrackingScheduler();
        var activationScheduler = new TrackingScheduler();
        await using var protocol = await PgTestPool.NewIsolatedAsync(o =>
        {
            o.ExecutionScheduler = executionScheduler;
            o.ActivationScheduler = activationScheduler;
        });
        Exception? failure = null;
        void Capture(Exception exception)
            => Interlocked.CompareExchange(ref failure, exception, null);

        var asyncLoop = Task.Run(async () =>
        {
            try
            {
                for (var i = 0; i < StressEnv.Iterations(32, 8_000)
                    && Volatile.Read(ref failure) is null; i++)
                {
                    var flow = protocol.Queue(contract.Create(async: true, Command.Create("select 1")));
                    var results = contract.GetAsyncEnumerator(flow);
                    var hasResult = await results.MoveNextAsync();
                    if (hasResult && executionScheduler.IsExecuting)
                        Capture(new InvalidOperationException(
                            $"{contract.Name} resumed its async consumer on the pipeline executor."));
                    while (hasResult)
                        hasResult = await results.MoveNextAsync();
                    await results.DisposeAsync();
                }
            }
            catch (Exception ex) { Capture(ex); }
        });

        var syncThread = new Thread(() =>
        {
            try
            {
                for (var i = 0; i < StressEnv.Iterations(32, 8_000)
                    && Volatile.Read(ref failure) is null; i++)
                {
                    var flow = protocol.Queue(contract.Create(async: false, Command.Create("select 1")));
                    using var results = contract.GetEnumerator(flow);
                    while (results.MoveNext()) { }
                }
            }
            catch (Exception ex) { Capture(ex); }
        }) { IsBackground = true, Name = $"{contract.Name}-sync-contract" };

        syncThread.Start();
        await asyncLoop;
        await Task.Run(syncThread.Join);
        if (failure is not null)
            Assert.Fail($"{contract.Name} violated mixed-consumer execution: {failure}");
    }

    sealed class TrackingScheduler : PipelineScheduler
    {
        [ThreadStatic]
        static TrackingScheduler? _executing;

        internal bool IsExecuting => ReferenceEquals(_executing, this);

        public override void SubmitDetached(
            Action<object?> action, object? state, bool preferLocal = true)
            => PipelineScheduler.ThreadPool.SubmitDetached(static state =>
            {
                var work = (Work)state!;
                var prior = _executing;
                _executing = work.Scheduler;
                try { work.Action(work.State); }
                finally { _executing = prior; }
            }, new Work(this, action, state), preferLocal);

        sealed record Work(TrackingScheduler Scheduler, Action<object?> Action, object? State);
    }
}
