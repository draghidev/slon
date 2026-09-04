using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg.FlowAuthoring;

public interface IAutonomousFlowContract
{
    string Name { get; }
    PgClientFlow Create(bool async, params Command[] commands);
    bool WasReleased(PgClientFlow flow);
}

sealed class AutonomousCommandFlowContract : IAutonomousFlowContract
{
    public static AutonomousCommandFlowContract Instance { get; } = new();
    public string Name => nameof(AutonomousCommandFlow);

    public PgClientFlow Create(bool async, params Command[] commands)
        => new AutonomousCommandFlow(async, commands);

    public bool WasReleased(PgClientFlow flow)
        => ((AutonomousCommandFlow)flow).Released;

    public override string ToString() => Name;
}

sealed class AutonomousCommandFlow : PgClientFlow
{
    readonly CommandList _commands;

    internal AutonomousCommandFlow(bool async, params Command[] commands)
        : base(supportsDeferredFlush: true)
    {
        IsAsync = async;
        _commands = new(commands);
    }

    internal bool Released { get; private set; }

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        var write = _commands.WriteCommandsAsync(context.GetEncoder(), appendSync: true);
        return new(new FlowTasks(write, DrainAsync(context)));

        static async ValueTask DrainAsync(Context context)
        {
            var decoder = await context.GetDecoderAsync().ConfigureAwait(false);
            while (context.OutstandingRfqCount is not 0)
                _ = await decoder.GetNextAsync().ConfigureAwait(false);
        }
    }

    protected override void OnReleasing(Exception? exception) => Released = true;
}

[TestClass]
public class AutonomousFlowAuthoringTests : ConnectionCreatingTest
{
    public static IEnumerable<object[]> Implementations
    {
        get
        {
            yield return [AutonomousCommandFlowContract.Instance];
        }
    }

    [TestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task NaturalCompletion_ReleasesAfterPipelineTask(
        IAutonomousFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(contract.Create(async: true,
            Command.Create("select 1"), Command.Create("select 2")));

        await flow.WaitForComplete();

        Assert.IsTrue(contract.WasReleased(flow));
        await PgTestPool.RunAsync(protocol, "select 3");
    }

    [TestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task SynchronousMode_DoesNotRequireCallerHandoff(
        IAutonomousFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var flow = protocol.Queue(contract.Create(async: false, Command.Create("select 1")));

        await flow.WaitForComplete();

        Assert.IsFalse(flow.NeedsSyncHandoff);
        await PgTestPool.RunAsync(protocol, "select 2");
    }

    [TestMethod]
    [DynamicData(nameof(Implementations))]
    public async Task ForcefulAbort_CompletesWithoutExternalDriver(
        IAutonomousFlowContract contract)
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
    public async Task PipelinedInstances_CompleteAndRelease(
        IAutonomousFlowContract contract)
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var first = protocol.Queue(contract.Create(async: true, Command.Create("select 1")));
        var second = protocol.Queue(contract.Create(async: true, Command.Create("select 2")));

        await first.WaitForComplete();
        await second.WaitForComplete();

        Assert.IsTrue(contract.WasReleased(first));
        Assert.IsTrue(contract.WasReleased(second));
        await PgTestPool.RunAsync(protocol, "select 3");
    }
}
