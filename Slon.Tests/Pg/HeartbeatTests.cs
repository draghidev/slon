using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Threading;

namespace Slon.Tests.Pg;

[TestClass]
public class HeartbeatTests
{
    [TestMethod]
    public async Task BackloggedFlow_ActivationTimeoutAdvancesBeforeDispatch()
    {
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol, protocol.FlowControl);
        var flow = new CommandFlow(async: true, Command.Create("select 1"));
        var control = flow.GetExecutionControl(protocol.FlowControl);

        source.Enqueue(flow);
        control.Bind(TimeSpan.FromSeconds(2));
        var activation = control.GetDecoderTask(CancellationToken.None);

        source.OnActivationHeartbeat(TimeSpan.FromSeconds(1));
        Assert.IsFalse(activation.IsCompleted);
        source.OnActivationHeartbeat(TimeSpan.FromSeconds(1));

        await Assert.ThrowsExactlyAsync<TimeoutException>(async () => await activation);
    }

    [TestMethod]
    public void RegisterAfterDispose_IsRejected()
    {
        var heartbeat = new Heartbeat(TimeSpan.FromHours(1));
        heartbeat.Dispose();

        Assert.ThrowsExactly<ObjectDisposedException>(
            () => heartbeat.Register(static _ => ValueTask.CompletedTask));
    }
}
