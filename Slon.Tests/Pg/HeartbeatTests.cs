using Slon.Pg.Protocol;

namespace Slon.Tests.Pg;

[TestClass]
public class HeartbeatTests
{
    [TestMethod]
    public void RegisterAfterDispose_IsRejected()
    {
        var heartbeat = new Heartbeat(TimeSpan.FromHours(1));
        heartbeat.Dispose();

        Assert.ThrowsExactly<ObjectDisposedException>(
            () => heartbeat.Register(static _ => ValueTask.CompletedTask));
    }
}
