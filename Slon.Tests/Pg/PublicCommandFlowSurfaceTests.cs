using System.Collections;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

[TestClass]
public class PublicCommandFlowSurfaceTests
{
    [TestMethod]
    public void ReplacementIsTheSealedPublicCommandFlowSurface()
    {
        var flow = typeof(Slon.Pg.Protocol.Flows.CommandFlow);
        Assert.IsTrue(flow.IsPublic);
        Assert.IsTrue(flow.IsSealed);
        Assert.IsTrue(typeof(Slon.Pg.Protocol.Flows.CommandFlowObserver).IsPublic);
        Assert.IsTrue(typeof(Slon.Pg.Protocol.Flows.CommandFlowOptions).IsPublic);
        Assert.IsTrue(typeof(IEnumerator<CommandResult>)
            .IsAssignableFrom(typeof(Slon.Pg.Protocol.Flows.CommandFlow.Enumerator)));

        Assert.IsNotNull(flow.GetConstructor([
            typeof(bool), typeof(ReadOnlySpan<Command>)
        ]));
        Assert.IsNotNull(flow.GetConstructor([
            typeof(bool), typeof(Slon.Pg.Protocol.Flows.CommandFlowOptions).MakeByRefType()
        ]));
        Assert.IsFalse(typeof(LegacyCommandFlow).IsPublic);
    }
}
