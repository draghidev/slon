using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// The routing gate that fec0355's waiter-presence design hinges on, asserted deterministically. A sync
// flow with NO handoff MRES (the base default) is AUTONOMOUS: no caller is parked to take it over, so it
// must route via async DISPATCH (executor drives it), NOT the caller-handoff park - which dereferenced
// the null MRES (`null!`) in WaitForExecutor and would otherwise hang held in OnExecutorSuspended.
//
// This is a pure decision test (no wire, no executor, no TP) on purpose: the end-to-end "the autonomous
// flow actually runs" path is just the async dispatch path every async flow already exercises, and an
// enqueue-and-await variant is TP-dispatch-bound, so it flakes under the full suite's concurrent stress
// load (TP starvation) without testing anything the routing gate + the async coverage don't already.
[TestClass]
public class AutonomousSyncFlowTests
{
    sealed class AutonomousSyncFlow : PgClientFlow
    {
        public AutonomousSyncFlow() : base(supportsPipelining: false) => IsAsync = false;
        // No GetHandoffMres override => null => autonomous (no caller waiting).
        protected override ValueTask<FlowTasks> ExecuteAuto(Context context) => new(new FlowTasks());
    }

    [TestMethod]
    public void RoutingGate_AutonomousSyncFlow_DispatchesNotHandoff()
    {
        // sync + null handoff MRES => autonomous => dispatch, never the caller-handoff park.
        var autonomous = new AutonomousSyncFlow();
        Assert.IsFalse(autonomous.NeedsSyncHandoff, "autonomous sync flow must route via dispatch, not the handoff park (the null! site)");

        // sync CommandFlow always has its caller (interactive) => it posts a handoff MRES => handoff.
        var sync = new CommandFlow(async: false, Command.Create("select 1"));
        Assert.IsTrue(sync.NeedsSyncHandoff, "a sync CommandFlow has its caller and must take the handoff");

        // async flow => dispatch regardless of waiter.
        var async = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsFalse(async.NeedsSyncHandoff, "an async flow never takes the sync handoff");
    }
}
