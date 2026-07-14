using Microsoft.Extensions.Time.Testing;
using Slon.Pg.Protocol;

namespace Slon.Tests.Pg;

// Unit tests for CloseSignal (no live Postgres). Verifies the structural materialize-before-trip
// invariant, set-once reason, and the linked-child cascade (parent trip reaches the child and the
// child resolves the parent's reason - R3).
[TestClass]
public class CloseSignalTests
{
    [TestMethod]
    public void Materialize_BeforeTrip_Abort()
    {
        using var signal = CloseSignal.CreateRoot(TimeProvider.System);
        // Register on the token BEFORE tripping; the callback must observe a non-null Reason. This is
        // the invariant the decoder/writer abort-translation sites depend on.
        PgClientClosedException? observed = null;
        signal.AbortToken.Register(() => observed = signal.Reason);
        signal.Abort();
        Assert.IsNotNull(observed, "Reason must be materialized before the abort token fires.");
        Assert.IsNotNull(signal.Reason);
    }

    [TestMethod]
    public async Task Materialize_BeforeTrip_Stop()
    {
        using var signal = CloseSignal.CreateRoot(TimeProvider.System);
        PgClientClosedException? observed = null;
        signal.StoppingToken.Register(() => observed = signal.Reason);
        await signal.StopAsync();
        Assert.IsNotNull(observed, "Reason must be materialized before the stopping token fires.");
    }

    [TestMethod]
    public void Materialize_SetOnce()
    {
        using var signal = CloseSignal.CreateRoot(TimeProvider.System);
        var cause = new InvalidOperationException("first");
        signal.MaterializeReason(cause);
        var first = signal.Reason;
        // A later token-only trip (null cause) must NOT overwrite the explicit reason.
        signal.Abort();
        Assert.AreSame(first, signal.Reason, "Reason is set once; later trips must not overwrite.");
        Assert.AreSame(cause, signal.Reason!.InnerException, "The explicit cause is preserved as inner.");
    }

    [TestMethod]
    public void LinkedChild_ParentAbort_Cascades()
    {
        using var parent = CloseSignal.CreateRoot(TimeProvider.System);
        using var child = CloseSignal.CreateLinked(parent, TimeProvider.System);

        Assert.IsFalse(child.AbortToken.IsCancellationRequested);
        parent.MaterializeReason(new InvalidOperationException("parent cause"));
        parent.Abort();

        // R3: the child token fires (linked), and the child resolves the parent's reason (falls through).
        Assert.IsTrue(child.AbortToken.IsCancellationRequested, "Parent abort must cascade to the child token.");
        Assert.IsNotNull(child.Reason, "Child must resolve a non-null reason after a parent trip.");
        Assert.AreSame(parent.Reason, child.Reason, "Child reason falls through to the parent's instance.");
    }

    [TestMethod]
    public void LinkedChild_ScopeOnlyAbort_DoesNotTripParent()
    {
        using var parent = CloseSignal.CreateRoot(TimeProvider.System);
        using var child = CloseSignal.CreateLinked(parent, TimeProvider.System);

        child.Abort();

        Assert.IsTrue(child.AbortToken.IsCancellationRequested, "Scope-only abort trips the child.");
        Assert.IsFalse(parent.AbortToken.IsCancellationRequested, "Scope-only abort must NOT trip the parent.");
        Assert.IsNull(parent.Reason, "Scope-only abort must not materialize the parent's reason.");
        Assert.IsNotNull(child.Reason, "The child has its own reason after a scope-only trip.");
    }

    [TestMethod]
    public void LinkedChild_Dispose_DoesNotDisposeParent()
    {
        var parent = CloseSignal.CreateRoot(TimeProvider.System);
        var child = CloseSignal.CreateLinked(parent, TimeProvider.System);

        child.Dispose();

        // The parent is still usable after the child is disposed (the child disposed only its own CTSes).
        parent.Abort();
        Assert.IsTrue(parent.AbortToken.IsCancellationRequested, "Parent must remain usable after child dispose.");
        parent.Dispose();
    }

    [TestMethod]
    public void ArmAbortTimeout_Escalates_OnTimeProvider()
    {
        var time = new FakeTimeProvider();
        using var signal = CloseSignal.CreateRoot(time);

        signal.ArmAbortTimeout(TimeSpan.FromSeconds(10));
        Assert.IsFalse(signal.AbortToken.IsCancellationRequested, "Abort must not fire before the timeout elapses.");

        time.Advance(TimeSpan.FromSeconds(10));
        Assert.IsTrue(signal.AbortToken.IsCancellationRequested, "Abort must fire once the armed timeout elapses.");
        Assert.IsNotNull(signal.Reason, "Arming materialized the reason, so it is non-null when abort fires.");
    }

    [TestMethod]
    public void DisarmAbortTimeout_PreventsEscalation()
    {
        var time = new FakeTimeProvider();
        using var signal = CloseSignal.CreateRoot(time);

        signal.ArmAbortTimeout(TimeSpan.FromSeconds(10));
        signal.DisarmAbortTimeout();
        time.Advance(TimeSpan.FromSeconds(60));

        Assert.IsFalse(signal.AbortToken.IsCancellationRequested, "Disarmed timeout must not escalate to abort.");
    }

    [TestMethod]
    public void LinkedChild_AbortTimeout_UsesTimeProvider()
    {
        var time = new FakeTimeProvider();
        using var parent = CloseSignal.CreateRoot(time);
        using var child = CloseSignal.CreateLinked(parent, time);

        child.ArmAbortTimeout(TimeSpan.FromSeconds(10));
        time.Advance(TimeSpan.FromSeconds(10));

        Assert.IsTrue(child.AbortToken.IsCancellationRequested);
        Assert.IsFalse(parent.AbortToken.IsCancellationRequested);
    }
}
