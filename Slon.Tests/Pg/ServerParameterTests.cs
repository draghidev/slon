using System.Collections.Immutable;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Types;

namespace Slon.Tests.Pg;

[TestClass]
public class ServerParameterTests
{
    [TestMethod]
    public void BackendInfo_SharesTheOwnedStartupSnapshot()
    {
        var snapshot = ImmutableDictionary.CreateRange(StringComparer.Ordinal,
            new Dictionary<string, string>
            {
                ["server_version"] = "17.0",
                ["server_encoding"] = "UTF8"
            });

        var info = new PgBackendInfoBuilder(snapshot).Build();

        Assert.AreSame(snapshot, info.StartupParameters);
    }

    [TestMethod]
    public void ParameterState_MaterializesOnlyDivergenceAndSnapsBackToBase()
    {
        var state = new PgServerParameterState();
        state.Set("first", "base-1");
        state.Set("second", "base-2");
        var @base = state.CompleteStartup();

        state.Set("first", "base-1");
        state.CommitFlow();
        Assert.AreEqual(0, state.Revision);
        Assert.AreSame(@base, state.CurrentSnapshot);

        state.Set("first", "transient");
        state.Set("first", "base-1");
        state.CommitFlow();
        Assert.AreEqual(0, state.Revision,
            "changes restored inside one flow should not publish a new generation");
        Assert.AreSame(@base, state.CurrentSnapshot);

        state.Set("first", "changed-1");
        state.Set("second", "changed-2");
        Assert.AreSame(@base, state.CurrentSnapshot,
            "the active flow must not publish partially observed ParameterStatus state");
        state.CommitFlow();
        var changed = state.CurrentSnapshot;
        Assert.AreEqual(1, state.Revision);
        Assert.AreEqual("changed-1", changed["first"]);
        Assert.AreEqual("changed-2", changed["second"]);
        Assert.AreSame(changed, state.CurrentSnapshot);

        state.Set("first", "base-1");
        state.CommitFlow();
        Assert.AreNotSame(@base, state.CurrentSnapshot,
            "one remaining delta must keep the connection divergent");
        state.Set("second", "base-2");
        state.CommitFlow();
        Assert.AreSame(@base, state.CurrentSnapshot);
    }

    [TestMethod]
    public void ScopeReset_UsesTheBackendCapabilitySnapshot()
    {
        var options = new ScopeResetOptions();
        var capabilities = new PgBackendCapabilities
        {
            SupportsCloseAll = true,
            SupportsResetAll = false,
            SupportsSessionAuthorization = false,
            SupportsUnlisten = false,
            SupportsAdvisoryLocks = false,
            SupportsDiscardTemp = true
        };

        var command = options.ResolveCommand(capabilities)!;

        StringAssert.Contains(command, "CLOSE ALL");
        StringAssert.Contains(command, "DISCARD TEMP");
        Assert.IsFalse(command.Contains("UNLISTEN", StringComparison.Ordinal));
        Assert.IsFalse(command.Contains("pg_advisory_unlock_all", StringComparison.Ordinal));
        Assert.IsFalse(command.Contains("RESET ALL", StringComparison.Ordinal));
        Assert.IsFalse(command.Contains("SESSION AUTHORIZATION", StringComparison.Ordinal));
    }
}
