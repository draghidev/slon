using System.Collections.Concurrent;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Assembly-scoped pool of PgClientProtocol instances for low-level Pg tests that complete
// their flows cleanly. Saves the connect + startup handshake per test (the dominant cost in
// clean-completion suites). Tests that intentionally fault the wire or destroy the protocol
// (RecoveryTests, ProtocolCompletionTests) MUST use NewIsolatedAsync instead so a regression
// in recovery doesn't drag the broader suite with it through a poisoned shared protocol.
//
// Contract for LeaseAsync callers: at dispose time the protocol must be idle (no flow in
// flight, wire on a fresh RFQ). The lease blindly returns the instance to the bag; a poisoned
// return is the caller's bug, and the next user will surface it loudly.
//
static class PgTestPool
{
    static readonly ConcurrentBag<PgClientProtocol> _idle = new();
    static readonly ConcurrentBag<PgClientProtocol> _allCreated = new();

    internal static PgClientOptions NewOptions() => new()
    {
        EndPoint = new IPEndPoint(IPAddress.Loopback, 5432),
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
    };

    // Lease a clean protocol from the shared pool. Use ONLY in tests that complete their
    // flows cleanly. The returned struct's DisposeAsync puts the protocol back in the bag.
    internal static async ValueTask<Lease> LeaseAsync()
    {
        if (_idle.TryTake(out var protocol))
            return new Lease(protocol);
        protocol = await CreateAsync();
        _allCreated.Add(protocol);
        return new Lease(protocol);
    }

    // Construct a fresh, non-pooled protocol the caller owns end to end. Use in tests that
    // fault the wire, destroy the protocol, or otherwise leave it in a state unfit for reuse.
    // Pass configureOptions when the test needs custom heartbeat/timeout/etc. settings.
    internal static Task<PgClientProtocol> NewIsolatedAsync(Action<PgClientProtocolOptions>? configureOptions = null)
        => CreateAsync(configureOptions);

    static async Task<PgClientProtocol> CreateAsync(Action<PgClientProtocolOptions>? configureOptions = null)
    {
        var options = NewOptions();
        var transport = await SocketStreamConnection.ConnectAsync((IPEndPoint)options.EndPoint);
        var protocolOptions = new PgClientProtocolOptions(options);
        configureOptions?.Invoke(protocolOptions);
        var protocol = PgClientProtocol.Create(protocolOptions);
        await protocol.StartAsync(options, transport);
        return protocol;
    }

    // Sync flow exerciser shared across the Pg-layer tests. Driving CommandFlow directly so
    // the assertions attribute to the protocol, not to any ADO surface.
    internal static async Task RunSync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: false, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetEnumerator();
        while (e.MoveNext()) { }
        await e.DisposeAsync();
    }

    internal static async Task RunAsync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    // Drains every protocol ever handed out. Called from TestAssemblyHooks so the assembly's
    // single permitted [AssemblyCleanup] sweeps every helper pool.
    internal static async Task DrainAsync()
    {
        while (_allCreated.TryTake(out var p))
        {
            try { await p.CompleteAsync(); }
            catch { }
        }
    }

    internal readonly struct Lease : IAsyncDisposable
    {
        public PgClientProtocol Protocol { get; }
        internal Lease(PgClientProtocol protocol) { Protocol = protocol; }

        public ValueTask DisposeAsync()
        {
            _idle.Add(Protocol);
            return default;
        }
    }
}
