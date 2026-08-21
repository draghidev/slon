using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Types;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Low-level protocol construction and driving helpers. Tests own every returned protocol; production-like
// pooled placement is exercised through AdoTestPool, where selection and flow enqueue are one atomic step.
static class PgTestPool
{
    internal const int IsolatedConnectionLimit = 4;

    // Bound explicit multi-protocol stress independently of machine width.
    internal static readonly int MaxConnections = Math.Max(2,
        int.TryParse(Environment.GetEnvironmentVariable("PG_TEST_POOL_MAX"), out var m) && m > 0
            ? m
            : Environment.ProcessorCount);

    internal static PgClientOptions NewOptions() => new()
    {
        EndPoint = TestEndPoint.Default,
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
        Ssl = new() { Mode = PostgreSqlSslMode.Disable }
    };

    internal static async ValueTask<IsolatedProtocols> NewIsolatedProtocolsAsync(int count)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, IsolatedConnectionLimit);
        var protocols = new PgClientProtocol[count];
        var created = 0;
        try
        {
            for (; created < protocols.Length; created++)
                protocols[created] = await NewIsolatedAsync().ConfigureAwait(false);
            return new(protocols);
        }
        catch
        {
            for (var i = 0; i < created; i++)
                await protocols[i].DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal sealed class IsolatedProtocols(PgClientProtocol[] protocols) : IAsyncDisposable
    {
        public PgClientProtocol[] Items { get; } = protocols;

        public async ValueTask DisposeAsync()
        {
            foreach (var protocol in Items)
                await protocol.DisposeAsync().ConfigureAwait(false);
        }
    }

    // Construct a fresh, non-pooled protocol the caller owns end to end. Use in tests that fault the wire,
    // destroy the protocol, or need custom heartbeat/timeout settings. Standalone heartbeat (no pool callback),
    // so flow activation timeouts work without a pool driving the tick.
    internal static async Task<PgClientProtocol> NewIsolatedAsync(Action<PgClientProtocolOptions>? configureOptions = null)
    {
        var options = NewOptions();
        var transport = await SocketStreamConnection.ConnectAsync(options.EndPoint);
        var protocolOptions = new PgClientProtocolOptions(options)
        {
            BackendProvider = DefaultPostgreSqlBackendProvider.Instance,
            // Tests commonly hold sender settlement and PostgreSQL locks deliberately. Individual
            // convergence tests override this; the shared helper must not turn harness gates into
            // ten-second protocol failures under parallel suite pressure.
            CancellationTimeout = TimeSpan.FromMinutes(1)
        };
        configureOptions?.Invoke(protocolOptions);
        var protocol = PgClientProtocol.Create(protocolOptions);
        await protocol.StartAsync(options, transport);
        return protocol;
    }

    internal static Func<int, int, CancellationToken, ValueTask<CancelRequestState>> CreateCancelSender(PgClientOptions options)
        => async (processId, secretKey, cancellationToken) =>
        {
            var transport = await SocketStreamConnection.ConnectAsync(
                options.EndPoint, cancellationToken: cancellationToken).ConfigureAwait(false);
            Exception? error = null;
            try
            {
                await PgClientProtocol.SendCancelRequestAsync(
                    transport, processId, secretKey, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                error = ex;
                throw;
            }
            finally
            {
                transport.Abort();
                await transport.Writer.CompleteAsync(error).ConfigureAwait(false);
                await transport.Reader.CompleteAsync().ConfigureAwait(false);
            }
            return CancelRequestState.Sent;
        };

    // Sync flow exerciser shared across the Pg-layer tests. Driving CommandFlow directly so
    // the assertions attribute to the protocol, not to any ADO surface.
    internal static Task RunSync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: false, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetEnumerator();
        while (e.MoveNext()) { }
        // Sync Dispose, like a real sync consumer. DisposeAsync's await-drain can pend on pipeline
        // retirement and resume on a TP thread, which breaks the caller-thread assertions awaiting this.
        e.Dispose();
        return Task.CompletedTask;
    }

    internal static async Task RunAsync(PgClientProtocol protocol, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

}
