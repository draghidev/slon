using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pooling;
using Slon.Transport;

namespace Slon;

// Adds the ADO/pool-owned PgConnection lifetime to the lower raw protocol factory.
sealed class PgConnectionFactory(
    PgClientOptions clientOptions,
    TransportConnection.Factory transportConnectionFactory,
    CommandTracker? tracker = null,
    Action<PgClientProtocolOptions>? configureOptions = null)
    : IPoolConnectionFactory<PgConnection>
{
    readonly PgClientProtocolFactory _protocolFactory = new(
        clientOptions, transportConnectionFactory, configureOptions);

    PgConnection Create(ConnectionPoolContext<PgConnection>? poolContext, TimeSpan timeout = default)
        => _protocolFactory.Create(startup =>
            PgConnection.Create(startup, tracker, poolContext), timeout);

    ValueTask<PgConnection> CreateAsync(ConnectionPoolContext<PgConnection>? poolContext,
        CancellationToken cancellationToken = default)
        => _protocolFactory.CreateAsync(
            (startup, token) => PgConnection.CreateAsync(startup, tracker, poolContext, token),
            cancellationToken);

    public PgConnection Create(TimeSpan timeout = default)
        => Create(null, timeout);

    public ValueTask<PgConnection> CreateAsync(CancellationToken cancellationToken = default)
        => CreateAsync(null, cancellationToken);

    internal ValueTask<CancelRequestState> SendCancelAsync(
        int processId, int secretKey, CancellationToken cancellationToken)
        => _protocolFactory.SendCancelAsync(processId, secretKey, cancellationToken);

    internal static bool ShouldRetry(PgClientOptions options, bool connected, bool encrypted,
        bool protocolStarted, Exception exception)
        => PgClientProtocolFactory.ShouldRetry(
            options, connected, encrypted, protocolStarted, exception);

    PgConnection IPoolConnectionFactory<PgConnection>.Create(
        ConnectionPoolContext<PgConnection> poolContext, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(poolContext);
        return Create(poolContext, timeout);
    }

    ValueTask<PgConnection> IPoolConnectionFactory<PgConnection>.CreateAsync(
        ConnectionPoolContext<PgConnection> poolContext,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(poolContext);
        return CreateAsync(poolContext, cancellationToken);
    }
}
