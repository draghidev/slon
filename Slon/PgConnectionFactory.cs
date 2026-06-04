using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pools;
using Slon.Transport;

namespace Slon;

sealed class PgConnectionFactory : IPoolConnectionFactory<PgConnection>
{
    readonly PgClientOptions _clientOptions;
    readonly TransportConnection.Factory _transportConnectionFactory;
    readonly CommandTracker? _tracker;
    readonly Action<PgClientProtocolOptions>? _configureOptions;
    readonly PgClientProtocolOptions? _sharedOptions;

    public PgConnectionFactory(PgClientOptions clientOptions, TransportConnection.Factory transportConnectionFactory, CommandTracker? tracker = null, Action<PgClientProtocolOptions>? configureOptions = null)
    {
        _clientOptions = clientOptions;
        _transportConnectionFactory = transportConnectionFactory;
        _tracker = tracker;
        _configureOptions = configureOptions;
        if (configureOptions is null)
            _sharedOptions = new PgClientProtocolOptions(clientOptions);
    }

    PgClientProtocolOptions CreateOptions()
    {
        if (_sharedOptions is not null)
            return _sharedOptions;
        var options = new PgClientProtocolOptions(_clientOptions);
        _configureOptions!.Invoke(options);
        return options;
    }

    PgConnection Create(ConnectionPoolContext<PgConnection>? poolContext, TimeSpan timeout = default)
    {
        var protocol = PgClientProtocol.Create(CreateOptions());
        var pgConnection = new PgConnection(protocol, _tracker, _clientOptions.MaintenanceInterval);
        var connection = _transportConnectionFactory.Connect(timeout);
        pgConnection.Start(_clientOptions, connection, poolContext, timeout);
        return pgConnection;
    }

    async ValueTask<PgConnection> CreateAsync(ConnectionPoolContext<PgConnection>? poolContext, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var protocol = PgClientProtocol.Create(CreateOptions());
        var pgConnection = new PgConnection(protocol, _tracker, _clientOptions.MaintenanceInterval);
        var connection = await _transportConnectionFactory.ConnectAsync(cancellationToken).ConfigureAwait(false);
        await pgConnection.StartAsync(_clientOptions, connection, poolContext, cancellationToken).ConfigureAwait(false);
        return pgConnection;
    }

    public PgConnection Create(TimeSpan timeout = default)
        => Create(null, timeout);

    public ValueTask<PgConnection> CreateAsync(CancellationToken cancellationToken = default)
        => CreateAsync(null, cancellationToken);

    PgConnection IPoolConnectionFactory<PgConnection>.Create(ConnectionPoolContext<PgConnection> poolContext, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(poolContext);
        return Create(poolContext, timeout);
    }

    ValueTask<PgConnection> IPoolConnectionFactory<PgConnection>.CreateAsync(ConnectionPoolContext<PgConnection> poolContext, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(poolContext);
        return CreateAsync(poolContext, cancellationToken);
    }
}
