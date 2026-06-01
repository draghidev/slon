using Slon.Pools;
using Slon.Transport;

namespace Slon.Pg.Protocol;

sealed class PgClientProtocolFactory : IPoolConnectionFactory<PgClientProtocol>
{
    readonly PgClientOptions _clientOptions;
    readonly TransportConnection.Factory _transportConnectionFactory;
    readonly Action<PgClientProtocolOptions>? _configureOptions;
    readonly PgClientProtocolOptions? _sharedOptions;

    public PgClientProtocolFactory(PgClientOptions clientOptions, TransportConnection.Factory transportConnectionFactory, Action<PgClientProtocolOptions>? configureOptions = null)
    {
        _clientOptions = clientOptions;
        _transportConnectionFactory = transportConnectionFactory;
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

    PgClientProtocol Create(ConnectionPoolContext<PgClientProtocol>? poolContext, TimeSpan timeout = default)
    {
        var protocol = PgClientProtocol.Create(CreateOptions());
        var connection = _transportConnectionFactory.Connect(timeout);
        protocol.Start(_clientOptions, connection, poolContext, timeout);
        return protocol;
    }

    async ValueTask<PgClientProtocol> CreateAsync(ConnectionPoolContext<PgClientProtocol>? poolContext, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var protocol = PgClientProtocol.Create(CreateOptions());
        var connection = await _transportConnectionFactory.ConnectAsync(cancellationToken).ConfigureAwait(false);
        await protocol.StartAsync(_clientOptions, connection, poolContext, cancellationToken).ConfigureAwait(false);
        return protocol;
    }

    public PgClientProtocol Create(TimeSpan timeout = default)
        => Create(null, timeout);

    public ValueTask<PgClientProtocol> CreateAsync(CancellationToken cancellationToken = default)
        => CreateAsync(null, cancellationToken);

    PgClientProtocol IPoolConnectionFactory<PgClientProtocol>.Create(ConnectionPoolContext<PgClientProtocol> poolContext, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(poolContext);
        return Create(poolContext, timeout);
    }

    ValueTask<PgClientProtocol> IPoolConnectionFactory<PgClientProtocol>.CreateAsync(ConnectionPoolContext<PgClientProtocol> poolContext, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(poolContext);
        return CreateAsync(poolContext, cancellationToken);
    }
}
