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
            _sharedOptions = new PgClientProtocolOptions(clientOptions) { CancelSender = SendCancelAsync };
    }

    PgClientProtocolOptions CreateOptions()
    {
        if (_sharedOptions is not null)
            return _sharedOptions;
        var options = new PgClientProtocolOptions(_clientOptions);
        options.CancelSender = SendCancelAsync;
        _configureOptions!.Invoke(options);
        return options;
    }

    // Side-channel cancel orchestration: opens a fresh transport matching the main connection's
    // policy, delivers the CancelRequest via the protocol-layer wire helper, disposes the
    // transport on every path. Passed to PgClientProtocolOptions.CancelSender so the protocol
    // layer can fire-and-forget without knowing about transports.
    async ValueTask SendCancelAsync(int processId, int secretKey, CancellationToken cancellationToken)
    {
        var transport = await _transportConnectionFactory.ConnectAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await CancelRequest.SendAsync(transport, processId, secretKey, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (transport is IAsyncDisposable disposable)
                await disposable.DisposeAsync().ConfigureAwait(false);
        }
    }

    PgConnection Create(ConnectionPoolContext<PgConnection>? poolContext, TimeSpan timeout = default)
    {
        var transport = _transportConnectionFactory.Connect(timeout);
        return PgConnection.Create(CreateOptions(), _clientOptions, transport, _tracker, poolContext, timeout);
    }

    async ValueTask<PgConnection> CreateAsync(ConnectionPoolContext<PgConnection>? poolContext, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var transport = await _transportConnectionFactory.ConnectAsync(cancellationToken).ConfigureAwait(false);
        return await PgConnection.CreateAsync(CreateOptions(), _clientOptions, transport, _tracker, poolContext, cancellationToken).ConfigureAwait(false);
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
