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
    async ValueTask<CancelRequestState> SendCancelAsync(int processId, int secretKey, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_clientOptions.ConnectionTimeout);
        var token = timeout.Token;
        TransportConnection transport;
        try
        {
            transport = await _transportConnectionFactory.ConnectAsync(token).ConfigureAwait(false);
        }
        catch
        {
            return CancelRequestState.NotSent;
        }
        Exception? sendError = null;
        var delivery = CancelRequestState.Sent;
        try
        {
            await CancelRequest.SendAsync(transport, processId, secretKey, token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            sendError = ex;
            delivery = CancelRequestState.Unknown;
        }
        finally
        {
            // TransportConnection has no Dispose surface; release via the abortive close (socket) plus
            // endpoint completion (returns the pooled buffers). The server tears down its side after a
            // CancelRequest, so an abortive RST on ours is fine. Error-complete the writer with sendError
            // so a packet half-written before a fault is discarded rather than flushed onto the closed
            // socket (a clean send leaves nothing buffered, so the null reason never starts a flush).
            transport.Abort();
            await transport.Writer.CompleteAsync(sendError).ConfigureAwait(false);
            await transport.Reader.CompleteAsync().ConfigureAwait(false);
        }
        return delivery;
    }

    PgConnection Create(ConnectionPoolContext<PgConnection>? poolContext, TimeSpan timeout = default)
    {
        var transport = _transportConnectionFactory.Connect(timeout);
        try
        {
            return PgConnection.Create(CreateOptions(), _clientOptions, transport, _tracker, poolContext, timeout);
        }
        catch (Exception ex)
        {
            ReleaseTransport(transport, ex);
            throw;
        }
    }

    async ValueTask<PgConnection> CreateAsync(ConnectionPoolContext<PgConnection>? poolContext, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var transport = await _transportConnectionFactory.ConnectAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await PgConnection.CreateAsync(CreateOptions(), _clientOptions, transport, _tracker, poolContext, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ReleaseTransport(transport, ex);
            throw;
        }
    }

    // The transport is the factory's until PgConnection takes ownership (inside its protocol.Start). A
    // throw before that - pool/idle-signal wiring, options, or Start's own pre-pipeline failure (which
    // self-releases; Abort is idempotent and Complete is a no-op once completed, so the double is
    // harmless) - would orphan the just-connected socket. Release it through the same vocabulary the
    // protocol uses: abortive close (socket) + error-complete the endpoints (discard, return buffers).
    static void ReleaseTransport(TransportConnection transport, Exception reason)
    {
        transport.Abort();
        transport.Writer.Complete(reason);
        transport.Reader.Complete(reason);
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
