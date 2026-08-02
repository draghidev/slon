using System.Net.Security;
using System.Net.Sockets;
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

    // Side-channel cancel orchestration. Apply the main connection's TLS policy before sending the
    // CancelRequest, then dispose the temporary transport on every path.
    internal async ValueTask<CancelRequestState> SendCancelAsync(int processId, int secretKey, CancellationToken cancellationToken)
    {
        using var timeout = CreateTimeoutSource(cancellationToken, _clientOptions.ConnectionTimeout);
        var token = timeout.Token;
        TransportConnection? transport = null;
        try
        {
            transport = await ConnectAsync(_clientOptions, token, static () => { }).ConfigureAwait(false);
            if (_clientOptions.Ssl.ShouldNegotiateTls(_clientOptions.EndPoint)
                && await PgClientProtocol.NegotiateSslAsync(
                    transport, _clientOptions.Ssl.Mode, token).ConfigureAwait(false))
                transport = await UpgradeAsync(
                    _clientOptions, transport, token, static () => { }).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (transport is not null)
            {
                transport.Abort();
                await transport.Writer.CompleteAsync(ex).ConfigureAwait(false);
                await transport.Reader.CompleteAsync().ConfigureAwait(false);
            }
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
        var deadline = new Deadline(timeout == default ? _clientOptions.ConnectionTimeout : timeout);
        var connected = false;
        var encrypted = false;
        var protocolStarted = false;
        var protocolOptions = CreateOptions();
        try
        {
            return CreateAttempt(_clientOptions);
        }
        catch (Exception ex) when (ShouldRetry(_clientOptions, connected, encrypted, protocolStarted, ex))
        {
            connected = encrypted = false;
            return CreateAttempt(_clientOptions.WithSsl(InverseFallback(_clientOptions.Ssl)));
        }

        PgConnection CreateAttempt(PgClientOptions options)
        {
            var transport = Connect(options, deadline, () => encrypted = true);
            connected = true;
            try
            {
                return PgConnection.Create(protocolOptions, options, transport, _tracker, poolContext,
                    deadline.GetRemaining(), (connection, remaining) =>
                        Upgrade(options, connection, remaining, () => encrypted = true),
                    () => protocolStarted = true);
            }
            catch (Exception ex)
            {
                ReleaseTransport(transport, ex);
                throw;
            }
        }
    }

    async ValueTask<PgConnection> CreateAsync(ConnectionPoolContext<PgConnection>? poolContext, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var timeout = CreateTimeoutSource(cancellationToken, _clientOptions.ConnectionTimeout);
        var token = timeout.Token;
        var connected = false;
        var encrypted = false;
        var protocolStarted = false;
        var protocolOptions = CreateOptions();
        try
        {
            return await CreateAttemptAsync(_clientOptions).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldRetry(_clientOptions, connected, encrypted, protocolStarted, ex))
        {
            connected = encrypted = false;
            return await CreateAttemptAsync(_clientOptions.WithSsl(InverseFallback(_clientOptions.Ssl))).ConfigureAwait(false);
        }

        async ValueTask<PgConnection> CreateAttemptAsync(PgClientOptions options)
        {
            var transport = await ConnectAsync(options, token, () => encrypted = true).ConfigureAwait(false);
            connected = true;
            try
            {
                return await PgConnection.CreateAsync(protocolOptions, options, transport, _tracker, poolContext,
                    token, (connection, ct) => UpgradeAsync(options, connection, ct, () => encrypted = true),
                    () => protocolStarted = true).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ReleaseTransport(transport, ex);
                throw;
            }
        }
    }

    static CancellationTokenSource CreateTimeoutSource(CancellationToken cancellationToken, TimeSpan timeout)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (timeout != default)
            source.CancelAfter(timeout);
        return source;
    }

    TransportConnection Connect(PgClientOptions options, Deadline deadline, Action onTlsEstablished)
    {
        if (!options.Ssl.ShouldUseDirectTls(options.EndPoint))
            return _transportConnectionFactory.Connect(deadline.GetRemaining());

        SslStream? ssl = null;
        var transport = _transportConnectionFactory.ConnectTransformed(stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false), deadline.GetRemaining());
        try
        {
            var remaining = ToStreamTimeout(deadline.GetRemaining());
            ssl!.ReadTimeout = remaining;
            ssl.WriteTimeout = remaining;
            ssl.AuthenticateAsClient(options.Ssl.CreateAuthenticationOptions(options.EndPoint));
            onTlsEstablished();
            return transport;
        }
        catch (Exception ex)
        {
            ReleaseTransport(transport, ex);
            throw;
        }
    }

    async ValueTask<TransportConnection> ConnectAsync(PgClientOptions options, CancellationToken cancellationToken, Action onTlsEstablished)
    {
        if (!options.Ssl.ShouldUseDirectTls(options.EndPoint))
            return await _transportConnectionFactory.ConnectAsync(cancellationToken).ConfigureAwait(false);

        SslStream? ssl = null;
        var transport = await _transportConnectionFactory.ConnectTransformedAsync(stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false), cancellationToken).ConfigureAwait(false);
        try
        {
            await ssl!.AuthenticateAsClientAsync(
                options.Ssl.CreateAuthenticationOptions(options.EndPoint), cancellationToken).ConfigureAwait(false);
            onTlsEstablished();
            return transport;
        }
        catch (Exception ex)
        {
            ReleaseTransport(transport, ex);
            throw;
        }
    }

    TransportConnection Upgrade(PgClientOptions options, TransportConnection connection, TimeSpan timeout, Action onTlsEstablished)
    {
        SslStream? ssl = null;
        var upgraded = _transportConnectionFactory.Upgrade(connection, stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false));
        try
        {
            var timeoutMilliseconds = ToStreamTimeout(timeout);
            ssl!.ReadTimeout = timeoutMilliseconds;
            ssl.WriteTimeout = timeoutMilliseconds;
            ssl.AuthenticateAsClient(options.Ssl.CreateAuthenticationOptions(options.EndPoint));
            onTlsEstablished();
            return upgraded;
        }
        catch (Exception ex)
        {
            ReleaseTransport(upgraded, ex);
            throw;
        }
    }

    async ValueTask<TransportConnection> UpgradeAsync(PgClientOptions options, TransportConnection connection,
        CancellationToken cancellationToken, Action onTlsEstablished)
    {
        SslStream? ssl = null;
        var upgraded = _transportConnectionFactory.Upgrade(connection, stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false));
        try
        {
            await ssl!.AuthenticateAsClientAsync(
                options.Ssl.CreateAuthenticationOptions(options.EndPoint), cancellationToken).ConfigureAwait(false);
            onTlsEstablished();
            return upgraded;
        }
        catch (Exception ex)
        {
            ReleaseTransport(upgraded, ex);
            throw;
        }
    }

    static int ToStreamTimeout(TimeSpan timeout)
        => timeout == Timeout.InfiniteTimeSpan
            ? Timeout.Infinite
            : (int)Math.Clamp(Math.Ceiling(timeout.TotalMilliseconds), 1, int.MaxValue);

    // Fallback applies only after the socket connected and before protocol startup completed.
    // Prefer retries only after a completed TLS handshake; Allow starts plaintext.
    static bool ShouldRetry(PgClientOptions options, bool connected, bool encrypted, bool protocolStarted, Exception exception)
        => options.EndPoint is not UnixDomainSocketEndPoint
            && connected && !protocolStarted && exception is not OperationCanceledException
            && (options.Ssl.Mode is PostgreSqlSslMode.Prefer && encrypted
                || options.Ssl.Mode is PostgreSqlSslMode.Allow && !encrypted);

    static PostgreSqlSslOptions InverseFallback(PostgreSqlSslOptions options)
        => options with
        {
            Mode = options.Mode is PostgreSqlSslMode.Prefer
                ? PostgreSqlSslMode.Disable
                : PostgreSqlSslMode.Require,
            Negotiation = PostgreSqlSslNegotiation.Automatic,
            EndpointVersion = null
        };

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
