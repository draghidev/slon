using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Slon.Pg.Protocol.Flows;
using Slon.Runtime;
using Slon.Transport;

namespace Slon.Pg.Protocol;

// Full wire-session construction shared by raw protocol hosts and PgConnection. This layer owns
// transport connection, TLS negotiation/fallback, protocol options, startup, and CancelRequest.
// Pooling, prepared-statement tracking, and ADO connection lifetime belong to PgConnectionFactory.
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed class PgClientProtocolFactory
{
    readonly PgClientOptions _clientOptions;
    readonly TransportConnection.Factory _transportConnectionFactory;
    readonly Action<PgClientProtocolOptions>? _configureOptions;
    readonly PgClientProtocolOptions? _sharedOptions;
    readonly ILogger _cancellationLogger;

    public PgClientProtocolFactory(PgClientOptions clientOptions,
        TransportConnection.Factory transportConnectionFactory,
        Action<PgClientProtocolOptions>? configureOptions = null)
    {
        _clientOptions = clientOptions;
        _transportConnectionFactory = transportConnectionFactory;
        _configureOptions = configureOptions;
        _cancellationLogger = clientOptions.LoggerFactory.CreateLogger("Slon.Pg.Cancellation");
        if (configureOptions is null)
            _sharedOptions = new(clientOptions) { CancelSender = SendCancelAsync };
    }

    PgClientProtocolOptions CreateOptions()
    {
        if (_sharedOptions is not null)
            return _sharedOptions;
        var options = new PgClientProtocolOptions(_clientOptions) { CancelSender = SendCancelAsync };
        _configureOptions!.Invoke(options);
        return options;
    }

    public PgClientProtocol Create(TimeSpan timeout = default)
        => Create(static startup =>
        {
            startup.Start();
            return startup.Protocol;
        }, timeout);

    public ValueTask<PgClientProtocol> CreateAsync(CancellationToken cancellationToken = default)
        => CreateAsync(static async (startup, token) =>
        {
            await startup.StartAsync(cancellationToken: token).ConfigureAwait(false);
            return startup.Protocol;
        }, cancellationToken);

    internal TResult Create<TResult>(
        Func<PgClientProtocol.Startup, TResult> create,
        TimeSpan timeout = default)
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
        catch (Exception ex) when (ShouldRetry(
                   _clientOptions, connected, encrypted, protocolStarted, ex))
        {
            connected = encrypted = false;
            return CreateAttempt(_clientOptions.WithSsl(_clientOptions.Ssl.CreateFallback()));
        }

        TResult CreateAttempt(PgClientOptions options)
        {
            X509Certificate? remoteCertificate = null;
            var transport = Connect(options, deadline, OnTlsEstablished);
            connected = true;
            try
            {
                if (options.Ssl.ShouldNegotiateTls(options.EndPoint)
                    && PgClientProtocol.NegotiateSsl(
                        transport, options.Ssl.Mode, deadline.GetRemaining()))
                    transport = Upgrade(options, transport, deadline.GetRemaining(), OnTlsEstablished);

                var startup = new StartupFlow(async: false, options, remoteCertificate,
                    deadline.GetRemaining());
                var protocol = PgClientProtocol.Create(protocolOptions);
                return create(new(protocol, transport, startup,
                    () => protocolStarted = true));
            }
            catch (Exception ex)
            {
                ReleaseTransport(transport, ex);
                throw;
            }

            void OnTlsEstablished(X509Certificate? certificate)
            {
                encrypted = true;
                remoteCertificate = certificate;
            }
        }
    }

    internal async ValueTask<TResult> CreateAsync<TResult>(
        Func<PgClientProtocol.Startup, CancellationToken, ValueTask<TResult>> create,
        CancellationToken cancellationToken = default)
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
        catch (Exception ex) when (ShouldRetry(
                   _clientOptions, connected, encrypted, protocolStarted, ex))
        {
            connected = encrypted = false;
            return await CreateAttemptAsync(
                _clientOptions.WithSsl(_clientOptions.Ssl.CreateFallback())).ConfigureAwait(false);
        }

        async ValueTask<TResult> CreateAttemptAsync(PgClientOptions options)
        {
            TransportConnection? transport = null;
            X509Certificate? remoteCertificate = null;
            try
            {
                transport = await ConnectAsync(options, token, OnTlsEstablished)
                    .ConfigureAwait(false);
                connected = true;
                if (options.Ssl.ShouldNegotiateTls(options.EndPoint)
                    && await PgClientProtocol.NegotiateSslAsync(
                        transport, options.Ssl.Mode, token).ConfigureAwait(false))
                    transport = await UpgradeAsync(options, transport, token, OnTlsEstablished)
                        .ConfigureAwait(false);

                var startup = new StartupFlow(async: true, options, remoteCertificate,
                    options.ConnectionTimeout);
                var protocol = PgClientProtocol.Create(protocolOptions);
                return await create(new(protocol, transport, startup,
                    () => protocolStarted = true), token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (transport is not null)
                    ReleaseTransport(transport, ex);
                if (token.IsCancellationRequested)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    token.ThrowIfCancellationRequested();
                }
                throw;
            }

            void OnTlsEstablished(X509Certificate? certificate)
            {
                encrypted = true;
                remoteCertificate = certificate;
            }
        }
    }

    // Side-channel cancel orchestration. Apply the main connection's TLS policy before sending the
    // CancelRequest, then dispose the temporary transport on every path.
    internal async ValueTask<CancelRequestState> SendCancelAsync(
        int processId, int secretKey, CancellationToken cancellationToken)
    {
        using var timeout = CreateTimeoutSource(cancellationToken, _clientOptions.ConnectionTimeout);
        var token = timeout.Token;
        TransportConnection? transport = null;
        try
        {
            transport = await ConnectAsync(_clientOptions, token, static _ => { }).ConfigureAwait(false);
            if (_clientOptions.Ssl.ShouldNegotiateTls(_clientOptions.EndPoint)
                && await PgClientProtocol.NegotiateSslAsync(
                    transport, _clientOptions.Ssl.Mode, token).ConfigureAwait(false))
                transport = await UpgradeAsync(
                    _clientOptions, transport, token, static _ => { }).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (!cancellationToken.IsCancellationRequested)
                SlonLogMessages.CancellationRequestFailed(
                    _cancellationLogger, ex, CancelRequestState.NotSent);
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
            await PgClientProtocol.SendCancelRequestAsync(
                transport, processId, secretKey, token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            sendError = ex;
            delivery = CancelRequestState.Unknown;
            if (!cancellationToken.IsCancellationRequested)
                SlonLogMessages.CancellationRequestFailed(_cancellationLogger, ex, delivery);
        }
        finally
        {
            // The server tears down its side after CancelRequest. Abort the socket and complete both
            // endpoints so buffers return; an error-completed writer discards any partial packet.
            transport.Abort();
            await transport.Writer.CompleteAsync(sendError).ConfigureAwait(false);
            await transport.Reader.CompleteAsync().ConfigureAwait(false);
        }
        return delivery;
    }

    static CancellationTokenSource CreateTimeoutSource(
        CancellationToken cancellationToken, TimeSpan timeout)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (timeout != default)
            source.CancelAfter(timeout);
        return source;
    }

    TransportConnection Connect(
        PgClientOptions options, Deadline deadline, Action<X509Certificate?> onTlsEstablished)
    {
        if (!options.Ssl.ShouldUseDirectTls(options.EndPoint))
            return _transportConnectionFactory.Connect(deadline.GetRemaining());

        SslStream? ssl = null;
        var transport = _transportConnectionFactory.ConnectTransformed(stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false), deadline.GetRemaining());
        try
        {
            var remaining = deadline.GetRemainingMilliseconds();
            ssl!.ReadTimeout = remaining;
            ssl.WriteTimeout = remaining;
            ssl.AuthenticateAsClient(options.Ssl.CreateAuthenticationOptions(options.EndPoint));
            onTlsEstablished(ssl.RemoteCertificate);
            return transport;
        }
        catch (Exception ex)
        {
            ReleaseTransport(transport, ex);
            throw;
        }
    }

    async ValueTask<TransportConnection> ConnectAsync(
        PgClientOptions options, CancellationToken cancellationToken,
        Action<X509Certificate?> onTlsEstablished)
    {
        if (!options.Ssl.ShouldUseDirectTls(options.EndPoint))
            return await _transportConnectionFactory.ConnectAsync(cancellationToken).ConfigureAwait(false);

        SslStream? ssl = null;
        var transport = await _transportConnectionFactory.ConnectTransformedAsync(stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false), cancellationToken)
            .ConfigureAwait(false);
        try
        {
            await ssl!.AuthenticateAsClientAsync(
                options.Ssl.CreateAuthenticationOptions(options.EndPoint), cancellationToken)
                .ConfigureAwait(false);
            onTlsEstablished(ssl.RemoteCertificate);
            return transport;
        }
        catch (Exception ex)
        {
            ReleaseTransport(transport, ex);
            throw;
        }
    }

    TransportConnection Upgrade(PgClientOptions options, TransportConnection connection,
        TimeSpan timeout, Action<X509Certificate?> onTlsEstablished)
    {
        SslStream? ssl = null;
        var upgraded = _transportConnectionFactory.Upgrade(connection, stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false));
        try
        {
            var timeoutMilliseconds = Deadline.ToTimeoutMilliseconds(timeout);
            ssl!.ReadTimeout = timeoutMilliseconds;
            ssl.WriteTimeout = timeoutMilliseconds;
            ssl.AuthenticateAsClient(options.Ssl.CreateAuthenticationOptions(options.EndPoint));
            onTlsEstablished(ssl.RemoteCertificate);
            return upgraded;
        }
        catch (Exception ex)
        {
            ReleaseTransport(upgraded, ex);
            throw;
        }
    }

    async ValueTask<TransportConnection> UpgradeAsync(PgClientOptions options,
        TransportConnection connection, CancellationToken cancellationToken,
        Action<X509Certificate?> onTlsEstablished)
    {
        SslStream? ssl = null;
        var upgraded = _transportConnectionFactory.Upgrade(connection, stream =>
            ssl = new SslStream(stream, leaveInnerStreamOpen: false));
        try
        {
            await ssl!.AuthenticateAsClientAsync(
                options.Ssl.CreateAuthenticationOptions(options.EndPoint), cancellationToken)
                .ConfigureAwait(false);
            onTlsEstablished(ssl.RemoteCertificate);
            return upgraded;
        }
        catch (Exception ex)
        {
            ReleaseTransport(upgraded, ex);
            throw;
        }
    }

    // Fallback applies only after the socket connected and before protocol startup completed.
    // Prefer retries only after a completed TLS handshake; Allow starts plaintext.
    internal static bool ShouldRetry(PgClientOptions options, bool connected, bool encrypted,
        bool protocolStarted, Exception exception)
        => options.EndPoint is not UnixDomainSocketEndPoint
            && connected && !protocolStarted && exception is not OperationCanceledException
            && (options.Ssl.Mode is PostgreSqlSslMode.Prefer && encrypted
                    && options.Ssl.ChannelBinding is not PostgreSqlChannelBinding.Require
                || options.Ssl.Mode is PostgreSqlSslMode.Allow && !encrypted);

    // Start owns the transport once it initializes. Double release after its pre-pipeline failure is
    // harmless because Abort is idempotent and completed endpoints ignore repeated completion.
    static void ReleaseTransport(TransportConnection transport, Exception reason)
    {
        transport.Abort();
        transport.Writer.Complete(reason);
        transport.Reader.Complete(reason);
    }
}
