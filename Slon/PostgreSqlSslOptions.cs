using System.Net;
using System.Net.Security;
using System.Net.Sockets;

namespace Slon;

public enum PostgreSqlSslMode
{
    /// <summary>Connect without TLS.</summary>
    Disable,
    /// <summary>Try plaintext first, then retry with TLS when startup fails.</summary>
    Allow,
    /// <summary>Try TLS first, then retry without it when encrypted startup fails.</summary>
    Prefer,
    /// <summary>Require encryption without validating the server certificate.</summary>
    Require,
    /// <summary>Require encryption and validate the certificate chain.</summary>
    VerifyCA,
    /// <summary>Require encryption and validate both the certificate chain and host name.</summary>
    VerifyFull
}

public enum PostgreSqlSslNegotiation
{
    /// <summary>Use direct TLS for an asserted PostgreSQL 17 or newer endpoint; otherwise use an SSLRequest.</summary>
    Automatic,
    /// <summary>Negotiate TLS with PostgreSQL's SSLRequest message.</summary>
    PostgreSql,
    /// <summary>Begin TLS immediately after connecting. The endpoint must support direct TLS.</summary>
    Direct
}

public enum PostgreSqlChannelBinding
{
    /// <summary>Do not use SCRAM channel binding.</summary>
    Disable,
    /// <summary>Use SCRAM channel binding when both PostgreSQL and the TLS transport support it.</summary>
    Prefer,
    /// <summary>Require SCRAM-SHA-256-PLUS authentication with channel binding.</summary>
    Require
}

public sealed class PostgreSqlSslOptions
{
    static readonly SslApplicationProtocol PostgreSqlAlpn = new("postgresql");
    static readonly RemoteCertificateValidationCallback TrustServerCertificate = static (_, _, _, _) => true;
    static readonly RemoteCertificateValidationCallback VerifyCertificateAuthority = static (_, _, _, errors)
        => (errors & ~SslPolicyErrors.RemoteCertificateNameMismatch) is SslPolicyErrors.None;

    public PostgreSqlSslMode Mode { get; set; } = PostgreSqlSslMode.Prefer;

    public PostgreSqlChannelBinding ChannelBinding { get; set; } = PostgreSqlChannelBinding.Prefer;
    public PostgreSqlSslNegotiation Negotiation { get; set; }
    /// <summary>
    /// Declares the PostgreSQL version implemented by the endpoint itself. An asserted version of
    /// 17 or newer selects direct TLS when <see cref="Negotiation"/> is automatic.
    /// </summary>
    public Version? EndpointVersion { get; set; }
    /// <summary>Configures the fresh TLS authentication options used for each connection.</summary>
    public Action<SslClientAuthenticationOptions>? ConfigureClientAuthenticationOptions { get; set; }

    internal bool UsesTlsInitially
        => Mode is PostgreSqlSslMode.Prefer or PostgreSqlSslMode.Require
            or PostgreSqlSslMode.VerifyCA or PostgreSqlSslMode.VerifyFull;

    internal bool SupportsDirectNegotiation
        => Mode is PostgreSqlSslMode.Require or PostgreSqlSslMode.VerifyCA or PostgreSqlSslMode.VerifyFull;

    internal bool UseDirectNegotiation
        => SupportsDirectNegotiation
           && (Negotiation is PostgreSqlSslNegotiation.Direct
               || Negotiation is PostgreSqlSslNegotiation.Automatic && EndpointVersion?.Major >= 17);

    internal bool ShouldNegotiateTls(EndPoint endpoint)
        => endpoint is not UnixDomainSocketEndPoint && UsesTlsInitially && !UseDirectNegotiation;

    internal bool ShouldUseDirectTls(EndPoint endpoint)
        => endpoint is not UnixDomainSocketEndPoint && UseDirectNegotiation;

    internal PostgreSqlSslOptions Snapshot()
        => (PostgreSqlSslOptions)MemberwiseClone();

    internal PostgreSqlSslOptions CreateFallback()
    {
        var copy = Snapshot();
        copy.Mode = Mode is PostgreSqlSslMode.Prefer
            ? PostgreSqlSslMode.Disable
            : PostgreSqlSslMode.Require;
        copy.Negotiation = PostgreSqlSslNegotiation.Automatic;
        copy.EndpointVersion = null;
        return copy;
    }

    internal void Validate()
    {
        if (!Enum.IsDefined(Mode))
            throw new ArgumentOutOfRangeException(nameof(Mode));
        if (!Enum.IsDefined(Negotiation))
            throw new ArgumentOutOfRangeException(nameof(Negotiation));
        if (!Enum.IsDefined(ChannelBinding))
            throw new ArgumentOutOfRangeException(nameof(ChannelBinding));
        if (Mode is PostgreSqlSslMode.Disable && Negotiation is not PostgreSqlSslNegotiation.Automatic)
            throw new InvalidOperationException("TLS negotiation cannot be selected while TLS is disabled.");
        if (Negotiation is PostgreSqlSslNegotiation.Direct && !SupportsDirectNegotiation)
            throw new InvalidOperationException("Direct TLS requires Require, VerifyCA, or VerifyFull mode.");
    }

    internal SslClientAuthenticationOptions CreateAuthenticationOptions(EndPoint endpoint)
    {
        var options = new SslClientAuthenticationOptions
        {
            ApplicationProtocols = [PostgreSqlAlpn],
            TargetHost = endpoint switch
            {
                DnsEndPoint dns => dns.Host,
                IPEndPoint ip => ip.Address.ToString(),
                _ => "localhost"
            }
        };
        if (Mode is PostgreSqlSslMode.Require or PostgreSqlSslMode.Prefer)
            options.RemoteCertificateValidationCallback = TrustServerCertificate;
        else if (Mode is PostgreSqlSslMode.VerifyCA)
            options.RemoteCertificateValidationCallback = VerifyCertificateAuthority;
        var validationCallback = options.RemoteCertificateValidationCallback;
        ConfigureClientAuthenticationOptions?.Invoke(options);
        if (Mode is PostgreSqlSslMode.VerifyCA or PostgreSqlSslMode.VerifyFull
            && options.RemoteCertificateValidationCallback != validationCallback)
            throw new InvalidOperationException($"{Mode} cannot replace certificate validation.");
        if (UseDirectNegotiation && options.ApplicationProtocols?.Contains(PostgreSqlAlpn) is not true)
            throw new InvalidOperationException("Direct TLS requires the PostgreSQL ALPN protocol.");
        if (string.IsNullOrWhiteSpace(options.TargetHost))
            throw new InvalidOperationException("TLS authentication requires a target host.");
        return options;
    }
}
