using System.Net;

namespace Slon;

/// Configures PostgreSQL GSS or SSPI integrated-security authentication.
public sealed class PostgreSqlIntegratedSecurityOptions
{
    /// Gets the network credential used for authentication.
    public NetworkCredential Credential { get; init; } = CredentialCache.DefaultNetworkCredentials;
    /// Gets the Kerberos service name. The default is <c>postgres</c>.
    public string ServiceName { get; init; } = "postgres";
    /// Gets the explicit service principal target name, when one is required.
    public string? TargetName { get; init; }
    /// <summary>Allows an SSPI request to negotiate NTLM when Kerberos is unavailable. GSS requests always use Kerberos.</summary>
    public bool AllowNtlm { get; init; }
    /// Gets a callback which configures each fresh integrated-authentication exchange.
    public Action<System.Net.Security.NegotiateAuthenticationClientOptions>? ConfigureAuthenticationOptions { get; init; }

    internal PostgreSqlIntegratedSecurityOptions Snapshot() => new()
    {
        Credential = new NetworkCredential(Credential.UserName, Credential.Password, Credential.Domain),
        ServiceName = ServiceName,
        TargetName = TargetName,
        AllowNtlm = AllowNtlm,
        ConfigureAuthenticationOptions = ConfigureAuthenticationOptions
    };

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Credential);
        if (string.IsNullOrWhiteSpace(ServiceName))
            throw new ArgumentException("A Kerberos service name is required.", nameof(ServiceName));
    }
}
