using System.Net;

namespace Slon;

public sealed class PostgreSqlIntegratedSecurityOptions
{
    public NetworkCredential Credential { get; init; } = CredentialCache.DefaultNetworkCredentials;
    public string ServiceName { get; init; } = "postgres";
    public string? TargetName { get; init; }
    /// <summary>Allows an SSPI request to negotiate NTLM when Kerberos is unavailable. GSS requests always use Kerberos.</summary>
    public bool AllowNtlm { get; init; }
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
