namespace Slon;

/// Configures PostgreSQL authentication policy shared by all supported mechanisms.
public sealed class PostgreSqlAuthenticationOptions
{
    /// <summary>Allows credentials such as cleartext passwords and bearer tokens over unencrypted TCP.</summary>
    public bool AllowInsecureTransport { get; init; }

    internal PostgreSqlAuthenticationOptions Snapshot() => new()
    {
        AllowInsecureTransport = AllowInsecureTransport
    };
}
