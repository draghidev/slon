using System.Net;
using System.Net.Security;
using System.Security.Authentication;

namespace Slon.Pg.Protocol.Flows;

sealed partial class StartupFlow
{
    internal sealed class GssAuthentication : IDisposable
    {
        readonly NegotiateAuthentication _authentication;

        public bool IsAuthenticated => _authentication.IsAuthenticated;

        public GssAuthentication(PostgreSqlIntegratedSecurityOptions options, EndPoint endpoint, bool requiresKerberos)
        {
            var host = endpoint switch
            {
                DnsEndPoint dns => dns.Host,
                IPEndPoint ip => ip.Address.ToString(),
                _ when options.TargetName is not null => string.Empty,
                _ => throw new NotSupportedException("Integrated authentication requires a TCP endpoint or an explicit target name.")
            };
            var authenticationOptions = new NegotiateAuthenticationClientOptions
            {
                Package = GetPackage(requiresKerberos, options.AllowNtlm),
                Credential = options.Credential,
                TargetName = options.TargetName ?? $"{options.ServiceName}/{host}",
                RequiredProtectionLevel = ProtectionLevel.None,
                RequireMutualAuthentication = true
            };
            options.ConfigureAuthenticationOptions?.Invoke(authenticationOptions);
            _authentication = new(authenticationOptions);
        }

        // PostgreSQL documents GSSAPI clients interoperating with SSPI servers, and Windows Negotiate
        // acceptors support raw Kerberos tokens as universal receivers. Selecting Kerberos for SSPI is
        // therefore intentional when NTLM is forbidden, rather than relying on SPNEGO to avoid NTLM.
        internal static string GetPackage(bool requiresKerberos, bool allowNtlm)
            => requiresKerberos || !allowNtlm ? "Kerberos" : "Negotiate";

        public byte[]? GetOutgoingBlob(ReadOnlySpan<byte> incoming)
        {
            var result = _authentication.GetOutgoingBlob(incoming, out var statusCode);
            if (statusCode is not (NegotiateAuthenticationStatusCode.Completed
                or NegotiateAuthenticationStatusCode.ContinueNeeded))
                throw new PgClientException(
                    new AuthenticationException($"Integrated authentication failed with status {statusCode}."));
            return result;
        }

        public void Dispose() => _authentication.Dispose();
    }
}
