using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Security.Authentication;
using System.Text;
using System.Globalization;

namespace Slon.Pg.Protocol.Flows;

sealed partial class StartupFlow
{
    internal sealed class ScramSha256 : IDisposable
    {
        internal const int MaximumIterationCount = 16_000_000;
        const string PlainMechanism = "SCRAM-SHA-256";
        const string PlusMechanism = "SCRAM-SHA-256-PLUS";

        readonly string _password;
        readonly string _clientNonce;
        readonly string _clientFirstBare;
        readonly string _channelBinding;
        byte[]? _expectedServerSignature;

        ScramSha256(string password, string mechanism, string gs2Header, byte[] channelBinding)
        {
            _password = password;
            Mechanism = mechanism;
            Gs2Header = gs2Header;
            _clientNonce = Convert.ToBase64String(RandomNumberGenerator.GetBytes(18));
            _clientFirstBare = $"n=*,r={_clientNonce}";
            var binding = new byte[Encoding.UTF8.GetByteCount(gs2Header) + channelBinding.Length];
            var written = Encoding.UTF8.GetBytes(gs2Header, binding);
            channelBinding.CopyTo(binding.AsSpan(written));
            _channelBinding = Convert.ToBase64String(binding);
            CryptographicOperations.ZeroMemory(binding);
        }

        public string Mechanism { get; }
        public string Gs2Header { get; }
        public bool IsChannelBound => Mechanism is PlusMechanism;

        public static ScramSha256 Create(IReadOnlyCollection<string> mechanisms, string password,
            PostgreSqlChannelBinding policy, X509Certificate? certificate)
        {
            var hasPlain = mechanisms.Contains(PlainMechanism);
            var hasPlus = mechanisms.Contains(PlusMechanism);
            byte[]? binding = null;
            if (policy is not PostgreSqlChannelBinding.Disable && hasPlus && certificate is not null)
            {
                using var certificate2 = new X509Certificate2(certificate);
                binding = TryGetTlsServerEndPoint(certificate2);
            }

            if (binding is not null)
            {
                try
                {
                    return new(password, PlusMechanism, "p=tls-server-end-point,,", binding);
                }
                finally
                {
                    CryptographicOperations.ZeroMemory(binding);
                }
            }

            if (policy is PostgreSqlChannelBinding.Require)
                throw new PgClientException(new AuthenticationException(
                    "SCRAM channel binding was required but SCRAM-SHA-256-PLUS is unavailable."));
            if (!hasPlain)
                throw new PgClientException(
                    new NotSupportedException("PostgreSQL did not offer a supported SCRAM mechanism."));

            var gs2Flag = policy is PostgreSqlChannelBinding.Disable || hasPlus ? "n,," : "y,,";
            return new(password, PlainMechanism, gs2Flag, []);
        }

        public byte[] CreateInitialResponse() => Encoding.UTF8.GetBytes(Gs2Header + _clientFirstBare);

        public byte[] ProcessServerFirst(ReadOnlySpan<byte> payload)
        {
            var serverFirst = Encoding.UTF8.GetString(payload);
            var attributes = ParseAttributes(serverFirst);
            var nonce = GetRequired(attributes, 'r');
            if (!nonce.StartsWith(_clientNonce, StringComparison.Ordinal) || nonce.Length == _clientNonce.Length)
                throw new PgProtocolException("The SCRAM server nonce does not extend the client nonce.");
            var saltText = GetRequired(attributes, 's');
            var salt = new byte[GetMaximumBase64DecodedLength(saltText.Length)];
            if (!Convert.TryFromBase64String(saltText, salt, out var saltLength))
                throw new PgProtocolException("The SCRAM salt is not valid Base64.");
            if (!int.TryParse(GetRequired(attributes, 'i'), NumberStyles.None, CultureInfo.InvariantCulture,
                    out var iterations)
                || iterations is <= 0 or > MaximumIterationCount)
                throw new PgProtocolException("The SCRAM iteration count is invalid.");

            var normalizedPassword = _password.Normalize(NormalizationForm.FormKC);
            var saltedPassword = Rfc2898DeriveBytes.Pbkdf2(normalizedPassword, salt.AsSpan(0, saltLength),
                iterations, HashAlgorithmName.SHA256, SHA256.HashSizeInBytes);
            CryptographicOperations.ZeroMemory(salt);
            var clientKey = HMACSHA256.HashData(saltedPassword, "Client Key"u8);
            var storedKey = SHA256.HashData(clientKey);
            var clientFinalWithoutProof = $"c={_channelBinding},r={nonce}";
            var authMessage = $"{_clientFirstBare},{serverFirst},{clientFinalWithoutProof}";
            var authBytes = Encoding.UTF8.GetBytes(authMessage);
            var clientSignature = HMACSHA256.HashData(storedKey, authBytes);
            for (var i = 0; i < clientKey.Length; i++)
                clientKey[i] ^= clientSignature[i];
            var serverKey = HMACSHA256.HashData(saltedPassword, "Server Key"u8);
            _expectedServerSignature = HMACSHA256.HashData(serverKey, authBytes);
            var proof = Convert.ToBase64String(clientKey);

            CryptographicOperations.ZeroMemory(saltedPassword);
            CryptographicOperations.ZeroMemory(clientKey);
            CryptographicOperations.ZeroMemory(storedKey);
            CryptographicOperations.ZeroMemory(clientSignature);
            CryptographicOperations.ZeroMemory(serverKey);
            CryptographicOperations.ZeroMemory(authBytes);
            return Encoding.UTF8.GetBytes($"{clientFinalWithoutProof},p={proof}");
        }

        public void ValidateServerFinal(ReadOnlySpan<byte> payload)
        {
            if (_expectedServerSignature is null)
                throw new InvalidOperationException("The SCRAM server-first message has not been processed.");
            var attributes = ParseAttributes(Encoding.UTF8.GetString(payload));
            if (attributes.TryGetValue('e', out var error))
                throw new PgClientException(
                    new AuthenticationException($"PostgreSQL rejected SCRAM authentication: {error}"));
            var signatureText = GetRequired(attributes, 'v');
            var supplied = new byte[GetMaximumBase64DecodedLength(signatureText.Length)];
            try
            {
                if (!Convert.TryFromBase64String(signatureText, supplied, out var suppliedLength)
                    || !CryptographicOperations.FixedTimeEquals(supplied.AsSpan(0, suppliedLength), _expectedServerSignature))
                    throw new PgProtocolException("The SCRAM server signature is invalid.");
            }
            finally
            {
                CryptographicOperations.ZeroMemory(supplied);
            }
        }

        public void Dispose()
        {
            if (_expectedServerSignature is { } signature)
                CryptographicOperations.ZeroMemory(signature);
            _expectedServerSignature = null;
        }

        static Dictionary<char, string> ParseAttributes(string value)
        {
            var result = new Dictionary<char, string>();
            foreach (var part in value.Split(','))
            {
                if (part.Length < 3 || part[1] != '=')
                    throw new PgProtocolException("The SCRAM response contains a malformed attribute.");
                if (part[0] == 'm')
                    throw new PgClientException(
                        new NotSupportedException("The SCRAM response requires an unsupported extension."));
                if (!result.TryAdd(part[0], part[2..]))
                    throw new PgProtocolException("The SCRAM response contains a duplicate attribute.");
            }
            return result;
        }

        static string GetRequired(Dictionary<char, string> attributes, char name)
            => attributes.TryGetValue(name, out var value) && value.Length != 0
                ? value
                : throw new PgProtocolException($"The SCRAM response is missing the '{name}' attribute.");

        static int GetMaximumBase64DecodedLength(int length) => checked((length + 3) / 4 * 3);

        static byte[]? TryGetTlsServerEndPoint(X509Certificate2 certificate)
        {
            var hash = GetTlsServerEndPointHashAlgorithm(certificate.SignatureAlgorithm.Value);
            return hash == default ? null : certificate.GetCertHash(hash);
        }

        internal static HashAlgorithmName GetTlsServerEndPointHashAlgorithm(string? signatureAlgorithmOid)
            => signatureAlgorithmOid switch
            {
                // MD5 and SHA-1 are promoted to SHA-256 by RFC 5929.
                "1.2.840.113549.1.1.4" or "1.2.840.113549.1.1.5" or "1.2.840.10040.4.3"
                    or "1.2.840.10045.4.1" => HashAlgorithmName.SHA256,
                "1.2.840.113549.1.1.11" or "1.2.840.10045.4.3.2" => HashAlgorithmName.SHA256,
                "1.2.840.113549.1.1.12" or "1.2.840.10045.4.3.3" => HashAlgorithmName.SHA384,
                "1.2.840.113549.1.1.13" or "1.2.840.10045.4.3.4" => HashAlgorithmName.SHA512,
                _ => default
            };
    }
}
