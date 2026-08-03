using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace Slon.Pg.Protocol.Flows;

sealed partial class StartupFlow
{
    static class Md5Password
    {
        public static string CreateResponse(string username, string password, ReadOnlySpan<byte> salt, Encoding encoding)
        {
            if (salt.Length != 4)
                throw new ArgumentException("A PostgreSQL MD5 challenge must contain a four-byte salt.", nameof(salt));

            var plaintext = ArrayPool<byte>.Shared.Rent(encoding.GetByteCount(password) + encoding.GetByteCount(username));
            var hash = ArrayPool<byte>.Shared.Rent(MD5.HashSizeInBytes);
            byte[]? challenge = null;
            try
            {
                var passwordLength = encoding.GetBytes(password, plaintext);
                var usernameLength = encoding.GetBytes(username, plaintext.AsSpan(passwordLength));
                MD5.HashData(plaintext.AsSpan(0, passwordLength + usernameLength), hash);
                var firstHash = Convert.ToHexStringLower(hash.AsSpan(0, MD5.HashSizeInBytes));

                challenge = ArrayPool<byte>.Shared.Rent(encoding.GetByteCount(firstHash) + salt.Length);
                var firstHashLength = encoding.GetBytes(firstHash, challenge);
                salt.CopyTo(challenge.AsSpan(firstHashLength));
                MD5.HashData(challenge.AsSpan(0, firstHashLength + salt.Length), hash);
                return string.Concat("md5", Convert.ToHexStringLower(hash.AsSpan(0, MD5.HashSizeInBytes)));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(plaintext, clearArray: true);
                ArrayPool<byte>.Shared.Return(hash, clearArray: true);
                if (challenge is not null)
                    ArrayPool<byte>.Shared.Return(challenge, clearArray: true);
            }
        }
    }
}
