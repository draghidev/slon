using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Slon.Tests;

static class TlsTestCertificate
{
    // Shared for the test-process lifetime: certificate generation is expensive and the immutable
    // certificate handle supports the concurrent SslStream handshakes exercised by these tests.
    public static X509Certificate2 Instance { get; } = Create();

    static X509Certificate2 Create()
    {
        using var ecdsa = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=localhost", ecdsa, HashAlgorithmName.SHA256);
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        request.CertificateExtensions.Add(names.Build());
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }
}
