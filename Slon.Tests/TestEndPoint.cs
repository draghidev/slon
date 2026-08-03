using System.Net;
using System.Net.Sockets;

namespace Slon.Tests;

// Connect over the PostgreSQL Unix domain socket to avoid loopback TCP churn (ephemeral-port and
// TIME_WAIT exhaustion under reconnect-heavy tests). Falls back to TCP when the socket is absent.
static class TestEndPoint
{
    const string UnixSocketPath = "/tmp/.s.PGSQL.5432";

    public static EndPoint Default
    {
        get
        {
            var host = Environment.GetEnvironmentVariable("SLON_TEST_HOST");
            var port = int.TryParse(Environment.GetEnvironmentVariable("SLON_TEST_PORT"), out var value)
                ? value
                : 5432;

            if (host is null && port == 5432 && File.Exists(UnixSocketPath))
                return new UnixDomainSocketEndPoint(UnixSocketPath);

            host ??= "127.0.0.1";
            return IPAddress.TryParse(host, out var address)
                ? new IPEndPoint(address, port)
                : new DnsEndPoint(host, port);
        }
    }
}
