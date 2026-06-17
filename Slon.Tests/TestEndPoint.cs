using System.Net;
using System.Net.Sockets;

namespace Slon.Tests;

// Connect over the PostgreSQL Unix domain socket to avoid loopback TCP churn (ephemeral-port and
// TIME_WAIT exhaustion under reconnect-heavy tests). Falls back to TCP when the socket is absent.
static class TestEndPoint
{
    const string UnixSocketPath = "/tmp/.s.PGSQL.5432";

    public static EndPoint Default => File.Exists(UnixSocketPath)
        ? new UnixDomainSocketEndPoint(UnixSocketPath)
        : new IPEndPoint(IPAddress.Loopback, 5432);
}
