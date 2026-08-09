using System.Net;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Draghi.Pipelining;
using Slon.Pg.Protocol;
using Slon.Threading;

namespace Slon.Pg;

sealed class PgClientOptions
{
    internal static readonly TimeSpan DefaultReadTimeout = TimeSpan.FromSeconds(30);

    internal TimeSpan HeartbeatInterval { get; init; } = Heartbeat.DefaultInterval;
    // Time-based subsampling on top of the heartbeat. Pushes batch up to this interval before a
    // maintenance flow is scheduled. Setting this larger than HeartbeatInterval grows batches at
    // the cost of cleanup latency.
    internal TimeSpan MaintenanceInterval { get; init; } = TimeSpan.FromSeconds(1);

    public required EndPoint EndPoint { get; init; }
    public required string Username { get; init; }
    public string? Password { get; init; }
    public string? Database { get; init; }
    public PostgreSqlSslOptions Ssl { get; internal set; } = new();
    internal bool AllowInsecureTransport { get; init; }
    internal OAuthTokenCache? OAuthTokens { get; init; }
    internal PostgreSqlIntegratedSecurityOptions? IntegratedSecurity { get; init; }
    internal ILoggerFactory LoggerFactory { get; init; } = NullLoggerFactory.Instance;

    public TimeSpan ReadTimeout { get; init; } = DefaultReadTimeout;
    public TimeSpan WriteTimeout { get; init; } = TimeSpan.FromSeconds(10);

    public TimeSpan ConnectionTimeout { get; init; } = Timeout.InfiniteTimeSpan;

    internal ScopeResetOptions ScopeReset { get; init; } = new();
    internal int DataRowStreamingThreshold { get; init; } = BackendMessageBatch.Segmenter.DefaultDataRowStreamingThreshold;
    internal int MaxInFlightFlowsPerWire { get; init; }
    internal PipelineScheduler? ExecutionScheduler { get; init; }

    // Hardcoded to UTF8 until a use for another encoding comes up.
    internal Encoding Encoding => Encoding.UTF8;
    internal Encoding PasswordEncoding => Encoding.UTF8;

    internal static Encoding PreStartupEncoding => Encoding.ASCII;

    internal PgClientOptions WithSsl(PostgreSqlSslOptions ssl)
    {
        var copy = (PgClientOptions)MemberwiseClone();
        copy.Ssl = ssl;
        return copy;
    }

    public static EndPoint ParseIpOrDnsEndPoint(string host) => IPOrDnsEndPoint.Parse(host, defaultPort: 5432);
}

static class IPOrDnsEndPoint
{
    public static EndPoint Parse(string host, int defaultPort = 0)
    {
        EndPoint endPoint;
        if (IPEndPoint.TryParse(host, out var ipEndPoint))
        {
            endPoint = defaultPort is not 0 && ipEndPoint.Port is 0
                ? new IPEndPoint(ipEndPoint.Address, defaultPort)
                : ipEndPoint;
        }
        else
        {
            var port = host.Substring(host.LastIndexOf(':') + 1);
            endPoint = new DnsEndPoint(host.Substring(0, host.Length - port.Length - 1), port.Length is 0 ? defaultPort : int.Parse(port));
        }
        return endPoint;
    }
}
