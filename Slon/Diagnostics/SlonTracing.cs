using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Slon.Pg.Protocol;

namespace Slon;

static class SlonTracing
{
    static readonly ActivitySource Source = new("Slon", "0.1.0");

    public static bool ShouldStart => Source.HasListeners() && Activity.Current?.Source != Source;

    public static Activity? Start(SlonDataSource dataSource, int operationCount)
    {
        var isBatch = operationCount > 1;
        var activity = Source.StartActivity(
            isBatch ? $"BATCH {dataSource.Database}" : dataSource.Database, ActivityKind.Client);
        if (activity is null)
            return null;

        activity.SetTag("db.system.name", "postgresql");
        activity.SetTag("db.namespace", dataSource.Database);
        if (isBatch)
        {
            activity.SetTag("db.operation.name", "BATCH");
            activity.SetTag("db.operation.batch.size", operationCount);
        }

        switch (dataSource.EndPoint)
        {
            case DnsEndPoint dns:
                activity.SetTag("server.address", dns.Host);
                activity.SetTag("server.port", dns.Port);
                break;
            case IPEndPoint ip:
                activity.SetTag("server.address", ip.Address.ToString());
                activity.SetTag("server.port", ip.Port);
                break;
            case UnixDomainSocketEndPoint unix:
                activity.SetTag("server.address", unix.ToString());
                break;
        }
        return activity;
    }

    public static void RecordException(Activity? activity, Exception exception)
    {
        if (activity is null)
            return;

        var errorType = exception.GetType().FullName;
        if (exception is PgErrorException pgError)
        {
            activity.SetTag("db.response.status_code", pgError.SqlState);
            errorType = pgError.SqlState;
        }
        activity.SetTag("error.type", errorType);
        activity.SetStatus(ActivityStatusCode.Error);
    }
}
