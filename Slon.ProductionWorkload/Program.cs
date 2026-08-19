using System.Diagnostics;
using System.Net;
using Slon;
using Slon.ProductionWorkload;

var options = WorkloadOptions.Parse(args);
await using var proxy = new TcpJitterProxy(options.Host, options.Port, options.JitterMilliseconds,
    options.MaximumChunkBytes, options.Seed);
await using var dataSource = new SlonDataSource(new SlonDataSourceOptions
{
    EndPoint = new IPEndPoint(IPAddress.Loopback, proxy.EndPoint.Port),
    Username = options.Username,
    Password = options.Password,
    Database = options.Database,
    Name = "production-workload",
    PoolSize = options.PoolSize,
    MaxInFlightOperationsPerWire = Math.Max(8, options.Workers),
    CommandTimeout = TimeSpan.FromSeconds(10),
    CancellationTimeout = TimeSpan.FromSeconds(5),
    CancellationRetryInterval = TimeSpan.FromMilliseconds(250)
});

var run = new ProductionWorkload(dataSource, options, proxy);
Console.WriteLine($"seed={options.Seed} workers={options.Workers} iterations={options.Iterations} " +
                  $"pool={options.PoolSize} proxy={proxy.EndPoint} upstream={options.Host}:{options.Port}");
var started = Stopwatch.StartNew();
try
{
    await run.ExecuteAsync();
    Console.WriteLine(run.Describe(started.Elapsed));
}
catch (Exception exception)
{
    Console.Error.WriteLine(run.Describe(started.Elapsed));
    Console.Error.WriteLine(exception);
    Environment.ExitCode = 1;
}
