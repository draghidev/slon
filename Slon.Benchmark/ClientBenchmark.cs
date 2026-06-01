using System.Net;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using Npgsql;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pools;
using Slon.Transport;

namespace Slon.Benchmark;

[WarmupCount(2)]
[IterationCount(10)]
[Config(typeof(Config))]
public class ClientBenchmark
{
#if LINUX
    static readonly DnsEndPoint EndPoint = new("docker.for.mac.localhost", 5432);
    static readonly string NpgsqlEndPoint = $"{EndPoint.Host}:{EndPoint.Port}";
#else
    static readonly IPEndPoint EndPoint = new(IPAddress.Loopback, 5432);
    static readonly string NpgsqlEndPoint = EndPoint.ToString();
#endif
    const string Username = "postgres";
    const string Password = "postgres123";
    const string Database = "te_fortunes";
    protected const int Connections = 1;

    const int TechEmpowerMaxConcurrency = 512;
    const int OneMillion = 1_000_000;
    protected const int Commands = TechEmpowerMaxConcurrency;

    static readonly string ConnectionString =
        $"Server={NpgsqlEndPoint};User ID={Username};Password={Password};Database={Database};SSL Mode=Disable;Pooling=true;MinPoolSize={Connections};MaxPoolSize={Connections};Max Auto Prepare=0;Multiplexing=true";

    protected static NpgsqlConnectionStringBuilder ConnectionStringBuilder => new(ConnectionString);

    protected static NpgsqlDataSource InitNpgsql(Action<NpgsqlDataSourceBuilder> configure)
    {
        var dataSourceBuilder = new NpgsqlDataSourceBuilder(ConnectionString);
        configure(dataSourceBuilder);
        var dataSource = dataSourceBuilder.Build();
        // Type loading etc.
        using var _ = dataSource.OpenConnection();
        return dataSource;
    }

    private protected static readonly PgClientOptions Options = new()
    {
        EndPoint = EndPoint,
        Username = Username,
        Password = Password,
        Database = Database,
        PoolSize = Connections
    };

    static readonly PgClientProtocolOptions DispatchingSyncOptions = new()
    {
        RunEnqueueAsynchronously = true
    };

    static readonly PgClientProtocolOptions MultiplexingOptions = new()
    {
        RunEnqueueAsynchronously = false
    };

    static readonly PgClientProtocolOptions ProtocolOptions = DispatchingSyncOptions;

    internal static PgClientProtocolFactory CreateProtocolFactory()
    {
        var transportFactory = SocketStreamConnection.CreateFactory(Options.EndPoint);
        return new PgClientProtocolFactory(Options, transportFactory, o =>
        {
            o.RunEnqueueAsynchronously = ProtocolOptions.RunEnqueueAsynchronously;
        });
    }

    private protected static ConnectionPool<PgClientProtocol> InitSlonPool(Func<PgClientProtocol, CancellationToken, ValueTask>? initializer, int? poolSize = null)
    {
        IPoolConnectionFactory<PgClientProtocol> factory = CreateProtocolFactory();
        if (initializer is not null)
            factory = new InitializingConnectionFactory<PgClientProtocol>(factory, asyncInitializer: initializer);

        return new(factory, new() { MaxConnections = poolSize ?? Options.PoolSize });
    }

    class Config : ManualConfig
    {
        public Config()
        {
            AddDiagnoser(MemoryDiagnoser.Default);
            // AddDiagnoser(ThreadingDiagnoser.Default);
            AddDiagnoser(new CpuDiagnoser());
            AddColumn(new TagColumn("Connections", _ => Connections.ToString()));
        }
    }
}
