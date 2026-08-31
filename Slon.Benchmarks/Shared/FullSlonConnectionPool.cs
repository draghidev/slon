using System.Net;
using Npgsql;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pooling;
using Slon.Text;
using Slon.Transport;

namespace Slon.Fortunes;

internal sealed class FullSlonConnectionPool : IAsyncDisposable
{
    const string Query = "SELECT id, message FROM fortune";
    readonly ConnectionPool<ProtocolConnection> _pool;
    readonly ReaderDrivenCommandOptions _options;
    readonly SlonConsumptionMode _consumptionMode;

    FullSlonConnectionPool(ConnectionPool<ProtocolConnection> pool, Command command,
        SlonConsumptionMode consumptionMode)
        => (_pool, _options, _consumptionMode) =
            (pool, new ReaderDrivenCommandOptions(command), consumptionMode);

    internal static async ValueTask<FullSlonConnectionPool> CreateAsync(
        string connectionString,
        int connectionCount,
        SlonConsumptionMode consumptionMode)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var clientOptions = new PgClientOptions
        {
            EndPoint = new DnsEndPoint(
                RequiredPostgreSqlValue("Host", builder.Host), builder.Port),
            Database = RequiredPostgreSqlValue("Database", builder.Database),
            Username = RequiredPostgreSqlValue("Username", builder.Username),
            Password = builder.Password,
            Ssl = new PostgreSqlSslOptions { Mode = PostgreSqlSslMode.Disable },
        };
        var protocolFactory = new PgClientProtocolFactory(
            clientOptions,
            SocketStreamConnection.CreateFactory(clientOptions.EndPoint, new TransportConnectionOptions
            {
                UseZeroByteReads = false,
            }));

        // Every pooled wire installs the same named statement. Obtain its immutable descriptor once;
        // later flows can be created before placement and use it on whichever wire the pool selects.
        Command command;
        await using (var bootstrap = await protocolFactory.CreateAsync().ConfigureAwait(false))
            command = await PrepareAsync(bootstrap).ConfigureAwait(false);

        var pool = new ConnectionPool<ProtocolConnection>(
            new ProtocolConnectionFactory(protocolFactory),
            new ConnectionPoolOptions
            {
                MinConnections = connectionCount,
                MaxConnections = connectionCount,
                ConnectionIdleLifetime = Timeout.InfiniteTimeSpan,
            });
        return new(pool, command, consumptionMode);
    }

    public async ValueTask<List<T>> LoadAsync<T>(
        Func<int, string, T> create,
        CancellationToken cancellationToken)
    {
        var flow = new ReaderDrivenCommandFlow(_options);
        await _pool.GetAsync(
            static (candidate, item) => candidate.Connection.Protocol.TryQueue(
                item,
                candidate.IsIdleCandidate
                    ? FlowEnqueueOptions.None
                    : FlowEnqueueOptions.RequireExistingPipeline,
                candidate.CancellationToken),
            flow,
            Timeout.InfiniteTimeSpan,
            cancellationToken).ConfigureAwait(false);

        var values = new CollectList<T>(create);
        if (_consumptionMode is SlonConsumptionMode.Collect)
        {
            await flow.CollectAsync(values, static (state, row) =>
            {
                var list = (CollectList<T>)state!;
                list.Add(list.Create(row.GetValue<int>(0), row.GetValue<string>(1)));
            }, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await foreach (var result in flow)
            await foreach (var row in result)
                values.Add(create(row.GetValue<int>(0), row.GetValue<string>(1)));
        }
        return values;
    }

    public async ValueTask ConsumeRetainedAsync<T, TState>(
        Func<int, ReadOnlyMemory<byte>, T> create,
        TState state,
        Func<TState, List<T>, ValueTask> consume,
        CancellationToken cancellationToken)
    {
        if (_consumptionMode is not SlonConsumptionMode.Stream)
            throw new InvalidOperationException(
                "Retained field memory requires streaming consumption.");

        var flow = new ReaderDrivenCommandFlow(_options);
        await _pool.GetAsync(
            static (candidate, item) => candidate.Connection.Protocol.TryQueue(
                item,
                candidate.IsIdleCandidate
                    ? FlowEnqueueOptions.None
                    : FlowEnqueueOptions.RequireExistingPipeline,
                candidate.CancellationToken),
            flow,
            Timeout.InfiniteTimeSpan,
            cancellationToken).ConfigureAwait(false);

        var values = new List<T>();
        var results = flow.GetAsyncEnumerator();
        try
        {
            if (await results.MoveNextAsync().ConfigureAwait(false))
            {
                var rows = results.Current.GetAsyncEnumerator();
                while (await rows.MoveNextAsync().ConfigureAwait(false))
                {
                    var reader = rows.Current.GetReader();
                    values.Add(create(reader.Read<int>(), reader.ReadMemory()));
                }
                await rows.DisposeAsync().ConfigureAwait(false);
            }
            await consume(state, values).ConfigureAwait(false);
        }
        finally
        {
            await results.DisposeAsync().ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync() => _pool.DisposeAsync();

    static async ValueTask<Command> PrepareAsync(PgClientProtocol protocol)
    {
        var command = Command.Create(Query, commandName: new EncodedCString("fortunes"));
        var flow = protocol.Queue(new CommandFlow(async: true, command));
        Command? prepared = null;
        await foreach (var result in flow)
        {
            var metadata = result.GetMetadata();
            prepared = Command.Create(CommandDescriptor.CreatePrepared(
                metadata.CommandName,
                metadata.ParameterTypes.Preserve(),
                metadata.RowDescription?.Preserve()));
            await foreach (var _ in result) { }
            _ = result.GetCommandComplete();
        }
        return prepared ??
            throw new InvalidOperationException("PostgreSQL preparation returned no command result.");
    }

    static string RequiredPostgreSqlValue(string name, string? value)
        => string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException($"PostgreSQL {name} is required.")
            : value;

    sealed class ProtocolConnection(PgClientProtocol protocol)
        : IPoolConnection<ProtocolConnection>
    {
        internal PgClientProtocol Protocol { get; } = protocol;
        public bool IsIdle => Protocol.Outstanding == 0;
        public bool IsSchedulable => Protocol.IsSchedulable;
        public Task Completion => Protocol.Completion;
        public Task CompleteAsync(Exception? exception = null) => Protocol.CompleteAsync(exception);
        public int CompareTo(ProtocolConnection? other)
            => other is null ? 1 : Protocol.Outstanding.CompareTo(other.Protocol.Outstanding);

        public void Start(ConnectionPool<ProtocolConnection>.Registration registration)
            => Protocol.SetAdmissionAvailableCallback(
                () => registration.SignalAvailability(Protocol.Outstanding == 0));
    }

    sealed class ProtocolConnectionFactory(PgClientProtocolFactory factory)
        : IPoolConnectionFactory<ProtocolConnection>
    {
        public ProtocolConnection Create(
            ConnectionPoolContext<ProtocolConnection> poolContext,
            TimeSpan timeout = default)
        {
            var protocol = factory.Create(timeout);
            try
            {
                _ = PrepareAsync(protocol).AsTask().GetAwaiter().GetResult();
                return new(protocol);
            }
            catch
            {
                protocol.Dispose();
                throw;
            }
        }

        public async ValueTask<ProtocolConnection> CreateAsync(
            ConnectionPoolContext<ProtocolConnection> poolContext,
            CancellationToken cancellationToken = default)
        {
            var protocol = await factory.CreateAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                _ = await PrepareAsync(protocol).ConfigureAwait(false);
                return new(protocol);
            }
            catch
            {
                await protocol.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
    }

    sealed class CollectList<T>(Func<int, string, T> create) : List<T>
    {
        internal Func<int, string, T> Create { get; } = create;
    }
}
