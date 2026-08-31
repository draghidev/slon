using System.Diagnostics;
using System.Net;
using Npgsql;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pooling;
using Slon.Text;
using Slon.Transport;

namespace Slon.Fortunes;

internal sealed class SlonConnectionPool : IAsyncDisposable
{
    const string Query = "SELECT id, message FROM fortune";
    readonly ConnectionPool<ProtocolConnection> _pool;
    readonly ReaderDrivenCommandOptions _options;

    SlonConnectionPool(ConnectionPool<ProtocolConnection> pool, Command command)
        => (_pool, _options) = (pool, new ReaderDrivenCommandOptions(command));

    internal static async ValueTask<SlonConnectionPool> CreateAsync(
        string connectionString,
        int connectionCount)
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
        var transportFactory = SocketStreamConnection.CreateFactory(
            clientOptions.EndPoint,
            new TransportConnectionOptions { UseZeroByteReads = false });
        var bootstrapFactory = new PgClientProtocolFactory(clientOptions, transportFactory);
        var protocolFactory = new PgClientProtocolFactory(
            clientOptions,
            transportFactory,
            static options => options.HeartbeatMode = PgClientProtocolHeartbeatMode.External);

        // Every pooled wire installs the same named statement. Obtain its immutable descriptor once;
        // later flows can be created before placement and use it on whichever wire the pool selects.
        Command command;
        await using (var bootstrap = await bootstrapFactory.CreateAsync().ConfigureAwait(false))
            command = await PrepareAsync(bootstrap).ConfigureAwait(false);

        var pool = new ConnectionPool<ProtocolConnection>(
            new ProtocolConnectionFactory(protocolFactory),
            new ConnectionPoolOptions
            {
                MinConnections = connectionCount,
                MaxConnections = connectionCount,
                ConnectionIdleLifetime = Timeout.InfiniteTimeSpan,
            });
        return new(pool, command);
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

        var values = new List<T>();
        await foreach (var result in flow.GetAsyncEnumerator(cancellationToken))
        await foreach (var row in result)
            values.Add(create(row.GetValue<int>(0), row.GetValue<string>(1)));
        return values;
    }

    public async ValueTask ConsumeRetainedAsync<T, TState>(
        Func<int, ReadOnlyMemory<byte>, T> create,
        TState state,
        Func<TState, List<T>, ValueTask> consume,
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

        var values = new List<T>();
        var results = flow.GetAsyncEnumerator(cancellationToken);
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
        IDisposable? _heartbeatRegistration;

        internal PgClientProtocol Protocol { get; } = protocol;
        public bool IsIdle => Protocol.Outstanding == 0;
        public bool IsSchedulable => Protocol.IsSchedulable;
        public Task Completion => Protocol.Completion;
        public Task CompleteAsync(Exception? exception = null)
        {
            var completion = Protocol.CompleteAsync(exception);
            if (completion.IsCompleted)
            {
                StopHeartbeat();
                return completion;
            }
            return CompleteAndStopHeartbeat(completion, this);

            static async Task CompleteAndStopHeartbeat(
                Task completion, ProtocolConnection connection)
            {
                try
                {
                    await completion.ConfigureAwait(false);
                }
                finally
                {
                    connection.StopHeartbeat();
                }
            }
        }
        public int CompareTo(ProtocolConnection? other)
            => other is null ? 1 : Protocol.Outstanding.CompareTo(other.Protocol.Outstanding);

        public void Start(ConnectionPool<ProtocolConnection>.Registration registration)
            => Protocol.SetAdmissionAvailableCallback(
                () => registration.SignalAvailability(Protocol.Outstanding == 0));

        internal void StartHeartbeat(ConnectionPoolContext<ProtocolConnection> poolContext)
        {
            Debug.Assert(_heartbeatRegistration is null);
            _heartbeatRegistration = poolContext.OnHeartbeat(
                static (connection, elapsed) => connection.Protocol.HeartbeatAsync(elapsed), this);
        }

        internal void StopHeartbeat()
            => Interlocked.Exchange(ref _heartbeatRegistration, null)?.Dispose();
    }

    sealed class ProtocolConnectionFactory(PgClientProtocolFactory factory)
        : IPoolConnectionFactory<ProtocolConnection>
    {
        public ProtocolConnection Create(
            ConnectionPoolContext<ProtocolConnection> poolContext,
            TimeSpan timeout = default)
        {
            var protocol = factory.Create(timeout);
            var connection = new ProtocolConnection(protocol);
            connection.StartHeartbeat(poolContext);
            try
            {
                _ = PrepareAsync(protocol).AsTask().GetAwaiter().GetResult();
                return connection;
            }
            catch
            {
                connection.StopHeartbeat();
                protocol.Dispose();
                throw;
            }
        }

        public async ValueTask<ProtocolConnection> CreateAsync(
            ConnectionPoolContext<ProtocolConnection> poolContext,
            CancellationToken cancellationToken = default)
        {
            var protocol = await factory.CreateAsync(cancellationToken).ConfigureAwait(false);
            var connection = new ProtocolConnection(protocol);
            connection.StartHeartbeat(poolContext);
            try
            {
                _ = await PrepareAsync(protocol).ConfigureAwait(false);
                return connection;
            }
            catch
            {
                connection.StopHeartbeat();
                await protocol.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
    }
}
