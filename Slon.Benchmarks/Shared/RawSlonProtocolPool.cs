using System.Net;
using Npgsql;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Text;
using Slon.Transport;

namespace Slon.Fortunes;

internal sealed class RawSlonProtocolPool : IAsyncDisposable
{
    const string Query = "SELECT id, message FROM fortune";
    readonly Slot[] _slots;
    int _nextSlot = -1;

    RawSlonProtocolPool(Slot[] slots) => _slots = slots;

    internal static async ValueTask<RawSlonProtocolPool> CreateAsync(
        string connectionString,
        int connectionCount)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var clientOptions = new PgClientOptions
        {
            EndPoint = new DnsEndPoint(
                RequiredPostgreSqlValue("Host", builder.Host),
                builder.Port),
            Database = RequiredPostgreSqlValue("Database", builder.Database),
            Username = RequiredPostgreSqlValue("Username", builder.Username),
            Password = builder.Password,
            Ssl = new PostgreSqlSslOptions { Mode = PostgreSqlSslMode.Disable },
        };
        var factory = new PgClientProtocolFactory(
            clientOptions,
            SocketStreamConnection.CreateFactory(clientOptions.EndPoint, new TransportConnectionOptions
            {
                // Match Apex's ordinary BCL read shape for this lower-layer ceiling comparison.
                UseZeroByteReads = false,
            }));
        var slots = new Slot[connectionCount];
        var created = 0;
        try
        {
            for (; created < slots.Length; created++)
            {
                var protocol = await factory.CreateAsync().ConfigureAwait(false);
                try
                {
                    slots[created] = new(protocol, await PrepareAsync(protocol).ConfigureAwait(false));
                }
                catch
                {
                    await protocol.DisposeAsync().ConfigureAwait(false);
                    throw;
                }
            }
            return new(slots);
        }
        catch
        {
            for (var i = 0; i < created; i++)
                await slots[i].Protocol.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal async ValueTask<List<T>> LoadAsync<T>(
        Func<int, string, T> create,
        CancellationToken cancellationToken)
    {
        var slot = GetSlot();
        var flow = new ReaderDrivenCommandFlow(slot.Command);
        if (!slot.Protocol.TryQueue(flow, cancellationToken: cancellationToken))
            throw new InvalidOperationException("The selected PostgreSQL protocol is unavailable.");

        List<T> values = [];
        await foreach (var result in flow)
        {
            await foreach (var row in result)
                values.Add(create(row.GetValue<int>(0), row.GetValue<string>(1)));
        }
        return values;
    }

    Slot GetSlot()
        => _slots[(int)((uint)Interlocked.Increment(ref _nextSlot) % (uint)_slots.Length)];

    public async ValueTask DisposeAsync()
    {
        List<Exception>? errors = null;
        foreach (var slot in _slots)
        {
            try
            {
                await slot.Protocol.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (errors ??= []).Add(exception);
            }
        }
        if (errors is not null)
            throw errors.Count is 1 ? errors[0] : new AggregateException(errors);
    }

    static async ValueTask<Command> PrepareAsync(PgClientProtocol protocol)
    {
        var command = Command.Create(Query, commandName: new EncodedCString("fortunes")) with
        {
            DescribeOnly = true,
            DescribeForPreparation = true,
        };
        var flow = protocol.Queue(new CommandFlow(async: true, command));
        await foreach (var result in flow)
            return Command.Create(result.GetMetadata().ToPreparedDescriptor());
        throw new InvalidOperationException("PostgreSQL preparation returned no command result.");
    }

    static string RequiredPostgreSqlValue(string name, string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException($"PostgreSQL {name} is required.")
            : value;

    sealed class Slot(PgClientProtocol protocol, Command command)
    {
        internal PgClientProtocol Protocol { get; } = protocol;
        internal Command Command { get; } = command;
    }
}
