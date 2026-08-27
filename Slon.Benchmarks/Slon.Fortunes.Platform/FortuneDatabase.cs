using System.Globalization;
using System.Net;
using Npgsql;

namespace Slon.Fortunes.Platform;

internal abstract class FortuneDatabase : IAsyncDisposable
{
    internal const string Query = "SELECT id, message FROM fortune";
    private const string AdditionalFortune = "Additional fortune added at request time.";

    public abstract ValueTask DisposeAsync();

    public abstract ValueTask<List<Fortune>> LoadAsync(
        CancellationToken cancellationToken);

    public static ValueTask<FortuneDatabase> CreateAsync(
        string? database,
        string? driver,
        string? connectionString)
    {
        var selectedDatabase = RequiredSelection("DATABASE", database);
        var selectedDriver = RequiredSelection("DRIVER", driver);
        var requiredConnectionString = string.IsNullOrWhiteSpace(connectionString)
            ? throw new InvalidOperationException("CONNECTION_STRING is required.")
            : connectionString;
        var connectionCount = PositiveEnvironment("DATABASE_CONNECTIONS");

        return (selectedDatabase, selectedDriver) switch
        {
            ("postgresql", "slon") => SlonFortuneDatabase.CreateAsync(
                requiredConnectionString,
                connectionCount,
                PositiveEnvironment("SLON_PIPELINING")),
            ("postgresql", "npgsql") =>
                ValueTask.FromResult<FortuneDatabase>(
                    new NpgsqlFortuneDatabase(requiredConnectionString, connectionCount)),
            ("postgresql", _) =>
                throw new InvalidOperationException(
                    $"DRIVER '{selectedDriver}' is not valid for DATABASE '{selectedDatabase}'."),
            _ => throw new ArgumentOutOfRangeException(
                nameof(database),
                selectedDatabase,
                "DATABASE must be 'postgresql'."),
        };
    }

    protected static List<Fortune> Complete(List<Fortune> fortunes)
    {
        fortunes.Add(new Fortune(0, AdditionalFortune));
        fortunes.Sort();
        return fortunes;
    }

    protected static int PositiveEnvironment(string name)
    {
        var value = Environment.GetEnvironmentVariable(name) ??
            throw new InvalidOperationException($"{name} is required.");

        return int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) &&
            parsed > 0
            ? parsed
            : throw new ArgumentOutOfRangeException(
                name,
                value,
                "Value must be a positive integer.");
    }

    private static string RequiredSelection(string name, string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException($"{name} is required.")
            : value.Trim().ToLowerInvariant();
}

internal sealed class SlonFortuneDatabase : FortuneDatabase
{
    private readonly SlonDataSource _dataSource;
    private readonly SlonCommand _command;

    private SlonFortuneDatabase(SlonDataSource dataSource, SlonCommand command)
    {
        _dataSource = dataSource;
        _command = command;
    }

    public static async ValueTask<FortuneDatabase> CreateAsync(
        string connectionString,
        int connectionCount,
        int pipeliningLimit)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var dataSource = new SlonDataSource(new SlonDataSourceOptions
        {
            EndPoint = new DnsEndPoint(
                RequiredPostgreSqlValue("Host", builder.Host),
                builder.Port),
            Database = RequiredPostgreSqlValue("Database", builder.Database),
            Username = RequiredPostgreSqlValue("Username", builder.Username),
            Password = builder.Password,
            PoolSize = connectionCount,
            MaxInFlightOperationsPerWire = pipeliningLimit,
            Ssl = new PostgreSqlSslOptions
            {
                Mode = PostgreSqlSslMode.Disable,
            },
        });

        try
        {
            var command = dataSource.CreateCommand(Query);
            try
            {
                await command.PrepareAsync();
                return new SlonFortuneDatabase(dataSource, command);
            }
            catch
            {
                await command.DisposeAsync();
                throw;
            }
        }
        catch
        {
            await dataSource.DisposeAsync();
            throw;
        }
    }

    public override async ValueTask<List<Fortune>> LoadAsync(
        CancellationToken cancellationToken)
    {
        await using var reader = await _command.ExecuteReaderAsync(cancellationToken);
        List<Fortune> fortunes = [];
        while (await reader.ReadAsync(cancellationToken))
        {
            fortunes.Add(new Fortune(reader.GetInt32(0), reader.GetString(1)));
        }

        return Complete(fortunes);
    }

    public override async ValueTask DisposeAsync()
    {
        try
        {
            await _command.DisposeAsync();
        }
        finally
        {
            await _dataSource.DisposeAsync();
        }
    }

    private static string RequiredPostgreSqlValue(string name, string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException($"PostgreSQL {name} is required.")
            : value;
}

internal sealed class NpgsqlFortuneDatabase : FortuneDatabase
{
    private readonly NpgsqlDataSource _dataSource;

    public NpgsqlFortuneDatabase(string connectionString, int connectionCount)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            MaxPoolSize = connectionCount,
        };
        _dataSource = new NpgsqlSlimDataSourceBuilder(builder.ConnectionString).Build();
    }

    public override async ValueTask<List<Fortune>> LoadAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(Query, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        List<Fortune> fortunes = [];
        while (await reader.ReadAsync(cancellationToken))
        {
            fortunes.Add(new Fortune(reader.GetInt32(0), reader.GetString(1)));
        }

        return Complete(fortunes);
    }

    public override ValueTask DisposeAsync() => _dataSource.DisposeAsync();
}
