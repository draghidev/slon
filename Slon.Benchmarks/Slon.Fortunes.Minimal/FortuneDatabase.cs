using System.Globalization;
using System.Net;
using Npgsql;

namespace Slon.Fortunes.Minimal;

internal abstract class FortuneDatabase : IAsyncDisposable
{
    protected const string Query = "SELECT id, message FROM fortune";
    private const string AdditionalFortune = "Additional fortune added at request time.";

    public abstract ValueTask DisposeAsync();

    public abstract ValueTask<List<Fortune>> LoadAsync(
        CancellationToken cancellationToken);

    public static ValueTask<FortuneDatabase> CreateAsync(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var database = RequiredDatabase(configuration["DATABASE"]);
        var driver = RequiredDriver(configuration["DRIVER"]);
        var connectionString = RequiredConnectionString(configuration["CONNECTION_STRING"]);
        var connectionCount = PositiveSetting(configuration, "DATABASE_CONNECTIONS");

        return (database, driver) switch
        {
            ("postgresql", "slon") => SlonFortuneDatabase.CreateAsync(
                connectionString,
                connectionCount,
                PositiveSetting(configuration, "SLON_PIPELINING")),
            ("postgresql", "npgsql") => ValueTask.FromResult<FortuneDatabase>(
                new NpgsqlFortuneDatabase(connectionString, connectionCount)),
            _ => throw new InvalidOperationException("The database selection is invalid."),
        };
    }

    protected static List<Fortune> Complete(List<Fortune> fortunes)
    {
        fortunes.Add(new Fortune(0, AdditionalFortune));
        fortunes.Sort();
        return fortunes;
    }

    private static string RequiredDatabase(string? value)
    {
        var database = RequiredValue("DATABASE", value);
        return database == "postgresql"
            ? database
            : throw new InvalidOperationException("DATABASE must be 'postgresql'.");
    }

    private static string RequiredDriver(string? value)
    {
        var driver = RequiredValue("DRIVER", value);
        return driver is "slon" or "npgsql"
            ? driver
            : throw new InvalidOperationException("DRIVER must be 'slon' or 'npgsql'.");
    }

    private static string RequiredConnectionString(string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException("CONNECTION_STRING is required.")
            : value;

    private static string RequiredValue(string name, string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException($"{name} is required.")
            : value.Trim().ToLowerInvariant();

    private static int PositiveSetting(IConfiguration configuration, string name)
    {
        var value = configuration[name] ??
            throw new InvalidOperationException($"{name} is required.");

        return int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) &&
            parsed > 0
            ? parsed
            : throw new InvalidOperationException($"{name} must be a positive integer.");
    }
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
