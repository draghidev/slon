using System.Globalization;
using Npgsql;
using Slon.Fortunes;

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
            ("postgresql", "slon") =>
                SlonFortuneDatabase.CreateAsync(connectionString, connectionCount),
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

internal sealed class SlonFortuneDatabase(SlonConnectionPool pool) : FortuneDatabase
{
    public static async ValueTask<FortuneDatabase> CreateAsync(
        string connectionString, int connectionCount)
        => new SlonFortuneDatabase(await SlonConnectionPool.CreateAsync(
            connectionString, connectionCount).ConfigureAwait(false));

    public override async ValueTask<List<Fortune>> LoadAsync(CancellationToken cancellationToken)
    {
        var fortunes = await pool.LoadAsync(
            static (id, message) => new Fortune(id, message), cancellationToken).ConfigureAwait(false);
        return Complete(fortunes);
    }

    public override ValueTask DisposeAsync() => pool.DisposeAsync();
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
