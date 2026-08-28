using System.Globalization;
using Npgsql;
using Slon.Fortunes;

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
            ("postgresql", "slon") => CreateSlonAsync(
                requiredConnectionString,
                connectionCount,
                Environment.GetEnvironmentVariable("SLON_POOL_MODE")),
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

    static ValueTask<FortuneDatabase> CreateSlonAsync(
        string connectionString, int connectionCount, string? configuredMode)
    {
        var mode = string.IsNullOrWhiteSpace(configuredMode)
            ? "raw"
            : configuredMode.Trim().ToLowerInvariant();
        Console.WriteLine($"Slon pool mode: {mode}.");
        return mode switch
        {
            "raw" => RawSlonFortuneDatabase.CreateAsync(connectionString, connectionCount),
            "connection" => ConnectionSlonFortuneDatabase.CreateAsync(connectionString, connectionCount),
            _ => throw new ArgumentOutOfRangeException(
                "SLON_POOL_MODE", configuredMode, "Expected 'raw' or 'connection'."),
        };
    }
}

internal sealed class RawSlonFortuneDatabase(RawSlonProtocolPool pool) : FortuneDatabase
{
    public static async ValueTask<FortuneDatabase> CreateAsync(
        string connectionString, int connectionCount)
        => new RawSlonFortuneDatabase(await RawSlonProtocolPool.CreateAsync(
            connectionString, connectionCount).ConfigureAwait(false));

    public override async ValueTask<List<Fortune>> LoadAsync(
        CancellationToken cancellationToken)
    {
        var fortunes = await pool.LoadAsync(
            static (id, message) => new Fortune(id, message), cancellationToken).ConfigureAwait(false);
        return Complete(fortunes);
    }

    public override ValueTask DisposeAsync() => pool.DisposeAsync();
}

internal sealed class ConnectionSlonFortuneDatabase(FullSlonConnectionPool pool) : FortuneDatabase
{
    public static async ValueTask<FortuneDatabase> CreateAsync(
        string connectionString, int connectionCount)
        => new ConnectionSlonFortuneDatabase(await FullSlonConnectionPool.CreateAsync(
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
