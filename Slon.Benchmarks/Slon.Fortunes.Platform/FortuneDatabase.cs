using System.Globalization;
using System.Text;
using Npgsql;
using Slon.Fortunes;

namespace Slon.Fortunes.Platform;

internal abstract class FortuneDatabase : IAsyncDisposable
{
    internal const string Query = "SELECT id, message FROM fortune";
    private static readonly ReadOnlyMemory<byte> AdditionalFortune =
        "Additional fortune added at request time."u8.ToArray();

    public abstract ValueTask DisposeAsync();

    public abstract ValueTask RenderAsync(
        BenchmarkApplication application,
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
                Environment.GetEnvironmentVariable("SLON_POOL_MODE"),
                Environment.GetEnvironmentVariable("SLON_CONSUMPTION_MODE")),
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
        string connectionString, int connectionCount, string? configuredPoolMode,
        string? configuredConsumptionMode)
    {
        var poolMode = string.IsNullOrWhiteSpace(configuredPoolMode)
            ? "raw"
            : configuredPoolMode.Trim().ToLowerInvariant();
        var consumptionMode = ParseConsumptionMode(configuredConsumptionMode);
        Console.WriteLine($"Slon pool mode: {poolMode}; consumption mode: {consumptionMode.ToString().ToLowerInvariant()}.");
        return poolMode switch
        {
            "raw" => RawSlonFortuneDatabase.CreateAsync(connectionString, connectionCount, consumptionMode),
            "connection" => ConnectionSlonFortuneDatabase.CreateAsync(connectionString, connectionCount, consumptionMode),
            _ => throw new ArgumentOutOfRangeException(
                "SLON_POOL_MODE", configuredPoolMode, "Expected 'raw' or 'connection'."),
        };
    }

    static SlonConsumptionMode ParseConsumptionMode(string? configuredMode)
        => string.IsNullOrWhiteSpace(configuredMode)
            ? SlonConsumptionMode.Stream
            : configuredMode.Trim().ToLowerInvariant() switch
            {
                "stream" => SlonConsumptionMode.Stream,
                "collect" => SlonConsumptionMode.Collect,
                _ => throw new ArgumentOutOfRangeException(
                    "SLON_CONSUMPTION_MODE", configuredMode, "Expected 'stream' or 'collect'."),
            };
}

internal sealed class RawSlonFortuneDatabase(RawSlonProtocolPool pool) : FortuneDatabase
{
    public static async ValueTask<FortuneDatabase> CreateAsync(
        string connectionString, int connectionCount, SlonConsumptionMode consumptionMode)
        => new RawSlonFortuneDatabase(await RawSlonProtocolPool.CreateAsync(
            connectionString, connectionCount, consumptionMode).ConfigureAwait(false));

    public override ValueTask RenderAsync(
        BenchmarkApplication application,
        CancellationToken cancellationToken)
        => pool.ConsumptionMode is SlonConsumptionMode.Stream
            ? pool.ConsumeRetainedAsync(
                static (id, message) => new Fortune(id, message),
                application,
                static (application, fortunes) =>
                    application.RenderFortunesAsync(Complete(fortunes)),
                cancellationToken)
            : RenderCollectedAsync(pool, application, cancellationToken);

    static async ValueTask RenderCollectedAsync(
        RawSlonProtocolPool pool,
        BenchmarkApplication application,
        CancellationToken cancellationToken)
    {
        var fortunes = await pool.LoadAsync(
            static (id, message) => new Fortune(id, Encoding.UTF8.GetBytes(message)),
            cancellationToken).ConfigureAwait(false);
        await application.RenderFortunesAsync(Complete(fortunes)).ConfigureAwait(false);
    }

    public override ValueTask DisposeAsync() => pool.DisposeAsync();
}

internal sealed class ConnectionSlonFortuneDatabase(FullSlonConnectionPool pool) : FortuneDatabase
{
    public static async ValueTask<FortuneDatabase> CreateAsync(
        string connectionString, int connectionCount, SlonConsumptionMode consumptionMode)
        => new ConnectionSlonFortuneDatabase(await FullSlonConnectionPool.CreateAsync(
            connectionString, connectionCount, consumptionMode).ConfigureAwait(false));

    public override ValueTask RenderAsync(
        BenchmarkApplication application,
        CancellationToken cancellationToken)
        => pool.ConsumptionMode is SlonConsumptionMode.Stream
            ? pool.ConsumeRetainedAsync(
                static (id, message) => new Fortune(id, message),
                application,
                static (application, fortunes) =>
                    application.RenderFortunesAsync(Complete(fortunes)),
                cancellationToken)
            : RenderCollectedAsync(pool, application, cancellationToken);

    static async ValueTask RenderCollectedAsync(
        FullSlonConnectionPool pool,
        BenchmarkApplication application,
        CancellationToken cancellationToken)
    {
        var fortunes = await pool.LoadAsync(
            static (id, message) => new Fortune(id, Encoding.UTF8.GetBytes(message)),
            cancellationToken).ConfigureAwait(false);
        await application.RenderFortunesAsync(Complete(fortunes)).ConfigureAwait(false);
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

    public override async ValueTask RenderAsync(
        BenchmarkApplication application,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(Query, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var fortunes = new List<Fortune>();
        while (await reader.ReadAsync(cancellationToken))
        {
            fortunes.Add(new Fortune(
                reader.GetInt32(0), Encoding.UTF8.GetBytes(reader.GetString(1))));
        }

        await application.RenderFortunesAsync(Complete(fortunes));
    }

    public override ValueTask DisposeAsync() => _dataSource.DisposeAsync();
}
