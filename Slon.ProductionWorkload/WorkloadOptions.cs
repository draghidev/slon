using System.Globalization;

namespace Slon.ProductionWorkload;

sealed record WorkloadOptions
{
    public string Host { get; init; } = "127.0.0.1";
    public int Port { get; init; } = 5432;
    public string Username { get; init; } = "postgres";
    public string Password { get; init; } = "postgres123";
    public string Database { get; init; } = "postgres";
    public int Workers { get; init; } = Math.Max(4, Environment.ProcessorCount * 2);
    public int Iterations { get; init; } = 100_000;
    public int PoolSize { get; init; } = 4;
    public int Seed { get; init; } = Environment.TickCount;
    public int JitterMilliseconds { get; init; } = 3;
    public int MaximumChunkBytes { get; init; } = 4096;
    public int CancellationEvery { get; init; } = 500;
    public int SqlErrorEvery { get; init; } = 250;
    public int TerminationEvery { get; init; } = 10_000;
    public int ReportEverySeconds { get; init; } = 10;

    public static WorkloadOptions Parse(string[] args)
    {
        if (args.Any(static arg => arg is "-h" or "--help"))
        {
            PrintHelp();
            Environment.Exit(0);
        }

        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length; i++)
        {
            var argument = args[i];
            if (!argument.StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Unexpected argument '{argument}'.");

            var separator = argument.IndexOf('=');
            if (separator >= 0)
                values[argument[2..separator]] = argument[(separator + 1)..];
            else if (++i < args.Length)
                values[argument[2..]] = args[i];
            else
                throw new ArgumentException($"Missing value for '{argument}'.");
        }

        var options = new WorkloadOptions
        {
            Host = GetString(values, "host", "SLON_WORKLOAD_HOST", "127.0.0.1"),
            Port = GetInt(values, "port", "SLON_WORKLOAD_PORT", 5432),
            Username = GetString(values, "username", "SLON_WORKLOAD_USERNAME", "postgres"),
            Password = GetString(values, "password", "SLON_WORKLOAD_PASSWORD", "postgres123"),
            Database = GetString(values, "database", "SLON_WORKLOAD_DATABASE", "postgres"),
            Workers = GetInt(values, "workers", "SLON_WORKLOAD_WORKERS",
                Math.Max(4, Environment.ProcessorCount * 2)),
            Iterations = GetInt(values, "iterations", "SLON_WORKLOAD_ITERATIONS", 100_000),
            PoolSize = GetInt(values, "pool-size", "SLON_WORKLOAD_POOL_SIZE", 4),
            Seed = GetInt(values, "seed", "SLON_WORKLOAD_SEED", Environment.TickCount),
            JitterMilliseconds = GetInt(values, "jitter-ms", "SLON_WORKLOAD_JITTER_MS", 3),
            MaximumChunkBytes = GetInt(values, "max-chunk", "SLON_WORKLOAD_MAX_CHUNK", 4096),
            CancellationEvery = GetInt(values, "cancel-every", "SLON_WORKLOAD_CANCEL_EVERY", 500),
            SqlErrorEvery = GetInt(values, "sql-error-every", "SLON_WORKLOAD_SQL_ERROR_EVERY", 250),
            TerminationEvery = GetInt(values, "terminate-every", "SLON_WORKLOAD_TERMINATE_EVERY", 10_000),
            ReportEverySeconds = GetInt(values, "report-every", "SLON_WORKLOAD_REPORT_EVERY", 10)
        };

        ArgumentOutOfRangeException.ThrowIfLessThan(options.Workers, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.Iterations, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.PoolSize, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(options.JitterMilliseconds);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaximumChunkBytes, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(options.CancellationEvery);
        ArgumentOutOfRangeException.ThrowIfNegative(options.SqlErrorEvery);
        ArgumentOutOfRangeException.ThrowIfNegative(options.TerminationEvery);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.ReportEverySeconds, 1);
        return options;
    }

    static string GetString(Dictionary<string, string> values, string name, string environmentName,
        string defaultValue)
        => values.TryGetValue(name, out var value)
            ? value
            : Environment.GetEnvironmentVariable(environmentName) ?? defaultValue;

    static int GetInt(Dictionary<string, string> values, string name, string environmentName, int defaultValue)
    {
        var text = GetString(values, name, environmentName, defaultValue.ToString(CultureInfo.InvariantCulture));
        return int.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture);
    }

    static void PrintHelp()
    {
        Console.WriteLine("""
            Slon production-shaped workload

              --host HOST                 PostgreSQL host (default 127.0.0.1)
              --port PORT                 PostgreSQL port (default 5432)
              --username USER             PostgreSQL user (default postgres)
              --password PASSWORD         PostgreSQL password (default postgres123)
              --database DATABASE         PostgreSQL database (default postgres)
              --workers N                 Concurrent workload drivers
              --iterations N              Total operations across all drivers
              --pool-size N               Physical Slon connections
              --seed N                    Reproducible workload seed
              --jitter-ms N               Maximum proxy delay per network read
              --max-chunk N               Maximum proxy read size
              --cancel-every N             1-in-N cancellation probability; zero disables
              --sql-error-every N          1-in-N invalid-SQL probability; zero disables
              --terminate-every N          1-in-N backend-termination probability; zero disables
              --report-every N             Progress interval in seconds

            Every option also has a SLON_WORKLOAD_* environment variable counterpart.
            """);
    }
}
