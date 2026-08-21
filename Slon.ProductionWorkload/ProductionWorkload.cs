using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Slon.ProductionWorkload;

sealed class ProductionWorkload
{
    readonly SlonDataSource _dataSource;
    readonly WorkloadOptions _options;
    readonly TcpJitterProxy _proxy;
    readonly string _stateTable;
    readonly string _auditTable;
    readonly ConcurrentQueue<string> _recentEvents = new();
    readonly OperationStatistics[] _statistics = Enum.GetValues<OperationKind>()
        .Select(static kind => new OperationStatistics(kind)).ToArray();
    long _nextOperation;
    long _completed;
    long _collateral;
    long _cancellations;
    long _naturalCancellationRaces;
    long _expectedSqlErrors;
    long _backendTerminations;
    long _unexpected;

    public ProductionWorkload(SlonDataSource dataSource, WorkloadOptions options, TcpJitterProxy proxy)
    {
        _dataSource = dataSource;
        _options = options;
        _proxy = proxy;
        var suffix = $"{Environment.ProcessId}_{unchecked((uint)options.Seed):x8}";
        _stateTable = $"slon_workload_state_{suffix}";
        _auditTable = $"slon_workload_audit_{suffix}";
    }

    public async Task ExecuteAsync()
    {
        await SetupAsync().ConfigureAwait(false);
        using var reporting = new CancellationTokenSource();
        var reporter = ReportAsync(reporting.Token);
        try
        {
            var workers = new Task[_options.Workers];
            for (var i = 0; i < workers.Length; i++)
                workers[i] = RunWorkerAsync(i);
            await Task.WhenAll(workers).ConfigureAwait(false);
            await VerifyAsync().ConfigureAwait(false);
        }
        finally
        {
            await reporting.CancelAsync().ConfigureAwait(false);
            await reporter.ConfigureAwait(false);
            await CleanupAsync().ConfigureAwait(false);
        }
    }

    async Task RunWorkerAsync(int workerId)
    {
        var random = new Random(HashCode.Combine(_options.Seed, workerId));
        while (true)
        {
            var operationId = Interlocked.Increment(ref _nextOperation);
            if (operationId > _options.Iterations)
                return;

            var kind = SelectOperation(random);
            var started = Stopwatch.GetTimestamp();
            try
            {
                await ExecuteOperationAsync(kind, operationId, random).ConfigureAwait(false);
            }
            catch (SlonException exception) when (exception.IsCollateral)
            {
                Interlocked.Increment(ref _collateral);
                Remember($"#{operationId} {kind}: collateral " +
                         $"{exception.PostgreSqlError?.SqlState ?? exception.GetType().Name}");
            }
            catch (Exception exception)
            {
                Interlocked.Increment(ref _unexpected);
                Remember($"#{operationId} {kind}: {exception.GetType().Name}: {exception.Message}");
                throw new InvalidOperationException($"Operation {operationId} ({kind}) failed.", exception);
            }
            finally
            {
                _statistics[(int)kind].Record(Stopwatch.GetElapsedTime(started));
                Interlocked.Increment(ref _completed);
            }
        }
    }

    OperationKind SelectOperation(Random random)
    {
        if (_options.TerminationEvery != 0 && random.Next(_options.TerminationEvery) == 0)
            return OperationKind.BackendTermination;
        if (_options.CancellationEvery != 0 && random.Next(_options.CancellationEvery) == 0)
            return OperationKind.Cancellation;
        if (_options.SqlErrorEvery != 0 && random.Next(_options.SqlErrorEvery) == 0)
            return OperationKind.SqlError;

        return random.Next(100) switch
        {
            < 50 => OperationKind.ShortCommand,
            < 60 => OperationKind.AsyncBatch,
            < 66 => OperationKind.SyncBatch,
            < 72 => OperationKind.PartialReader,
            < 77 => OperationKind.AsyncCommit,
            < 81 => OperationKind.AsyncRollback,
            < 85 => OperationKind.SyncCommit,
            < 88 => OperationKind.SyncRollback,
            < 90 => OperationKind.DisposeTransaction,
            < 94 => OperationKind.DataSourceCommand,
            < 96 => OperationKind.SyncCommand,
            < 98 => OperationKind.ConnectionRoundTrip,
            < 99 => OperationKind.AsyncErrorBarrierBatch,
            _ => OperationKind.SyncErrorBarrierBatch
        };
    }

    Task ExecuteOperationAsync(OperationKind kind, long operationId, Random random)
        => kind switch
        {
            OperationKind.ShortCommand => ShortCommandAsync(operationId),
            OperationKind.AsyncBatch => AsyncBatchAsync(operationId),
            OperationKind.SyncBatch => SyncBatchAsync(operationId),
            OperationKind.PartialReader => PartialReaderAsync(random),
            OperationKind.AsyncCommit => TransactionAsync(operationId, commit: true),
            OperationKind.AsyncRollback => TransactionAsync(operationId, commit: false),
            OperationKind.SyncCommit => SyncTransactionAsync(operationId, commit: true),
            OperationKind.SyncRollback => SyncTransactionAsync(operationId, commit: false),
            OperationKind.DisposeTransaction => DisposeTransactionAsync(operationId),
            OperationKind.DataSourceCommand => DataSourceCommandAsync(operationId),
            OperationKind.SqlError => SqlErrorAsync(),
            OperationKind.SyncCommand => SyncCommandAsync(operationId),
            OperationKind.ConnectionRoundTrip => ConnectionRoundTripAsync(operationId),
            OperationKind.AsyncErrorBarrierBatch => AsyncErrorBarrierBatchAsync(operationId),
            OperationKind.SyncErrorBarrierBatch => SyncErrorBarrierBatchAsync(operationId),
            OperationKind.Cancellation => CancellationAsync(),
            OperationKind.BackendTermination => BackendTerminationAsync(),
            _ => throw new UnreachableException()
        };

    async Task ShortCommandAsync(long operationId)
    {
        await using var command = new SlonCommand(_dataSource, $"select {operationId}::bigint");
        var value = await command.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false);
        Ensure(Convert.ToInt64(value, CultureInfo.InvariantCulture) == operationId,
            $"short command returned {value}, expected {operationId}");
    }

    async Task AsyncBatchAsync(long operationId)
    {
        await using var batch = _dataSource.CreateBatch();
        for (var i = 0; i < 3; i++)
            batch.BatchCommands.Add(batch.CreateBatchCommand($"select {operationId + i}::bigint"));
        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None).ConfigureAwait(false);
        for (var i = 0; i < 3; i++)
        {
            Ensure(await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false), "batch result was empty");
            Ensure(reader.GetInt64(0) == operationId + i, "batch result was reordered");
            Ensure(!await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false),
                "batch result contained an extra row");
            if (i != 2)
                Ensure(await reader.NextResultAsync(CancellationToken.None).ConfigureAwait(false),
                    $"batch result set {i + 1} was missing");
        }
    }

    Task SyncBatchAsync(long operationId)
    {
        using var batch = _dataSource.CreateBatch();
        for (var i = 0; i < 3; i++)
            batch.BatchCommands.Add(batch.CreateBatchCommand($"select {operationId + i}::bigint"));
        using var reader = batch.ExecuteReader();
        for (var i = 0; i < 3; i++)
        {
            Ensure(reader.Read(), "sync batch result was empty");
            Ensure(reader.GetInt64(0) == operationId + i, "sync batch result was reordered");
            Ensure(!reader.Read(), "sync batch result contained an extra row");
            if (i != 2)
                Ensure(reader.NextResult(), $"sync batch result set {i + 1} was missing");
        }
        return Task.CompletedTask;
    }

    async Task PartialReaderAsync(Random random)
    {
        await using var command = new SlonCommand(_dataSource, "select i from generate_series(1, 64) i");
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None).ConfigureAwait(false);
        var rows = random.Next(1, 5);
        for (var i = 1; i <= rows; i++)
        {
            Ensure(await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false), "partial reader ended early");
            Ensure(reader.GetInt32(0) == i, "partial reader returned a reordered row");
        }
    }

    async Task TransactionAsync(long operationId, bool commit)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(CancellationToken.None)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(CancellationToken.None)
            .ConfigureAwait(false);
        await ExecuteNonQueryAsync(connection, $"update {_stateTable} set value = value + 1 where id = 1")
            .ConfigureAwait(false);
        await ExecuteNonQueryAsync(connection, $"insert into {_auditTable} values ({operationId}, {commit})")
            .ConfigureAwait(false);
        if (commit)
            await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
        else
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
    }

    Task SyncTransactionAsync(long operationId, bool commit)
    {
        using var connection = _dataSource.OpenConnection();
        using var transaction = connection.BeginTransaction();
        ExecuteNonQuery(connection, $"update {_stateTable} set value = value + 1 where id = 1");
        ExecuteNonQuery(connection, $"insert into {_auditTable} values ({operationId}, {commit})");
        if (commit)
            transaction.Commit();
        else
            transaction.Rollback();
        return Task.CompletedTask;
    }

    async Task DisposeTransactionAsync(long operationId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(CancellationToken.None)
            .ConfigureAwait(false);
        await using (var transaction = await connection.BeginTransactionAsync(CancellationToken.None)
                         .ConfigureAwait(false))
        {
            await ExecuteNonQueryAsync(connection, $"update {_stateTable} set value = value + 1 where id = 1")
                .ConfigureAwait(false);
            await ExecuteNonQueryAsync(connection, $"insert into {_auditTable} values ({operationId}, false)")
                .ConfigureAwait(false);
        }
    }

    async Task DataSourceCommandAsync(long operationId)
    {
        await using var command = _dataSource.CreateCommand($"select {operationId}::bigint");
        var value = await command.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false);
        Ensure(Convert.ToInt64(value, CultureInfo.InvariantCulture) == operationId,
            $"data-source command returned {value}, expected {operationId}");
    }

    async Task SqlErrorAsync()
    {
        await using var command = new SlonCommand(_dataSource, "select * from slon_workload_missing_relation");
        try
        {
            await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            throw new InvalidOperationException("The deliberately invalid query succeeded.");
        }
        catch (PostgreSqlException exception) when (exception.SqlState == "42P01")
        {
            Interlocked.Increment(ref _expectedSqlErrors);
        }
    }

    Task SyncCommandAsync(long operationId)
    {
        using var command = new SlonCommand(_dataSource, $"select {operationId}::bigint");
        var value = command.ExecuteScalar();
        Ensure(Convert.ToInt64(value, CultureInfo.InvariantCulture) == operationId,
            $"sync command returned {value}, expected {operationId}");
        return Task.CompletedTask;
    }

    async Task ConnectionRoundTripAsync(long operationId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(CancellationToken.None)
            .ConfigureAwait(false);
        await using var command = new SlonCommand(connection, $"select {operationId}::bigint");
        var value = await command.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false);
        Ensure(Convert.ToInt64(value, CultureInfo.InvariantCulture) == operationId,
            $"connection command returned {value}, expected {operationId}");
    }

    async Task AsyncErrorBarrierBatchAsync(long operationId)
    {
        await using var batch = CreateErrorBarrierBatch(operationId);
        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None).ConfigureAwait(false);
        Ensure(await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false),
            "async error-barrier batch first result was empty");
        Ensure(reader.GetInt64(0) == operationId, "async error-barrier batch first result was reordered");
        Ensure(await reader.NextResultAsync(CancellationToken.None).ConfigureAwait(false),
            "async error-barrier batch error result was missing");
        try
        {
            await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false);
            throw new InvalidOperationException("The async error-barrier command succeeded.");
        }
        catch (PostgreSqlException exception) when (exception.SqlState == "42P01")
        {
            Interlocked.Increment(ref _expectedSqlErrors);
        }
        Ensure(await reader.NextResultAsync(CancellationToken.None).ConfigureAwait(false),
            "async error-barrier batch successor was missing");
        Ensure(await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false),
            "async error-barrier batch successor was empty");
        Ensure(reader.GetInt64(0) == operationId + 1, "async error-barrier batch successor was reordered");
    }

    Task SyncErrorBarrierBatchAsync(long operationId)
    {
        using var batch = CreateErrorBarrierBatch(operationId);
        using var reader = batch.ExecuteReader();
        Ensure(reader.Read(), "sync error-barrier batch first result was empty");
        Ensure(reader.GetInt64(0) == operationId, "sync error-barrier batch first result was reordered");
        Ensure(reader.NextResult(), "sync error-barrier batch error result was missing");
        try
        {
            reader.Read();
            throw new InvalidOperationException("The sync error-barrier command succeeded.");
        }
        catch (PostgreSqlException exception) when (exception.SqlState == "42P01")
        {
            Interlocked.Increment(ref _expectedSqlErrors);
        }
        Ensure(reader.NextResult(), "sync error-barrier batch successor was missing");
        Ensure(reader.Read(), "sync error-barrier batch successor was empty");
        Ensure(reader.GetInt64(0) == operationId + 1, "sync error-barrier batch successor was reordered");
        return Task.CompletedTask;
    }

    SlonBatch CreateErrorBarrierBatch(long operationId)
    {
        var batch = _dataSource.CreateBatch();
        batch.EnableErrorBarriers = true;
        batch.BatchCommands.Add(batch.CreateBatchCommand($"select {operationId}::bigint"));
        batch.BatchCommands.Add(batch.CreateBatchCommand("select * from slon_workload_missing_relation"));
        batch.BatchCommands.Add(batch.CreateBatchCommand($"select {operationId + 1}::bigint"));
        return batch;
    }

    async Task CancellationAsync()
    {
        await using var command = new SlonCommand(_dataSource, "select pg_sleep(0.05)");
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(5));
        try
        {
            await command.ExecuteNonQueryAsync(cancellation.Token).ConfigureAwait(false);
            Interlocked.Increment(ref _naturalCancellationRaces);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            Interlocked.Increment(ref _cancellations);
        }
        catch (PostgreSqlException exception) when (exception.SqlState == "57014")
        {
            Interlocked.Increment(ref _cancellations);
        }
    }

    async Task BackendTerminationAsync()
    {
        await using var command = new SlonCommand(_dataSource,
            "select pg_terminate_backend(pg_backend_pid())");
        try
        {
            await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (SlonException)
        {
            Interlocked.Increment(ref _backendTerminations);
        }
    }

    async Task SetupAsync()
    {
        await ExecuteNonQueryAsync($"drop table if exists {_auditTable}").ConfigureAwait(false);
        await ExecuteNonQueryAsync($"drop table if exists {_stateTable}").ConfigureAwait(false);
        await ExecuteNonQueryAsync($"create table {_stateTable} " +
                                   "(id int primary key, value bigint not null)").ConfigureAwait(false);
        await ExecuteNonQueryAsync($"insert into {_stateTable} values (1, 0)").ConfigureAwait(false);
        await ExecuteNonQueryAsync($"create table {_auditTable} " +
                                   "(operation_id bigint primary key, committed boolean not null)")
            .ConfigureAwait(false);
    }

    async Task VerifyAsync()
    {
        await using var command = new SlonCommand(_dataSource,
            $"select s.value, count(a.*), count(a.*) filter (where not a.committed) " +
            $"from {_stateTable} s left join {_auditTable} a on true where s.id = 1 group by s.value");
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None).ConfigureAwait(false);
        Ensure(await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false), "verification returned no row");
        var state = reader.GetInt64(0);
        var audits = reader.GetInt64(1);
        var rolledBackAudits = reader.GetInt64(2);
        Ensure(state == audits, $"state/audit mismatch: state={state}, audits={audits}");
        Ensure(rolledBackAudits == 0, $"{rolledBackAudits} rolled-back audit rows persisted");

        await ShortCommandAsync(long.MaxValue - 1).ConfigureAwait(false);
    }

    async Task CleanupAsync()
    {
        try
        {
            await ExecuteNonQueryAsync($"drop table if exists {_auditTable}").ConfigureAwait(false);
            await ExecuteNonQueryAsync($"drop table if exists {_stateTable}").ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Remember($"cleanup: {exception.GetType().Name}: {exception.Message}");
        }
    }

    async Task<int> ExecuteNonQueryAsync(string sql)
    {
        await using var command = new SlonCommand(_dataSource, sql);
        return await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    static async Task<int> ExecuteNonQueryAsync(SlonConnection connection, string sql)
    {
        await using var command = new SlonCommand(connection, sql);
        return await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    static int ExecuteNonQuery(SlonConnection connection, string sql)
    {
        using var command = connection.CreateCommand(sql);
        return command.ExecuteNonQuery();
    }

    async Task ReportAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_options.ReportEverySeconds));
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                Console.WriteLine(DescribeProgress());
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }

    string DescribeProgress()
        => $"completed={Volatile.Read(ref _completed)}/{_options.Iterations} " +
           $"collateral={Volatile.Read(ref _collateral)} unexpected={Volatile.Read(ref _unexpected)} " +
           $"connections={_proxy.Connections} forwarded={_proxy.BytesForwarded / 1024 / 1024}MiB";

    public string Describe(TimeSpan elapsed)
    {
        var builder = new StringBuilder();
        builder.AppendLine($"elapsed={elapsed:g} completed={Volatile.Read(ref _completed)} " +
                           $"rate={Volatile.Read(ref _completed) / Math.Max(elapsed.TotalSeconds, 0.001):F1}/s");
        builder.AppendLine($"cancellations={Volatile.Read(ref _cancellations)} " +
                           $"natural-cancel-races={Volatile.Read(ref _naturalCancellationRaces)} " +
                           $"collateral={Volatile.Read(ref _collateral)} " +
                           $"sql-errors={Volatile.Read(ref _expectedSqlErrors)} " +
                           $"backend-terminations={Volatile.Read(ref _backendTerminations)} " +
                           $"unexpected={Volatile.Read(ref _unexpected)}");
        builder.AppendLine($"proxy-connections={_proxy.Connections} " +
                           $"proxy-bytes={_proxy.BytesForwarded} seed={_options.Seed}");
        foreach (var statistics in _statistics)
            if (statistics.Count != 0)
                builder.AppendLine(statistics.Describe());
        foreach (var entry in _recentEvents)
            builder.AppendLine("  " + entry);
        return builder.ToString().TrimEnd();
    }

    void Remember(string value)
    {
        _recentEvents.Enqueue(value);
        while (_recentEvents.Count > 64)
            _recentEvents.TryDequeue(out _);
    }

    static void Ensure(bool condition, string message)
    {
        if (!condition)
            throw new InvalidOperationException(message);
    }

    enum OperationKind
    {
        ShortCommand,
        AsyncBatch,
        SyncBatch,
        PartialReader,
        AsyncCommit,
        AsyncRollback,
        SyncCommit,
        SyncRollback,
        DisposeTransaction,
        DataSourceCommand,
        SqlError,
        SyncCommand,
        ConnectionRoundTrip,
        AsyncErrorBarrierBatch,
        SyncErrorBarrierBatch,
        Cancellation,
        BackendTermination
    }

    sealed class OperationStatistics(OperationKind kind)
    {
        long _count;
        long _totalTicks;
        long _maximumTicks;

        public long Count => Volatile.Read(ref _count);

        public void Record(TimeSpan elapsed)
        {
            Interlocked.Increment(ref _count);
            Interlocked.Add(ref _totalTicks, elapsed.Ticks);
            var maximum = Volatile.Read(ref _maximumTicks);
            while (elapsed.Ticks > maximum)
            {
                var observed = Interlocked.CompareExchange(ref _maximumTicks, elapsed.Ticks, maximum);
                if (observed == maximum)
                    break;
                maximum = observed;
            }
        }

        public string Describe()
        {
            var count = Count;
            var average = TimeSpan.FromTicks(Volatile.Read(ref _totalTicks) / count);
            var maximum = TimeSpan.FromTicks(Volatile.Read(ref _maximumTicks));
            return $"{kind,-20} count={count,8} avg={average.TotalMilliseconds,8:F2}ms " +
                   $"max={maximum.TotalMilliseconds,8:F2}ms";
        }
    }
}
