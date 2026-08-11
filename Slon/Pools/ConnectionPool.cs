using Slon.Runtime;
using Slon.Threading;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Collections.Concurrent;
using static Slon.Pools.ConnectionPool;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Slon.Pools;

sealed class ConnectionPoolOptions
{
    /// Minimum number of connections preserved by statistical idle pruning.
    public int MinConnections { get; set; }
    public int MaxConnections { get; set; }
    /// Duration over which minimum idle-capacity samples are collected before pruning.
    /// Set to <see cref="Timeout.InfiniteTimeSpan"/> to allow growth without shrinking.
    /// Pruning is also disabled when <see cref="MinConnections"/> equals <see cref="MaxConnections"/>.
    public TimeSpan ConnectionIdleLifetime { get; set; } = TimeSpan.FromMinutes(5);
    /// Interval over which the minimum idle capacity is sampled.
    public TimeSpan ConnectionPruningInterval { get; set; } = TimeSpan.FromSeconds(10);
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
    public TimeSpan HeartbeatInterval { get; set; } = Heartbeat.DefaultInterval;
    public ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;
    public string? MetricsName { get; set; }
}

sealed class ConnectionPool<T> : IDisposable, IAsyncDisposable, IPoolMetricsSource
    where T : class, IPoolConnection<T>
{
    bool _disposed;
    object SyncObj { get; } = new();

    readonly object?[] _connections;

    /// Test diagnostic. This unsynchronized slot snapshot may be stale.
    internal string DescribeSlots()
    {
        var sb = new System.Text.StringBuilder();
        for (var i = 0; i < _connections.Length; i++)
        {
            var value = Volatile.Read(ref _connections[i]);
            sb.Append(i).Append('=');
            sb.Append(value switch
            {
                null => "empty",
                ConnectionFuture { IsCompleted: true } => "future-done",
                ConnectionFuture => "future-pending",
                T c => $"conn(idle={c.IsIdle} schedulable={c.IsSchedulable} completed={c.Completion.IsCompleted})",
                _ => "?",
            }).Append(' ');
        }
        return sb.ToString();
    }

    readonly ConnectionPoolContext<T> _context;
    readonly IPoolConnectionFactory<T> _factory;

    readonly ConcurrentQueue<T> _idle = new();
    readonly AcquisitionCoordinator<Placement> _acquisitions = new();
    ConcurrentDictionary<Task, byte>? _detachedTasks;
    internal int WaiterCount => _acquisitions.Count;

    PoolMetricsSnapshot IPoolMetricsSource.GetMetricsSnapshot()
    {
        var open = 0;
        var idle = 0;
        for (var i = 0; i < _connections.Length; i++)
        {
            var connection = Volatile.Read(ref _connections[i]) switch
            {
                T value => value,
                ConnectionFuture { IsCompleted: true } future => future.Result,
                _ => null
            };
            if (connection is null || connection.Completion.IsCompleted)
                continue;
            open++;
            if (connection.IsIdle)
                idle++;
        }
        return new(open, idle, _connections.Length, _acquisitions.Count);
    }

    readonly Heartbeat _heartbeat;
    readonly ILogger _logger;
    readonly PoolMetricsReporter? _metrics;
    readonly TimeProvider _timeProvider;
    readonly int _minConnections;
    readonly TimeSpan _pruningInterval;
    readonly int[]? _idleSamples;
    TimeSpan _pruningElapsed;
    int _idleSampleIndex;
    int _pruningIdleCount;
    int _pruningIdleMinimum;

    public ConnectionPool(IPoolConnectionFactory<T> factory, ConnectionPoolOptions options)
    {
        ArgumentNullException.ThrowIfNull(options.LoggerFactory);
        var maxConnections = options.MaxConnections;
        if (maxConnections <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxConnections), "Cannot be zero or negative.");
        if ((uint)options.MinConnections > (uint)maxConnections)
            throw new ArgumentOutOfRangeException(nameof(options.MinConnections),
                "Must be between zero and MaxConnections.");
        var pruningEnabled = options.ConnectionIdleLifetime != Timeout.InfiniteTimeSpan &&
            options.MinConnections < maxConnections;
        if (options.ConnectionIdleLifetime < TimeSpan.Zero &&
            options.ConnectionIdleLifetime != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(options.ConnectionIdleLifetime),
                "Must be non-negative or Timeout.InfiniteTimeSpan.");
        if (pruningEnabled && options.ConnectionPruningInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options.ConnectionPruningInterval),
                "Must be positive.");
        if (pruningEnabled && options.ConnectionPruningInterval < options.HeartbeatInterval)
            throw new ArgumentOutOfRangeException(nameof(options.ConnectionPruningInterval),
                "Must be at least HeartbeatInterval when pruning is enabled.");
        if (pruningEnabled && options.ConnectionIdleLifetime < options.ConnectionPruningInterval)
            throw new ArgumentOutOfRangeException(nameof(options.ConnectionIdleLifetime),
                "Must be at least ConnectionPruningInterval.");

        _connections = new object?[maxConnections];

        _factory = factory;
        _minConnections = options.MinConnections;
        _pruningInterval = options.ConnectionPruningInterval;
        if (pruningEnabled)
        {
            var lifetimeTicks = options.ConnectionIdleLifetime.Ticks;
            var intervalTicks = _pruningInterval.Ticks;
            var sampleCount = lifetimeTicks / intervalTicks;
            if (lifetimeTicks % intervalTicks != 0)
                sampleCount++;
            if (sampleCount > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(options.ConnectionIdleLifetime),
                    "The lifetime requires too many pruning samples.");
            _idleSamples = new int[(int)sampleCount];
        }
        _timeProvider = options.TimeProvider;
        _logger = options.LoggerFactory.CreateLogger("Slon.Pool");
        _heartbeat = new(options.HeartbeatInterval, _timeProvider, _logger);
        if (_idleSamples is not null)
            _heartbeat.Register(OnPoolHeartbeat);
        _context = new ConnectionPoolContext<T>(this,
            (connection, isIdle) =>
            {
                if (isIdle)
                    PublishIdle(connection);
                else
                    SignalAvailability();
            },
            (conn, action)  => _heartbeat.Register(interval => action(conn, interval))
        );
        if (options.MetricsName is { } metricsName)
            _metrics = SlonMetrics.Register(this, metricsName);
    }

    ValueTask OnPoolHeartbeat(TimeSpan interval)
    {
        _pruningElapsed += interval;
        if (_pruningElapsed < _pruningInterval)
            return ValueTask.CompletedTask;

        _pruningElapsed -= _pruningInterval;
        var samples = _idleSamples!;
        var minimum = Interlocked.Exchange(ref _pruningIdleMinimum, int.MaxValue);
        ObserveIdleMinimum(Volatile.Read(ref _pruningIdleCount));
        samples[_idleSampleIndex++] = minimum;
        if (_idleSampleIndex != samples.Length)
            return ValueTask.CompletedTask;

        _idleSampleIndex = 0;
        Array.Sort(samples);
        // The lower median is deliberately conservative for even-sized windows.
        var idle = samples[(samples.Length - 1) / 2];
        PruneIdleConnections(idle);
        return ValueTask.CompletedTask;
    }

    void PruneIdleConnections(int count)
    {
        if (count <= 0 || _minConnections == _connections.Length)
            return;

        var live = 0;
        for (var i = 0; i < _connections.Length; i++)
        {
            if (TryGetConnection(ref _connections[i], out var connection) &&
                !connection.Completion.IsCompleted)
                live++;
        }
        count = Math.Min(count, live - _minConnections);
        if (count <= 0)
            return;

        // Refused claims return their token. Bound the walk so retained-session connections
        // cannot make a pruning tick cycle forever.
        var candidates = _idle.Count;
        while (candidates-- > 0 && count > 0 && _idle.TryDequeue(out var connection))
        {
            if (connection.TryBeginPruning())
            {
                RecordIdleRemovalForPruning();
                count--;
                _ = CompletePrunedConnectionAsync(connection);
            }
            else
            {
                if (ReturnIdleToken(connection))
                    SignalAvailability();
                else
                    RecordIdleRemovalForPruning();
            }
        }
    }

    async Task CompletePrunedConnectionAsync(T connection)
    {
        try
        {
            await connection.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SlonLogMessages.PoolConnectionTeardownFailed(_logger, ex, "pruning");
        }
    }

    void ObserveCompletion(T connection)
    {
        var completion = connection.Completion;
        if (completion.IsCompleted)
        {
            SignalAvailability();
            return;
        }

        completion.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(SignalAvailability);
    }

    void PublishIdle(T connection)
    {
        if (_idleSamples is not null)
            Interlocked.Increment(ref _pruningIdleCount);
        _idle.Enqueue(connection);
        // Capacity must be visible before the packed-word bell linearizes this publication.
        SignalAvailability();
    }

    void SignalAvailability() => _acquisitions.NotifyAvailability();

    void RecordIdleRemovalForPruning()
    {
        if (_idleSamples is null)
            return;

        var count = Interlocked.Decrement(ref _pruningIdleCount);
        Debug.Assert(count >= 0);
        ObserveIdleMinimum(count);
    }

    void ObserveIdleMinimum(int count)
    {
        var minimum = Volatile.Read(ref _pruningIdleMinimum);
        while (count < minimum)
        {
            var observed = Interlocked.CompareExchange(ref _pruningIdleMinimum, count, minimum);
            if (observed == minimum)
                return;
            minimum = observed;
        }
    }

    bool DoSchedule<TState>(ConnectionCandidate<T> context, Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state)
    {
        ThrowIfDisposed();

        // Advisory only; the connection's scheduling gate makes the definitive decision.
        if (!context.Connection.IsSchedulable)
            return false;

        return schedule is not null && schedule(context, state);
    }

    bool ReturnIdleToken(T connection)
    {
        // A dequeue owns the idle token until scheduling succeeds or the token is returned.
        if (connection.IsIdle && connection.IsSchedulable)
        {
            _idle.Enqueue(connection);
            return true;
        }
        return false;
    }

    async ValueTask<T> WaitForAvailabilityAsync<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state,
        TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = new Deadline(timeout, _timeProvider);
        var timeoutSource = RentTimeoutSource(deadline.GetRemaining(), _timeProvider, cancellationToken);
        var waitToken = timeoutSource?.Token ?? CancellationToken.None;
        var waiter = _acquisitions.CreateWaiter(
            static placement => placement.Pool.TryPlace(
                placement.Schedule, placement.State, placement.CancellationToken),
            new PlacementState<TState>(this, schedule, state, cancellationToken));
        using var registration = _acquisitions.Enqueue(waiter, timeoutSource?.Token ?? cancellationToken);
        try
        {
            var completion = await waiter.AsValueTask().ConfigureAwait(false);
            await ReleaseTimeoutSource().ConfigureAwait(false);
            return await ResolvePlacementAsync(completion, schedule, state, deadline, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var token = CancellationToken.None;
            if (timeoutSource is not null)
            {
                token = timeoutSource.Token;
                await timeoutSource.DisposeAsync().ConfigureAwait(false);
            }

            if (IsCancellationTokenException(ex, token == CancellationToken.None ? waitToken : token))
                ThrowSourceExhausted(ex);

            throw;
        }
        async ValueTask ReleaseTimeoutSource()
        {
            if (timeoutSource is not { } source)
                return;
            timeoutSource = null;
            await source.DisposeAsync().ConfigureAwait(false);
        }
    }

    T WaitForAvailability<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout)
    {
        var deadline = new Deadline(timeout, _timeProvider);
        var timeoutSource = RentTimeoutSource(deadline.GetRemaining(), _timeProvider);
        using var waiter = _acquisitions.CreateWaiter(
            static placement => placement.Pool.TryPlace(
                placement.Schedule, placement.State, CancellationToken.None),
            new PlacementState<TState>(this, schedule, state, CancellationToken.None), synchronous: true);
        using var registration = _acquisitions.Enqueue(waiter, timeoutSource?.Token ?? CancellationToken.None);
        try
        {
            return ResolvePlacement(waiter.Wait(), schedule, state, deadline);
        }
        catch (OperationCanceledException ex) when (timeoutSource is not null &&
            IsCancellationTokenException(ex, timeoutSource.Token))
        {
            ThrowSourceExhausted(ex);
            return default!;
        }
        finally
        {
            timeoutSource?.Dispose();
        }
    }

    PlacementAttempt<Placement> TryPlace<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule,
        TState state, CancellationToken cancellationToken)
    {
        if (TrySchedule(schedule, state, cancellationToken, exhaustive: true, out var future, out var connection))
            return PlacementAttempt<Placement>.Placed(new(connection!));
        return future is null
            ? PlacementAttempt<Placement>.Unavailable
            : PlacementAttempt<Placement>.Placed(new(future));
    }

    T ResolvePlacement<TState>(Completion<Placement> completion,
        Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, Deadline deadline)
    {
        if (!completion.HasResult)
            Throw(completion.Exception!);

        var placement = completion.Result;
        if (completion.Exception is { } termination)
        {
            SettleTerminatedPlacement(placement, termination);
            Throw(termination);
        }

        return placement.Future is { } future
            ? OpenConnection(future, schedule, state, deadline.GetRemaining())
            : placement.Connection!;
    }

    async ValueTask<T> ResolvePlacementAsync<TState>(Completion<Placement> completion,
        Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, Deadline deadline,
        CancellationToken cancellationToken)
    {
        if (!completion.HasResult)
            Throw(completion.Exception!);

        var placement = completion.Result;
        if (completion.Exception is { } termination)
        {
            SettleTerminatedPlacement(placement, termination);
            Throw(termination);
        }

        return placement.Future is { } future
            ? await OpenConnectionAsync(future, schedule, state, deadline.GetRemaining(), cancellationToken)
                .ConfigureAwait(false)
            : placement.Connection!;
    }

    void SettleTerminatedPlacement(Placement placement, Exception termination)
    {
        if (placement.Future is { } future)
        {
            SettleOpener(future, null);
            return;
        }

        var connection = placement.Connection!;
        // Successful scheduling may already have transferred retirement to placed work. Only an
        // untouched idle claim is returned here; pool disposal owns all installed connections.
        if (termination is not ObjectDisposedException && connection.IsIdle && connection.IsSchedulable)
            PublishIdle(connection);
    }

    [DoesNotReturn]
    static void Throw(Exception exception)
        => ExceptionDispatchInfo.Capture(exception).Throw();

    bool TrySchedule<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state,
        CancellationToken cancellationToken, bool exhaustive, out ConnectionFuture? future,
        [NotNullWhen(true)]out T? connection)
    {
        // Prefer idle reuse, then growth, then multiplexing.

        // A rejected token returns at the tail. Walk at most one bounded cycle so later tokens
        // remain visible without repeatedly presenting the first rejection to this renter.
        var idleBudget = _connections.Length;
        T? returnedSentinel = null;
        while (idleBudget-- != 0 && _idle.TryDequeue(out var idle))
        {
            if (ReferenceEquals(idle, returnedSentinel))
            {
                if (!ReturnIdleToken(idle))
                    RecordIdleRemovalForPruning();
                break;
            }

            if (idle.IsIdle)
            {
                try
                {
                    if (DoSchedule(new(idle, cancellationToken), schedule, state))
                    {
                        RecordIdleRemovalForPruning();
                        future = null;
                        connection = idle;
                        return true;
                    }
                }
                catch
                {
                    if (ReturnIdleToken(idle))
                        SignalAvailability();
                    else
                        RecordIdleRemovalForPruning();
                    throw;
                }
            }

            if (ReturnIdleToken(idle))
            {
                returnedSentinel ??= idle;
                SignalAvailability();
            }
            else
                RecordIdleRemovalForPruning();
        }

        var connections = _connections;
        // Randomize the walk start so concurrent renters do not converge on slot zero.
        var startIndex = connections.Length > 1
            ? NextRandomStart(connections.Length)
            : 0;

        future = null;
        // Claim empty/completed slots immediately; retain two busy candidates for load comparison.
        T? busyFirst = null, busySecond = null;
        for (var i = startIndex; i < startIndex + connections.Length; i++)
        {
            ref var item = ref connections[i < connections.Length ? i : i - connections.Length];
            if (TryGetConnection(ref item, out var conn))
            {
                // Completed slot, reclaim and open new in its place.
                if (conn.Completion.IsCompleted && Interlocked.CompareExchange(ref item, future ??= new ConnectionFuture(), conn) == conn)
                {
                    connection = default;
                    return false;
                }

                // The idle queue is the sole idle rendezvous; the slot walk only samples busy work.
                if (!exhaustive && !conn.IsIdle && conn.IsSchedulable)
                {
                    if (busyFirst is null) busyFirst = conn;
                    else busySecond ??= conn;
                }
            }
            else if (Interlocked.CompareExchange(ref item, future ??= new ConnectionFuture(), conn) == conn)
            {
                // Empty slot, open new.
                connection = default;
                return false;
            }
        }

        if (exhaustive)
        {
            for (var i = startIndex; i < startIndex + connections.Length; i++)
            {
                ref var item = ref connections[i < connections.Length ? i : i - connections.Length];
                if (TryGetConnection(ref item, out var candidate) && !candidate.IsIdle &&
                    DoSchedule(new(candidate, cancellationToken, isIdleCandidate: false), schedule, state))
                {
                    future = null;
                    connection = candidate;
                    return true;
                }
            }

            future = null;
            connection = default;
            return false;
        }

        // Selection reads are advisory, so try both sampled candidates before parking.
        if (busyFirst is not null)
        {
            var pick = busySecond is not null && busyFirst.CompareTo(busySecond) > 0 ? busySecond : busyFirst;
            if (DoSchedule(new(pick, cancellationToken, isIdleCandidate: false), schedule, state))
            {
                future = null;
                connection = pick;
                return true;
            }
            var runnerUp = ReferenceEquals(pick, busyFirst) ? busySecond : busyFirst;
            if (runnerUp is not null && DoSchedule(new(runnerUp, cancellationToken, isIdleCandidate: false), schedule, state))
            {
                future = null;
                connection = runnerUp;
                return true;
            }
        }

        future = null;
        connection = default;
        return false;
    }

    // Must complete the future before exiting.
    T OpenConnection<TState>(ConnectionFuture future, Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout)
    {
        Debug.Assert(!future.IsCompleted);

        T? conn = null;
        var admitted = false;
        bool scheduled;
        try
        {
            var started = _metrics?.StartConnectionCreate() ?? 0;
            try
            {
                conn = _factory.Create(_context, timeout);
                _metrics?.ReportConnectionCreated(started);
            }
            catch
            {
                _metrics?.ReportConnectionCreateFailed();
                throw;
            }

            // The claimed future makes disposal wait for this opener. Check disposal before admission;
            // visibility alone does not make the connection rentable.
            lock (SyncObj)
                ThrowIfDisposed();

            // Admit before scheduling so synchronous completion can publish its idle edge.
            conn.Start();
            admitted = true;
            ObserveCompletion(conn);
            scheduled = DoSchedule(new(conn, CancellationToken.None), schedule, state);
            if (!scheduled)
                PublishIdle(conn);
        }
        catch (Exception ex)
        {
            if (!admitted)
            {
                // Creation or admission failed. The pool owns cleanup after installation, but the
                // unopened slot must remain replaceable.
                conn?.CompleteAsync(ex).GetAwaiter().GetResult();
            }
            else
            {
                // Placement failure does not revoke pool ownership. Publish a connection on
                // which the delegate made no progress.
                if (conn!.IsIdle && conn.IsSchedulable)
                    PublishIdle(conn);
            }
            throw;
        }
        finally
        {
            SettleOpener(future, admitted ? conn : null);
        }

        return scheduled || schedule is null ? conn : throw new InvalidOperationException("Could not schedule work on a new connection.");
    }

    // Must complete the future before exiting.
    async ValueTask<T> OpenConnectionAsync<TState>(ConnectionFuture future, Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken)
    {
        Debug.Assert(!future.IsCompleted);

        T? conn = null;
        var admitted = false;
        PooledLinkedSource? timeoutSource = null;
        bool scheduled;
        try
        {
            timeoutSource = RentTimeoutSource(timeout, _timeProvider, cancellationToken);

            var started = _metrics?.StartConnectionCreate() ?? 0;
            try
            {
                conn = await _factory.CreateAsync(_context, cancellationToken: timeoutSource?.Token ?? cancellationToken).ConfigureAwait(false);
                _metrics?.ReportConnectionCreated(started);
            }
            catch
            {
                _metrics?.ReportConnectionCreateFailed();
                throw;
            }

            // Clear ownership before returning the source to its thread-local cache.
            if (timeoutSource is { } source)
            {
                timeoutSource = null;
                await source.DisposeAsync().ConfigureAwait(false);
            }

            // The pending future already owns the slot. Check disposal before admitting the connection.
            lock (SyncObj)
                ThrowIfDisposed();
            conn.Start();
            admitted = true;
            ObserveCompletion(conn);
            scheduled = DoSchedule(new(conn, cancellationToken), schedule, state);
            if (!scheduled)
                PublishIdle(conn);
        }
        catch (Exception ex)
        {
            var wasTimeout = false;
            if (timeoutSource is not null)
            {
                wasTimeout = IsCancellationTokenException(ex, timeoutSource.Token);
                await timeoutSource.DisposeAsync().ConfigureAwait(false);
            }

            if (!admitted)
            {
                // See the synchronous path: creation/admission failure leaves the slot replaceable.
                await (conn?.CompleteAsync(ex) ?? Task.CompletedTask).ConfigureAwait(false);
            }
            else
            {
                // See the synchronous path: keep an admitted, still-idle connection reachable.
                if (conn!.IsIdle && conn.IsSchedulable)
                    PublishIdle(conn);
            }
            if (wasTimeout)
                throw new TimeoutException("The operation has timed out.", ex);

            throw;
        }
        finally
        {
            SettleOpener(future, admitted ? conn : null);
        }

        return scheduled || schedule is null ? conn : throw new InvalidOperationException("Could not schedule work on a new connection.");
    }

    // Publication transfers opener ownership. Wake after it becomes observable: an earlier
    // connection signal may have raced while the slot still contained this future.
    void SettleOpener(ConnectionFuture future, T? conn)
    {
        lock (SyncObj)
            future.Complete(conn);
        SignalAvailability();
    }

    T GetCore<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout)
    {
        var reportAdmissions = _metrics?.AdmissionsEnabled is true;
        var reportTimeouts = _metrics?.AdmissionTimeoutsEnabled is true;
        if (reportAdmissions || reportTimeouts)
            return Observed(schedule, state, timeout, reportAdmissions, reportTimeouts);

        ThrowIfDisposed();

        if (!_acquisitions.HasDemand)
        {
            if (TrySchedule(schedule, state, CancellationToken.None, exhaustive: false, out var future, out var conn))
                return conn;

            if (future is not null)
                return OpenConnection(future, schedule, state, timeout);
        }

        return WaitForAvailability(schedule, state, timeout);

        T Observed(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout,
            bool reportAdmissions, bool reportTimeouts)
        {
            try
            {
                ThrowIfDisposed();

                if (!_acquisitions.HasDemand)
                {
                    if (TrySchedule(schedule, state, CancellationToken.None, exhaustive: false, out var future, out var conn))
                        return ReportAdmission(conn, waited: false, reportAdmissions);

                    if (future is not null)
                        return ReportAdmission(OpenConnection(future, schedule, state, timeout), waited: false, reportAdmissions);
                }

                return ReportAdmission(WaitForAvailability(schedule, state, timeout), waited: true, reportAdmissions);
            }
            catch (TimeoutException) when (reportTimeouts)
            {
                _metrics!.ReportAdmissionTimeout();
                throw;
            }
        }
    }

    public T Get(TimeSpan timeout)
        => GetCore(AlwaysTrue, null, timeout);
    public T Get<TState>(Func<ConnectionCandidate<T>, TState, bool> schedule, TState state, TimeSpan timeout)
        => GetCore(schedule, state, timeout);

    static readonly Func<ConnectionCandidate<T>, object?, bool> AlwaysTrue = static (_, _) => true;
    ValueTask<T> GetCoreAsync<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var reportAdmissions = _metrics?.AdmissionsEnabled is true;
        var reportTimeouts = _metrics?.AdmissionTimeoutsEnabled is true;
        if (!_acquisitions.HasDemand)
        {
            if (TrySchedule(schedule, state, cancellationToken, exhaustive: false, out var future, out var conn))
                return new(ReportAdmission(conn, waited: false, reportAdmissions));

            if (future is not null)
            {
                var task = OpenConnectionAsync(future, schedule, state, timeout, cancellationToken);
                return reportAdmissions || reportTimeouts
                    ? ObserveAdmissionAsync(task, waited: false, reportAdmissions, reportTimeouts, _metrics!)
                    : task;
            }
        }

        var waitTask = WaitForAvailabilityAsync(schedule, state, timeout, cancellationToken);
        return reportAdmissions || reportTimeouts
            ? ObserveAdmissionAsync(waitTask, waited: true, reportAdmissions, reportTimeouts, _metrics!)
            : waitTask;
    }

    T ReportAdmission(T connection, bool waited, bool enabled)
    {
        if (enabled)
            _metrics!.ReportAdmission(waited);
        return connection;
    }

    static ValueTask<T> ObserveAdmissionAsync(ValueTask<T> task, bool waited,
        bool reportAdmissions, bool reportTimeouts, PoolMetricsReporter metrics)
    {
        if (task.IsCompletedSuccessfully)
        {
            if (reportAdmissions)
                metrics.ReportAdmission(waited);
            return new(task.Result);
        }
        return Awaited(task, waited, reportAdmissions, reportTimeouts, metrics);

        static async ValueTask<T> Awaited(ValueTask<T> task, bool waited,
            bool reportAdmissions, bool reportTimeouts, PoolMetricsReporter metrics)
        {
            try
            {
                var connection = await task.ConfigureAwait(false);
                if (reportAdmissions)
                    metrics.ReportAdmission(waited);
                return connection;
            }
            catch (TimeoutException) when (reportTimeouts)
            {
                metrics.ReportAdmissionTimeout();
                throw;
            }
        }
    }

    public ValueTask<T> GetAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        => GetCoreAsync(AlwaysTrue, null, timeout, cancellationToken);
    public ValueTask<T> GetAsync<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken = default)
        => GetCoreAsync(schedule, state, timeout, cancellationToken);

    // Get normally transfers the idle token to work queued by its caller; that work returns the
    // token when the connection becomes idle again. A caller that deliberately acquired without
    // queuing anything must return that token explicitly.
    internal void ReturnUnscheduled(T connection)
    {
        ThrowIfDisposed();
        if (!connection.IsIdle || !connection.IsSchedulable)
            throw new InvalidOperationException("Only an idle, schedulable connection can be returned without work.");
        PublishIdle(connection);
    }

    // Prefer DisposeAsync, so keep this explicitly implemented.
    void IDisposable.Dispose()
    {
        var tasks = DisposeCore(out var ownsDisposal);
        if (!ownsDisposal)
            return;

        try
        {
            if (tasks is not null)
                Task.WhenAll(tasks).GetAwaiter().GetResult();
            WaitForDetachedTasksAsync().GetAwaiter().GetResult();
        }
        finally
        {
            _heartbeat.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        var tasks = DisposeCore(out var ownsDisposal);
        if (!ownsDisposal)
            return;

        try
        {
            if (tasks is not null)
                await Task.WhenAll(tasks).ConfigureAwait(false);
            await WaitForDetachedTasksAsync().ConfigureAwait(false);
        }
        finally
        {
            _heartbeat.Dispose();
        }
    }

    internal void TrackDetached(Task task)
    {
        var tasks = Volatile.Read(ref _detachedTasks);
        if (tasks is null)
        {
            var created = new ConcurrentDictionary<Task, byte>();
            tasks = Interlocked.CompareExchange(ref _detachedTasks, created, null) ?? created;
        }
        tasks.TryAdd(task, 0);
        _ = RemoveWhenCompletedAsync(tasks, task);

        static async Task RemoveWhenCompletedAsync(ConcurrentDictionary<Task, byte> tasks, Task tracked)
        {
            try { await tracked.ConfigureAwait(false); }
            catch { }
            finally { tasks.TryRemove(tracked, out _); }
        }
    }

    async Task WaitForDetachedTasksAsync()
    {
        var tasks = Volatile.Read(ref _detachedTasks);
        while (tasks is not null && !tasks.IsEmpty)
            await Task.WhenAll(tasks.Keys).ConfigureAwait(false);
    }

    Task[]? DisposeCore(out bool ownsDisposal)
    {
        if (Volatile.Read(ref _disposed))
        {
            ownsDisposal = false;
            return null;
        }

        List<T>? connections = null;
        List<Task<T?>>? openings = null;
        lock (SyncObj)
        {
            if (_disposed)
            {
                ownsDisposal = false;
                return null;
            }

            Volatile.Write(ref _disposed, true);
            ownsDisposal = true;
            _acquisitions.Dispose();
            for (var i = 0; i < _connections.Length; i++)
            {
                ref var connSlot = ref _connections[i];
                while (true)
                {
                    var value = Volatile.Read(ref connSlot);
                    if (value is T conn)
                    {
                        (connections ??= []).Add(conn);
                        break;
                    }

                    if (value is not ConnectionFuture future)
                        break;

                    if (!future.IsCompleted)
                    {
                        // SyncObj makes lazy waiter installation atomic with opener settlement.
                        (openings ??= []).Add(future.GetCompletionTask());
                        break;
                    }

                    // A lock-free walker may unwrap first; retry against the new slot value.
                    var result = future.Result;
                    if (ReferenceEquals(Interlocked.CompareExchange(ref connSlot, result, future), future))
                    {
                        if (result is not null)
                            (connections ??= []).Add(result);
                        break;
                    }
                }
            }
        }

        _metrics?.Dispose();

        // Never invoke connection code while holding SyncObj: terminal continuations may settle an
        // opener through the same lock. Initiating directly here avoids a thread-pool hop without
        // admitting re-entrancy into the pool's ownership walk.
        var count = (connections?.Count ?? 0) + (openings?.Count ?? 0);
        if (count is 0)
            return null;
        var pending = new Task[count];
        var index = 0;
        if (connections is not null)
            foreach (var connection in connections)
                pending[index++] = CompleteConnectionAsync(connection);
        if (openings is not null)
            foreach (var opening in openings)
                pending[index++] = CompleteOpeningAsync(opening);
        return pending;

        async Task CompleteConnectionAsync(T connection)
        {
            try
            {
                await connection.CompleteAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SlonLogMessages.PoolConnectionTeardownFailed(_logger, ex, "disposing");
            }
        }

        async Task CompleteOpeningAsync(Task<T?> completion)
        {
            try
            {
                if (await completion.ConfigureAwait(false) is { } connection)
                    await connection.CompleteAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SlonLogMessages.PoolConnectionTeardownFailed(_logger, ex, "disposing");
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static bool TryGetConnection(ref object? item, [NotNullWhen(true)]out T? connection)
    {
        // A slot contains null, a connection, or an open-in-progress future.
        var value = item;
        if (value is T conn)
        {
            connection = conn;
            return true;
        }

        // Unwrap completed futures so later reads regain the direct connection fast path.
        if (value is ConnectionFuture { IsCompleted: true } future)
        {
            // Do not overwrite a slot that advanced while this future was being observed.
            var result = future.Result;
            if (ReferenceEquals(Interlocked.CompareExchange(ref item, result, future), future))
            {
                connection = result;
                return connection is not null;
            }
        }

        Debug.Assert(value is null or ConnectionFuture);
        connection = null;
        return false;
    }

    [ThreadStatic]
    static uint _xorShiftState;

    static int NextRandomStart(int bound)
    {
        var s = _xorShiftState;
        if (s is 0)
            s = (uint)Environment.CurrentManagedThreadId | 1u;
        s ^= s << 13;
        s ^= s >> 17;
        s ^= s << 5;
        _xorShiftState = s;
        return (int)(s % (uint)bound);
    }

    void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed))
            ThrowObjectDisposed();

        static void ThrowObjectDisposed() => throw new ObjectDisposedException(nameof(ConnectionPool<T>));
    }

    readonly record struct Placement
    {
        internal Placement(T connection) => Connection = connection;
        internal Placement(ConnectionFuture future) => Future = future;

        internal T? Connection { get; }
        internal ConnectionFuture? Future { get; }
    }

    readonly record struct PlacementState<TState>(
        ConnectionPool<T> Pool,
        Func<ConnectionCandidate<T>, TState, bool>? Schedule,
        TState State,
        CancellationToken CancellationToken);

    sealed class ConnectionFuture
    {
        T? _conn;
        bool _published;
        TaskCompletionSource<T?>? _completion;

        public void Complete(T? conn)
        {
            Debug.Assert(!_published, "A connection future must settle exactly once.");
            _conn = conn;
            Volatile.Write(ref _published, true);
            _completion?.SetResult(conn);
        }

        // Allocated only when disposal overlaps an unpublished open.
        public Task<T?> GetCompletionTask()
            => (_completion ??= new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;

        public T? Result => Volatile.Read(ref _conn);
        public bool IsCompleted => Volatile.Read(ref _published);
    }

}

static class ConnectionPool
{
    [ThreadStatic]
    static PooledLinkedSource? TimeoutSource;

    internal static bool IsCancellationTokenException(Exception ex, CancellationToken cancellationToken)
        => ex is OperationCanceledException { CancellationToken: var token } && cancellationToken.IsCancellationRequested && token == cancellationToken;

    internal static void ThrowSourceExhausted(Exception? inner = null)
        => throw new TimeoutException($"{nameof(ConnectionPool)} is exhausted, there are no empty spots or connections idle enough to take new work in time.", inner);

    internal static PooledLinkedSource? RentTimeoutSource(TimeSpan timeout, TimeProvider timeProvider,
        CancellationToken cancellationToken = default)
    {
        if (timeout == default || timeout == Timeout.InfiniteTimeSpan)
            return null;

        return Core(timeout, timeProvider, cancellationToken);

        static PooledLinkedSource Core(TimeSpan timeout, TimeProvider timeProvider,
            CancellationToken cancellationToken)
        {
            if (timeout < TimeSpan.Zero)
                throw new TimeoutException("The operation has timed out.");

            PooledLinkedSource timeoutSource;
            if (ReferenceEquals(timeProvider, TimeProvider.System))
            {
                timeoutSource = TimeoutSource ?? new PooledLinkedSource(ReturnTimeoutSource);
                TimeoutSource = null;
                timeoutSource.CancelAfter(timeout);
            }
            else
                timeoutSource = new PooledLinkedSource(timeout, timeProvider);
            timeoutSource.Initialize(cancellationToken.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), timeoutSource));
            return timeoutSource;
        }
    }

    static void ReturnTimeoutSource(PooledLinkedSource timeoutSource)
    {
        if (timeoutSource.TryReset() && TimeoutSource is null)
        {
            TimeoutSource = timeoutSource;
            return;
        }

        ((CancellationTokenSource)timeoutSource).Dispose();
    }
}
