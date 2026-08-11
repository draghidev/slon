using Slon.Runtime;
using Slon.Threading;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
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
    readonly ConnectionWaitQueue _waitQueue = new();
    ConcurrentDictionary<Task, byte>? _detachedTasks;
    internal int WaiterCount => _waitQueue.Count;

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
        return new(open, idle, _connections.Length, _waitQueue.Count);
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
        _waitQueue.Signal();
    }

    void SignalAvailability() => _waitQueue.Signal();

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
        ConnectionWaitQueue.Waiter? waiter = null;
        ConnectionWaitQueue.Wake wake = default;
        try
        {
            while (true)
            {
                waiter ??= _waitQueue.Enqueue();
                if (TryAcquire(waiter, ref wake, schedule, state, cancellationToken,
                    out var future, out var available))
                {
                    waiter = null;
                    await ReleaseTimeoutSource().ConfigureAwait(false);
                    return future is null
                        ? available!
                        : await OpenConnectionAsync(future, schedule, state, deadline.GetRemaining(), cancellationToken).ConfigureAwait(false);
                }

                wake = await waiter.Task.WaitAsync(timeoutSource?.Token ?? cancellationToken).ConfigureAwait(false);
                waiter = null;

                // The signal carries no resource. Recheck pool state before handing the bounded
                // pass to the next waiter.
                if (TryAcquire(null, ref wake, schedule, state, cancellationToken,
                    out future, out available))
                {
                    await ReleaseTimeoutSource().ConfigureAwait(false);
                    return future is null
                        ? available!
                        : await OpenConnectionAsync(future, schedule, state, deadline.GetRemaining(), cancellationToken).ConfigureAwait(false);
                }

                waiter = _waitQueue.Requeue(wake, !_idle.IsEmpty);
                wake = default;
            }
        }
        catch (Exception ex)
        {
            if (waiter is not null)
                _waitQueue.Remove(waiter, !_idle.IsEmpty);
            if (wake.Remaining != 0)
                _waitQueue.Pass(wake, !_idle.IsEmpty);

            var token = CancellationToken.None;
            if (timeoutSource is not null)
            {
                token = timeoutSource.Token;
                await timeoutSource.DisposeAsync().ConfigureAwait(false);
            }

            if (IsCancellationTokenException(ex, token))
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
        ConnectionWaitQueue.Waiter? waiter = null;
        ConnectionWaitQueue.Wake wake = default;
        try
        {
            while (true)
            {
                waiter ??= _waitQueue.Enqueue(synchronous: true);
                if (TryAcquire(waiter, ref wake, schedule, state, CancellationToken.None,
                    out var future, out var available))
                {
                    waiter = null;
                    return future is null
                        ? available!
                        : OpenConnection(future, schedule, state, deadline.GetRemaining());
                }

                var settling = waiter;
                waiter = null;
                try
                {
                    wake = _waitQueue.Wait(settling, timeoutSource?.Token ?? CancellationToken.None);
                }
                catch (OperationCanceledException ex) when (timeoutSource is not null &&
                    IsCancellationTokenException(ex, timeoutSource.Token))
                {
                    ThrowSourceExhausted(ex);
                    return default!;
                }

                if (TryAcquire(null, ref wake, schedule, state, CancellationToken.None,
                    out future, out available))
                {
                    return future is null
                        ? available!
                        : OpenConnection(future, schedule, state, deadline.GetRemaining());
                }

                waiter = _waitQueue.Requeue(wake, !_idle.IsEmpty, synchronous: true);
                wake = default;
            }
        }
        catch
        {
            if (waiter is not null)
                _waitQueue.Remove(waiter, !_idle.IsEmpty);
            if (wake.Remaining != 0)
                _waitQueue.Pass(wake, !_idle.IsEmpty);
            throw;
        }
        finally
        {
            timeoutSource?.Dispose();
        }
    }

    bool TryAcquire<TState>(ConnectionWaitQueue.Waiter? waiter, ref ConnectionWaitQueue.Wake wake,
        Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, CancellationToken cancellationToken,
        out ConnectionFuture? future, out T? connection)
    {
        if (waiter is not null && !waiter.TryTakeRescan())
        {
            future = null;
            connection = null;
            return false;
        }
        if (!TrySchedule(schedule, state, cancellationToken, out future, out connection) && future is null)
            return false;

        if (wake.Remaining != 0)
        {
            _waitQueue.Consume(wake, !_idle.IsEmpty);
            wake = default;
        }
        else if (waiter is not null)
        {
            _waitQueue.Remove(waiter, !_idle.IsEmpty);
        }
        return true;
    }

    bool TrySchedule<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state,
        CancellationToken cancellationToken, out ConnectionFuture? future, [NotNullWhen(true)]out T? connection)
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
                if (!conn.IsIdle && conn.IsSchedulable)
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

        if (!_waitQueue.HasDemand)
        {
            if (TrySchedule(schedule, state, CancellationToken.None, out var future, out var conn))
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

                if (!_waitQueue.HasDemand)
                {
                    if (TrySchedule(schedule, state, CancellationToken.None, out var future, out var conn))
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
        if (!_waitQueue.HasDemand)
        {
            if (TrySchedule(schedule, state, cancellationToken, out var future, out var conn))
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
            _waitQueue.Dispose();
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

    internal sealed class ConnectionWaitQueue
    {
        readonly Lock _lock = new();
        Waiter? _head;
        Waiter? _tail;
        int _count;
        int _activeWakes;
        int _demand;
        bool _signalPending;
        bool _disposed;

        // Advisory rent-path gate, deliberately lock-free. Publish queued and awakened demand as
        // one level so a newcomer cannot observe a false zero while a waiter changes form.
        public bool HasDemand
            => Volatile.Read(ref _demand) != 0;

        public int Count
        {
            get
            {
                lock (_lock)
                    return _count;
            }
        }

        public Waiter Enqueue(bool synchronous = false)
        {
            var waiter = new Waiter(synchronous);
            Waiter? signaled = null;
            Wake wake = default;
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                waiter.CanRescan = _head is null && _activeWakes == 0;
                if (waiter.CanRescan)
                    _signalPending = false;
                waiter.Previous = _tail;
                if (_tail is null)
                    _head = waiter;
                else
                    _tail.Next = waiter;
                _tail = waiter;
                _count++;

                // A prior waiter may have rejected an available candidate before this waiter
                // existed. Reattach that retained edge to the FIFO now that it can be passed.
                if (!waiter.CanRescan && _signalPending && _activeWakes == 0)
                {
                    signaled = _head!;
                    wake = new Wake(_count);
                    _activeWakes++;
                    Unlink(signaled);
                    _signalPending = false;
                    signaled.Wake = wake;
                }
                PublishDemand();
            }
            signaled?.Complete(wake);
            return waiter;
        }

        public void Signal()
        {
            Waiter? waiter;
            Wake wake;
            lock (_lock)
            {
                waiter = _head;
                if (waiter is null)
                {
                    if (_activeWakes != 0)
                        _signalPending = true;
                    return;
                }

                var count = _count;
                wake = new Wake(count);
                _activeWakes++;
                Unlink(waiter);
                _signalPending = false;
                waiter.Wake = wake;
                PublishDemand();
            }
            waiter.Complete(wake);
        }

        public void Pass(Wake wake, bool idleAvailable = false)
        {
            Waiter? waiter;
            Wake next;
            lock (_lock)
            {
                waiter = _head;
                if ((wake.Remaining > 1 || idleAvailable) && waiter is not null)
                {
                    Unlink(waiter);
                    next = new Wake(Math.Max(1, wake.Remaining - 1));
                    _signalPending = false;
                    waiter.Wake = next;
                }
                else if (_signalPending && waiter is not null)
                {
                    Unlink(waiter);
                    next = new Wake(1);
                    _signalPending = false;
                    waiter.Wake = next;
                }
                else
                {
                    Debug.Assert(_activeWakes > 0);
                    _activeWakes--;
                    _signalPending = false;
                    PublishDemand();
                    return;
                }
                PublishDemand();
            }
            waiter.Complete(next);
        }

        // Converts the current wake ownership back into a queued wait without publishing a
        // demand-free interval. A newcomer must not enter between those two forms of the same
        // rent operation.
        public Waiter Requeue(Wake wake, bool idleAvailable, bool synchronous = false)
        {
            var current = new Waiter(synchronous);
            Waiter? waiter = null;
            Wake next = default;
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                Debug.Assert(_activeWakes > 0);

                // Only a waiter which was already queued may inherit the availability this
                // caller just rejected. Waking the caller's replacement for the same idle
                // candidate would make an incompatible renter spin indefinitely.
                var successor = _head;

                current.Previous = _tail;
                if (_tail is null)
                    _head = current;
                else
                    _tail.Next = current;
                _tail = current;
                _count++;

                if ((wake.Remaining > 1 || idleAvailable || _signalPending) && successor is not null)
                {
                    waiter = successor;
                    Unlink(successor);
                    next = new Wake(wake.Remaining > 1 ? wake.Remaining - 1 : 1);
                    _signalPending = false;
                    waiter.Wake = next;
                }
                else
                {
                    _activeWakes--;
                    if (idleAvailable || _signalPending)
                    {
                        _signalPending = true;
                    }
                    else if (successor is null)
                    {
                        current.CanRescan = true;
                    }
                }
                PublishDemand();
            }
            waiter?.Complete(next);
            return current;
        }

        public void Consume(Wake wake, bool idleAvailable)
        {
            Debug.Assert(wake.Remaining != 0);
            Waiter? waiter = null;
            Wake next = default;
            lock (_lock)
            {
                Debug.Assert(_activeWakes > 0);
                if ((_signalPending || idleAvailable) && (waiter = _head) is not null)
                {
                    Unlink(waiter);
                    next = new Wake(1);
                    _signalPending = false;
                    waiter.Wake = next;
                }
                else
                {
                    _activeWakes--;
                    _signalPending = false;
                }
                PublishDemand();
            }
            waiter?.Complete(next);
        }

        // Removes a queued waiter or preserves a wake already detached for it.
        public void Remove(Waiter waiter, bool idleAvailable = false)
        {
            var detached = TryRemove(waiter, out var wake, out var successor);
            successor?.Complete(successor.Wake);
            if (detached)
            {
                Pass(wake, idleAvailable);
                waiter.WaitForDetachedSignal();
            }
            waiter.Dispose();
        }

        bool TryRemove(Waiter waiter, out Wake wake, out Waiter? successor)
        {
            lock (_lock)
            {
                successor = null;
                if (waiter.IsQueued)
                {
                    var wasHead = ReferenceEquals(waiter, _head);
                    Unlink(waiter);
                    if (wasHead && _activeWakes == 0 && _head is { } head)
                    {
                        wake = new(1);
                        _activeWakes++;
                        Unlink(head);
                        _signalPending = false;
                        head.Wake = wake;
                        successor = head;
                    }
                    else
                    {
                        wake = default;
                    }
                    PublishDemand();
                    return false;
                }

                wake = waiter.Wake;
                return wake.Remaining != 0;
            }
        }

        public Wake Wait(Waiter waiter, CancellationToken cancellationToken)
        {
            try
            {
                try
                {
                    waiter.Wait(cancellationToken);
                    waiter.ThrowIfFailed();
                    return waiter.Wake;
                }
                catch (OperationCanceledException)
                {
                }
                var detached = TryRemove(waiter, out var wake, out var successor);
                successor?.Complete(successor.Wake);
                if (!detached)
                {
                    waiter.ThrowIfFailed();
                    cancellationToken.ThrowIfCancellationRequested();
                }

                // Signaling detached the waiter just as cancellation fired.
                waiter.Wait(CancellationToken.None);
                waiter.ThrowIfFailed();
                return wake;
            }
            finally
            {
                waiter.Dispose();
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed)
                    return;
                _disposed = true;
                var waiters = _head;
                _head = _tail = null;
                _count = 0;
                PublishDemand();
                // Publish failure before a timed sync waiter can observe detachment and dispose
                // its signal. Async waiters complete asynchronously. Sync completion only sets the event.
                while (waiters is not null)
                {
                    var next = waiters.Next;
                    waiters.Next = waiters.Previous = null;
                    waiters.IsQueued = false;
                    waiters.Fail();
                    waiters = next;
                }
            }
        }

        void Unlink(Waiter waiter)
        {
            if (waiter.Previous is null)
                _head = waiter.Next;
            else
                waiter.Previous.Next = waiter.Next;
            if (waiter.Next is null)
                _tail = waiter.Previous;
            else
                waiter.Next.Previous = waiter.Previous;

            waiter.Next = waiter.Previous = null;
            waiter.IsQueued = false;
            _count--;
        }

        void PublishDemand()
        {
            Debug.Assert(_lock.IsHeldByCurrentThread);
            Volatile.Write(ref _demand, _count + _activeWakes);
        }

        internal sealed class Waiter : IDisposable
        {
            readonly TaskCompletionSource<Wake>? _completion;
            readonly ManualResetEventSlim? _signal;
            Exception? _failure;

            internal Waiter(bool synchronous)
            {
                if (synchronous)
                    _signal = new();
                else
                    _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            internal Waiter? Previous;
            internal Waiter? Next;
            internal bool IsQueued = true;
            internal bool CanRescan;
            internal Wake Wake;

            public Task<Wake> Task => _completion!.Task;
            internal bool TryTakeRescan()
            {
                if (!CanRescan)
                    return false;
                CanRescan = false;
                return true;
            }
            internal void Wait(CancellationToken cancellationToken) => _signal!.Wait(cancellationToken);
            internal void WaitForDetachedSignal() => _signal?.Wait();
            public void Dispose() => _signal?.Dispose();
            internal void ThrowIfFailed()
            {
                if (_failure is { } failure)
                    throw failure;
            }
            internal void Complete(Wake wake)
            {
                if (_completion is not null)
                    _completion.TrySetResult(wake);
                else
                    _signal!.Set();
            }
            internal void Fail()
            {
                var failure = new ObjectDisposedException(nameof(ConnectionPool<T>));
                if (_completion is not null)
                    _completion.TrySetException(failure);
                else
                {
                    _failure = failure;
                    _signal!.Set();
                }
            }
        }

        internal readonly record struct Wake(int Remaining);
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
