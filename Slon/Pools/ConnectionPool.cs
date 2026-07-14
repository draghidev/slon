using Slon.Pg.Protocol;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using static Slon.Pools.ConnectionPool;

namespace Slon.Pools;

public class ConnectionPoolOptions
{
    public int MaxConnections { get; set; }
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(1);
}

public sealed class ConnectionPool<T> : IDisposable, IAsyncDisposable
    where T : class, IPoolConnection<T>
{
    volatile bool _disposed;
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

    readonly ChannelWriter<T> _idleWriter;
    readonly ChannelReader<T> _idleReader;

    readonly Heartbeat _heartbeat;

    public ConnectionPool(IPoolConnectionFactory<T> factory, ConnectionPoolOptions options)
    {
        var maxConnections = options.MaxConnections;
        if (maxConnections <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxConnections), "Cannot be zero or negative.");

        _connections = new object?[maxConnections];

        _factory = factory;
        var channel = Channel.CreateUnbounded<T>(new() { AllowSynchronousContinuations = false });
        (_idleWriter, _idleReader) = (channel.Writer, channel.Reader);
        _heartbeat = new(options.HeartbeatInterval, options.TimeProvider);
        _context = new ConnectionPoolContext<T>(
            conn => _idleWriter.TryWrite(conn),
            (conn, action)  => _heartbeat.Register(interval => action(conn, interval))
        );
    }

    bool DoSchedule<TState>(ConnectionCandidate<T> context, Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, bool newConnection = false)
    {
        ThrowIfDisposed();

        // Advisory only; the connection's scheduling gate makes the definitive decision.
        if (!context.Connection.IsSchedulable)
            return false;

        if (schedule is not null && schedule(context, state))
            return true;

        return false;
    }

    void ReturnIdleToken(T connection)
    {
        // A channel read owns the idle token until scheduling succeeds or the token is returned.
        if (connection.IsIdle && connection.IsSchedulable)
            _idleWriter.TryWrite(connection);
    }

    async ValueTask<T> ScheduleOnIdleAsync<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var timeoutSource = RentTimeoutSource(timeout, cancellationToken);
        try
        {
            while (true)
            {
                // TODO this is unfair, starvation may occur if other callers keep all connections fully pipelined.
                // TODO maybe track idle waiters and force some number of connections to go idle.
                var conn = await _idleReader.ReadAsync(timeoutSource?.Token ?? cancellationToken).ConfigureAwait(false);
                if (conn.IsIdle)
                {
                    bool scheduled;
                    try
                    {
                        scheduled = DoSchedule(new(conn, cancellationToken), schedule, state);
                    }
                    catch
                    {
                        ReturnIdleToken(conn);
                        throw;
                    }

                    if (scheduled)
                    {
                        // Clear ownership before returning the source to its thread-local cache.
                        if (timeoutSource is { } source)
                        {
                            timeoutSource = null;
                            await source.DisposeAsync().ConfigureAwait(false);
                        }
                        return conn;
                    }
                }

                ReturnIdleToken(conn);
                // Let another waiter consume a token we may just have returned.
                await Task.Yield();
            }
        }
        catch (Exception ex)
        {
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
    }

    bool TrySchedule<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, CancellationToken cancellationToken, out ConnectionFuture? future, [NotNullWhen(true)]out T? connection)
    {
        // Prefer idle reuse, then growth, then multiplexing.

        while (_idleReader.TryRead(out var idle))
        {
            if (idle.IsIdle)
            {
                try
                {
                    if (DoSchedule(new(idle, cancellationToken), schedule, state))
                    {
                        future = null;
                        connection = idle;
                        return true;
                    }
                }
                catch
                {
                    ReturnIdleToken(idle);
                    throw;
                }
            }

            ReturnIdleToken(idle);
            // Do not consume our own re-publication in this synchronous drain.
            if (idle.IsIdle && idle.IsSchedulable)
                break;
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

                // The channel is the sole idle rendezvous; the slot walk only samples busy work.
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
            if (DoSchedule(new(pick, cancellationToken, wasIdle: false), schedule, state))
            {
                future = null;
                connection = pick;
                return true;
            }
            var runnerUp = ReferenceEquals(pick, busyFirst) ? busySecond : busyFirst;
            if (runnerUp is not null && DoSchedule(new(runnerUp, cancellationToken, wasIdle: false), schedule, state))
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
        var installed = false;
        bool scheduled;
        try
        {
            conn = _factory.Create(_context, timeout);

            // Install ownership before admission; visibility alone does not make an idle connection rentable.
            lock (SyncObj)
            {
                ThrowIfDisposed();
                installed = true;
            }

            // Admit before scheduling so synchronous completion can publish its idle edge.
            conn.Start();
            scheduled = DoSchedule(new(conn, CancellationToken.None), schedule, state, newConnection: true);
            if (!scheduled)
                _idleWriter.TryWrite(conn);
        }
        catch (Exception ex)
        {
            if (!installed)
            {
                // The connection never became a pool resource.
                conn?.CompleteAsync(ex).GetAwaiter().GetResult();
            }
            else
            {
                // Placement failure does not revoke pool ownership. If the delegate made no
                // progress, restore the idle token it consumed; otherwise the connection's own
                // eventual busy-to-idle edge republishes it.
                ReturnIdleToken(conn!);
            }
            throw;
        }
        finally
        {
            // Publication transfers opener ownership and wakes a racing disposal waiter.
            lock (SyncObj)
                future.Complete(installed ? conn : null);
        }

        return scheduled || schedule is null ? conn : throw new InvalidOperationException("Could not schedule work on a new connection.");
    }

    // Must complete the future before exiting.
    async ValueTask<T> OpenConnectionAsync<TState>(ConnectionFuture future, Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken)
    {
        Debug.Assert(!future.IsCompleted);

        T? conn = null;
        var installed = false;
        PooledLinkedSource? timeoutSource = null;
        bool scheduled;
        try
        {
            timeoutSource = RentTimeoutSource(timeout, cancellationToken);

            conn = await _factory.CreateAsync(_context, cancellationToken: timeoutSource?.Token ?? cancellationToken).ConfigureAwait(false);

            // Clear ownership before returning the source to its thread-local cache.
            if (timeoutSource is { } source)
            {
                timeoutSource = null;
                await source.DisposeAsync().ConfigureAwait(false);
            }

            // Install, admit, then allow work to begin.
            lock (SyncObj)
            {
                ThrowIfDisposed();
                installed = true;
            }
            conn.Start();
            scheduled = DoSchedule(new(conn, cancellationToken), schedule, state, newConnection: true);
            if (!scheduled)
                _idleWriter.TryWrite(conn);
        }
        catch (Exception ex)
        {
            var wasTimeout = false;
            if (timeoutSource is not null)
            {
                wasTimeout = IsCancellationTokenException(ex, timeoutSource.Token);
                await timeoutSource.DisposeAsync().ConfigureAwait(false);
            }

            if (!installed)
            {
                // The connection never became a pool resource.
                await (conn?.CompleteAsync(ex) ?? Task.CompletedTask).ConfigureAwait(false);
            }
            else
            {
                // See the synchronous path: keep an admitted, still-idle connection reachable.
                ReturnIdleToken(conn!);
            }
            if (wasTimeout)
                throw new TimeoutException("The operation has timed out.", ex);

            throw;
        }
        finally
        {
            // Publication transfers opener ownership and wakes a racing disposal waiter.
            lock (SyncObj)
                future.Complete(installed ? conn : null);
        }

        return scheduled || schedule is null ? conn : throw new InvalidOperationException("Could not schedule work on a new connection.");
    }

    public ValueTask<T> GetConnectionAsync(long id, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (GetInternal(id, out var future, out var result))
            return new(result);

        if (future is not null)
            return OpenConnectionAsync<object?>(future, static (_,_) => true, null, timeout, cancellationToken);

        return _idleReader.ReadAsync(cancellationToken);

        bool GetInternal(long id, out ConnectionFuture? future, [NotNullWhen(true)]out T? result)
        {
            // Unsigned modulo handles negative ids.
            var connections = _connections.AsSpan();
            var index = (int)((ulong)id % (ulong)connections.Length);
            T? connection = null;
            for (var i = index; i < index + connections.Length; i++)
            {
                ref var item = ref connections[i < connections.Length ? i : i - connections.Length];
                ConnectionFuture? mfuture = null;
                if (TryGetConnection(ref item, out connection))
                {
                    // Replace completed connections in place.
                    if (connection.Completion.IsCompleted && Interlocked.CompareExchange(ref item, mfuture ??= new(), connection) == connection)
                    {
                        future = mfuture;
                        result = default;
                        return false;
                    }
                }
                else if (item is null && Interlocked.CompareExchange(ref item, mfuture ??= new(), null) == null)
                {
                    future = mfuture;
                    result = default;
                    return false;
                }
            }

            future = default;
            result = connection;
            return connection is not null;
        }
    }

    public async ValueTask OpenAllConnectionsAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var connections = _connections;
        for (var i = 0; i < connections.Length; i++)
        {
            ConnectionFuture? future = null;
            if (!TryGetConnection(ref connections[i], out var conn) && Interlocked.CompareExchange(ref connections[i], future ??= new(), conn) == conn)
                await OpenConnectionAsync<object?>(future, null, null, timeout, cancellationToken).ConfigureAwait(false);
        }
    }

    T GetCore<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout)
    {
        ThrowIfDisposed();

        if (TrySchedule(schedule, state, CancellationToken.None, out var future, out var conn))
            return conn;

        return future is not null
            ? OpenConnection(future, schedule, state, timeout)
            : ScheduleOnIdleAsync(schedule, state, timeout, CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    public T Get(TimeSpan timeout)
        => GetCore(AlwaysTrue, (object?)null, timeout);
    public T Get<TState>(Func<ConnectionCandidate<T>, TState, bool> schedule, TState state, TimeSpan timeout)
        => GetCore(schedule, state, timeout);

    static readonly Func<ConnectionCandidate<T>, object?, bool> AlwaysTrue = static (_, _) => true;

    ValueTask<T> GetCoreAsync<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (TrySchedule(schedule, state, cancellationToken, out var future, out var conn))
            return new(conn);

        return future is not null
            ? OpenConnectionAsync(future, schedule, state, timeout, cancellationToken)
            : ScheduleOnIdleAsync(schedule, state, timeout, cancellationToken);
    }

    public ValueTask<T> GetAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        => GetCoreAsync(AlwaysTrue, (object?)null, timeout, cancellationToken);
    public ValueTask<T> GetAsync<TState>(Func<ConnectionCandidate<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken = default)
        => GetCoreAsync(schedule, state, timeout, cancellationToken);

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
        }
        finally
        {
            _heartbeat.Dispose();
        }
    }

    Task[]? DisposeCore(out bool ownsDisposal)
    {
        if (_disposed)
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

            _disposed = true;
            ownsDisposal = true;
            _idleWriter.Complete();
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

        static async Task CompleteConnectionAsync(T connection)
        {
            try
            {
                await connection.CompleteAsync().ConfigureAwait(false);
            }
            catch
            {
                // TODO This 'should' log something.
            }
        }

        static async Task CompleteOpeningAsync(Task<T?> completion)
        {
            try
            {
                if (await completion.ConfigureAwait(false) is { } connection)
                    await connection.CompleteAsync().ConfigureAwait(false);
            }
            catch
            {
                // TODO This 'should' log something.
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
        if (_disposed)
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
}

static class ConnectionPool
{
    internal static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(1);

    [ThreadStatic]
    static PooledLinkedSource? TimeoutSource;

    internal static bool IsCancellationTokenException(Exception ex, CancellationToken cancellationToken)
        => ex is OperationCanceledException { CancellationToken: var token } && cancellationToken.IsCancellationRequested && token == cancellationToken;

    internal static void ThrowSourceExhausted(Exception? inner = null)
        => throw new TimeoutException($"{nameof(ConnectionPool)} is exhausted, there are no empty spots or connections idle enough to take new work in time.", inner);

    internal static PooledLinkedSource? RentTimeoutSource(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (timeout == default || timeout == Timeout.InfiniteTimeSpan)
            return null;

        return Core(timeout, cancellationToken);

        static PooledLinkedSource Core(TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (timeout < TimeSpan.Zero)
                throw new TimeoutException("The operation has timed out.");

            var timeoutSource = TimeoutSource;
            TimeoutSource = null;
            timeoutSource ??= new PooledLinkedSource(ReturnTimeoutSource);
            timeoutSource.CancelAfter(timeout);
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
