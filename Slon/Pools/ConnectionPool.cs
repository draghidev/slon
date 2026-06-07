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

    readonly StripedRef<object?> _stripedConnections;

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

        // TODO only do at some point, until a certain max, etc.
        var count = maxConnections < Environment.ProcessorCount ? 1 : Environment.ProcessorCount;

        _stripedConnections = new StripedRef<object?>(count,
            maxConnections < Environment.ProcessorCount ? maxConnections : Math.Max(1, maxConnections / Environment.ProcessorCount)
            );

        _factory = factory;
        var channel = Channel.CreateUnbounded<T>(new() { AllowSynchronousContinuations = false });
        (_idleWriter, _idleReader) = (channel.Writer, channel.Reader);
        _heartbeat = new(options.HeartbeatInterval, options.TimeProvider);
        _context = new ConnectionPoolContext<T>(
            conn => _idleWriter.TryWrite(conn),
            (conn, action)  => _heartbeat.Register(interval => action(conn, interval))
        );
    }

    bool DoSchedule<TState>(SchedulingContext<T> context, Func<SchedulingContext<T>, TState, bool>? schedule, TState state, bool newConnection = false)
    {
        // We may have raced against a dispose.
        ThrowIfDisposed();

        if (context.Connection.IsCompleted)
            return false;

        if (schedule is not null && schedule(context, state))
            return true;

        // No re-enqueue on stripe-walk failure: those conns are non-idle, putting them on the
        // idle channel would pollute it (every channel reader would have to defend against
        // non-idle conns). If the conn actually goes idle, its own idle signal puts it on the
        // channel. We don't have to anticipate.
        return false;
    }

    async ValueTask<T> ScheduleOnIdleAsync<TState>(Func<SchedulingContext<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var timeoutSource = RentTimeoutSource(timeout, cancellationToken);
        try
        {
            while (true)
            {
                // TODO this is unfair, starvation may occur if other callers keep all connections fully pipelined.
                // TODO maybe track idle waiters and force some number of connections to go idle.
                var conn = await _idleReader.ReadAsync(timeoutSource?.Token ?? cancellationToken).ConfigureAwait(false);
                if (conn.IsIdle && DoSchedule(new(conn, cancellationToken), schedule, state))
                {
                    if (timeoutSource is not null)
                        await timeoutSource.DisposeAsync().ConfigureAwait(false);
                    return conn;
                }
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

    bool TrySchedule<TState>(Func<SchedulingContext<T>, TState, bool>? schedule, TState state, CancellationToken cancellationToken, out ConnectionFuture? future, [NotNullWhen(true)]out T? connection)
    {
        // Preference order: idle channel > empty slot (open new) > multiplex onto busy conn.
        // Idle-channel reuse is the cheapest path. Opening a new conn is preferred over
        // multiplexing onto a non-idle one so capacity is used before pipelining starts.

        while (_idleReader.TryRead(out var idle))
        {
            if (idle.IsIdle && DoSchedule(new(idle, cancellationToken), schedule, state))
            {
                future = null;
                connection = idle;
                return true;
            }
        }

        var connections = _stripedConnections.Current;
        // Thread-local XorShift start: zero atomic ops, just a TLS field and one mix step. Each
        // thread picks a different offset so concurrent walkers don't pile onto slot 0. Works
        // correctly under TPC pinning and general-thread schedulers alike.
        var startIndex = connections.Length > 1
            ? NextRandomStart(connections.Length)
            : 0;

        future = null;
        // Single pass through stripes. Empty / completed slots claim immediately (open new).
        // Non-idle conns are remembered as the multiplex fallback. Only taken after the walk
        // confirms no growth path is available.
        T? multiplexCandidate = null;
        for (var i = startIndex; i < startIndex + connections.Length; i++)
        {
            ref var item = ref connections[i < connections.Length ? i : i - connections.Length];
            if (TryGetConnection(ref item, out var conn))
            {
                // Completed slot, reclaim and open new in its place.
                if (conn.IsCompleted && Interlocked.CompareExchange(ref item, future ??= new ConnectionFuture(), conn) == conn)
                {
                    connection = default;
                    return false;
                }

                // Remember first non-idle as the multiplex fallback. Don't take yet.
                if (multiplexCandidate is null && !conn.IsIdle)
                    multiplexCandidate = conn;
            }
            else if (Interlocked.CompareExchange(ref item, future ??= new ConnectionFuture(), conn) == conn)
            {
                // Empty slot, open new.
                connection = default;
                return false;
            }
        }

        // No growth path. Multiplex onto a busy conn we saw during the walk.
        if (multiplexCandidate is not null && DoSchedule(new(multiplexCandidate, cancellationToken, idle: false), schedule, state))
        {
            future = null;
            connection = multiplexCandidate;
            return true;
        }

        future = null;
        connection = default;
        return false;
    }

    // Must complete the future before exiting.
    T OpenConnection<TState>(ConnectionFuture future, Func<SchedulingContext<T>, TState, bool>? schedule, TState state, TimeSpan timeout)
    {
        Debug.Assert(!future.IsCompleted);

        T? conn = null;
        bool scheduled;
        try
        {
            conn = _factory.Create(_context, timeout);

            scheduled = DoSchedule(new(conn, CancellationToken.None), schedule, state, newConnection: true);

            // Complete within the lock to observe any ongoing disposals.
            lock (SyncObj)
            {
                // If this throws we'll complete the future with null and close the connection in the catch handler.
                ThrowIfDisposed();
                if (!scheduled)
                    _idleWriter.TryWrite(conn);
                future.Complete(conn);
            }
            // Start after the lease commit so the connection's idle channel publication is
            // unblocked from here on. Suppressed during startup so a depth-to-zero transition
            // can't publish to the channel before this conn has been assigned to its lessee.
            conn.Start();
        }
        catch (Exception ex)
        {
            future.Complete(null);
            // It's a fresh connection so tearing it down wont take long.
            conn?.CompleteAsync(ex).GetAwaiter().GetResult();

            throw;
        }

        return scheduled || schedule is null ? conn : throw new InvalidOperationException("Could not schedule work on a new connection.");
    }

    // Must complete the future before exiting.
    async ValueTask<T> OpenConnectionAsync<TState>(ConnectionFuture future, Func<SchedulingContext<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken)
    {
        Debug.Assert(!future.IsCompleted);

        T? conn = null;
        PooledLinkedSource? timeoutSource = null;
        bool scheduled;
        try
        {
            timeoutSource = RentTimeoutSource(timeout, cancellationToken);

            conn = await _factory.CreateAsync(_context, cancellationToken: timeoutSource?.Token ?? cancellationToken).ConfigureAwait(false);

            scheduled = DoSchedule(new(conn, cancellationToken), schedule, state, newConnection: true);

            if (timeoutSource is { } source)
                await source.DisposeAsync().ConfigureAwait(false);

            // Complete within the lock to observe any ongoing disposals.
            lock (SyncObj)
            {
                // If this throws we'll complete the future with null and close the connection in the catch handler.
                ThrowIfDisposed();
                if (!scheduled)
                    _idleWriter.TryWrite(conn);
                future.Complete(conn);
            }
            // See OpenConnection for the rationale on Start placement.
            conn.Start();
        }
        catch (Exception ex)
        {
            // First stop any timers.
            var wasTimeout = false;
            if (timeoutSource is not null)
            {
                wasTimeout = IsCancellationTokenException(ex, timeoutSource.Token);
                await timeoutSource.DisposeAsync().ConfigureAwait(false);
            }

            future.Complete(null);
            // It's a fresh connection so tearing it down wont take long.
            await (conn?.CompleteAsync(ex) ?? new()).ConfigureAwait(false);

            if (wasTimeout)
                throw new TimeoutException("The operation has timed out.", ex);

            throw;
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
            // Cast to ulong to avoid Math.Abs(int.MinValue) returning int.MinValue (overflow). The
            // sign bit gets dropped cleanly. Modulo on the unsigned space wraps correctly.
            var index = (int)((ulong)id % (ulong)_stripedConnections.LengthPerStripe);
            var connections = _stripedConnections[(int)((ulong)id % (ulong)_stripedConnections.Length)].AsSpan();
            T? connection = null;
            for (var i = index; i < index + connections.Length; i++)
            {
                ref var item = ref connections[i < connections.Length ? i : i - connections.Length];
                ConnectionFuture? mfuture = null;
                if (TryGetConnection(ref item, out connection))
                {
                    // This is a completed connection which we can replace with a new one.
                    if (connection.IsCompleted && Interlocked.CompareExchange(ref item, mfuture ??= new(), connection) == connection)
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
        for (var i = 0; i < _stripedConnections.Length; i++)
        {
            var connections = _stripedConnections[i];
            for (var j = 0; j < connections.Length; j++)
            {
                ConnectionFuture? future = null;
                if (!TryGetConnection(ref connections[j], out var conn) && Interlocked.CompareExchange(ref connections[j], future ??= new(), conn) == conn)
                    await OpenConnectionAsync<object?>(future, null, null, timeout, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    T GetCore<TState>(Func<SchedulingContext<T>, TState, bool>? schedule, TState state, TimeSpan timeout)
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
    public T Get<TState>(Func<SchedulingContext<T>, TState, bool> schedule, TState state, TimeSpan timeout)
        => GetCore(schedule, state, timeout);

    static readonly Func<SchedulingContext<T>, object?, bool> AlwaysTrue = static (_, _) => true;

    ValueTask<T> GetCoreAsync<TState>(Func<SchedulingContext<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken = default)
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
    public ValueTask<T> GetAsync<TState>(Func<SchedulingContext<T>, TState, bool>? schedule, TState state, TimeSpan timeout, CancellationToken cancellationToken = default)
        => GetCoreAsync(schedule, state, timeout, cancellationToken);

    // Prefer DisposeAsync, so keep this explicitly implemented.
    void IDisposable.Dispose()
    {
        var tasks = DisposeCore();
        if (tasks is not null)
            Task.WhenAll(tasks).GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        var tasks = DisposeCore();
        if (tasks is not null)
            await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    Task[]? DisposeCore()
    {
        if (_disposed)
            return null;

        lock (SyncObj)
        {
            if (_disposed)
                return null;

            _disposed = true;
            _idleWriter.Complete();
            var pending = new List<Task>();
            for (var i = 0; i < _stripedConnections.Length; i++)
            {
                var array = _stripedConnections[i];
                if (array is null)
                    continue;

                for (var j = 0; j < array.Length; j++)
                {
                    ref var connSlot = ref array[j];
                    if (TryGetConnection(ref connSlot, out var conn))
                    {
                        pending.Add(Task.Run(async () =>
                        {
                            try
                            {
                                await conn.CompleteAsync().ConfigureAwait(false);
                            }
                            catch
                            {
                                // TODO This 'should' log something.
                            }
                        }));
                    }
                }
            }

            return pending.Count is 0 ? null : pending.ToArray();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static bool TryGetConnection(ref object? item, [NotNullWhen(true)]out T? connection)
    {
        var value = item;
        if (value?.GetType() == typeof(ConnectionFuture) && (ConnectionFuture)value is { IsCompleted: true } future)
        {
            // We unwrap any previously completed future onto the array element to remove the indirection for future uses.
            // When a connecion open fails the future is completed with a null result.
            // As such the item will be made null and the caller is free to try and open a connection again.
            item = connection = future.Result;
        }
        else
        {
            Debug.Assert(value is null or T);
            connection = Unsafe.As<T?>(value);
        }

        return connection is not null;
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
        bool _reserved;
        bool _published;

        public void Complete(T? conn)
        {
            // Reserve via CAS so only one writer proceeds. Then store _conn before publishing,
            // so any reader observing _published=true is guaranteed to see the conn store. Two
            // flags (reserve + publish) keep the writer ordering clear: reserve gates entry,
            // publish gates visibility.
            if (Interlocked.CompareExchange(ref _reserved, true, false))
                return;
            Volatile.Write(ref _conn, conn);
            Volatile.Write(ref _published, true);
        }

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
            timeoutSource.Initialize(cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), timeoutSource));
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
