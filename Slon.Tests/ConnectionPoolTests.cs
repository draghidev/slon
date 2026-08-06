using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Time.Testing;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pools;
using Slon.Tests.Pg;
using Slon.Transport;

namespace Slon.Tests;

// Adds connection lifecycle management (lease/release via the idle queue, pool-driven
// heartbeat). If cross-connection blocking surfaces here, the lease path or the pool's
// shared heartbeat thread is the coupling. Each test builds a fresh pool so lease/release
// semantics are tested in isolation.
[TestClass]
public class ConnectionPoolTests
{
    sealed class AdmissionConnection : IPoolConnection<AdmissionConnection>
    {
        readonly ConnectionPoolContext<AdmissionConnection> _poolContext;
        int _started;
        int _idle = 1;
        int _schedulable = 1;
        readonly bool _allowPruning;
        readonly bool _throwOnStart;
        TaskCompletionSource? _admissionEntered;
        ManualResetEventSlim? _admissionRelease;
        TaskCompletionSource? _pruningEntered;
        TaskCompletionSource? _pruningRelease;
        readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int _heartbeatCount;
        IDisposable? _heartbeatRegistration;

        public AdmissionConnection(ConnectionPoolContext<AdmissionConnection> context,
            bool registerHeartbeat = false, bool allowPruning = true, bool throwOnStart = false)
        {
            _poolContext = context;
            _allowPruning = allowPruning;
            _throwOnStart = throwOnStart;
            if (registerHeartbeat)
                _heartbeatRegistration = context.OnHeartbeat(static (connection, _) =>
                {
                    Interlocked.Increment(ref connection._heartbeatCount);
                    return ValueTask.CompletedTask;
                }, this);
        }

        public bool Started => Volatile.Read(ref _started) != 0;
        public bool IsIdle => Volatile.Read(ref _idle) != 0;
        public bool IsSchedulable => Volatile.Read(ref _schedulable) != 0 && !Completion.IsCompleted;
        public Task Completion => _completion.Task;
        public int HeartbeatCount => Volatile.Read(ref _heartbeatCount);
        public Task PruningEntered => _pruningEntered?.Task ?? Task.CompletedTask;

        public void Start()
        {
            _admissionEntered?.TrySetResult();
            _admissionRelease?.Wait();
            if (_throwOnStart)
                throw new InvalidOperationException("admission failed");
            Volatile.Write(ref _started, 1);
        }
        public Task AdmissionEntered => _admissionEntered?.Task ?? Task.CompletedTask;
        public void GateAdmission()
        {
            _admissionEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _admissionRelease = new();
        }
        public void ReleaseAdmission() => _admissionRelease?.Set();
        public void GatePruning()
        {
            _pruningEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _pruningRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        public void ReleasePruning() => _pruningRelease?.TrySetResult();

        public void RunInitialWorkToIdle()
        {
            Assert.IsTrue(Started, "initial work must not begin before pool admission");
            Volatile.Write(ref _idle, 0);
            Volatile.Write(ref _idle, 1);
            _poolContext.SignalAvailability(this, isIdle: true);
        }

        public void EnterRecovery()
        {
            Volatile.Write(ref _idle, 0);
            Volatile.Write(ref _schedulable, 0);
        }

        public void MarkBusy() => Volatile.Write(ref _idle, 0);

        public void CompleteRecovery()
        {
            Volatile.Write(ref _schedulable, 1);
            _poolContext.SignalAvailability(this, isIdle: false);
        }

        public void MarkCompletedExternally()
        {
            Interlocked.Exchange(ref _heartbeatRegistration, null)?.Dispose();
            _completion.TrySetResult();
        }

        public int CompareTo(AdmissionConnection? other) => 0;

        public bool TryBeginPruning()
        {
            _pruningEntered?.TrySetResult();
            _pruningRelease?.Task.GetAwaiter().GetResult();
            if (!_allowPruning || Volatile.Read(ref _idle) == 0 ||
                Interlocked.CompareExchange(ref _schedulable, 0, 1) != 1)
                return false;
            return true;
        }

        public Task CompleteAsync(Exception? exception = null)
        {
            Interlocked.Exchange(ref _heartbeatRegistration, null)?.Dispose();
            _completion.TrySetResult();
            return _completion.Task;
        }
    }

    sealed class AdmissionConnectionFactory : AdmissionFactoryBase
    {
        readonly bool _registerHeartbeat;
        readonly bool _allowPruning;
        readonly ConcurrentQueue<AdmissionConnection> _created = new();

        public AdmissionConnectionFactory(bool registerHeartbeat = false, bool allowPruning = true)
            => (_registerHeartbeat, _allowPruning) = (registerHeartbeat, allowPruning);

        public AdmissionConnection? LastCreated { get; private set; }
        public AdmissionConnection[] Created => _created.ToArray();

        protected override AdmissionConnection Add(ConnectionPoolContext<AdmissionConnection> context)
        {
            var connection = new AdmissionConnection(context, _registerHeartbeat, _allowPruning);
            LastCreated = connection;
            _created.Enqueue(connection);
            return connection;
        }
    }

    // Shared sync/async forwarding for factories whose creation is a synchronous Add.
    abstract class AdmissionFactoryBase : IPoolConnectionFactory<AdmissionConnection>
    {
        public AdmissionConnection Create(ConnectionPoolContext<AdmissionConnection> context, TimeSpan timeout = default)
            => Add(context);

        public ValueTask<AdmissionConnection> CreateAsync(ConnectionPoolContext<AdmissionConnection> context,
            CancellationToken cancellationToken = default)
            => new(Add(context));

        protected abstract AdmissionConnection Add(ConnectionPoolContext<AdmissionConnection> context);
    }

    sealed class GatedAdmissionConnectionFactory : IPoolConnectionFactory<AdmissionConnection>
    {
        readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Started => _started.Task;
        public AdmissionConnection? Connection { get; private set; }
        public void Release() => _release.TrySetResult();

        public AdmissionConnection Create(ConnectionPoolContext<AdmissionConnection> context, TimeSpan timeout = default)
            => throw new NotSupportedException();

        public async ValueTask<AdmissionConnection> CreateAsync(ConnectionPoolContext<AdmissionConnection> context, CancellationToken cancellationToken = default)
        {
            Connection = new AdmissionConnection(context);
            _started.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken);
            return Connection;
        }
    }

    sealed class FailingAdmissionFactory : AdmissionFactoryBase
    {
        readonly ConcurrentQueue<AdmissionConnection> _created = new();
        int _attempt;

        public AdmissionConnection[] Created => _created.ToArray();

        protected override AdmissionConnection Add(ConnectionPoolContext<AdmissionConnection> context)
        {
            var connection = new AdmissionConnection(context,
                throwOnStart: Interlocked.Increment(ref _attempt) == 1);
            _created.Enqueue(connection);
            return connection;
        }
    }

    sealed class GatedFailingAdmissionFactory : AdmissionFactoryBase
    {
        AdmissionConnection? _connection;
        public AdmissionConnection? Connection => Volatile.Read(ref _connection);

        protected override AdmissionConnection Add(ConnectionPoolContext<AdmissionConnection> context)
        {
            var connection = new AdmissionConnection(context, throwOnStart: true);
            connection.GateAdmission();
            Volatile.Write(ref _connection, connection);
            return connection;
        }
    }

    sealed class FailingThenSuccessfulFactory : IPoolConnectionFactory<AdmissionConnection>
    {
        readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int _attempt;

        public Task Started => _started.Task;
        public void Release() => _release.TrySetResult();

        public AdmissionConnection Create(ConnectionPoolContext<AdmissionConnection> context, TimeSpan timeout = default)
            => throw new NotSupportedException();

        public async ValueTask<AdmissionConnection> CreateAsync(ConnectionPoolContext<AdmissionConnection> context,
            CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _attempt) == 1)
            {
                _started.TrySetResult();
                await _release.Task.WaitAsync(cancellationToken);
                throw new InvalidOperationException("first open failed");
            }

            return new(context);
        }
    }

    static PgClientOptions NewOptions() => new()
    {
        EndPoint = TestEndPoint.Default,
        Username = "postgres",
        Password = "postgres123",
        Database = "postgres",
    };

    static ConnectionPool<PgConnection> NewPool(int maxConnections = 4, CommandTracker? sharedTracker = null)
    {
        var options = NewOptions();
        var transportFactory = SocketStreamConnection.CreateFactory(options.EndPoint);
        var factory = new PgConnectionFactory(options, transportFactory, tracker: sharedTracker);
        return new ConnectionPool<PgConnection>(factory, new() { MaxConnections = maxConnections });
    }

    static async Task RunSyncOn(PgConnection conn, string sql)
    {
        var flow = new CommandFlow(async: false, Command.Create(sql));
        Assert.IsTrue(conn.TryQueue(flow));
        var e = flow.GetEnumerator();
        while (e.MoveNext()) { }
        await e.DisposeAsync();
    }

    static async Task RunAsyncOn(PgConnection conn, string sql)
    {
        var flow = new CommandFlow(async: true, Command.Create(sql));
        Assert.IsTrue(conn.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    static async Task DrainAsync(CommandFlow.Enumerator enumerator)
    {
        while (await enumerator.MoveNextAsync()) { }
    }

    [TestMethod]
    public async Task InitialSchedule_RunsAfterAdmission_AndPublishesIdleExactlyOnce()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(),
            new() { MaxConnections = 1 });

        var scheduled = await pool.GetAsync(
            static (context, _) =>
            {
                context.Connection.RunInitialWorkToIdle();
                return true;
            },
            state: 0,
            timeout: TimeSpan.FromSeconds(1));

        var leased = await pool.GetAsync(TimeSpan.FromSeconds(1));
        Assert.AreSame(scheduled, leased, "the synchronous idle edge should publish the admitted connection");

        using var cancellation = new CancellationTokenSource();
        var second = pool.GetAsync(Timeout.InfiniteTimeSpan, cancellation.Token).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1, TimeSpan.FromSeconds(1),
            "a correctly depleted idle set must park the second lease");
        Assert.IsFalse(second.IsCompleted,
            "a second idle entry would allow the same connection to be leased twice");
        cancellation.Cancel();
        await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => second);
    }

    [TestMethod]
    public async Task InitialScheduleException_DoesNotDestroyInstalledConnection()
    {
        var factory = new AdmissionConnectionFactory();
        await using var pool = new ConnectionPool<AdmissionConnection>(
            factory,
            new() { MaxConnections = 1 });

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await pool.GetAsync<int>(
                static (_, _) => throw new InvalidOperationException("placement failed"),
                state: 0,
                timeout: TimeSpan.FromSeconds(1)));

        Assert.IsNotNull(factory.LastCreated);
        Assert.IsTrue(factory.LastCreated.Started, "the connection was installed and admitted before placement");
        Assert.IsFalse(factory.LastCreated.Completion.IsCompleted,
            "placement failure must not be promoted to connection failure");

        var reacquired = await pool.GetAsync(TimeSpan.FromSeconds(1));
        Assert.AreSame(factory.LastCreated, reacquired,
            "an installed connection must remain reachable after placement throws");
    }

    [TestMethod]
    public async Task DeclinedIdleConnection_RemainsAvailable()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(),
            new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(TimeSpan.FromSeconds(1));
        connection.RunInitialWorkToIdle();

        var attempts = new StrongBox<int>();
        var reacquired = await pool.GetAsync(
            static (_, attempts) => Interlocked.Increment(ref attempts.Value) > 1,
            attempts,
            TimeSpan.FromSeconds(1));

        Assert.AreSame(connection, reacquired);
    }

    [TestMethod]
    public async Task Dispose_StopsPoolHeartbeat()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(registerHeartbeat: true);
        var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 1,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            TimeProvider = time,
        });

        await pool.GetAsync(TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => factory.LastCreated!.HeartbeatCount > 0, TimeSpan.FromSeconds(1),
            "the heartbeat should tick before pool disposal");

        await pool.DisposeAsync();
        var countAtDispose = factory.LastCreated!.HeartbeatCount;
        time.Advance(TimeSpan.FromSeconds(10));
        await Task.Yield();

        Assert.AreEqual(countAtDispose, factory.LastCreated.HeartbeatCount,
            "a disposed pool must not continue dispatching heartbeat callbacks");
    }

    [TestMethod]
    public async Task CompletedConnection_UnregistersFromPoolHeartbeat()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(registerHeartbeat: true);
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 1,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            TimeProvider = time,
        });

        var retired = await pool.GetAsync(TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => retired.HeartbeatCount > 0, TimeSpan.FromSeconds(1),
            "the original connection should receive heartbeat ticks");

        // Model a terminal protocol abort that completes beneath the pool wrapper. The pool must
        // release pool-membership resources when it CAS-replaces the completed slot.
        retired.MarkCompletedExternally();
        var retiredCount = retired.HeartbeatCount;
        var replacement = await pool.GetAsync(TimeSpan.FromSeconds(1));
        Assert.AreNotSame(retired, replacement);

        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => replacement.HeartbeatCount > 0, TimeSpan.FromSeconds(1),
            "the replacement connection should receive heartbeat ticks");

        Assert.AreEqual(retiredCount, retired.HeartbeatCount,
            "a connection removed from the pool registry must also leave heartbeat traversal");
    }

    [TestMethod]
    public async Task IdlePruning_ClosesMedianIdlePopulationAboveMinimum()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(registerHeartbeat: true);
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MinConnections = 1,
            MaxConnections = 3,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            ConnectionPruningInterval = TimeSpan.FromSeconds(1),
            ConnectionIdleLifetime = TimeSpan.FromSeconds(3),
            TimeProvider = time,
        });
        await PopulateAsync(pool, 3);

        var connections = factory.Created;
        Assert.HasCount(3, connections);
        for (var tick = 1; tick <= 2; tick++)
        {
            time.Advance(TimeSpan.FromSeconds(1));
            var expected = tick;
            await WaitUntilAsync(() => connections.All(c => c.HeartbeatCount >= expected), TimeSpan.FromSeconds(1),
                "the pruning lifetime must be measured from complete heartbeat samples");
            Assert.IsTrue(connections.All(c => !c.Completion.IsCompleted),
                "no connection should be pruned before the full idle lifetime");
        }

        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => connections.Count(c => c.Completion.IsCompleted) == 2, TimeSpan.FromSeconds(1),
            "the median idle population above the minimum should be pruned");
        Assert.AreEqual(1, connections.Count(c => !c.Completion.IsCompleted));
    }

    [TestMethod]
    public async Task InfiniteIdleLifetime_DisablesPruning()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(registerHeartbeat: true);
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 2,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            ConnectionPruningInterval = TimeSpan.Zero,
            ConnectionIdleLifetime = Timeout.InfiniteTimeSpan,
            TimeProvider = time,
        });
        await PopulateAsync(pool, 2);

        time.Advance(TimeSpan.FromHours(1));

        Assert.IsTrue(factory.Created.All(c => !c.Completion.IsCompleted));
    }

    [TestMethod]
    public async Task FixedSizePool_DisablesPruning()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(registerHeartbeat: true);
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MinConnections = 2,
            MaxConnections = 2,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            ConnectionPruningInterval = TimeSpan.Zero,
            ConnectionIdleLifetime = TimeSpan.Zero,
            TimeProvider = time,
        });
        await PopulateAsync(pool, 2);

        time.Advance(TimeSpan.FromHours(1));

        Assert.IsTrue(factory.Created.All(c => !c.Completion.IsCompleted));
    }

    [TestMethod]
    public void PruningIntervalBelowHeartbeat_Throws()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new ConnectionPool<AdmissionConnection>(new AdmissionConnectionFactory(), new()
            {
                MaxConnections = 1,
                HeartbeatInterval = TimeSpan.FromSeconds(2),
                ConnectionPruningInterval = TimeSpan.FromSeconds(1),
            }));
    }

    [TestMethod]
    public async Task IdlePruning_RefusedClaimsReturnTheirIdleTokens()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(registerHeartbeat: true, allowPruning: false);
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 2,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            ConnectionPruningInterval = TimeSpan.FromSeconds(1),
            ConnectionIdleLifetime = TimeSpan.FromSeconds(1),
            TimeProvider = time,
        });
        await PopulateAsync(pool, 2);

        var connections = factory.Created;
        time.Advance(TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => connections.All(c => c.HeartbeatCount > 0), TimeSpan.FromSeconds(1),
            "the pruning tick should inspect both idle connections");

        Assert.IsTrue(connections.All(c => !c.Completion.IsCompleted));
        var first = await pool.GetAsync(TimeSpan.FromSeconds(1));
        first.RunInitialWorkToIdle();
        var second = await pool.GetAsync(TimeSpan.FromSeconds(1));
        Assert.AreNotSame(first, second, "a refused prune must return every claimed idle token");
    }

    [TestMethod]
    public async Task RefusedPruning_WakesWaiterThatRegisteredWhileTokenWasClaimed()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(allowPruning: false);
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 1,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            ConnectionPruningInterval = TimeSpan.FromSeconds(1),
            ConnectionIdleLifetime = TimeSpan.FromSeconds(1),
            TimeProvider = time,
        });
        await PopulateAsync(pool, 1);

        var connection = factory.LastCreated!;
        time.Advance(TimeSpan.FromSeconds(1));
        connection.GatePruning();
        var tick = Task.Run(() => time.Advance(TimeSpan.FromSeconds(1)));
        await connection.PruningEntered.WaitAsync(TimeSpan.FromSeconds(1));

        var waiting = pool.GetAsync(TimeSpan.FromSeconds(1)).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1, TimeSpan.FromSeconds(1),
            "the waiter must register while pruning owns the idle token");
        connection.ReleasePruning();
        await tick;

        Assert.AreSame(connection, await waiting);
    }

    [TestMethod]
    public async Task IdlePruning_CountsBusyConnectionsTowardMinimum()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(registerHeartbeat: true);
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MinConnections = 1,
            MaxConnections = 3,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            ConnectionPruningInterval = TimeSpan.FromSeconds(1),
            ConnectionIdleLifetime = TimeSpan.FromSeconds(1),
            TimeProvider = time,
        });
        await PopulateAsync(pool, 3);

        var busy = await pool.GetAsync(TimeSpan.FromSeconds(1));
        busy.MarkBusy();
        var connections = factory.Created;
        time.Advance(TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => connections.Count(c => c.Completion.IsCompleted) == 2, TimeSpan.FromSeconds(1),
            "consistently idle capacity may be removed while a busy connection satisfies the minimum");

        Assert.IsFalse(busy.Completion.IsCompleted);
        Assert.AreEqual(1, connections.Count(c => !c.Completion.IsCompleted));
    }

    [TestMethod]
    public async Task PrunedConnection_ReleasesCapacityForReplacement()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(registerHeartbeat: true);
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 1,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            ConnectionPruningInterval = TimeSpan.FromSeconds(1),
            ConnectionIdleLifetime = TimeSpan.FromSeconds(1),
            TimeProvider = time,
        });
        await PopulateAsync(pool, 1);

        var pruned = factory.LastCreated!;
        time.Advance(TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        await pruned.Completion.WaitAsync(TimeSpan.FromSeconds(1));

        var replacement = await pool.GetAsync(TimeSpan.FromSeconds(1));
        Assert.AreNotSame(pruned, replacement);
        Assert.IsFalse(replacement.Completion.IsCompleted);
    }

    [TestMethod]
    public async Task IdlePruning_ObservesDemandBetweenHeartbeatSamples()
    {
        var time = new FakeTimeProvider();
        var factory = new AdmissionConnectionFactory(registerHeartbeat: true);
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 3,
            HeartbeatInterval = TimeSpan.FromSeconds(1),
            ConnectionPruningInterval = TimeSpan.FromSeconds(1),
            ConnectionIdleLifetime = TimeSpan.FromSeconds(3),
            TimeProvider = time,
        });
        await PopulateAsync(pool, 3);

        time.Advance(TimeSpan.FromSeconds(1));
        var rented = new AdmissionConnection[3];
        for (var i = 0; i < rented.Length; i++)
        {
            rented[i] = await pool.GetAsync(TimeSpan.FromSeconds(1));
            rented[i].MarkBusy();
        }
        foreach (var connection in rented)
            connection.RunInitialWorkToIdle();

        time.Advance(TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.IsTrue(factory.Created.All(c => !c.Completion.IsCompleted),
            "demand entirely between heartbeat samples must lower the interval sample");

        for (var i = 0; i < 3; i++)
            time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => factory.Created.All(c => c.Completion.IsCompleted), TimeSpan.FromSeconds(1),
            "a later lifetime window with uninterrupted idle capacity should prune");
    }

    [TestMethod]
    public async Task TerminalCompletion_WakesWaiterForFreedCapacity()
    {
        var factory = new AdmissionConnectionFactory();
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 1,
        });

        var retired = await pool.GetAsync(TimeSpan.FromSeconds(1));
        var waiting = pool.GetAsync(TimeSpan.FromSeconds(1)).AsTask();
        await Task.Yield();
        Assert.IsFalse(waiting.IsCompleted,
            "the sole idle token is held by the first lease, so the second acquisition must wait");

        retired.MarkCompletedExternally();
        var replacement = await waiting;

        Assert.AreNotSame(retired, replacement);
        Assert.IsFalse(replacement.Completion.IsCompleted);
    }

    [TestMethod]
    public async Task SaturationTimeout_HasSameSyncAndAsyncSurface()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });
        await pool.GetAsync(TimeSpan.FromSeconds(1));

        var sync = Assert.ThrowsExactly<TimeoutException>(
            () => pool.Get(TimeSpan.FromMilliseconds(20)));
        var async = await Assert.ThrowsExactlyAsync<TimeoutException>(
            async () => await pool.GetAsync(TimeSpan.FromMilliseconds(20)));

        Assert.AreEqual(async.Message, sync.Message);
    }

    [TestMethod]
    public async Task FailedOpening_WakesWaiterForFreedCapacity()
    {
        var factory = new FailingThenSuccessfulFactory();
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 1,
        });

        var failedOpen = pool.GetAsync(TimeSpan.FromSeconds(1)).AsTask();
        await factory.Started;
        var waiting = pool.GetAsync(TimeSpan.FromSeconds(1)).AsTask();
        await Task.Yield();
        Assert.IsFalse(waiting.IsCompleted);

        factory.Release();
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => failedOpen);

        var connection = await waiting;
        Assert.IsFalse(connection.Completion.IsCompleted);
    }

    [TestMethod]
    public async Task AdmittedConnectionPublication_WakesWaiter()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });
        var scheduling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();

        var opening = Task.Run(async () => await pool.GetAsync(
            static (candidate, state) =>
            {
                candidate.Connection.MarkBusy();
                state.Scheduling.TrySetResult();
                state.Release.Wait();
                return true;
            },
            (Scheduling: scheduling, Release: release),
            TimeSpan.FromSeconds(10)));

        await scheduling.Task;
        var waiting = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: TimeSpan.FromSeconds(1)).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1, TimeSpan.FromSeconds(1),
            "the second renter must park while the admitted connection is unpublished");

        release.Set();
        var connection = await opening;

        Assert.AreSame(connection, await waiting,
            "publishing an admitted connection must expose its remaining scheduling capacity");
    }

    [TestMethod]
    public async Task RecoveryAvailability_PassesWaiterWhosePlacementRejectsBusyConnection()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(TimeSpan.FromSeconds(1));
        connection.EnterRecovery();

        using var cancellation = new CancellationTokenSource();
        var exclusive = pool.GetAsync(
            static (candidate, _) => candidate.Connection.IsIdle,
            state: 0,
            timeout: Timeout.InfiniteTimeSpan,
            cancellationToken: cancellation.Token).AsTask();
        var multiplexed = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: TimeSpan.FromSeconds(1)).AsTask();

        await WaitUntilAsync(() => pool.WaiterCount == 2, TimeSpan.FromSeconds(1),
            "both placement attempts must be parked before recovery restores continuity");
        connection.CompleteRecovery();

        Assert.AreSame(connection, await multiplexed);
        Assert.IsFalse(exclusive.IsCompleted,
            "the exclusive waiter must remain parked while the recovered connection is busy");
        cancellation.Cancel();
        await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => exclusive);
    }

    [TestMethod]
    public async Task RecoveryAvailability_PassesToNextCompatibleWaiterAfterPolicyRejection()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(TimeSpan.FromSeconds(1));
        connection.EnterRecovery();

        using var cancellation = new CancellationTokenSource();
        var rejecting = pool.GetAsync(
            static (_, _) => false,
            state: 0,
            timeout: Timeout.InfiniteTimeSpan,
            cancellationToken: cancellation.Token).AsTask();
        var accepting = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: TimeSpan.FromSeconds(1)).AsTask();

        await WaitUntilAsync(() => pool.WaiterCount == 2, TimeSpan.FromSeconds(1),
            "both waiters must be parked before recovery restores continuity");
        connection.CompleteRecovery();

        Assert.AreSame(connection, await accepting,
            "policy rejection must pass the availability edge to the next waiter");
        Assert.IsFalse(rejecting.IsCompleted,
            "the rejecting waiter must remain parked after passing the availability edge");
        cancellation.Cancel();
        await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => rejecting);
    }

    [TestMethod]
    public async Task CancelledWaiter_DoesNotConsumeLaterAvailability()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(TimeSpan.FromSeconds(1));
        connection.EnterRecovery();

        using var cancellation = new CancellationTokenSource();
        var cancelled = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: Timeout.InfiniteTimeSpan,
            cancellationToken: cancellation.Token).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1, TimeSpan.FromSeconds(1),
            "the cancellable waiter must be queued before cancellation");

        cancellation.Cancel();
        try
        {
            await cancelled;
            Assert.Fail("the cancelled waiter should not complete successfully");
        }
        catch (OperationCanceledException ex)
        {
            Assert.AreEqual(cancellation.Token, ex.CancellationToken);
        }
        await WaitUntilAsync(() => pool.WaiterCount == 0, TimeSpan.FromSeconds(1),
            "cancellation must physically unlink the waiter");

        var accepting = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: TimeSpan.FromSeconds(1)).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1, TimeSpan.FromSeconds(1),
            "the replacement waiter must be queued before availability is restored");

        connection.CompleteRecovery();
        Assert.AreSame(connection, await accepting);
    }

    [TestMethod]
    public async Task IdleAvailability_PreservesFifoAcrossPlacementPolicies()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(TimeSpan.FromSeconds(1));
        var exclusive = pool.GetAsync(
            static (candidate, _) => candidate.IsIdleCandidate,
            state: 0,
            timeout: TimeSpan.FromSeconds(1)).AsTask();
        var shared = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: TimeSpan.FromSeconds(1)).AsTask();

        await WaitUntilAsync(() => pool.WaiterCount == 2, TimeSpan.FromSeconds(1),
            "both placement policies must be queued before the idle edge");
        connection.RunInitialWorkToIdle();

        Assert.AreSame(connection, await exclusive,
            "an idle edge must select the oldest waiter regardless of placement policy");
        Assert.IsFalse(shared.IsCompleted);

        connection.RunInitialWorkToIdle();
        Assert.AreSame(connection, await shared);
    }

    [TestMethod]
    public async Task Newcomer_DoesNotBargePastAwakenedWaiter()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(TimeSpan.FromSeconds(1));
        var queued = pool.GetAsync(TimeSpan.FromSeconds(1)).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1, TimeSpan.FromSeconds(1),
            "the older renter must be queued before the idle edge");

        connection.RunInitialWorkToIdle();
        var newcomer = pool.GetAsync(TimeSpan.FromSeconds(1)).AsTask();

        Assert.AreSame(connection, await queued,
            "the detached wake must retain priority over a racing newcomer");
        Assert.IsFalse(newcomer.IsCompleted,
            "the newcomer must wait for availability after the older renter");

        connection.RunInitialWorkToIdle();
        Assert.AreSame(connection, await newcomer);
    }

    [TestMethod]
    public async Task ThrowingAwakenedWaiter_PassesAvailability()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(TimeSpan.FromSeconds(1));
        var throwing = pool.GetAsync(
            static (_, _) => throw new InvalidOperationException("placement failed"),
            state: 0,
            timeout: TimeSpan.FromSeconds(1)).AsTask();
        var follower = pool.GetAsync(TimeSpan.FromSeconds(1)).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 2, TimeSpan.FromSeconds(1),
            "both renters must be queued before the idle edge");

        connection.RunInitialWorkToIdle();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => throwing);
        Assert.AreSame(connection, await follower,
            "an awakened placement failure must pass the availability edge");
    }

    [TestMethod]
    public async Task DisposeAsync_FaultsQueuedWaitersWithDifferentPlacementPolicies()
    {
        var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        await pool.GetAsync(TimeSpan.FromSeconds(1));
        var exclusive = pool.GetAsync(
            static (candidate, _) => candidate.IsIdleCandidate,
            state: 0,
            timeout: Timeout.InfiniteTimeSpan).AsTask();
        var shared = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: Timeout.InfiniteTimeSpan).AsTask();

        await WaitUntilAsync(() => pool.WaiterCount == 2, TimeSpan.FromSeconds(1),
            "both waiters must be queued before disposal");
        await pool.DisposeAsync();

        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => exclusive);
        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => shared);
    }

    [TestMethod]
    public async Task AvailabilityRacingWaiterRegistration_IsNotLost()
    {
        for (var i = 0; i < 100; i++)
        {
            await using var pool = new ConnectionPool<AdmissionConnection>(
                new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

            var connection = await pool.GetAsync(TimeSpan.FromSeconds(1));
            connection.EnterRecovery();
            var waiting = pool.GetAsync(
                static (_, _) => true,
                state: 0,
                timeout: TimeSpan.FromSeconds(1)).AsTask();

            // WaiterCount becomes visible immediately after registration, potentially before
            // the mandatory state rescan which closes the registration/signal race.
            await WaitUntilAsync(() => pool.WaiterCount == 1, TimeSpan.FromSeconds(1),
                "the waiter must publish itself before availability changes");
            connection.CompleteRecovery();

            Assert.AreSame(connection, await waiting);
        }
    }

    [TestMethod]
    public async Task DisposeAsync_RacingPendingOpen_AwaitsItsCleanup()
    {
        var factory = new GatedAdmissionConnectionFactory();
        var pool = new ConnectionPool<AdmissionConnection>(factory, new() { MaxConnections = 1 });
        var opening = pool.GetAsync(TimeSpan.FromSeconds(10)).AsTask();
        await factory.Started;

        var disposing = pool.DisposeAsync().AsTask();
        Assert.IsFalse(disposing.IsCompleted,
            "disposal must install a lazy waiter for the opening slot");

        factory.Release();
        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => opening);
        await disposing;

        Assert.IsNotNull(factory.Connection);
        Assert.IsTrue(factory.Connection.Completion.IsCompleted,
            "the failed installer must finish local connection cleanup before pool disposal returns");
    }

    [TestMethod]
    public async Task DisposeAsync_RacingFailedAdmission_AwaitsItsCleanup()
    {
        var factory = new GatedFailingAdmissionFactory();
        var pool = new ConnectionPool<AdmissionConnection>(factory, new() { MaxConnections = 1 });
        var opening = Task.Run(() => pool.Get(TimeSpan.FromSeconds(10)));
        while (factory.Connection is null)
            await Task.Yield();
        await factory.Connection.AdmissionEntered;

        var disposing = pool.DisposeAsync().AsTask();
        Assert.IsFalse(disposing.IsCompleted,
            "disposal must wait while the claimed slot is still admitting its connection");

        factory.Connection.ReleaseAdmission();
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => opening);
        await disposing;

        Assert.IsTrue(factory.Connection.Completion.IsCompleted,
            "failed admission cleanup must finish before pool disposal returns");
    }

    [TestMethod]
    public async Task DisposeAsync_RacingInitialSchedule_AwaitsOpenerSettlement()
    {
        var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(),
            new() { MaxConnections = 1 });
        var scheduling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();

        var opening = Task.Run(async () => await pool.GetAsync(
            static (_, state) =>
            {
                state.Scheduling.TrySetResult();
                state.Release.Wait();
                return true;
            },
            (Scheduling: scheduling, Release: release),
            TimeSpan.FromSeconds(10)));

        await scheduling.Task;
        var disposing = pool.DisposeAsync().AsTask();
        Assert.IsFalse(disposing.IsCompleted,
            "an installed connection is not settled while its initial scheduler still owns it");

        release.Set();
        var connection = await opening;
        await disposing;

        Assert.IsTrue(connection.Completion.IsCompleted,
            "disposal must complete the connection yielded by the settling opener");
    }

    // Once an open returns its timeout source to the thread-static cache, a later placement or
    // admission failure must not return that source again. A double return disposes the cached
    // instance in place, making the next rent throw from CancelAfter.
    [TestMethod]
    public async Task OpenFailureAfterTimeoutSourceReturn_DoesNotPoisonRentCache()
    {
        // The pre-fix poison threw deterministically on the second same-thread rent of every
        // iteration; five iterations keep repeated reuse coverage without the timer bill of fifty.
        for (var i = 0; i < 5; i++)
        {
            var factory = new AdmissionConnectionFactory();
            await using var pool = new ConnectionPool<AdmissionConnection>(
                factory,
                new() { MaxConnections = 1 });

            await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
                await pool.GetAsync<int>(
                    static (_, _) => throw new InvalidOperationException("placement failed"),
                    state: 0,
                    timeout: TimeSpan.FromSeconds(1)));

            // Placement failure republishes the admitted connection. Consume that token, then
            // rent again immediately on this thread: the second rent parks and exercises the
            // returned timeout source. On the pre-fix poisoned cache this throws
            // ObjectDisposedException instead of timing out.
            Assert.AreSame(factory.LastCreated, await pool.GetAsync(TimeSpan.FromSeconds(1)));
            await Assert.ThrowsExactlyAsync<TimeoutException>(async () =>
                await pool.GetAsync(TimeSpan.FromMilliseconds(5)));
        }
    }

    [TestMethod]
    public async Task AsyncAdmissionFailure_ClosesConnectionAndReleasesSlot()
    {
        var factory = new FailingAdmissionFactory();
        await using var pool = new ConnectionPool<AdmissionConnection>(
            factory,
            new() { MaxConnections = 1 });

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await pool.GetAsync(TimeSpan.FromSeconds(1)));

        var failed = factory.Created[0];
        Assert.IsTrue(failed.Completion.IsCompleted,
            "a connection that failed admission must be closed rather than published");

        var replacement = await pool.GetAsync(TimeSpan.FromSeconds(1));
        Assert.AreNotSame(failed, replacement);
        Assert.IsTrue(replacement.Started);
    }

    [TestMethod]
    public async Task SyncAdmissionFailure_ClosesConnectionAndReleasesSlot()
    {
        var factory = new FailingAdmissionFactory();
        await using var pool = new ConnectionPool<AdmissionConnection>(
            factory,
            new() { MaxConnections = 1 });

        Assert.ThrowsExactly<InvalidOperationException>(() =>
            pool.Get(TimeSpan.FromSeconds(1)));

        var failed = factory.Created[0];
        Assert.IsTrue(failed.Completion.IsCompleted,
            "a connection that failed admission must be closed rather than published");

        var replacement = await pool.GetAsync(TimeSpan.FromSeconds(1));
        Assert.AreNotSame(failed, replacement);
        Assert.IsTrue(replacement.Started);
    }

    [TestMethod]
    public void ConnectionInitializers_MustBeConfiguredAsPair()
    {
        var inner = new AdmissionConnectionFactory();

        Assert.ThrowsExactly<ArgumentException>(() =>
            new InitializingConnectionFactory<AdmissionConnection>(
                inner, initializer: static (_, _) => { }));
        Assert.ThrowsExactly<ArgumentException>(() =>
            new InitializingConnectionFactory<AdmissionConnection>(
                inner, asyncInitializer: static (_, _) => ValueTask.CompletedTask));
    }

    [TestMethod]
    public async Task AsyncInitializerFailure_ClosesCreatedConnection()
    {
        var inner = new AdmissionConnectionFactory();
        var factory = new InitializingConnectionFactory<AdmissionConnection>(
            inner,
            initializer: static (_, _) => { },
            asyncInitializer: static (_, _) =>
                ValueTask.FromException(new InvalidOperationException("initializer failed")));
        await using var pool = new ConnectionPool<AdmissionConnection>(
            factory,
            new() { MaxConnections = 1 });

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await pool.GetAsync(TimeSpan.FromSeconds(1)));

        Assert.IsTrue(inner.LastCreated!.Completion.IsCompleted,
            "the initializing factory must close a connection it cannot return");
    }

    [TestMethod]
    public async Task SyncInitializerFailure_ClosesCreatedConnection()
    {
        var inner = new AdmissionConnectionFactory();
        var factory = new InitializingConnectionFactory<AdmissionConnection>(
            inner,
            initializer: static (_, _) => throw new InvalidOperationException("initializer failed"),
            asyncInitializer: static (_, _) => ValueTask.CompletedTask);
        await using var pool = new ConnectionPool<AdmissionConnection>(
            factory,
            new() { MaxConnections = 1 });

        Assert.ThrowsExactly<InvalidOperationException>(() =>
            pool.Get(TimeSpan.FromSeconds(1)));

        Assert.IsTrue(inner.LastCreated!.Completion.IsCompleted,
            "the initializing factory must close a connection it cannot return");
    }

    [TestMethod]
    public async Task Sync_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetAsync(TimeSpan.FromSeconds(10));
        await RunSyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task Async_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetAsync(TimeSpan.FromSeconds(10));
        await RunAsyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task SyncWhileAsyncInFlight_SameLeasedConn_BothComplete()
    {
        await using var pool = NewPool();
        var conn = await pool.GetAsync(TimeSpan.FromSeconds(10));

        await RunAsyncOn(conn, "select 1"); // warm

        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        var slow = new CommandFlow(async: true,
            Command.Create("select 1") with { WithSync = true }, blocker.WaitCommand);
        Assert.IsTrue(conn.TryQueue(slow));
        var slowEnum = slow.GetAsyncEnumerator();
        Assert.IsTrue(await slowEnum.MoveNextAsync());
        var slowTask = DrainAsync(slowEnum);

        var sync = new CommandFlow(async: false, Command.Create("select 1"));
        Assert.IsTrue(conn.TryQueue(sync));
        var syncTask = Task.Run(async () =>
        {
            var e = sync.GetEnumerator();
            while (e.MoveNext()) { }
            await e.DisposeAsync();
        });

        await blocker.ReleaseAsync();
        await syncTask.WaitAsync(TimeSpan.FromSeconds(2));

        await slowTask;
        await slowEnum.DisposeAsync();
    }

    static readonly TimeSpan Cap = TimeSpan.FromSeconds(10);

    static Exception Root(Exception ex)
    {
        while (ex is not PgClientClosedException && ex.InnerException is not null)
            ex = ex.InnerException;
        return ex;
    }

    static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, string because)
    {
        var sw = Stopwatch.StartNew();
        while (!condition() && sw.Elapsed < timeout)
            await Task.Delay(10);
        Assert.IsTrue(condition(), because);
    }

    static async Task PopulateAsync(ConnectionPool<AdmissionConnection> pool, int count)
    {
        var connections = new AdmissionConnection[count];
        for (var i = 0; i < connections.Length; i++)
        {
            connections[i] = await pool.GetAsync(
                static (candidate, _) =>
                {
                    if (!candidate.IsIdleCandidate)
                        return false;
                    candidate.Connection.MarkBusy();
                    return true;
                },
                (object?)null,
                TimeSpan.FromSeconds(1));
        }

        foreach (var connection in connections)
            connection.RunInitialWorkToIdle();
    }

    // Terminal abort is the wire-dead path: forceful DisposeAsync RSTs the socket, fires AbortToken, and
    // drives Shutdown to Completed. The terminal Completion task is what the pool
    // evicts on; here we verify end-to-end that an aborted connection reaches Completed and the pool
    // reclaims its slot and opens a fresh, healthy connection in its place rather than handing the corpse
    // back. maxConnections:1 forces the same slot to be reused so the reclaim path is the one under test.
    [TestMethod]
    public async Task TerminalAbort_EvictsFromPool_ReacquireYieldsHealthy()
    {
        await using var tracker = new CommandTracker(maxAuto: 1, autoMinimumUses: 1);
        await using var pool = NewPool(maxConnections: 1, sharedTracker: tracker);
        var conn1 = await pool.GetAsync(Cap);
        await RunAsyncOn(conn1, "select 1"); // healthy before the abort
        Assert.AreEqual(1, tracker.RegisteredConnectionCount);

        await conn1.Protocol.DisposeAsync(); // forceful terminal abort (fire-and-forget; teardown runs async)
        // Completion settles at the END of background shutdown, so it is eventually consistent,
        // not immediate. The pool's eviction gate keys off it, so it reclaims once it lands.
        await WaitUntilAsync(() => conn1.Completion.IsCompleted, Cap,
            "a terminally aborted connection must reach Completed — that is the pool's eviction gate.");
        await WaitUntilAsync(() => tracker.RegisteredConnectionCount == 0, Cap,
            "terminal completion must promptly release tracker membership");

        var conn2 = await pool.GetAsync(Cap);
        Assert.AreNotSame(conn1, conn2, "the pool must replace the aborted connection, not hand it back.");
        Assert.IsFalse(conn2.Completion.IsCompleted, "the replacement connection must be live.");
        Assert.AreEqual(1, tracker.RegisteredConnectionCount);
        await RunAsyncOn(conn2, "select 1"); // the replacement actually works
    }

    // Every flow queued behind the abort point (in-flight + backlog) must receive PgClientClosedException,
    // none may strand. The flows are queued but never driven, so they sit outstanding when the abort lands;
    // forceful DisposeAsync faults the in-flight ones via the pipeline completion and the backlog via the
    // inert drain. Draining each enumerator must surface the closed exception, not hang.
    [TestMethod]
    public async Task TerminalAbort_OutstandingPipelinedFlows_AllFaultClosed()
    {
        await using var pool = NewPool(maxConnections: 1);
        var conn = await pool.GetAsync(Cap);
        await using var blocker = await PgAdvisoryLock.AcquireAsync();

        const int N = 8;
        var enums = new CommandFlow.Enumerator[N];
        for (var i = 0; i < N; i++)
        {
            var flow = new CommandFlow(async: true, i == 0 ? blocker.WaitCommand : Command.Create("select 1"));
            Assert.IsTrue(conn.TryQueue(flow));
            enums[i] = flow.GetAsyncEnumerator();
        }

        var abort = conn.Protocol.DisposeAsync().AsTask();
        await blocker.ReleaseAsync();
        await abort; // forceful terminal abort while all N are outstanding

        for (var i = 0; i < N; i++)
        {
            Exception? observed = null;
            try
            {
                await DrainAsync(enums[i]).WaitAsync(Cap);
            }
            catch (Exception ex)
            {
                observed = ex;
            }

            Assert.IsNotNull(observed, $"flow {i} behind the abort point should fault, not complete or hang.");
            Assert.IsInstanceOfType<PgClientClosedException>(Root(observed!),
                $"flow {i} surfaced {Root(observed!).GetType().Name}, expected PgClientClosedException.");
        }
    }
}
