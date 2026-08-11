using System.Collections.Concurrent;
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
    [TestMethod]
    public async Task DisposeAsync_JoinsDetachedWork()
    {
        var detached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });
        pool.TrackDetached(detached.Task);

        var disposal = pool.DisposeAsync().AsTask();
        Assert.IsFalse(disposal.IsCompleted);

        detached.SetResult();
        await disposal;
    }

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
            timeout: default);

        var leased = await pool.GetAsync(default);
        Assert.AreSame(scheduled, leased, "the synchronous idle edge should publish the admitted connection");

        using var cancellation = new CancellationTokenSource();
        var second = pool.GetAsync(default, cancellation.Token).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
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
                timeout: default));

        Assert.IsNotNull(factory.LastCreated);
        Assert.IsTrue(factory.LastCreated.Started, "the connection was installed and admitted before placement");
        Assert.IsFalse(factory.LastCreated.Completion.IsCompleted,
            "placement failure must not be promoted to connection failure");

        var reacquired = await pool.GetAsync(default);
        Assert.AreSame(factory.LastCreated, reacquired,
            "an installed connection must remain reachable after placement throws");
    }

    [TestMethod]
    public async Task DeclinedIdleConnection_RemainsAvailable()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(),
            new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(default);
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

        await pool.GetAsync(default);
        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => factory.LastCreated!.HeartbeatCount > 0,
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

        var retired = await pool.GetAsync(default);
        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => retired.HeartbeatCount > 0,
            "the original connection should receive heartbeat ticks");

        // Model a terminal protocol abort that completes beneath the pool wrapper. The pool must
        // release pool-membership resources when it CAS-replaces the completed slot.
        retired.MarkCompletedExternally();
        var retiredCount = retired.HeartbeatCount;
        var replacement = await pool.GetAsync(default);
        Assert.AreNotSame(retired, replacement);

        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => replacement.HeartbeatCount > 0,
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
            await WaitUntilAsync(() => connections.All(c => c.HeartbeatCount >= expected));
            Assert.IsTrue(connections.All(c => !c.Completion.IsCompleted),
                "no connection should be pruned before the full idle lifetime");
        }

        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => connections.Count(c => c.Completion.IsCompleted) == 2,
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
        await WaitUntilAsync(() => connections.All(c => c.HeartbeatCount > 0));

        Assert.IsTrue(connections.All(c => !c.Completion.IsCompleted));
        var first = await pool.GetAsync(default);
        first.RunInitialWorkToIdle();
        var second = await pool.GetAsync(default);
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
        await connection.PruningEntered;

        var waiting = pool.GetAsync(default).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
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

        var busy = await pool.GetAsync(default);
        busy.MarkBusy();
        var connections = factory.Created;
        time.Advance(TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => connections.Count(c => c.Completion.IsCompleted) == 2,
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
        await pruned.Completion;

        var replacement = await pool.GetAsync(default);
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
            rented[i] = await pool.GetAsync(default);
            rented[i].MarkBusy();
        }
        foreach (var connection in rented)
            connection.RunInitialWorkToIdle();

        time.Advance(TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.IsTrue(factory.Created.All(c => !c.Completion.IsCompleted));

        for (var i = 0; i < 3; i++)
            time.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => factory.Created.All(c => c.Completion.IsCompleted));
    }

    [TestMethod]
    public async Task TerminalCompletion_WakesWaiterForFreedCapacity()
    {
        var factory = new AdmissionConnectionFactory();
        await using var pool = new ConnectionPool<AdmissionConnection>(factory, new()
        {
            MaxConnections = 1,
        });

        var retired = await pool.GetAsync(default);
        var waiting = pool.GetAsync(default).AsTask();
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
        var time = new FakeTimeProvider();
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1, TimeProvider = time });
        await pool.GetAsync(default);

        var syncWait = Task.Run(() => Assert.ThrowsExactly<TimeoutException>(
            () => pool.Get(TimeSpan.FromSeconds(1))));
        await WaitUntilAsync(() => pool.WaiterCount == 1,
            "the synchronous acquisition must park before its deadline advances");
        time.Advance(TimeSpan.FromSeconds(1));
        var sync = await syncWait;

        var asyncWait = pool.GetAsync(TimeSpan.FromSeconds(1)).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
            "the asynchronous acquisition must park before its deadline advances");
        time.Advance(TimeSpan.FromSeconds(1));
        var async = await Assert.ThrowsExactlyAsync<TimeoutException>(() => asyncWait);

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

        var failedOpen = pool.GetAsync(default).AsTask();
        await factory.Started;
        var waiting = pool.GetAsync(default).AsTask();
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
            default));

        await scheduling.Task;
        var waiting = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: default).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
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

        var connection = await pool.GetAsync(default);
        connection.EnterRecovery();

        using var cancellation = new CancellationTokenSource();
        var exclusive = pool.GetAsync(
            static (candidate, _) => candidate.Connection.IsIdle,
            state: 0,
            timeout: default,
            cancellationToken: cancellation.Token).AsTask();
        var multiplexed = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: default).AsTask();

        await WaitUntilAsync(() => pool.WaiterCount == 2,
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

        var connection = await pool.GetAsync(default);
        connection.EnterRecovery();

        using var cancellation = new CancellationTokenSource();
        var rejecting = pool.GetAsync(
            static (_, _) => false,
            state: 0,
            timeout: default,
            cancellationToken: cancellation.Token).AsTask();
        var accepting = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: default).AsTask();

        await WaitUntilAsync(() => pool.WaiterCount == 2,
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
    [Ignore("Legacy wait queue regression; enable when the single-flight coordinator replaces it in Stage 4.")]
    public async Task AvailabilityPublishedBeforeWaiters_DrivesCompatibleNewcomer()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(default);
        connection.EnterRecovery();
        connection.CompleteRecovery();

        using var cancellation = new CancellationTokenSource();
        var idleOnly = pool.GetAsync(
            static (candidate, _) => candidate.IsIdleCandidate,
            state: 0,
            timeout: default,
            cancellationToken: cancellation.Token).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
            "the incompatible renter must park before the compatible newcomer arrives");

        var accepting = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: default).AsTask();

        Assert.AreSame(connection, await accepting.WaitAsync(TimeSpan.FromSeconds(1)),
            "a newcomer must drive capacity which predates every queued waiter");
        cancellation.Cancel();
        await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => idleOnly);
    }

    [TestMethod]
    [Ignore("Legacy wait queue regression; enable when the single-flight coordinator replaces it in Stage 4.")]
    public async Task ReturnedIdleCandidate_DoesNotHideLaterCompatibleToken()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 2 });

        var first = pool.Get(default);
        first.EnterRecovery();
        var second = pool.Get(default);
        second.EnterRecovery();

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var rent = Task.Factory.StartNew(() => pool.Get(
                static (candidate, state) =>
                {
                    if (ReferenceEquals(candidate.Connection, state.First))
                    {
                        state.Entered.TrySetResult();
                        state.Release.Wait();
                        return false;
                    }
                    return ReferenceEquals(candidate.Connection, state.Second);
                },
                (First: first, Second: second, Entered: entered, Release: release),
                Timeout.InfiniteTimeSpan),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        await WaitUntilAsync(() => pool.WaiterCount == 1,
            "the renter must park before the first idle token is published");
        first.CompleteRecovery();
        first.RunInitialWorkToIdle();
        await entered.Task;
        second.CompleteRecovery();
        second.RunInitialWorkToIdle();
        release.Set();

        Assert.AreSame(second, await rent.WaitAsync(TimeSpan.FromSeconds(1)),
            "returning the first rejected token must not truncate the idle scan before the second token");
    }

    [TestMethod]
    [Ignore("Legacy wait queue regression; enable when the single-flight coordinator replaces it in Stage 4.")]
    public async Task LateCompatibleWaiter_IsDrivenAfterAwakenedWaiterRejectsMultiplexCandidate()
    {
        await AssertLateCompatibleWaiterIsDriven(static (_, state) =>
        {
            state.Entered.TrySetResult();
            state.Release.Wait();
            return false;
        });
    }

    [TestMethod]
    [Ignore("Legacy wait queue regression; enable when the single-flight coordinator replaces it in Stage 4.")]
    public async Task LateCompatibleWaiter_IsDrivenAfterAwakenedWaiterThrows()
    {
        await AssertLateCompatibleWaiterIsDriven(static (_, state) =>
        {
            state.Entered.TrySetResult();
            state.Release.Wait();
            throw new InvalidOperationException("reject the restored multiplex candidate");
        }, expectFirstFailure: true);
    }

    [TestMethod]
    [Ignore("Legacy wait queue regression; enable when the single-flight coordinator replaces it in Stage 4.")]
    public async Task LateCompatibleWaiter_IsDrivenAfterAwakenedWaiterConsumesMultiplexCandidate()
    {
        await AssertLateCompatibleWaiterIsDriven(static (_, state) =>
        {
            state.Entered.TrySetResult();
            state.Release.Wait();
            return true;
        }, expectFirstSuccess: true);
    }

    static async Task AssertLateCompatibleWaiterIsDriven(
        Func<ConnectionCandidate<AdmissionConnection>,
            (TaskCompletionSource Entered, ManualResetEventSlim Release), bool> firstSchedule,
        bool expectFirstFailure = false,
        bool expectFirstSuccess = false)
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });
        var connection = await pool.GetAsync(default);
        connection.EnterRecovery();

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        using var cancellation = new CancellationTokenSource();
        var first = pool.GetAsync(firstSchedule, (Entered: entered, Release: release), default,
            cancellation.Token).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
            "the first waiter must park while the connection is recovering");

        connection.CompleteRecovery();
        await entered.Task;

        var accepting = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: default).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
            "the compatible waiter must link while the first waiter owns the availability edge");
        release.Set();

        if (expectFirstFailure)
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => first);
        else if (expectFirstSuccess)
            Assert.AreSame(connection, await first);

        Assert.AreSame(connection, await accepting.WaitAsync(TimeSpan.FromSeconds(1)),
            "settling the first attempt must drive the compatible successor");

        if (!expectFirstFailure && !expectFirstSuccess)
        {
            cancellation.Cancel();
            await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => first);
        }
    }

    [TestMethod]
    public async Task ReturnedIdleCandidate_RemainsVisiblePastRejectingSyncWaiter()
    {
        var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });
        var connection = pool.Get(default);
        connection.RunInitialWorkToIdle();

        var rejecting = Task.Factory.StartNew(() => pool.Get(
                static (_, _) => false, state: 0, Timeout.InfiniteTimeSpan),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await WaitUntilAsync(() => pool.WaiterCount == 1,
            "the rejecting waiter must park after returning the idle candidate");

        var accepting = Task.Factory.StartNew(() => pool.Get(
                static (_, _) => true, state: 0, Timeout.InfiniteTimeSpan),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        Assert.AreSame(connection, await accepting,
            "a returned idle token must remain discoverable past an incompatible waiter");

        await pool.DisposeAsync();
        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => rejecting);
    }

    [TestMethod]
    public async Task CancelledWaiter_DoesNotConsumeLaterAvailability()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(default);
        connection.EnterRecovery();

        using var cancellation = new CancellationTokenSource();
        var cancelled = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: default,
            cancellationToken: cancellation.Token).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
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
        await WaitUntilAsync(() => pool.WaiterCount == 0,
            "cancellation must physically unlink the waiter");

        var accepting = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: default).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
            "the replacement waiter must be queued before availability is restored");

        connection.CompleteRecovery();
        Assert.AreSame(connection, await accepting);
    }

    [TestMethod]
    public async Task IdleAvailability_PreservesFifoAcrossPlacementPolicies()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(default);
        var exclusive = pool.GetAsync(
            static (candidate, _) => candidate.IsIdleCandidate,
            state: 0,
            timeout: default).AsTask();
        var shared = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: default).AsTask();

        await WaitUntilAsync(() => pool.WaiterCount == 2,
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

        var connection = await pool.GetAsync(default);
        var queued = pool.GetAsync(default).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 1,
            "the older renter must be queued before the idle edge");

        connection.RunInitialWorkToIdle();
        var newcomer = pool.GetAsync(default).AsTask();

        Assert.AreSame(connection, await queued,
            "the detached wake must retain priority over a racing newcomer");
        Assert.IsFalse(newcomer.IsCompleted,
            "the newcomer must wait for availability after the older renter");

        connection.RunInitialWorkToIdle();
        Assert.AreSame(connection, await newcomer);
    }

    [TestMethod]
    public async Task SignalWithoutQueuedWaiter_IsPassedToLaterPublication()
    {
        var queue = new ConnectionPool<AdmissionConnection>.ConnectionWaitQueue();
        var owner = queue.Enqueue();
        queue.Signal();
        var wake = await owner.Task;

        queue.Signal();
        var follower = queue.Enqueue();
        Assert.IsFalse(follower.CanRescan);

        queue.Pass(wake, idleAvailable: false);
        var passed = await follower.Task;
        queue.Consume(passed, idleAvailable: false);
    }

    [TestMethod]
    [Ignore("Legacy wait queue regression; enable when the single-flight coordinator replaces it in Stage 4.")]
    public async Task RetiringDetachedWake_DoesNotEraseOutstandingAvailability()
    {
        var queue = new ConnectionPool<AdmissionConnection>.ConnectionWaitQueue();
        var first = queue.Enqueue();
        var second = queue.Enqueue();
        queue.Signal();
        var firstWake = await first.Task;
        queue.Signal();
        var secondWake = await second.Task;
        queue.Signal();

        queue.Pass(firstWake, idleAvailable: false);
        var follower = queue.Enqueue();
        _ = queue.Requeue(secondWake, idleAvailable: false);

        Assert.IsTrue(follower.Task.Wait(TimeSpan.FromSeconds(1)),
            "retiring one detached wake must not erase an availability edge owed to a follower");
    }

    [TestMethod]
    public void CancellationAfterDetachment_PreservesOwnedWake()
    {
        var queue = new ConnectionPool<AdmissionConnection>.ConnectionWaitQueue();
        var waiter = queue.Enqueue(synchronous: true);
        queue.Signal();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        var wake = queue.Wait(waiter, cancellation.Token);

        Assert.AreNotEqual(0, wake.Remaining,
            "cancellation must not discard a wake already detached for this waiter");
        queue.Consume(wake, idleAvailable: false);
    }

    [TestMethod]
    public async Task RemovingDetachedWaiter_PassesWakeToQueuedFollower()
    {
        var queue = new ConnectionPool<AdmissionConnection>.ConnectionWaitQueue();
        var removed = queue.Enqueue();
        var follower = queue.Enqueue();
        queue.Signal();
        await removed.Task;

        queue.Remove(removed);

        var wake = await follower.Task;
        queue.Consume(wake, idleAvailable: false);
    }

    [TestMethod]
    public async Task RemovingQueuedHead_TransfersRescanRightToFollower()
    {
        var queue = new ConnectionPool<AdmissionConnection>.ConnectionWaitQueue();
        var removed = queue.Enqueue();
        var follower = queue.Enqueue();
        Assert.IsTrue(removed.CanRescan);
        Assert.IsFalse(follower.CanRescan);

        queue.Remove(removed);

        Assert.IsTrue(follower.Task.IsCompleted,
            "removing the sole rescan owner must not leave its barred follower parked");
        var wake = await follower.Task;
        queue.Consume(wake, idleAvailable: false);
    }

    [TestMethod]
    public async Task Dispose_FaultsQueuedWaiterWithoutRevokingDetachedWake()
    {
        var queue = new ConnectionPool<AdmissionConnection>.ConnectionWaitQueue();
        var owner = queue.Enqueue();
        var queued = queue.Enqueue();
        queue.Signal();
        var wake = await owner.Task;

        queue.Dispose();

        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(async () => await queued.Task);
        queue.Consume(wake, idleAvailable: false);
    }

    [TestMethod]
    public async Task RequeueAfterDispose_RejectsWithoutRevokingDetachedWake()
    {
        var queue = new ConnectionPool<AdmissionConnection>.ConnectionWaitQueue();
        var waiter = queue.Enqueue();
        queue.Signal();
        var wake = await waiter.Task;

        queue.Dispose();

        Assert.ThrowsExactly<ObjectDisposedException>(() => queue.Requeue(wake, idleAvailable: false));
        queue.Pass(wake);
    }

    [TestMethod]
    public async Task ExclusiveIdleRenters_ConcurrentSyncAsync_NeverLoseAvailability_Stress()
    {
        var iterations = Pg.StressEnv.Iterations(fallback: 32, cap: 200_000);
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });
        var connection = pool.Get(default);
        connection.MarkBusy();
        connection.RunInitialWorkToIdle();

        const int workerCount = 4;
        using var start = new ManualResetEventSlim();
        var completed = 0;
        var workers = new Task[workerCount];
        for (var worker = 0; worker < workerCount; worker++)
        {
            var offset = worker;
            workers[worker] = Task.Factory.StartNew(() =>
            {
                start.Wait();
                for (var i = offset; i < iterations; i += workerCount)
                {
                    AdmissionConnection rented;
                    if ((i & 1) == 0)
                    {
                        rented = pool.Get(
                            static (candidate, _) => TryClaimIdle(candidate),
                            state: 0,
                            Timeout.InfiniteTimeSpan);
                    }
                    else
                    {
                        rented = pool.GetAsync(
                                static (candidate, _) => TryClaimIdle(candidate),
                                state: 0,
                                timeout: default)
                            .AsTask().GetAwaiter().GetResult();
                    }

                    if ((i & 7) == 0)
                        Thread.Yield();
                    rented.RunInitialWorkToIdle();
                    Interlocked.Increment(ref completed);
                }
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        start.Set();
        await Task.WhenAll(workers);
        Assert.AreEqual(iterations, Volatile.Read(ref completed));

        static bool TryClaimIdle(ConnectionCandidate<AdmissionConnection> candidate)
        {
            if (!candidate.IsIdleCandidate)
                return false;
            candidate.Connection.MarkBusy();
            return true;
        }
    }

    [TestMethod]
    public async Task AwakenedWaiter_ConcurrentNewcomer_NeverBarges_Stress()
    {
        var iterations = Pg.StressEnv.Iterations(fallback: 32, cap: 50_000);
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(allowPruning: false), new() { MaxConnections = 1 });
        var connection = await Rent();
        using var publish = new AutoResetEvent(false);
        using var published = new AutoResetEvent(false);
        var publisher = Task.Factory.StartNew(() =>
        {
            for (var i = 0; i < iterations; i++)
            {
                publish.WaitOne();
                connection.RunInitialWorkToIdle();
                published.Set();
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        for (var i = 0; i < iterations; i++)
        {
            var awakened = Rent().AsTask();
            Assert.IsTrue(SpinWait.SpinUntil(() => pool.WaiterCount == 1, TimeSpan.FromSeconds(1)));

            publish.Set();
            var newcomer = Rent().AsTask();
            published.WaitOne();

            Assert.AreSame(connection, await awakened,
                "a newcomer must not bypass the demand moving from queued to awakened");
            Assert.IsFalse(newcomer.IsCompleted);
            connection.RunInitialWorkToIdle();
            Assert.AreSame(connection, await newcomer);
        }

        await publisher;

        ValueTask<AdmissionConnection> Rent()
            => pool.GetAsync(static (candidate, _) =>
            {
                if (!candidate.IsIdleCandidate)
                    return false;
                candidate.Connection.MarkBusy();
                return true;
            }, state: 0, timeout: default);
    }

    [TestMethod]
    public async Task ThrowingAwakenedWaiter_PassesAvailability()
    {
        await using var pool = new ConnectionPool<AdmissionConnection>(
            new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

        var connection = await pool.GetAsync(default);
        var throwing = pool.GetAsync(
            static (_, _) => throw new InvalidOperationException("placement failed"),
            state: 0,
            timeout: default).AsTask();
        var follower = pool.GetAsync(default).AsTask();
        await WaitUntilAsync(() => pool.WaiterCount == 2,
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

        await pool.GetAsync(default);
        var exclusive = pool.GetAsync(
            static (candidate, _) => candidate.IsIdleCandidate,
            state: 0,
            timeout: default).AsTask();
        var shared = pool.GetAsync(
            static (_, _) => true,
            state: 0,
            timeout: default).AsTask();

        await WaitUntilAsync(() => pool.WaiterCount == 2,
            "both waiters must be queued before disposal");
        await pool.DisposeAsync();

        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => exclusive);
        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => shared);
    }

    [TestMethod]
    public async Task AvailabilityRacingWaiterRegistration_IsNotLost()
    {
        for (var i = 0; i < 32; i++)
        {
            await using var pool = new ConnectionPool<AdmissionConnection>(
                new AdmissionConnectionFactory(), new() { MaxConnections = 1 });

            var connection = await pool.GetAsync(default);
            connection.EnterRecovery();
            var waiting = pool.GetAsync(
                static (_, _) => true,
                state: 0,
                timeout: default).AsTask();

            // WaiterCount becomes visible immediately after registration, potentially before
            // the mandatory state rescan which closes the registration/signal race.
            await WaitUntilAsync(() => pool.WaiterCount == 1,
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
        var opening = pool.GetAsync(default).AsTask();
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
        var opening = Task.Run(() => pool.Get(default));
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
            default));

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
            var time = new FakeTimeProvider();
            var factory = new AdmissionConnectionFactory();
            await using var pool = new ConnectionPool<AdmissionConnection>(
                factory,
                new() { MaxConnections = 1, TimeProvider = time });

            await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
                await pool.GetAsync<int>(
                    static (_, _) => throw new InvalidOperationException("placement failed"),
                    state: 0,
                    timeout: default));

            // Placement failure republishes the admitted connection. Consume that token, then
            // rent again immediately on this thread: the second rent parks and exercises the
            // returned timeout source. On the pre-fix poisoned cache this throws
            // ObjectDisposedException instead of timing out.
            Assert.AreSame(factory.LastCreated, await pool.GetAsync(default));
            var waiting = pool.GetAsync(TimeSpan.FromSeconds(1)).AsTask();
            await WaitUntilAsync(() => pool.WaiterCount == 1,
                "the rent-cache probe must arm its returned timeout source");
            time.Advance(TimeSpan.FromSeconds(1));
            await Assert.ThrowsExactlyAsync<TimeoutException>(() => waiting);
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
            await pool.GetAsync(default));

        var failed = factory.Created[0];
        Assert.IsTrue(failed.Completion.IsCompleted,
            "a connection that failed admission must be closed rather than published");

        var replacement = await pool.GetAsync(default);
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
            pool.Get(default));

        var failed = factory.Created[0];
        Assert.IsTrue(failed.Completion.IsCompleted,
            "a connection that failed admission must be closed rather than published");

        var replacement = await pool.GetAsync(default);
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
            await pool.GetAsync(default));

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
            pool.Get(default));

        Assert.IsTrue(inner.LastCreated!.Completion.IsCompleted,
            "the initializing factory must close a connection it cannot return");
    }

    [TestMethod]
    public async Task Sync_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetAsync(default);
        await RunSyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task Async_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetAsync(default);
        await RunAsyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task SyncWhileAsyncInFlight_SameLeasedConn_BothComplete()
    {
        await using var pool = NewPool();
        var conn = await pool.GetAsync(default);

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
        await syncTask;

        await slowTask;
        await slowEnum.DisposeAsync();
    }


    static Exception Root(Exception ex)
    {
        while (ex is not PgClientClosedException && ex.InnerException is not null)
            ex = ex.InnerException;
        return ex;
    }

    static async Task WaitUntilAsync(Func<bool> condition, string? _ = null)
    {
        while (!condition())
            await Task.Yield();
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
        var conn1 = await pool.GetAsync(default);
        await RunAsyncOn(conn1, "select 1"); // healthy before the abort
        Assert.AreEqual(1, tracker.RegisteredConnectionCount);

        await conn1.Protocol.DisposeAsync(); // forceful terminal abort (fire-and-forget; teardown runs async)
        // Completion settles at the END of background shutdown, so it is eventually consistent,
        // not immediate. The pool's eviction gate keys off it, so it reclaims once it lands.
        await WaitUntilAsync(() => conn1.Completion.IsCompleted,
            "a terminally aborted connection must reach Completed — that is the pool's eviction gate.");
        await WaitUntilAsync(() => tracker.RegisteredConnectionCount == 0,
            "terminal completion must promptly release tracker membership");

        var conn2 = await pool.GetAsync(default);
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
        var conn = await pool.GetAsync(default);
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
                await DrainAsync(enums[i]);
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
