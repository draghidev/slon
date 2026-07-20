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

// Adds connection lifecycle management (lease/release via the idle channel, pool-driven
// heartbeat). If cross-connection blocking surfaces here, the lease path or the pool's
// shared heartbeat thread is the coupling. Each test builds a fresh pool so lease/release
// semantics are tested in isolation.
[TestClass]
public class ConnectionPoolTests
{
    sealed class AdmissionConnection : IPoolConnection<AdmissionConnection>
    {
        readonly Action _signalIdle;
        int _started;
        int _idle = 1;
        readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int _heartbeatCount;
        IDisposable? _heartbeatRegistration;

        public AdmissionConnection(ConnectionPoolContext<AdmissionConnection> context, bool registerHeartbeat = false)
        {
            _signalIdle = context.CreateConnectionIdleSignal(this);
            if (registerHeartbeat)
                _heartbeatRegistration = context.OnHeartbeat(static (connection, _) =>
                {
                    Interlocked.Increment(ref connection._heartbeatCount);
                    return ValueTask.CompletedTask;
                }, this);
        }

        public bool Started => Volatile.Read(ref _started) != 0;
        public bool IsIdle => Volatile.Read(ref _idle) != 0;
        public bool IsSchedulable => !Completion.IsCompleted;
        public Task Completion => _completion.Task;
        public int HeartbeatCount => Volatile.Read(ref _heartbeatCount);

        public void Start() => Volatile.Write(ref _started, 1);

        public void RunInitialWorkToIdle()
        {
            Assert.IsTrue(Started, "initial work must not begin before pool admission");
            Volatile.Write(ref _idle, 0);
            Volatile.Write(ref _idle, 1);
            _signalIdle();
        }

        public void MarkCompletedExternally()
        {
            Interlocked.Exchange(ref _heartbeatRegistration, null)?.Dispose();
            _completion.TrySetResult();
        }

        public int CompareTo(AdmissionConnection? other) => 0;

        public Task CompleteAsync(Exception? exception = null)
        {
            Interlocked.Exchange(ref _heartbeatRegistration, null)?.Dispose();
            _completion.TrySetResult();
            return _completion.Task;
        }
    }

    sealed class AdmissionConnectionFactory : IPoolConnectionFactory<AdmissionConnection>
    {
        readonly bool _registerHeartbeat;

        public AdmissionConnectionFactory(bool registerHeartbeat = false)
            => _registerHeartbeat = registerHeartbeat;

        public AdmissionConnection? LastCreated { get; private set; }

        public AdmissionConnection Create(ConnectionPoolContext<AdmissionConnection> context, TimeSpan timeout = default)
            => LastCreated = new(context, _registerHeartbeat);

        public ValueTask<AdmissionConnection> CreateAsync(ConnectionPoolContext<AdmissionConnection> context, CancellationToken cancellationToken = default)
            => new(LastCreated = new AdmissionConnection(context, _registerHeartbeat));
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
            "a second channel entry would allow the same idle connection to be leased twice");
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
                await pool.GetAsync(TimeSpan.FromMilliseconds(20)));
        }
    }

    [TestMethod]
    public async Task Sync_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        await RunSyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task Async_OnLeasedConnection_Completes()
    {
        await using var pool = NewPool();
        var conn = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));
        await RunAsyncOn(conn, "select 1");
    }

    [TestMethod]
    public async Task SyncWhileAsyncInFlight_SameLeasedConn_BothComplete()
    {
        await using var pool = NewPool();
        var conn = await pool.GetConnectionAsync(0L, TimeSpan.FromSeconds(10));

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

    // Terminal abort is the wire-dead path: forceful DisposeAsync RSTs the socket, fires AbortToken, and
    // drives Shutdown to Completed. The terminal Completion task is what the pool
    // evicts on; here we verify end-to-end that an aborted connection reaches Completed and the pool
    // reclaims its slot and opens a fresh, healthy connection in its place rather than handing the corpse
    // back. maxConnections:1 forces the same slot to be reused so the reclaim path is the one under test.
    [TestMethod]
    public async Task TerminalAbort_EvictsFromPool_ReacquireYieldsHealthy()
    {
        await using var pool = NewPool(maxConnections: 1);
        var conn1 = await pool.GetConnectionAsync(0L, Cap);
        await RunAsyncOn(conn1, "select 1"); // healthy before the abort

        await conn1.Protocol.DisposeAsync(); // forceful terminal abort (fire-and-forget; teardown runs async)
        // Completion settles at the END of background shutdown, so it is eventually consistent,
        // not immediate. The pool's eviction gate keys off it, so it reclaims once it lands.
        await WaitUntilAsync(() => conn1.Completion.IsCompleted, Cap,
            "a terminally aborted connection must reach Completed — that is the pool's eviction gate.");

        var conn2 = await pool.GetConnectionAsync(0L, Cap);
        Assert.AreNotSame(conn1, conn2, "the pool must replace the aborted connection, not hand it back.");
        Assert.IsFalse(conn2.Completion.IsCompleted, "the replacement connection must be live.");
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
        var conn = await pool.GetConnectionAsync(0L, Cap);
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
