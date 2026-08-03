using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Tests;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pipelines;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Drives PgClientProtocol.BeginExclusiveScope directly (no ADO surface yet), so the assertions
// attribute to the wire-takeover + nested-pipeline composition itself. This is also the shell's first
// real execution - the acceptance test that the exclusive flow actually owns the wire and runs user
// subflows on its inner pipeline.
[TestClass]
public class ExclusiveAccessFlowTests
{
    static async Task DrainAsync(CommandFlow flow)
    {
        var e = flow.GetAsyncEnumerator();
        while (await e.MoveNextAsync()) { }
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task Scope_RoundTrip_RunsCommandOnInnerPipeline()
    {
        var protocol = await PgTestPool.GetProtocolAsync();
        var scope = protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;   // acquired exclusive access; the flow owns the wire

        var cmd = scope.Queue(new CommandFlow(async: true, Command.Create("select 1")));
        await DrainAsync(cmd);

        await scope.CompleteScopeAsync();
    }

    [TestMethod]
    public async Task Scope_MultipleCommands_RunSequentially()
    {
        var protocol = await PgTestPool.GetProtocolAsync();
        var scope = protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;

        for (int i = 0; i < 5; i++)
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));

        await scope.CompleteScopeAsync();
    }

    [TestMethod]
    public async Task Scope_FlyweightReuse_AcrossScopes()
    {
        var protocol = await PgTestPool.GetProtocolAsync();
        for (int i = 0; i < 3; i++)
        {
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
            await scope.CompleteScopeAsync();
        }
    }

    [TestMethod]
    public async Task Scope_Release_ResetsSessionState()
    {
        await using var protocol = await PgTestPool.NewIsolatedAsync();
        var suffix = Guid.NewGuid().ToString("N");
        var tempTable = "slon_reset_" + suffix;
        var channel = "slon_reset_" + suffix;

        var first = protocol.BeginExclusiveScope(async: true);
        await first.HandoffReady;
        try
        {
            await DrainSuccessfullyAsync(first.Queue(new CommandFlow(async: true, Command.Create($"CREATE TEMP TABLE {tempTable}(value integer)"))));
            await DrainSuccessfullyAsync(first.Queue(new CommandFlow(async: true, Command.Create("SET application_name = 'slon-reset-probe'"))));
            await DrainSuccessfullyAsync(first.Queue(new CommandFlow(async: true, Command.Create($"LISTEN {channel}"))));
        }
        finally
        {
            await first.CompleteScopeAsync();
        }

        var second = protocol.BeginExclusiveScope(async: true);
        await second.HandoffReady;
        try
        {
            Assert.AreEqual(TransactionStatus.Idle, protocol.TransactionStatus);
            await DrainSuccessfullyAsync(second.Queue(new CommandFlow(async: true, Command.Create(
                $"DO $$ BEGIN IF to_regclass('pg_temp.{tempTable}') IS NOT NULL THEN RAISE EXCEPTION 'temp table survived reset'; END IF; END $$"))));
            await DrainSuccessfullyAsync(second.Queue(new CommandFlow(async: true, Command.Create(
                "DO $$ BEGIN IF current_setting('application_name') = 'slon-reset-probe' THEN RAISE EXCEPTION 'GUC survived reset'; END IF; END $$"))));
            await DrainSuccessfullyAsync(second.Queue(new CommandFlow(async: true, Command.Create(
                $"DO $$ BEGIN IF EXISTS (SELECT FROM pg_listening_channels() channel_name WHERE channel_name = '{channel}') THEN RAISE EXCEPTION 'LISTEN survived reset'; END IF; END $$"))));
        }
        finally
        {
            await second.CompleteScopeAsync();
        }

        static async Task DrainSuccessfullyAsync(CommandFlow flow)
        {
            var enumerator = flow.GetAsyncEnumerator();
            while (await enumerator.MoveNextAsync())
            {
                var rows = enumerator.Current.GetAsyncEnumerator();
                while (await rows.MoveNextAsync()) { }
                await rows.DisposeAsync();
                enumerator.Current.GetCommandComplete();
            }
            await enumerator.DisposeAsync();
        }
    }

    [TestMethod]
    public async Task Scope_Release_CanPreserveTemporaryObjects()
    {
        var protocol = await PgTestPool.NewIsolatedAsync(
            options => options.ScopeReset.DropTemporaryObjects = false);
        var table = "slon_preserved_" + Guid.NewGuid().ToString("N");
        try
        {
            var first = protocol.BeginExclusiveScope(async: true);
            await first.HandoffReady;
            await DrainAsync(first.Queue(new CommandFlow(async: true, Command.Create(
                $"CREATE TEMP TABLE {table}(value integer)"))));
            await first.CompleteScopeAsync();

            var second = protocol.BeginExclusiveScope(async: true);
            await second.HandoffReady;
            await DrainAsync(second.Queue(new CommandFlow(async: true, Command.Create(
                $"INSERT INTO {table} VALUES (1)"))));
            await DrainAsync(second.Queue(new CommandFlow(async: true, Command.Create(
                $"DROP TABLE {table}"))));
            await second.CompleteScopeAsync();
        }
        finally
        {
            await protocol.CompleteAsync();
        }
    }

    [TestMethod]
    public async Task Scope_Release_WithOpenTransaction_FailsAndRecoversWire()
    {
        var protocol = await PgTestPool.GetProtocolAsync();
        var scope = protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;

        await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("BEGIN"))));

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            async () => await scope.CompleteScopeAsync());
        StringAssert.Contains(exception.Message, "must be committed or rolled back");
        Assert.AreEqual(TransactionStatus.Idle, protocol.TransactionStatus);

        var next = protocol.BeginExclusiveScope(async: true);
        await next.HandoffReady;
        await DrainAsync(next.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
        await next.CompleteScopeAsync();
    }

    [TestMethod]
    public async Task Scope_Recovery_WithResetDisabled_DrainsOnlyRollback()
    {
        var protocol = await PgTestPool.NewIsolatedAsync(options =>
        {
            var reset = options.ScopeReset;
            reset.CloseCursors = false;
            reset.ResetSessionAuthorization = false;
            reset.ResetParameters = false;
            reset.ClearListeners = false;
            reset.ReleaseAdvisoryLocks = false;
            reset.DropTemporaryObjects = false;
        });
        try
        {
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady.WaitAsync(TimeSpan.FromSeconds(5));
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("BEGIN"))));
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                async () => await scope.CompleteScopeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));

            var next = protocol.BeginExclusiveScope(async: true);
            await next.HandoffReady.WaitAsync(TimeSpan.FromSeconds(5));
            await DrainAsync(next.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
            await next.CompleteScopeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        }
        finally
        {
            await protocol.CompleteAsync();
        }
    }

    [TestMethod]
    public Task GracefulProtocolShutdown_WhileScopeOpen_CascadesToInnerTeardown()
        => ProtocolShutdownWhileScopeOpenCascadesToInnerTeardown(null);

    [TestMethod]
    public Task ForcefulProtocolShutdown_WhileScopeOpen_CascadesToInnerTeardown()
        => ProtocolShutdownWhileScopeOpenCascadesToInnerTeardown(new InvalidOperationException("test shutdown"));

    static async Task ProtocolShutdownWhileScopeOpenCascadesToInnerTeardown(Exception? cause)
    {
        var protocol = await PgTestPool.NewIsolatedAsync(o => o.HeartbeatInterval = TimeSpan.FromMilliseconds(50));
        try
        {
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;

            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));

            try
            {
                await protocol.CompleteAsync(cause).WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (TimeoutException)
            {
                // A hang here is a distinct face from the collision below - dump the slot/pump state
                // so it self-classifies (the shutdown x recovery bailout face was pinned by exactly
                // this readout in the racing-teardown family).
                Assert.Fail("cascade CompleteAsync did not converge: " +
                    $"[unflushed={protocol.UnflushedBytes} scope: pending={scope.IsPending} started={scope.IsStarted} completed={scope.IsCompleted}]\n" +
                    $"{ProtocolDiag.Gauges(protocol)}\nsource: {ProtocolDiag.SourceState(protocol)}");
            }
            catch (InvalidOperationException ex) when (!ReferenceEquals(ex, cause))
            {
                // Pump collision capture: record the premise the deterministic-repro attempts keep
                // missing (see the amplifier test below).
                Assert.Fail("flush-promise collision: " +
                    $"[unflushed={protocol.UnflushedBytes} scope: pending={scope.IsPending} started={scope.IsStarted} completed={scope.IsCompleted} " +
                    $"protocol: draining={protocol.IsDraining} completed={protocol.IsCompleted}]\n" +
                    $"{ProtocolDiag.Gauges(protocol)}\nsource: {ProtocolDiag.SourceState(protocol)}\n{ex}");
            }

            Assert.IsTrue(protocol.IsCompleted, "Protocol must reach Completed; a hang means the inner executor was stranded.");
            Assert.AreSame(cause, protocol.CompletionException, "Shutdown reason must be preserved.");
        }
        finally
        {
            await protocol.DisposeAsync();
        }
    }

    // R1 allocation invariant (verification step 5): the per-connection scope machinery (inner pipeline
    // flyweight, ExclusiveAccessFlow, and the scope CloseSignal) is created once and reused, and the
    // normal completion path spares the scope CTS trip so its linked CTSes stay pristine and reusable.
    // After one warm-up cycle, repeated open/run/close cycles must not allocate per-cycle for the scope
    // signal. We don't assert exact zero (the subflow's CommandFlow + per-row materialization allocate);
    // we assert the steady-state per-cycle allocation does not GROW when the cascade is exercised vs a
    // baseline - i.e. the scope signal itself is amortized to zero. The proof that the CTS-sparing path
    // held is really ExclusiveAccessFlowStressTests.Stress_RepeatedScopes_Reuse at 20k (a tripped linked
    // CTS could not be reused); this is the direct steady-state check.
    // Homomorphism seam (NOT a re-test of flow behavior): the inner pipeline is the SAME machine as the
    // outer (same Control/source/executor), so command behavior - multi-result, errors, RFQ resync - is
    // inherited from the outer suite by construction. The ONE place inner deliberately diverges is the
    // inner Control's RecoversWireFailures => false. A backend SQL error is a NORMAL completion (RFQ
    // follows, recovery is NOT engaged - recovery is wire-fault-only), so it flows through the identical
    // homomorphic path at both levels. This asserts the inner Control routes a SQL error exactly as the
    // outer does: the error surfaces on result consumption, the inner pipeline resyncs to RFQ, and the
    // scope stays usable for a subsequent subflow. The subflow is the probe; the assertion is about the
    // inner Control's normal-completion routing being a faithful image of the outer's.
    //
    // SCOPE: this covers INPUT-CAUSED errors (the normal majority - a function of the caller's inputs;
    // backend sends ErrorResponse then ReadyForQuery, the session is fine). FATAL/PANIC/admin-shutdown/
    // protocol-violation errors (session-terminating, often no clean RFQ) are a SEPARATE not-yet-built
    // out-of-band path that must route like a wire fault (materialize the close reason, route to the
    // root/teardown, skip the in-band resync) rather than be treated as a normal result. A future test
    // for that class must NOT assume the scope stays usable.
    [TestMethod]
    public async Task SqlErrorSubflow_InScope_ResyncsAndScopeStaysUsable()
    {
        // Input-caused SQL error, scope stays usable (resyncs to RFQ), so use the shared pool.
        var protocol = await PgTestPool.GetProtocolAsync();
        {
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;

            // A subflow producing a backend SQL error (its own command, its own Sync/RFQ - a separate
            // batch from the next subflow, the realistic "a statement errored, run another" shape). The
            // error surfaces only on result consumption (MoveNextAsync itself does not throw), and the
            // inner pipeline resyncs to RFQ - recovery is NOT engaged (recovery is wire-fault-only).
            var errFlow = scope.Queue(new CommandFlow(async: true, Command.Create("select 1/0")));
            var e = errFlow.GetAsyncEnumerator();
            Assert.IsTrue(await e.MoveNextAsync(), "the error command's result should be delivered");
            PgErrorException? thrown = null;
            try { e.Current.GetCommandComplete(); }
            catch (PgErrorException ex) { thrown = ex; }
            Assert.IsNotNull(thrown, "division by zero should surface a PgErrorException on consumption");
            StringAssert.StartsWith(thrown!.SqlState, "22", "numeric error class (division by zero = 22012)");
            while (await e.MoveNextAsync()) { }
            await e.DisposeAsync();

            // The scope is still usable: a fresh subflow runs cleanly on the same inner pipeline after the
            // error - the inner Control routed the error exactly as the outer would (resync, no recovery).
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));

            await scope.CompleteScopeAsync();
        }
    }

    // Homomorphism seam: the inner SOURCE instance must escalate + pipeline exactly as the outer one
    // does. The one-at-a-time tests never overlap subflows, so the inner SlotEscalatingQueue stays on its
    // slot fast path and the inner executor never pipelines. Queue N async subflows BEFORE draining any,
    // forcing the inner source to escalate past the slot and the inner executor to carry multiple
    // in-flight - proving the nested source instance is a faithful image of the outer (single caller, so
    // this is queue-ahead async pipelining, the only in-scope pipelining a single connection can produce).
    [TestMethod]
    public async Task PipelinedSubflows_InScope_InnerSourceEscalatesAndPipelines()
    {
        var protocol = await PgTestPool.GetProtocolAsync();
        var scope = protocol.BeginExclusiveScope(async: true);
        await scope.HandoffReady;

        const int batch = 8;
        var flows = new CommandFlow[batch];
        // Queue all before draining any - this is what forces inner escalation + pipelining.
        for (int i = 0; i < batch; i++)
            flows[i] = scope.Queue(new CommandFlow(async: true, Command.Create("select " + i)));

        // Drain in order (FIFO = submission order = execution order on the inner single-pump executor).
        for (int i = 0; i < batch; i++)
            await DrainAsync(flows[i]);

        await scope.CompleteScopeAsync();
    }

    // Allocation oracle: GC.GetAllocatedBytesForCurrentThread() measures the WHOLE thread, so any
    // concurrent test's allocations (and JIT warmup, finalizers, async continuations landing here)
    // pollute the per-cycle delta in a parallel suite run - it false-fails under contention while
    // passing comfortably solo. Isolation must come from a quiet process, not a narrower metric (a
    // local counter could not see a per-cycle scope-signal alloc landing off-thread). Run solo:
    //   dotnet test --filter Scope_RepeatedCycles_NoPerCycleScopeSignalAlloc
    [TestMethod]
    [Ignore("Per-thread allocation oracle needs a quiet process; run solo when touching the exclusive-scope flyweight. A concurrent test's allocations pollute GC.GetAllocatedBytesForCurrentThread.")]
    public async Task Scope_RepeatedCycles_NoPerCycleScopeSignalAlloc()
    {
        var protocol = await PgTestPool.GetProtocolAsync();

        async Task Cycle()
        {
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;
            await DrainAsync(scope.Queue(new CommandFlow(async: true, Command.Create("select 1"))));
            await scope.CompleteScopeAsync();
        }

        // Warm up: first cycle builds the flyweight scope machinery (one-time alloc) + JITs the path.
        await Cycle();

        const int cycles = 50;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < cycles; i++)
            await Cycle();
        var perCycle = (GC.GetAllocatedBytesForCurrentThread() - before) / (double)cycles;

        // A fresh scope CloseSignal + its two linked CTSes per cycle would add well over a kilobyte each;
        // the flyweight reuse keeps the scope signal off the per-cycle budget. Generous bound (the cycle's
        // own CommandFlow/result allocations dominate) - it would be blown only by a per-cycle scope-signal
        // regression.
        Assert.IsTrue(perCycle < 8192,
            $"Per-cycle steady-state allocation {perCycle:F0} bytes suggests the scope signal is no longer a reused flyweight.");
    }

    // Hold the inner teardown flush until the test releases it. If the outer flow retires first,
    // its source pump collides with the held single-caller flush promise.
    [TestMethod]
    public async Task ForcefulShutdown_ScopeOpen_NoPumpCollision()
    {
        // No real server is involved (canned handshake), so options only need the reset pinned on:
        // the scope reset write is the teardown's unflushed-bytes fuel.
        var options = new PgClientOptions
        {
            EndPoint = TestEndPoint.Default,
            Username = "postgres",
            Password = "postgres123",
            Database = "postgres",
            Ssl = new() { Mode = PostgreSqlSslMode.Disable },
            ScopeReset = new ScopeResetOptions { ClearListeners = true },
        };

        var protocolOptions = new PgClientProtocolOptions(options)
        {
            CompletionTimeout = TimeSpan.FromMilliseconds(2),
            HeartbeatInterval = TimeSpan.FromMilliseconds(5),
        };
        var transport = new GatedWriteTransport(StartupHandshake());
        var protocol = PgClientProtocol.Create(protocolOptions);
        await protocol.StartAsync(options, transport);

        var scope = protocol.BeginExclusiveScope(async: true);
        try
        {
            await scope.HandoffReady.WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (TimeoutException)
        {
            Assert.Fail($"scope handoff stranded pre-arm\n{ProtocolDiag.Gauges(protocol)}\nsource: {ProtocolDiag.SourceState(protocol)}");
        }

        transport.ArmWriteFaults();
        var completion = protocol.CompleteAsync(new Exception("test shutdown"));
        try
        {
            await transport.HeldWriteEntered.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsFalse(completion.IsCompleted, "Outer teardown advanced while the inner flush was held.");
        }
        catch (TimeoutException)
        {
            // The premise itself failed: teardown never reached the held write. Distinct from both the
            // collision (IOE below) and a held-write hang; dump so it self-classifies.
            Assert.Fail($"teardown never entered the held write (amplifier premise not reached)\n" +
                $"{ProtocolDiag.Gauges(protocol)}\nsource: {ProtocolDiag.SourceState(protocol)}");
        }
        finally
        {
            transport.ReleaseHeldWrite();
        }
        try
        {
            await completion.WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (PgClientClosedException)
        {
        }
        catch (TimeoutException)
        {
            Assert.Fail($"teardown did not converge after held-write release\n{ProtocolDiag.Gauges(protocol)}\nsource: {ProtocolDiag.SourceState(protocol)}");
        }
        catch (InvalidOperationException ex)
        {
            Assert.Fail($"Teardown pumps collided on the shared flush promise: {ex}\n{ProtocolDiag.Gauges(protocol)}\nsource: {ProtocolDiag.SourceState(protocol)}");
        }
        Assert.IsTrue(protocol.IsCompleted, "Protocol must reach Completed.");
        await protocol.DisposeAsync();
    }

    static byte[] StartupHandshake()
    {
        var b = new byte[64];
        int o = 0;
        b[o++] = (byte)'R'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 0); o += 4;
        b[o++] = (byte)'K'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 12); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 4321); o += 4; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 8765); o += 4;
        b[o++] = (byte)'Z'; BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(o), 5); o += 4; b[o++] = (byte)'I';
        return b.AsSpan(0, o).ToArray();
    }

    // After arming, the first write faults immediately and the second is held until released.
    // A third flush attempted while it is held collides with the writer's single-caller promise.
    sealed class GateStream : Stream
    {
        readonly TaskCompletionSource _heldWriteEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _releaseHeldWrite = new(TaskCreationOptions.RunContinuationsAsynchronously);
        bool _faultWrites;
        int _faultedWrites;

        public Task HeldWriteEntered => _heldWriteEntered.Task;

        public void ArmWriteFaults() => Volatile.Write(ref _faultWrites, true);

        public void ReleaseHeldWrite() => _releaseHeldWrite.TrySetResult();

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => 0;
        public override long Position { get => 0; set { } }
        public override void Flush() { }
        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) { }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (Volatile.Read(ref _faultWrites))
            {
                if (Interlocked.Increment(ref _faultedWrites) is 2)
                {
                    _heldWriteEntered.TrySetResult();
                    await _releaseHeldWrite.Task.ConfigureAwait(false);
                }
                throw new IOException("simulated wire abort");
            }
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    // Canned-handshake reader (left open so reads park like an idle socket) over a real
    // DefaultStreamPipeWriter, so the write side exercises the production single-caller promise.
    sealed class GatedWriteTransport : TransportConnection
    {
        readonly Pipe _toClient = new();
        readonly GateStream _stream = new();

        public GatedWriteTransport(byte[] canned)
        {
            _toClient.Writer.WriteAsync(canned).AsTask().GetAwaiter().GetResult();
            Writer = new DefaultStreamPipeWriter(_stream, new StreamPipeWriterOptions(), supportCancelPending: false);
        }

        public override PipeReader Reader => _toClient.Reader;
        public override PipeWriter Writer { get; }
        public override void WaitWritable() { }

        public Task HeldWriteEntered => _stream.HeldWriteEntered;

        public void ArmWriteFaults() => _stream.ArmWriteFaults();

        public void ReleaseHeldWrite() => _stream.ReleaseHeldWrite();
    }

    [TestMethod]
    public async Task ScopeAbort_BreaksSubflowParkedOnRead_ProtocolSurvives()
    {
        var protocol = await PgTestPool.NewIsolatedAsync(o =>
            o.HeartbeatInterval = TimeSpan.FromMilliseconds(50));
        try
        {
            await using var blocker = await PgAdvisoryLock.AcquireAsync();
            var scope = protocol.BeginExclusiveScope(async: true);
            await scope.HandoffReady;
            var sub = scope.Queue(new CommandFlow(async: true, blocker.WaitCommand));

            var run = Task.Run(async () =>
            {
                var e = sub.GetAsyncEnumerator();
                try
                {
                    while (await e.MoveNextAsync()) { }
                    await e.DisposeAsync();
                    return (Exception?)null;
                }
                catch (Exception ex)
                {
                    return ex;
                }
            });

            protocol.AbortActiveScope();
            await blocker.ReleaseAsync();

            var observed = await run.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.IsNotNull(observed);
            while (observed is not PgClientClosedException && observed.InnerException is not null)
                observed = observed.InnerException;
            Assert.IsInstanceOfType<PgClientClosedException>(observed);
            Assert.IsFalse(protocol.IsCompleted);
        }
        finally
        {
            await protocol.DisposeAsync();
        }
    }

    [TestMethod]
    public async Task ScopeAbort_BreaksWriterParkedOnFlush()
    {
        var closedProtocol = await PgTestPool.NewIsolatedAsync();
        await closedProtocol.DisposeAsync();
        var control = new PgClientProtocol.Control(closedProtocol, poolFacing: true);

        using var scopeAbort = new CancellationTokenSource();
        var sink = new ParkOnFlushSink();
        var baseWriter = new ProtocolDataWriter(sink, Encoding.UTF8, static () => { }, default, control);
        var scopeWriter = ProtocolDataWriter.CreateScopeShell(baseWriter, scopeAbort.Token, control);
        var flush = scopeWriter.FlushAsync(CancellationToken.None);
        Assert.IsFalse(flush.IsCompleted);

        scopeAbort.Cancel();
        await Assert.ThrowsExactlyAsync<PgClientClosedException>(async () => await flush);
    }

    sealed class ParkOnFlushSink : IOutputWriter
    {
        readonly byte[] _buffer = new byte[4096];

        public long UnflushedBytes => 0;
        public void Advance(int count) { }
        public Memory<byte> GetMemory(int sizeHint = 0) => _buffer;
        public Span<byte> GetSpan(int sizeHint = 0) => _buffer;
        public void Flush(TimeSpan timeout = default) { }

        public async ValueTask FlushAsync(CancellationToken cancellationToken)
        {
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource)state!).TrySetCanceled(), completion);
            await completion.Task.ConfigureAwait(false);
        }
    }
}
