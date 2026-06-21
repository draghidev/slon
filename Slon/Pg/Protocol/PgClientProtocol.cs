using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Pipelines;
using Slon.Runtime.CompilerServices;
using Draghi.Pipelining;
using Slon.Buffers;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Pg.Protocol;

enum ProtocolStatus
{
    Created,
    Ready,
    Draining,
    Completed
}

interface IProtocolStatic<T>
{
    ref readonly T Value { get; }
}

sealed class PgClientProtocolOptions
{
    public PgClientProtocolOptions()
    {
        DefaultClientEncoding = Encoding.UTF8;
    }

    public PgClientProtocolOptions(PgClientOptions options)
    {
        DefaultClientEncoding = options.Encoding;
        ReadTimeout = options.ReadTimeout;
        HeartbeatInterval = options.HeartbeatInterval;
        FlowActivationTimeout = options.ConnectionTimeout;
    }

    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
    // How much time to give CompleteAsync before forcefully aborting flows.
    public TimeSpan CompletionTimeout { get; set; } = TimeSpan.FromSeconds(10);
    /// The scheduler used to dispatch pipeline wake-signal continuations.
    /// Defaults to null, in which case the pipeline falls back to the ThreadPool.
    /// Set to a custom <see cref="PipelineScheduler"/> implementation to route continuations elsewhere.
    public PipelineScheduler? ExecutionScheduler { get; set; }
    /// The scheduler used to dispatch item activations (notifying consumers their item is ready).
    /// Defaults to null, in which case activations fall back to the ThreadPool.
    public PipelineScheduler? ActivationScheduler { get; set; }
    public Encoding DefaultClientEncoding { get; set; }
    public TimeSpan FlowActivationTimeout { get; set; }
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan ReadTimeout { get; set; } = Timeout.InfiniteTimeSpan;

    /// Cancel-request sender for the protocol's side-channel CancelRequest. Receives
    /// (processId, secretKey, cancellationToken); the implementation opens a fresh transport,
    /// sends via <see cref="CancelRequest.SendAsync"/>, and disposes it. Null = no sender wired,
    /// cancel feature unavailable.
    public Func<int, int, CancellationToken, ValueTask>? CancelSender { get; set; }
}

sealed class PgClientProtocol : IDisposable, IAsyncDisposable
{
    readonly PgClientProtocolOptions _options;
    TransportConnection _connection = null!;
    IOutputWriter<byte> _pipeWriter = null!;
    PgProtocolDataWriter _protocolDataWriter = null!;
    PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> _pipeSegmentEnumerator = null!;
    PgDecoder _pgDecoder = null!;

    int _pipelineStalls;
    // Scoring inputs (the stall count, and the tick/age/throughput counters added for load scoring) are
    // a POOL concern: a standalone protocol has no pool consuming CompareTo, so it shouldn't pay to
    // maintain them. Set from onIdle in Initialize - non-null means an orchestrator (pool) drives us.
    bool _scoringEnabled;
    // Coarse monotonic clock for load scoring: incremented once per heartbeat tick (~HeartbeatInterval),
    // so flow-age and throughput are measured in ticks with no wall-clock reads on any hot path. Single
    // writer (the heartbeat); readers (flow-dispatch stamp, CompareTo) use a plain atomic int read.
    int _heartbeatTick;
    // Throughput (completions per tick), EWMA-smoothed in the heartbeat so a single quiet tick doesn't
    // tank the rate. _completionCount is the running total (bumped at retirement); the heartbeat diffs it
    // against _lastTickCompletions and folds the delta in. _currentFlowStartTick is the active (head)
    // flow's start tick - currentTick minus it is the head's age, the "stuck on a long query" signal.
    int _completionCount;
    int _lastTickCompletions;
    double _throughputPerTick;
    int _currentFlowStartTick;
    Heartbeat? _heartbeat;
    Action? _poolConnectionIdleSignal;

    // Backend identity from BackendKeyData (received during StartupFlow). Kept as two separate
    // fields rather than a struct because the consumers differ: process id is for diagnostics,
    // secret key is only ever payload for the side-channel CancelRequest. Both default to 0
    // pre-startup; the cancel arm site asserts non-zero process id before issuing.
    int _backendProcessId;
    int _backendSecretKey;

    // Two-token cancellation cascade:
    // StoppingToken = graceful drain signal. Body polls at handoff/coordination boundaries and
    // switches to drain mode. I/O keeps running so the wire reaches a clean state. Fired by
    // Shutdown on the graceful path.
    // AbortToken = forceful "wire dead" signal. I/O ops observe via construction-time wiring.
    // Body is passive: catches OCE, attributes via ex.CancellationToken == AbortToken, propagates.
    // Fired immediately by Shutdown on the forceful path, or after CompletionTimeout on the
    // graceful path's escalation.
    // Owns the close reason + the stopping/abort tokens as one object so "materialize the reason before
    // tripping any token" is structural. The canonical PgClientClosedException (materialized once on
    // Shutdown entry, wrapping the closeReason) is _close.Reason.
    readonly CloseSignal _close;
    Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator> _pipeline = null!;
    PgClientFlowSource _source;
    // Cached exclusive-scope machinery. The inner pipeline is a flyweight (reused via Initialize) and
    // the hosting flow is pooled with it - on the common ADO path an open connection is an exclusive
    // scope, so allocating per scope would tax every execute. One scope at a time per connection (an
    // exclusive scope is, by definition, not concurrent with another on the same connection).
    Control? _exclusiveInnerControl;
    Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>? _exclusiveInnerPipeline;
    Flows.ExclusiveAccessFlow? _exclusiveFlow;
    // The active scope's linked close signal (a child of _close). Per-connection flyweight, created once
    // with the exclusive machinery, reused across scopes (it stays untripped on the normal completion
    // path). Inner flows read it through the inner Control so a protocol stop/abort cascades into the
    // scope via the link, while a scope-only trip leaves the protocol token untouched.
    CloseSignal? _exclusiveScopeClose;
    readonly Lock _syncRoot = new();
    ProtocolStatus _status = ProtocolStatus.Created;
    // Track draining count so overlapping recovery starts/ends don't signal ready too early.
    // Any concurrent CompleteAsync (which also transitions to draining) is respected the same way.
    int _drainingCount;

    PgClientProtocol(PgClientProtocolOptions options)
    {
        _options = options;
        _close = CloseSignal.CreateRoot(options.TimeProvider);
        FlowControl = new Control(this, poolFacing: true);
    }

    public string CurrentSearchPath { get; set; } = "public";

    Control FlowControl { get; }
    CancellationToken AbortToken => _close.AbortToken;
    CancellationToken StoppingToken => _close.StoppingToken;
    public int PipelineDepth => _pipeline.Depth;
    ProtocolStatus Status => _status;

    // Source-side accessors. The PgClientFlowSource's pre-park hook reads these to decide whether
    // it must flush before the executor goes idle. Null-safe for the pre-Initialize window: a
    // protocol not yet wired to a transport has zero unflushed bytes by definition.
    internal long UnflushedBytes => _protocolDataWriter?.UnflushedBytes ?? 0;
    internal ValueTask FlushAsync(CancellationToken cancellationToken) => _protocolDataWriter.FlushAsync(cancellationToken);

    // Pool-unit accessors. PgConnection forwards its IPoolConnection<PgConnection> implementation
    // to these. Keeps the protocol package decoupled from Slon.Pools' typed context.
    internal bool IsIdle => PipelineDepth is 0;
    internal bool IsCompleted => Status is ProtocolStatus.Completed;
    // The cause that closed the protocol, or null if it completed cleanly. _closedException wraps the
    // shutdown's closeReason as its inner; the inner is the raw cause (a fault from FailProtocol / wire
    // death), null for a graceful CompleteAsync or a clean forceful DisposeAsync. A null check tells
    // clean-vs-faulted and the value tells why. No separate status is needed: a faulted connection still
    // reaches Completed, so IsCompleted already evicts it.
    internal Exception? CompletionException => _close.Reason?.InnerException;
    internal int CompareTo(PgClientProtocol? other)
    {
        // null instances are always better, they represent empty connection slots.
        if (other is null)
            return 1;

        var score = LoadScore();
        var otherScore = other.LoadScore();
        return score < otherScore ? -1 : score == otherScore ? 0 : 1;
    }

    // Estimated backlog in ticks (lower = better target). Little's Law core: expected wait = depth /
    // throughput (W = L/λ), so a deep-but-fast connection scores lower than a shallow-but-stuck one. The
    // throughput floor turns a zero-rate-with-work connection into a large W (correctly "stuck"); an idle
    // connection short-circuits to 0. Stalls (non-pipelined flows serialize the wire) and a head flow
    // running past the age threshold (stuck on a long op) add tick-equivalent penalties on top. All knobs
    // are deliberately rough - power-of-two selection only needs the comparison to be directionally right.
    double LoadScore()
    {
        var depth = PipelineDepth;
        if (depth == 0)
            return 0;

        const double RateFloor = 0.5, StallWeight = 2, AgePenalty = 5;
        const int AgeThresholdTicks = 3;

        var rate = Math.Max(_throughputPerTick, RateFloor);
        var score = depth / rate + _pipelineStalls * StallWeight;
        if (_heartbeatTick - _currentFlowStartTick > AgeThresholdTicks)
            score += AgePenalty;
        return score;
    }

    public static PgClientProtocol Create(PgClientProtocolOptions protocolOptions)
        => new(protocolOptions);

    void Initialize(TransportConnection connection, Action? onIdle)
    {
        _connection = connection;
        _pipeWriter = connection.Writer as IOutputWriter<byte> ?? new PipeStreamingWriter(connection.Writer);
        _protocolDataWriter = new(_pipeWriter, PgClientOptions.PreStartupEncoding, connection.WaitWritable, AbortToken, FlowControl);
        _pipeSegmentEnumerator = new(connection.Reader, new(), ownsReader: true);
        _pgDecoder = new(_pipeSegmentEnumerator, AbortToken, _options.ReadTimeout);

        // Scoring is a pool concern: only maintain its inputs when an orchestrator drives us.
        _scoringEnabled = onIdle is not null;

        // A non-null onIdle means an external orchestrator (pool, PgConnection) drives us,
        // including the heartbeat tick. When null, we run our own heartbeat so standalone
        // consumers get working flow activation timeouts.
        if (onIdle is null)
        {
            _heartbeat = new(_options.HeartbeatInterval, _options.TimeProvider);
            _heartbeat.Register(period => Heartbeat(period));
        }
        else
        {
            _poolConnectionIdleSignal = onIdle;
        }
    }

    public void Start(PgClientOptions options, TransportConnection connection, Action? onIdle = null, TimeSpan timeout = default)
    {
        try
        {
            if (connection.Reader is not StreamPipeReader || connection.Writer is not StreamPipeWriter)
                ThrowHelper.ThrowInvalidOperation("Transport does not support synchronous I/O.");

            Initialize(connection, onIdle);
            var flow = new StartupFlow(async: false, options, timeout == default ? options.ConnectionTimeout : timeout);
            var task = StartAsync(flow, flow.WaitForComplete());
            Debug.Assert(task.IsCompleted);
            task.AsTask().GetAwaiter().GetResult();
        }
        catch (Exception ex) when (Status is ProtocolStatus.Created)
        {
            ReleaseTransportOnStartFailure(connection, ex);
            throw;
        }
    }

    public async ValueTask StartAsync(PgClientOptions options, TransportConnection connection, Action? onIdle = null, CancellationToken cancellationToken = default)
    {
        try
        {
            Initialize(connection, onIdle);
            var flow = new StartupFlow(async: true, options, options.ConnectionTimeout);
            await StartAsync(flow, flow.WaitForComplete(cancellationToken), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (Status is ProtocolStatus.Created)
        {
            ReleaseTransportOnStartFailure(connection, ex);
            throw;
        }
    }

    // Startup failed before the protocol could take over teardown - the sync-capability check,
    // Initialize, pipeline construction, or queueing the startup flow, all before FailProtocol can run
    // (it needs the pipeline). Release the just-connected transport so the socket doesn't leak. The
    // callers' Status==Created filter skips failures the startup flow itself raised: those go through
    // FailProtocol -> Shutdown, which transitions past Created and owns the teardown.
    static void ReleaseTransportOnStartFailure(TransportConnection connection, Exception reason)
    {
        connection.Abort();
        connection.Writer.Complete(reason);
        connection.Reader.Complete(reason);
    }

    async ValueTask StartAsync(StartupFlow flow, ValueTask flowCompletion, CancellationToken cancellationToken = default)
    {
        _source = PgClientFlowSource.Create(this, _options.ExecutionScheduler);
        _pipeline = Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(new Policy(this, FlowControl), _source);
        FlowControl.BindPipeline(new PipelineFlowSlots<Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(_pipeline));
        if (!TryQueueFlow(flow, ProtocolStatus.Created))
            throw new InvalidOperationException("Could not enqueue starting flow, protocol is not in a valid state to start.");
        try
        {
            if (flowCompletion != default)
                await flowCompletion.ConfigureAwait(false);
            // Pull the BackendKeyData values once startup has settled. The flow's task chain is
            // the happens-before edge, so the values are visible here. After this single write the
            // fields are effectively readonly.
            _backendProcessId = flow.BackendProcessId;
            _backendSecretKey = flow.BackendSecretKey;
            SignalReady();
        }
        catch (Exception ex)
        {
            FailProtocol(ex);
            throw;
        }
    }

    void SignalReady()
    {
        lock (_syncRoot)
        {
            if (_drainingCount > 0)
                _drainingCount--;

            if (_drainingCount is 0 && _status is not ProtocolStatus.Completed)
            {
                _status = ProtocolStatus.Ready;
            }
        }
    }

    bool SignalDraining()
    {
        lock (_syncRoot)
        {
            if (_status is not ProtocolStatus.Completed)
                _status = ProtocolStatus.Draining;
            _drainingCount++;
            return _status is ProtocolStatus.Draining;
        }
    }

    void SignalCompleted()
    {
        lock (_syncRoot)
        {
            _status = ProtocolStatus.Completed;
        }
    }

    Enumerator GetFlows() => new(this);

    public T Queue<T>(T flow, CancellationToken cancellationToken = default) where T : PgClientFlow
    {
        if (!TryQueue(flow, cancellationToken: cancellationToken))
            ThrowHelper.ThrowInvalidOperation("Protocol is unavailable.");
        return flow;
    }

    public bool TryQueue(PgClientFlow flow, bool mustPipeline = false, CancellationToken cancellationToken = default)
    {
        // Bind the caller token before enqueue so the eager write reads it (published with the flow
        // by the enqueue). Only when cancelable - the common no-token submit pays no field write.
        if (cancellationToken.CanBeCanceled)
            flow.BindCallerToken(cancellationToken);

        if (mustPipeline)
        {
            if (!TryQueueFlow(flow, static protocol => protocol.PipelineDepth > 0, this))
                return false;
        }
        else if (!TryQueueFlow(flow, null, (object?)null))
            return false;

        // Bind
        var control = flow.GetExecutionControl(FlowControl);
        control.Bind(_options.FlowActivationTimeout);
        if (_scoringEnabled && !control.IsPipelined)
            Interlocked.Increment(ref _pipelineStalls);

        return true;
    }

    // Begin an exclusive-access scope: the user-driven sibling of the startup handshake. Builds (or
    // reuses) a nested pipeline (poolFacing:false, so no pool-unit signaling and no inner recovery)
    // and queues the cached ExclusiveAccessFlow on the outer pipeline. The returned flow is the scope
    // handle: await HandoffReady to acquire, Submit subflows, CompleteScopeAsync to release. One scope
    // at a time per connection.
    //
    // R2 (ADO ship-blocker, scoped out of this slice). An ADO connection IS an exclusive scope (the
    // protocol underneath is pooled and outlives the connection), so connection-dispose is scope
    // teardown, not protocol teardown. The scope CloseSignal can be tripped independently, but the
    // decoder/writer abort-translation sites key on the PROTOCOL token (captured at Initialize), so a
    // scope-only abort cannot break a subflow parked on a wire read - a consumer disposing a connection
    // over their own hung read would hang or be forced to kill the pooled protocol. Today no caller
    // trips a scope-only abort (protocol abort is the only read-breaker), so it is latent; it must be
    // closed before ADO ships. The fix is a scope-bound, pooled decoder/encoder shell carrying the
    // scope's token over a SHARED per-channel wire-state object (one ReadChannel + one WriteChannel,
    // since read and write are concurrently active on the one socket and want separate single-producer
    // state) - NOT dynamic scope-aware token branching in the shared decoder (it taxes the read/write
    // hot path AND re-points a shared instance per scope, reintroducing the wrong-tenure / stale-residue
    // translation hazard when an exclusive-flow tenure fails; the same factoring serves recovery's
    // per-channel wire takeover, so it is not single-use). Cost of the shell is just tokens + two channel
    // references, poolable to zero via this same flyweight.
    internal Flows.ExclusiveAccessFlow BeginExclusiveScope(bool async)
    {
        var innerSource = PgClientFlowSource.Create(this, _options.ExecutionScheduler);
        if (_exclusiveInnerPipeline is null)
        {
            var innerControl = _exclusiveInnerControl = new Control(this, poolFacing: false);
            // Scope signal: a child linked to the protocol's _close, so a protocol stop/abort cascades
            // into the scope's tokens through the link. Created once with the rest of the flyweight
            // machinery and reused across scopes (it stays untripped on the normal completion path, so
            // its linked CTSes stay pristine - the per-execute zero-alloc invariant).
            var scopeClose = _exclusiveScopeClose = CloseSignal.CreateLinked(_close, _options.TimeProvider);
            innerControl.BindScopeClose(scopeClose);
            var innerPipeline = _exclusiveInnerPipeline = Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(
                new Policy(this, innerControl), innerSource);
            innerControl.BindPipeline(new PipelineFlowSlots<Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(innerPipeline));
            _exclusiveFlow = new Flows.ExclusiveAccessFlow(innerControl, reason => _exclusiveInnerPipeline!.CompleteAsync(reason));
        }
        else
        {
            // Flyweight reuse. The cascade (OnStopping/OnAbort) is a second driver of the inner pipeline,
            // so a prior scope that hasn't fully completed is now reachable here (it was not when only the
            // user drove teardown). Refuse to re-init over a live prior scope - a lifecycle bug, not a
            // recoverable state.
            if (_exclusiveFlow is { IsCompleted: false })
                ThrowHelper.ThrowInvalidOperation("Cannot begin an exclusive scope while the prior scope is still completing.");
            Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(
                new Policy(this, _exclusiveInnerControl!), innerSource, _exclusiveInnerPipeline);
            _exclusiveFlow!.Reset();
        }
        _exclusiveFlow!.PrepareScope(async, innerSource, _options.FlowActivationTimeout);
        return Queue(_exclusiveFlow);
    }

    bool TryQueueFlow<TState>(PgClientFlow flow, Func<TState, bool>? predicate = null, TState state = default!)
        => TryQueueFlow(flow, ProtocolStatus.Ready, predicate, state);

    bool TryQueueFlow(PgClientFlow flow, ProtocolStatus requiredStatus) => TryQueueFlow<bool>(flow, requiredStatus);
    bool TryQueueFlow<TState>(PgClientFlow flow, ProtocolStatus requiredStatus, Func<TState, bool>? predicate = null, TState state = default!)
    {
        var isAsync = flow.IsAsyncForEnqueue;
        PgClientFlowSource.EnqueueResult enqueue = default;
        lock (_syncRoot)
        {
            if (_status != requiredStatus)
                return false;

            if (predicate?.Invoke(state) == false)
                return false;

            if (isAsync)
                enqueue = _source.Enqueue(flow);
            else
                // Depth increment under _syncRoot: DepthState.IncrementDepth is non-atomic single-
                // producer, so the sync path's increment must serialize with the async Enqueue's
                // (line above, same lock) or concurrent same-protocol callers race it (lost increment
                // -> negative depth -> "double completion" assert). The blocking handoff rendezvous
                // stays OUTSIDE the lock.
                _source.RegisterEnqueue();
        }
        if (isAsync)
        {
            enqueue.Execute(runContinuationsAsynchronously: true);
        }
        else
        {
            _source.EnqueueSyncWithHandoff(flow, invokeOnEnqueue: false);
        }
        return true;
    }

    /// Graceful shutdown. Gives flows up to CompletionTimeout to drain, then escalates to
    /// forceful by firing AbortToken. Returns when the pipeline has fully drained and all
    /// in-flight flows have completed. This is the only entry point that observes the drain;
    /// <see cref="DisposeAsync"/> / <see cref="Dispose"/> fire-and-forget.
    public ValueTask CompleteAsync(Exception? closeReason = null)
        => Shutdown(closeReason, forceful: false);

    /// Async forceful tear-down. Fires AbortToken immediately, fails activations for pipelined
    /// flows. The pipeline drain unwinds in the background; this method does NOT await it (the
    /// returned task is the entry-point handle, not the drain). Callers that need to observe
    /// drain completion should call <see cref="CompleteAsync"/> first.
    public ValueTask DisposeAsync()
    {
        try
        {
            FireAndForget(DisposeAsyncCore(closeReason: null));
            return ValueTask.CompletedTask;
        }
        catch (Exception ex)
        {
            return ValueTask.FromException(ex);
        }
    }

    /// Synchronous tear-down for callers that can't go async (the canonical case is
    /// <see cref="System.Data.Common.DbDataSource.Dispose"/>'s sync contract bubbling down to
    /// connection/protocol cleanup). Same fire-and-forget semantics as <see cref="DisposeAsync"/>:
    /// AbortToken fires immediately, pipeline drain happens in the background. Idempotent.
    public void Dispose()
        => FireAndForget(DisposeAsyncCore(closeReason: null));

    /// Internal emergency self-evict for the two framework-internal "we cannot continue" sites
    /// (startup catch, OnParameterStatus encoding failure). Fire-and-forget shape so it can run
    /// from the message-processing thread. Pool eviction picks up via the status flag.
    internal void FailProtocol(Exception? reason)
        => FireAndForget(DisposeAsyncCore(reason));

    /// Shared core for the Dispose paths: forceful Shutdown wrapped with resource disposal as a
    /// single fire-and-forget unit. The OBSERVABLE resources (the cancellation sources, whose tokens
    /// a caller may still read after a graceful <see cref="CompleteAsync"/>) stay alive until a
    /// Dispose path releases them here. The transport is not observable and is released by Shutdown
    /// at completion regardless of path. Idempotent via <c>_disposed</c>.
    bool _disposed;
    async ValueTask DisposeAsyncCore(Exception? closeReason)
    {
        if (Interlocked.Exchange(ref _disposed, true))
            return;
        try
        {
            await Shutdown(closeReason, forceful: true).ConfigureAwait(false);
        }
        finally
        {
            _heartbeat?.Dispose();
            // The transport was already released by Shutdown's completion (it's not observable state).
            // Here we release only the OBSERVABLE resources - the cancellation sources whose tokens a
            // caller may still read after a graceful CompleteAsync. That's what makes these Dispose-
            // gated rather than completion-gated. Disposing the abort CTS also drops the scheduled
            // CancelAfter(CompletionTimeout) timer without firing it. The scope signal (a linked child)
            // is disposed too, releasing its registration on _close; the scope signal must be disposed
            // BEFORE its parent _close so the link registration is gone first.
            _exclusiveScopeClose?.Dispose();
            _close.Dispose();
        }
    }

    /// async void (not a discard) so the background drain's exceptions are observed here rather than
    /// lost. Swallowed for now; route to a logging/unobserved-exception hook once one exists.
    static async void FireAndForget(ValueTask task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        {
            // TODO route to logging/unobserved-exception hook
        }
    }

    // Single-winner drain. The first Shutdown caller claims this and runs the body; concurrent and later
    // callers await the same completion. Two bodies would race the CTS lifecycle (a forceful
    // DisposeAsyncCore disposing _abortCts while a graceful body is still in its finally) and double-arm
    // the drain signal. SignalDraining can't gate this alone - it returns true throughout the Draining
    // window, so a graceful CompleteAsync and a forceful DisposeAsync both pass during the drain.
    TaskCompletionSource? _shutdownCompletion;

    ValueTask Shutdown(Exception? closeReason, bool forceful)
    {
        bool owner;
        TaskCompletionSource completion;
        lock (_syncRoot)
        {
            owner = _shutdownCompletion is null;
            if (owner)
            {
                // Materialize the canonical closed exception BEFORE any cancellation can fire (the forceful
                // escalation below, or the body's graceful cancel). A sync read/flow faulting on the
                // abortive close or AbortToken translates to it (PgDecoder reads _close.Reason); if it's
                // still null when the wire breaks, the raw ObjectDisposedException leaks instead. The owner
                // sets it once; losers read the same instance. Wraps closeReason as inner. CloseSignal also
                // re-materializes on every trip, so the invariant is structural, not just this ordering.
                _close.MaterializeReason(closeReason);
                _shutdownCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            completion = _shutdownCompletion!;
        }

        // Forceful escalation: idempotent, applied by ANY forceful caller - including one that loses the
        // drain claim to a concurrent graceful CompleteAsync - so a forceful Dispose can break a graceful
        // drain that would otherwise hang on a wedged peer. AbortToken + the abortive close (RST, never
        // blocks) fault parked sync I/O into the translation path; async I/O already unblocks off
        // AbortToken. Runs AFTER the claim so _closedException is set: a loser's lock acquisition
        // happens-after the owner's materialize. _abortCts is live: DisposeAsyncCore (the only forceful
        // caller, gated by _disposed so it runs once) fires this before its await and disposes the CTSes
        // only afterwards.
        if (forceful)
        {
            _close.Abort();
            _connection?.Abort();
        }

        return owner ? DriveShutdownAsync(forceful, completion) : new ValueTask(completion.Task);
    }

    // Owns the single drain body and publishes its outcome to every awaiting caller. Run outside
    // _syncRoot (the gate only claims under the lock) so the body's awaits / cancellation callbacks
    // never execute while the lock is held.
    async ValueTask DriveShutdownAsync(bool forceful, TaskCompletionSource completion)
    {
        try
        {
            await RunShutdownAsync(forceful).ConfigureAwait(false);
            completion.SetResult();
        }
        catch (Exception ex)
        {
            completion.SetException(ex);
            throw;
        }
    }

    async Task RunShutdownAsync(bool forceful)
    {
        SignalDraining();

        // Set by the Shutdown gate under _syncRoot before any cancellation fired.
        var closedException = _close.Reason!;

        // Graceful: bound the drain with CompletionTimeout (AbortToken escalates on expiry) and fire
        // StoppingToken so the body drains to a clean RFQ. Forceful already fired AbortToken + the
        // abortive Abort in the Shutdown gate, so it goes straight to the drain. Parked-flow propagation
        // is heartbeat-driven either way (ExecutionControl.OnHeartbeat fails the activation source within
        // HeartbeatInterval; forceful disposal accepts that latency too).
        if (!forceful)
        {
            _close.ArmAbortTimeout(_options.CompletionTimeout);
            await _close.StopAsync().ConfigureAwait(false);
        }

        // Coordinate the residual drain with the executor. The source fires DrainSignal once its pull
        // resolves completed (WaitForNextAsync delivers false), i.e. the executor stopped dequeuing.
        // Draining earlier would contend with the executor (concurrent SPSC dequeue = torn read).
        // RunContinuationsAsynchronously so the signal never resumes us inline under the wake lock.
        var executorStopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _source.SetDrainSignal(executorStopped);

        // AsTask once: consumed by both the drain gate below and the final await.
        var completeTask = _pipeline.CompleteAsync(closedException).AsTask();
        try
        {
            // Drain the inert head the moment the executor stops pulling, rather than make the
            // residual's parked consumers wait out the dispatched flows' drain. The executor stops by
            // resolving completed (executorStopped) or by exiting outright (completeTask); both mean
            // it is no longer dequeuing, so the drain is the sole consumer. Items still in the SPSC
            // queue were never dispatched, so faulting each unblocks its consumer's MoveNextAsync.
            await Task.WhenAny(executorStopped.Task, completeTask).ConfigureAwait(false);
            _source.DrainInertItems(flow => flow.GetExecutionControl(FlowControl).Complete(closedException));

            // Drain remaining (dispatched) items. closedException is delivered to each via
            // policy.CompleteItem.
            await completeTask.ConfigureAwait(false);
        }
        catch (PgClientClosedException)
        {
            // Expected forced-close outcome, not a fault to surface. When the wire is torn down mid-drain
            // (a forceful Abort, or a forceful sibling racing this graceful drain), the executor's pre-park
            // flush faults with the closed exception. ExecuteSource's sanctioned-shutdown catch swallows
            // only a token-matched OCE, but the writer translates the abort to PgClientClosedException, so
            // it escapes into completeTask. The pipeline still ran its own teardown (DrainOnCompletionAsync
            // + enumerator dispose) in its finally, so the residual is drained - only the exception bubbles
            // here. Swallow it so CompleteAsync/DisposeAsync complete normally. Catch the type, not a single
            // instance: a concurrent graceful+forceful pair each materialize their own closed exception and
            // either may win _closedException, so both are equally expected here.
        }
        finally
        {
            // Disarm the CTS scheduled by the graceful path.
            _close.DisarmAbortTimeout();
            SignalCompleted();
            // Release the transport once the drain has completed. Single-winner gating runs this body
            // exactly once, so the wire is closed exactly once at completion - NOT gated on Dispose. The
            // transport is not observable protocol state: unlike the cancellation sources (whose tokens a
            // caller may still read after a graceful CompleteAsync), a completed protocol's wire can't be
            // reached by anyone. Without this, a CompleteAsync never followed by Dispose leaked its socket
            // (max_connections).
            if (_connection is not null)
            {
                // Release the transport through the endpoints the protocol holds - the connection owns
                // no teardown. Error-complete the writer (DISCARDS any buffered write rather than
                // flushing it: graceful reached a clean RFQ, forceful already Abort'd the socket; the
                // error completion writes nothing and never starts the flush promise), then complete
                // the reader via the enumerator that owns it. Both dispose the shared stream
                // (idempotent), closing the socket.
                await _connection.Writer.CompleteAsync(closedException).ConfigureAwait(false);
                await _pipeSegmentEnumerator.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    internal ValueTask Heartbeat(TimeSpan period)
    {
        if (_scoringEnabled)
        {
            _heartbeatTick++;
            // Completions since last tick = throughput-per-tick; fold into the EWMA. Single writer (this
            // handler), so plain arithmetic; the score reads the smoothed value.
            var completedThisTick = _completionCount - _lastTickCompletions;
            _lastTickCompletions = _completionCount;
            const double Alpha = 0.3;
            _throughputPerTick = Alpha * completedThisTick + (1 - Alpha) * _throughputPerTick;
        }

        var control = FlowControl;
        // Wrong-tenure hazard if a timeout-armed flow is ever pooled (enforced against in
        // PgClientFlow.Reset). The fix's per-flow placement-stamp capture lands here.
        foreach (var flow in GetFlows())
        {
            try
            {
                flow.GetExecutionControl(control).OnHeartbeat(period);
            }
            catch (Exception)
            {
                // TODO log it
            }
        }
        return new();
    }

    public struct Enumerator
    {
        Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>.Enumerator _inner;

        internal Enumerator(PgClientProtocol protocol) => _inner = protocol._pipeline.GetEnumerator();

        public PgClientFlow Current => _inner.Current;
        public Enumerator GetEnumerator() => this;
        public bool MoveNext() => _inner.MoveNext();
    }

    readonly struct Policy : IPipelinePolicy<PgClientFlow>
    {
        readonly Control _control;
        readonly ValueTaskSourcePromise<PipelineItemResult> _promise;

        // Parameterized by Control (not the protocol) so the same policy drives both the protocol's
        // outer pipeline (FlowControl) and an exclusive flow's inner pipeline (its own Control reading
        // the inner pipeline's slots). The divergence lives in the Control, not here.
        public Policy(PgClientProtocol protocol, Control control)
        {
            _control = control;
            _promise = new();
            ActivationScheduler = protocol._options.ActivationScheduler;
        }

        PipelineScheduler? ActivationScheduler { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CompleteItem(PgClientFlow item, int remainingDepth, Exception? exception)
        {
            // OnCompleted (protocol bookkeeping: ActivatedFlow release, read-state recycle) must run
            // BEFORE Complete (user-visible completion): Complete fires the flow's completion action,
            // which may Reset() and re-enqueue the SAME instance. If that next tenure's Activate lands
            // before OnCompleted's depth-0 CAS, the comparand matches the new activation (ABA) and
            // severs a live binding. Ordering the release first closes this by causality. Recovery
            // items take the hardened path (capture + try/finally) out-of-line to keep this inlineable.
            if (item is ResyncRecoveryFlow { FailedFlow: { } failedFlow } recovery)
            {
                CompleteRecoveryItem(recovery, failedFlow, remainingDepth, exception);
                return;
            }

            _control.OnCompleted(item, remainingDepth);
            item.GetExecutionControl(_control).Complete(exception);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void CompleteRecoveryItem(ResyncRecoveryFlow resyncRecovery, PgClientFlow failedFlow, int remainingDepth, Exception? exception)
        {
            // Capture the binding BEFORE Complete fires the resyncRecovery's completion action:
            // completion is the reuse gate, and a Reset on reuse clears the binding (same
            // causality as the OnCompleted-before-Complete ordering below).
            var failureException = resyncRecovery.FailureException!;

            _control.OnCompleted(resyncRecovery, remainingDepth);
            try
            {
                resyncRecovery.GetExecutionControl(_control).Complete(exception);
            }
            finally
            {
                // A resyncRecovery's completion ends its supplanted flow's extended lifetime: the wire is
                // resynced (or dead) and nothing references the failed tenure. The supplanted flow
                // completes on EVERY exit (including the resyncRecovery's own fault), or its caller strands.
                // A resyncRecovery that also died attaches its fault behind the original failure as inner.
                // Single-level by construction: TryRecoverItemFailure refuses ResyncRecoveryFlow items.
                failedFlow.GetExecutionControl(_control).Complete(
                    exception is null ? failureException : new AggregateException(failureException, exception));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PipelineItemResult> ExecuteItemAsync(PgClientFlow item, CancellationToken cancellationToken)
        {
            item.GetExecutionControl(_control).Start();
            PromiseAsyncValueTaskMethodBuilder<PipelineItemResult>.Promise = _promise;
            try
            {
                return ExecuteCore(_control, item, cancellationToken);
            }
            finally
            {
                PromiseAsyncValueTaskMethodBuilder<PipelineItemResult>.Promise = null;
            }

            [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder<>))]
            static async ValueTask<PipelineItemResult> ExecuteCore(
                Control control, PgClientFlow item, CancellationToken cancellationToken)
            {
                // No cross-item pre-flush: buffered bytes are flushed by the writing flow's own
                // end-of-write flush once accumulation crosses the writer's threshold (which reads the
                // shared, cumulative UnflushedBytes), and any sub-threshold remainder is drained by the
                // source's arm gate / idle flush before the executor parks. A pre-flush here would
                // re-check the same cumulative bound the source and the flows already enforce.
                var tasks = await control.Execute(item).ConfigureAwait(false);
                return new PipelineItemResult(tasks.TrailingExecutionTask, tasks.PipelineTask);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ActivateHeadItem(PgClientFlow item, bool preferAsync = true)
        {
            // Bind the decoder synchronously now: the pipeline has just published item to the
            // ActivatedItem slot on this thread, so the bind reads the slot when it agrees with item.
            // Only the body wake is deferred below.
            _control.BindDecoder(item);

            // Inline-activate when the framework allows it (preferAsync=false) or the flow is sync:
            // sync flows park on a kernel wait-handle signal, bounded cost, safe under the advancer
            // latch. Async flows can attach arbitrary await continuations, so they go through TP.
            if (preferAsync && item.IsAsyncAtBind)
            {
                // The flow itself is the work item: an immutable (flow, control) pairing per queued
                // activation, zero-alloc. A single cached mutable work item was a lost-update box -
                // two activations in flight let the second Initialize overwrite the first, so both
                // ran the later flow and the earlier never activated. One pending activation per flow
                // tenure makes the per-flow field safe.
                item.PrepareActivationDispatch(_control);
                // SubmitDetached must not throw (the PipeScheduler.Schedule-style dispatch contract); a
                // caller handing us a fallible scheduler owns the resulting connection breakage. No guard.
                if (ActivationScheduler is { } scheduler)
                    scheduler.SubmitDetached(ActivationWorkItemAction, item, preferLocal: true);
                else
                    ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: true);
            }
            else
                _control.Activate(item);
        }

        static readonly Action<object?> ActivationWorkItemAction = static state => ((IThreadPoolWorkItem)state!).Execute();

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, PgClientFlow failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out PgClientFlow? recoveryItem)
        {
            // Recovery-on-recovery does not exist (the framework guarantees it: a committed
            // recovery's late fault travels as a marker exception and completes directly,
            // never consulted here).
            System.Diagnostics.Debug.Assert(failedItem is not ResyncRecoveryFlow,
                "Recovery item routed back into TryRecoverItemFailure - recovery-on-recovery must not exist.");

            // Pipeline is shutting down: skip recovery and let the framework propagate the failure.
            // Check BOTH the executor token AND ClosedException: a racing close materializes the reason
            // (and faults the in-flight write with it) before the executor's token cancellation is
            // observed here, so the token alone misses it. A deliberate close is teardown, not a
            // recoverable wire desync - recovering it drives a resync drain over a wire that's going
            // away, which races the close-torn buffer state into a negative-bufferedBytes assertion.
            if (cancellationToken.IsCancellationRequested || _control.ClosedException is not null)
            {
                recoveryItem = null;
                return false;
            }

            // Nested exclusive-scope pipeline: don't recover here. The failure propagates to the root
            // pipeline, which owns the wire and performs the takeover/resync.
            if (!_control.RecoversWireFailures)
            {
                recoveryItem = null;
                return false;
            }

            var failedItemControl = failedItem.GetExecutionControl(_control);

            // Substitute-write gate. Both must hold for recovery to inject a terminating Sync:
            //   - The failure kind hasn't closed the failed flow's write window (PipelineTaskWaiter
            //     is the closed-window case, identity already released from the writer).
            //   - The wire isn't already RFQ-terminated. If the last write was Query/Sync the server
            //     emits the inherited RFQs and recovery is pure read-drain; if it ended mid extended-
            //     query, recovery's Sync brings the wire back to a defined state.
            var canWriteSync = context.Kind is not PipelineItemFailureKind.PipelineTaskWaiter
                && !failedItemControl.LastMessageInducesRfq;

            // The outstanding phase task to sequence against, by failure kind:
            //   - PipelineTask: the failed flow's in-flight WRITE (trailing). Recovery's TrailingPhase
            //     awaits it before WriteSync so it doesn't collide on the single-producer writer.
            //   - TrailingExecutionTask: the failed flow's in-flight READ. It continues on its own
            //     control via the decoder permit; recovery's DrainPhase awaits it so it never resolves
            //     the read-turn out from under it. Without forwarding, the robbed read decodes the
            //     wrong message and its late fault re-enters nonexistent recovery-of-recovery.
            var outstandingIsRead = context.Kind is PipelineItemFailureKind.TrailingExecutionTask;
            var outstandingPhase =
                outstandingIsRead || (canWriteSync && context.Kind is PipelineItemFailureKind.PipelineTask)
                    ? context.OutstandingPhaseTask
                    : default;

            // The framework will NOT complete a supplanted item - that's this policy's job
            // (CompleteItem fires when the recovery completes). The failed item's lifetime extends as
            // far as the recovery, so its dispatch state, RFQ bookkeeping, and registrations release
            // before the instance can be reused.
            recoveryItem = ResyncRecoveryFlow.Create(
                _control, failedItem, context.Exception, outstandingPhase, outstandingIsRead, failedItemControl.RfqCount, canWriteSync);
            return true;
        }

    }

    internal sealed class Control(PgClientProtocol protocol, bool poolFacing) : IProtocolStatic<CommandFlow.ReadState>
    {
        // The pipeline whose slots this Control reads, bound right after that pipeline is created. The
        // outer (pool-facing) Control reads the protocol's own pipeline; an exclusive flow's inner
        // Control reads its inner pipeline - both through the same IPipelineSlots handle, so any
        // nesting depth composes. ExecutorFlow / ActivatedFlow are the single source of truth (the
        // single-pump invariant + in-order Activate-before-Complete): ExecutorFlow is the write-phase
        // identity (ThrowIfCannotWrite); ActivatedFlow is the read-channel current-reader handle
        // (PgDecoder routes messages to it).
        IFlowSlots _slots = null!;
        public void BindPipeline(IFlowSlots slots) => _slots = slots;

        // The scope's linked close signal, set once for an exclusive-scope inner Control; null for the
        // pool-facing FlowControl (which reads the protocol's _close directly). Inner flows read the
        // scope signal's tokens so a protocol stop/abort cascades through the link, while a scope-only
        // trip stays off the protocol token.
        CloseSignal? _scopeClose;
        public void BindScopeClose(CloseSignal scopeClose) => _scopeClose = scopeClose;

        // Only the root (pool-facing) control owns wire recovery. A nested exclusive-scope pipeline
        // lets an inner subflow's failure propagate to the root, which performs the wire takeover /
        // resync - an inner recovery would fight the root for the single writer. (Exclusive = no scope
        // recovery, yes wire recovery, mediated by the root.)
        public bool RecoversWireFailures => poolFacing;

        public PgClientFlow? ExecutorFlow => _slots.ExecutingItem;
        public PgClientFlow? ActivatedFlow => _slots.ActivatedItem;

        public PgProtocolDataWriter Writer => protocol._protocolDataWriter;

        // Backend identity from BackendKeyData (pulled from StartupFlow after startup completes).
        // Process id is the diagnostic-safe value (logs, "which backend"); secret key is restricted
        // to the CancelRequest payload. 0 = not yet received.
        public int BackendProcessId => protocol._backendProcessId;
        public int BackendSecretKey => protocol._backendSecretKey;

        // Tokens come from the scope signal for an inner Control (so the scope cascade reaches inner
        // flows), else the protocol's _close. Both are stable across a flow's tenure. Surfaced through
        // Control so ExecutionControl and the body read them without per-flow storage.
        CloseSignal Close => _scopeClose ?? protocol._close;
        public CancellationToken AbortToken => Close.AbortToken;
        public CancellationToken StoppingToken => Close.StoppingToken;

        /// The canonical PgClientClosedException once Shutdown has entered, null otherwise. Single
        /// instance per lifetime, materialized before any cancellation fires so an observer waking on
        /// AbortToken/StoppingToken sees a non-null value. For an inner Control a scope-only trip resolves
        /// the scope reason; a protocol trip chains through the link to the protocol reason.
        public PgClientClosedException? ClosedException => Close.Reason;

        /// Throws PgClientClosedException if closed, no-op otherwise. For the OCE catch path inside
        /// existing async I/O frames, converting our abort-token OCE to the typed exception without an
        /// extra wrapping frame.
        public void ThrowIfClosed()
        {
            if (Close.Reason is { } ex)
                throw ex;
        }

        public void OnParameterStatus(BackendMessage message)
        {
            message.DebugEnsureExpected(PgTypes.BackendType.ParameterStatus);
            message.DebugEnsureBuffered();

            var reader = message.BodyReader;
            _ = reader.TryPeek(out var nameStart);
            switch ((char)nameStart)
            {
            case 'c':
                // If Postgres supported ASCII incompatible encodings there would be a catch-22
                // reporting the new encoding value encoded in the new encoding.
                // As it doesn't support e.g. utf16 we can always rely on the ascii bytes,
                // which is enough to transmit encoding names.
                if (reader.IsNext("client_encoding\0"u8, advancePast: true))
                {
                    _ = reader.TryReadTo(out ReadOnlySequence<byte> value, [(byte)'\0']);
                    var newEncoding = protocol._protocolDataWriter.ClientEncoding.GetString(value);
                    // Map from PG names to ICU/IANA names https://www.iana.org/assignments/character-sets/character-sets.xhtml.
                    // https://github.com/postgres/postgres/blob/713d9a847e6409a2a722aed90975eef6d75dc701/src/common/encnames.c#L414
                    // Server reports a new client_encoding (typically from SET CLIENT_ENCODING). Map the PG name
                    // to a .NET / IANA name (per src/common/encnames.c) and refresh.
                    // SQL_ASCII is special, it explicitly means "no encoding conversion on the wire," so the .NET
                    // side keeps whatever DefaultClientEncoding the caller chose to interpret the raw bytes.
                    // Other PG names without a .NET equivalent (MULE_INTERNAL, EUC_JIS_2004, LATIN10, WIN874) are
                    // real encodings .NET can't decode, let Encoding.GetEncoding throw and break the connection.
                    newEncoding = newEncoding switch
                    {
                        "SQL_ASCII" => newEncoding,
                        "EUC_JP" => "EUC-JP",
                        "EUC_CN" => "EUC-CN",
                        "EUC_KR" => "EUC-KR",
                        "EUC_TW" => "EUC-TW",
                        "EUC_JIS_2004" => newEncoding,
                        "UTF8" => "UTF-8",
                        "MULE_INTERNAL" => newEncoding,
                        "LATIN1" => "ISO-8859-1",
                        "LATIN2" => "ISO-8859-2",
                        "LATIN3" => "ISO-8859-3",
                        "LATIN4" => "ISO-8859-4",
                        "LATIN5" => "ISO-8859-9",
                        "LATIN6" => "ISO-8859-10",
                        "LATIN7" => "ISO-8859-13",
                        "LATIN8" => "ISO-8859-14",
                        "LATIN9" => "ISO-8859-15",
                        "LATIN10" => newEncoding,
                        "WIN1256" => "CP1256",
                        "WIN1258" => "CP1258",
                        "WIN866" => "CP866",
                        "WIN874" => newEncoding,
                        "KOI8R" => "KOI8-R",
                        "WIN1251" => "CP1251",
                        "WIN1252" => "CP1252",
                        "ISO_8859_5" => "ISO-8859-5",
                        "ISO_8859_6" => "ISO-8859-6",
                        "ISO_8859_7" => "ISO-8859-7",
                        "ISO_8859_8" => "ISO-8859-8",
                        "WIN1250" => "CP1250",
                        "WIN1253" => "CP1253",
                        "WIN1254" => "CP1254",
                        "WIN1255" => "CP1255",
                        "WIN1257" => "CP1257",
                        "KOI8U" => "KOI8-U",
                        _ => newEncoding
                    };

                    try
                    {
                        if (newEncoding == "SQL_ASCII")
                            protocol._protocolDataWriter.ClientEncoding = protocol._options.DefaultClientEncoding;

                        protocol._protocolDataWriter.ClientEncoding = Encoding.GetEncoding(newEncoding);
                    }
                    catch (ArgumentException ex)
                    {
                        protocol.FailProtocol(ex);
                        throw;
                    }
                }
                break;
            case 's':
                if (reader.IsNext("search_path\0"u8, advancePast: true))
                {
                    _ = reader.TryReadTo(out ReadOnlySequence<byte> value, [(byte)'\0']);
                    var newSearchPath = protocol._protocolDataWriter.ClientEncoding.GetString(value);
                    protocol.CurrentSearchPath = newSearchPath;
                }
                break;
            default:
                // TODO log ignored parameter status.
                break;
            }
        }

        // Allows the protocol to do any bookkeeping of transaction state and any cleanup.
        public void OnFlowRfq(BackendMessage message)
        {
            var rfq = ReadyForQueryMessage.Create(message);
        }

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        internal ValueTask<FlowTasks> Execute(PgClientFlow flow)
        {
            return flow.GetExecutionControl(this).ExecuteAuto();
        }

        // Bind the shared decoder to the flow being activated. Runs synchronously inside the policy's
        // ActivateHeadItem, where the pipeline has just published this flow to the ActivatedItem slot
        // on the same thread, so Initialize reads the slot when it provably agrees with the flow.
        // Deferring the bind into the TP wake let a dispatch outlive the flow's retirement and bind
        // against a depth-0-cleared slot.
        internal void BindDecoder(PgClientFlow flow)
        {
            // Stamp the head flow's start tick at activation (this is the active reader). currentTick
            // minus it gives the head's running age for the load score.
            if (protocol._scoringEnabled)
                protocol._currentFlowStartTick = protocol._heartbeatTick;
            protocol._pgDecoder.Initialize(this);
        }

        // Wake the flow's body with the bound decoder. Resumes the body inline, so async flows run this
        // off the executor via the TP dispatch. Safe to lag the flow's retirement: TrySetResult no-ops
        // on a flow the abort already faulted.
        internal void Activate(PgClientFlow flow)
            => flow.GetExecutionControl(this).Activate(protocol._pgDecoder);

        // Self-evict route for the flow layer's completion-callback seam (see ExecutionControl.Complete).
        internal void FailProtocol(Exception? reason) => protocol.FailProtocol(reason);

        internal void OnCompleted(PgClientFlow flow, int remainingDepth)
        {
            // Scoring inputs maintained at retirement (the universal completion point, fires for every
            // flow including ones faulted before bind). Throughput: every retirement counts. Stalls: a
            // non-pipelined flow held the wire serialized from queue until its RFQ here (not just until
            // bind), so decrement here - measures the serialization window, never orphans an increment.
            // Pool-facing only: an inner (exclusive-flow) subflow's retirement isn't a pool-unit event,
            // so it neither feeds the load score nor signals pool idle below.
            if (poolFacing && protocol._scoringEnabled)
            {
                Interlocked.Increment(ref protocol._completionCount);
                if (!flow.GetExecutionControl(this).IsPipelined)
                    Interlocked.Decrement(ref protocol._pipelineStalls);
            }

            // At pipeline park (remainingDepth == 0) release the rooted read-state buffers and signal
            // the pool's idle hook. The framework manages the ActivatedItem slot (cleared right after
            // this), and the in-order Activate-before-Complete invariant means no successor Activate
            // races this completion.
            if (remainingDepth is 0)
            {
                _commandFlowReadState = new();
                // The pool idle hook runs in the advancer/retirement work-item context: a raw throw
                // would crash that thread unobserved. But don't just swallow either - if the hook that
                // reclaims this connection is throwing, the integration is broken and the pipeline won't
                // get cleaned up naturally, so tear down via FailProtocol (fire-and-forget self-evict;
                // the pool picks it up through the status flag).
                if (poolFacing)
                {
                    try { protocol._poolConnectionIdleSignal?.Invoke(); }
                    catch (Exception ex) { /* TODO log */ protocol.FailProtocol(ex); }
                }
            }
        }

        CommandFlow.ReadState _commandFlowReadState = new();
        ref readonly CommandFlow.ReadState IProtocolStatic<CommandFlow.ReadState>.Value
            => ref _commandFlowReadState;
    }
}

