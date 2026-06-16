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
    IOutputWriter<byte> _pipeWriter = null!;
    PgProtocolDataWriter _protocolDataWriter = null!;
    PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> _pipeSegmentEnumerator = null!;
    PgDecoder _pgDecoder = null!;

    int _pipelineStalls;
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
    readonly CancellationTokenSource _abortCts;
    readonly CancellationToken _abortToken;
    readonly CancellationTokenSource _stoppingCts;
    readonly CancellationToken _stoppingToken;
    // Canonical closed exception, materialized once on Shutdown entry. Cached so all observers
    // see the same instance with the same closeReason wrapped.
    PgClientClosedException? _closedException;
    Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator> _pipeline = null!;
    PgClientFlowSource _source;
    readonly Lock _syncRoot = new();
    ProtocolStatus _status = ProtocolStatus.Created;
    // Track draining count so overlapping recovery starts/ends don't signal ready too early.
    // Any concurrent CompleteAsync (which also transitions to draining) is respected the same way.
    int _drainingCount;

    PgClientProtocol(PgClientProtocolOptions options)
    {
        _options = options;
        _abortCts = new(Timeout.InfiniteTimeSpan, options.TimeProvider);
        _abortToken = _abortCts.Token;
        _stoppingCts = new();
        _stoppingToken = _stoppingCts.Token;
        FlowControl = new Control(this);
    }

    public string CurrentSearchPath { get; set; } = "public";

    Control FlowControl { get; }
    CancellationToken AbortToken => _abortToken;
    CancellationToken StoppingToken => _stoppingToken;
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
    internal int CompareTo(PgClientProtocol? other)
    {
        // null instances are always better, they represent empty connection slots.
        if (other is null)
            return 1;

        // Arbitrary factor for stalls :)
        var score = PipelineDepth + (_pipelineStalls * 4);
        var otherScore = other.PipelineDepth + (other._pipelineStalls * 4);

        return score < otherScore ? -1 : score == otherScore ? 0 : 1;
    }

    public static PgClientProtocol Create(PgClientProtocolOptions protocolOptions)
        => new(protocolOptions);

    void Initialize(TransportConnection connection, Action? onIdle)
    {
        _pipeWriter = connection.Writer as IOutputWriter<byte> ?? new PipeStreamingWriter(connection.Writer);
        _protocolDataWriter = new(_pipeWriter, PgClientOptions.PreStartupEncoding, connection.WaitWritable, AbortToken, FlowControl);
        _pipeSegmentEnumerator = new(connection.Reader, new(), ownsReader: true);
        _pgDecoder = new(_pipeSegmentEnumerator, AbortToken, _options.ReadTimeout);

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
        if (connection.Reader is not StreamPipeReader || connection.Writer is not StreamPipeWriter)
            ThrowHelper.ThrowInvalidOperation("Transport does not support synchronous I/O.");

        Initialize(connection, onIdle);
        var flow = new StartupFlow(async: false, options, timeout == default ? options.ConnectionTimeout : timeout);
        var task = StartAsync(flow, flow.WaitForComplete());
        Debug.Assert(task.IsCompleted);
        task.AsTask().GetAwaiter().GetResult();
    }

    public async ValueTask StartAsync(PgClientOptions options, TransportConnection connection, Action? onIdle = null, CancellationToken cancellationToken = default)
    {
        Initialize(connection, onIdle);
        var flow = new StartupFlow(async: true, options, options.ConnectionTimeout);
        await StartAsync(flow, flow.WaitForComplete(cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    async ValueTask StartAsync(StartupFlow flow, ValueTask flowCompletion, CancellationToken cancellationToken = default)
    {
        _source = PgClientFlowSource.Create(this, _options.ExecutionScheduler);
        _pipeline = Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(new Policy(this), _source);
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

    public T Queue<T>(T flow) where T : PgClientFlow
    {
        if (!TryQueue(flow))
            ThrowHelper.ThrowInvalidOperation("Protocol is unavailable.");
        return flow;
    }

    public bool TryQueue(PgClientFlow flow, bool mustPipeline = false)
    {
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
        if (!control.IsPipelined)
            Interlocked.Increment(ref _pipelineStalls);

        return true;
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
        }
        if (isAsync)
        {
            enqueue.Execute(runContinuationsAsynchronously: true);
        }
        else
        {
            _source.EnqueueSyncWithHandoff(flow);
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
    /// single fire-and-forget unit. Resources stay alive across a graceful
    /// <see cref="CompleteAsync"/> so the caller can still observe state via the cached tokens;
    /// only Dispose paths release them. Idempotent via <c>_disposed</c>.
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
            // Disposing _abortCts releases the scheduled CancelAfter(CompletionTimeout) timer without
            // firing it. By here the protocol is Completed, so nothing registers against the dead source.
            _abortCts.Dispose();
            _stoppingCts.Dispose();
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

    async ValueTask Shutdown(Exception? closeReason, bool forceful)
    {
        // First-writer-wins. SignalDraining returns false when status is already Completed.
        if (!SignalDraining())
            return;

        // Materialize the canonical closed exception BEFORE firing any cancellation so an observer
        // waking on AbortToken / StoppingToken sees the same instance. It wraps closeReason as inner.
        var closedException = new PgClientClosedException(closeReason);
        _closedException = closedException;

        // Parked-flow propagation is heartbeat-driven for both paths: ExecutionControl.OnHeartbeat
        // observes AbortToken and fails the activation source within HeartbeatInterval. Forceful
        // disposal accepts that latency too.
        if (forceful)
        {
            // Sync Cancel: AbortToken is internal-only, callbacks are framework-authored and audited.
            _abortCts.Cancel();
        }
        else
        {
            _abortCts.CancelAfter(_options.CompletionTimeout);
            await _stoppingCts.CancelAsync().ConfigureAwait(false);
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
        finally
        {
            // Disarm the CTS scheduled by the graceful path.
            _abortCts.CancelAfter(Timeout.InfiniteTimeSpan);
            SignalCompleted();
        }
    }

    internal ValueTask Heartbeat(TimeSpan period)
    {
        var control = FlowControl;
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
        readonly PgClientProtocol _protocol;
        readonly ValueTaskSourcePromise<PipelineItemResult> _promise;

        public Policy(PgClientProtocol protocol)
        {
            _protocol = protocol;
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
            if (item is RecoveryDrainFlow { FailedFlow: { } failedFlow } recovery)
            {
                CompleteRecoveryItem(recovery, failedFlow, remainingDepth, exception);
                return;
            }

            _protocol.FlowControl.OnCompleted(item, remainingDepth);
            item.GetExecutionControl(_protocol.FlowControl).Complete(exception);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void CompleteRecoveryItem(RecoveryDrainFlow recovery, PgClientFlow failedFlow, int remainingDepth, Exception? exception)
        {
            // Capture the binding BEFORE Complete fires the recovery's completion action:
            // completion is the reuse gate, and a Reset on reuse clears the binding (same
            // causality as the OnCompleted-before-Complete ordering below).
            var failureException = recovery.FailureException!;

            _protocol.FlowControl.OnCompleted(recovery, remainingDepth);
            try
            {
                recovery.GetExecutionControl(_protocol.FlowControl).Complete(exception);
            }
            finally
            {
                // A recovery's completion ends its supplanted flow's extended lifetime: the wire is
                // resynced (or dead) and nothing references the failed tenure. The supplanted flow
                // completes on EVERY exit (including the recovery's own fault), or its caller strands.
                // A recovery that also died attaches its fault behind the original failure as inner.
                // Single-level by construction: TryRecoverItemFailure refuses RecoveryDrainFlow items.
                failedFlow.GetExecutionControl(_protocol.FlowControl).Complete(
                    exception is null ? failureException : new AggregateException(failureException, exception));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PipelineItemResult> ExecuteItemAsync(PgClientFlow item, CancellationToken cancellationToken)
        {
            item.GetExecutionControl(_protocol.FlowControl).Start();
            PromiseAsyncValueTaskMethodBuilder<PipelineItemResult>.Promise = _promise;
            try
            {
                return ExecuteCore(_protocol, item, cancellationToken);
            }
            finally
            {
                PromiseAsyncValueTaskMethodBuilder<PipelineItemResult>.Promise = null;
            }

            [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder<>))]
            static async ValueTask<PipelineItemResult> ExecuteCore(
                PgClientProtocol protocol, PgClientFlow item, CancellationToken cancellationToken)
            {
                // Pre-flush of cross-item buffered bytes, lifted from Control.Execute: it's policy-level
                // writer hygiene between items, not part of any flow's ExecuteAuto. A recovery whose
                // failed flow's trailing is still in-flight awaits it inside its own ExecuteAuto. The
                // pre-flush race with that trailing is intentionally unhandled - the single-producer
                // writer fail-fasts on overlapping flush, surfacing a real bug rather than hiding it.
                var writer = protocol._protocolDataWriter;
                if (writer.UnflushedBytes > 1000)
                    await writer.FlushAsync(protocol._abortToken).ConfigureAwait(false);
                var tasks = await protocol.FlowControl.Execute(item).ConfigureAwait(false);
                return new PipelineItemResult(tasks.TrailingExecutionTask, tasks.PipelineTask);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ActivateHeadItem(PgClientFlow item, bool preferAsync = true)
        {
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
                item.PrepareActivationDispatch(_protocol.FlowControl);
                if (ActivationScheduler is { } scheduler)
                    scheduler.SubmitDetached(ActivationWorkItemAction, item, preferLocal: true);
                else
                    ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: true);
            }
            else
                _protocol.FlowControl.Activate(item);
        }

        static readonly Action<object?> ActivationWorkItemAction = static state => ((IThreadPoolWorkItem)state!).Execute();

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, PgClientFlow failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out PgClientFlow? recoveryItem)
        {
            // Recovery-on-recovery does not exist (the framework guarantees it: a committed
            // recovery's late fault travels as a marker exception and completes directly,
            // never consulted here).
            System.Diagnostics.Debug.Assert(failedItem is not RecoveryDrainFlow,
                "Recovery item routed back into TryRecoverItemFailure - recovery-on-recovery must not exist.");

            // Pipeline is shutting down: skip recovery and let the framework propagate the failure.
            if (cancellationToken.IsCancellationRequested)
            {
                recoveryItem = null;
                return false;
            }

            var failedItemControl = failedItem.GetExecutionControl(_protocol.FlowControl);

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
            recoveryItem = RecoveryDrainFlow.Create(
                _protocol.FlowControl, failedItem, context.Exception, outstandingPhase, outstandingIsRead, failedItemControl.RfqCount, canWriteSync);
            return true;
        }

    }

    internal sealed class Control(PgClientProtocol protocol) : IProtocolStatic<CommandFlow.ReadState>
    {
        // ExecutorFlow / ActivatedFlow source directly from the pipeline's slots - the single-pump
        // invariant + in-order Activate-before-Complete make these the single source of truth.
        // ExecutorFlow is the write-phase identity (ThrowIfCannotWrite); ActivatedFlow is the
        // read-channel current-reader handle (PgDecoder routes messages to it).
        public PgClientFlow? ExecutorFlow => protocol._pipeline.ExecutingItem;
        public PgClientFlow? ActivatedFlow => protocol._pipeline.ActivatedItem;

        public PgProtocolDataWriter Writer => protocol._protocolDataWriter;

        // Backend identity from BackendKeyData (pulled from StartupFlow after startup completes).
        // Process id is the diagnostic-safe value (logs, "which backend"); secret key is restricted
        // to the CancelRequest payload. 0 = not yet received.
        public int BackendProcessId => protocol._backendProcessId;
        public int BackendSecretKey => protocol._backendSecretKey;

        // Tokens live on the protocol (stable across a flow's tenure), surfaced through Control so
        // ExecutionControl and the body read them without per-flow storage.
        public CancellationToken AbortToken => protocol._abortToken;
        public CancellationToken StoppingToken => protocol._stoppingToken;

        /// The canonical PgClientClosedException for this protocol once Shutdown has entered, null
        /// otherwise. Single instance per lifetime, materialized before any cancellation fires so an
        /// observer waking on AbortToken/StoppingToken sees a non-null value.
        public PgClientClosedException? ClosedException => protocol._closedException;

        /// Throws PgClientClosedException if the protocol is closed, no-op otherwise. For the OCE
        /// catch path inside existing async I/O frames, converting our abort-token OCE to the typed
        /// exception without an extra wrapping frame.
        public void ThrowIfClosed()
        {
            if (protocol._closedException is { } ex)
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

        internal void Activate(PgClientFlow flow)
        {
            var control = flow.GetExecutionControl(this);
            if (!control.IsPipelined)
                Interlocked.Decrement(ref protocol._pipelineStalls);
            var decoder = protocol._pgDecoder;
            decoder.Initialize(this);
            control.Activate(decoder);
        }

        internal void OnCompleted(PgClientFlow flow, int remainingDepth)
        {
            // At pipeline park (remainingDepth == 0) release the rooted read-state buffers and signal
            // the pool's idle hook. The framework manages the ActivatedItem slot (cleared right after
            // this), and the in-order Activate-before-Complete invariant means no successor Activate
            // races this completion.
            if (remainingDepth is 0)
            {
                _commandFlowReadState = new();
                protocol._poolConnectionIdleSignal?.Invoke();
            }
        }

        CommandFlow.ReadState _commandFlowReadState = new();
        ref readonly CommandFlow.ReadState IProtocolStatic<CommandFlow.ReadState>.Value
            => ref _commandFlowReadState;
    }
}

