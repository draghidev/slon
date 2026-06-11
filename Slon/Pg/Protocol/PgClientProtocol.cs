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

    // Framework state, formerly inherited from Protocol<TFlow>.
    // Two-token cancellation cascade:
    // StoppingToken = graceful drain signal. Body polls at handoff/coordination boundaries (per-
    // CommandResult for CommandFlow) and switches to drain mode. I/O keeps running so the wire
    // reaches a clean state. Fired by Shutdown on the graceful path (CompleteAsync entry).
    // AbortToken = forceful "wire dead" signal. I/O ops observe via construction-time wiring
    // (PgDecoder, PgProtocolDataWriter). Body is passive: catches OCE, attributes via
    // ex.CancellationToken == AbortToken, propagates. Fired immediately by Shutdown on the
    // forceful path, or after CompletionTimeout on the graceful path's escalation.
    readonly CancellationTokenSource _abortCts;
    readonly CancellationToken _abortToken;
    readonly CancellationTokenSource _stoppingCts;
    readonly CancellationToken _stoppingToken;
    // Canonical closed exception, materialized once on Shutdown entry. Cached so observers
    // (heartbeat checking parked-flow propagation, future I/O surfaces converting OCE to the
    // typed closed exception) all see the same instance with the same closeReason wrapped.
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
    // it must flush before the executor goes idle.
    internal long UnflushedBytes => _protocolDataWriter.UnflushedBytes;
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

        // onIdle being non-null is the signal that an external orchestrator (pool, PgConnection)
        // is driving us, including the heartbeat tick. When null, we run our own heartbeat so
        // standalone consumers of the protocol package get working flow activation timeouts out
        // of the box.
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

    async ValueTask StartAsync(PgClientFlow? flow, ValueTask flowCompletion, CancellationToken cancellationToken = default)
    {
        _source = PgClientFlowSource.Create(this, _options.ExecutionScheduler);
        _pipeline = Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(new Policy(this), _source);
        if (flow is not null && !TryQueueFlow(flow, ProtocolStatus.Created))
            throw new InvalidOperationException("Could not enqueue starting flow, protocol is not in a valid state to start.");
        try
        {
            if (flowCompletion != default)
                await flowCompletion.ConfigureAwait(false);
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
    /// forceful by firing AbortToken. Awaitable: returns when the pipeline has fully drained
    /// and all in-flight flows have completed. This is the only entry point that observes the
    /// drain - <see cref="DisposeAsync"/> / <see cref="Dispose"/> fire-and-forget. If you need
    /// to observe drain completion, call this first, then Dispose afterwards.
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
            // Disposing _abortCts releases the scheduled CancelAfter(CompletionTimeout) timer
            // without firing it. Downstream linked CTSes (PgDecoder, PgProtocolDataWriter)
            // observe a dead source - state already captured stays valid, new registrations
            // against the cached token would throw, but by here the protocol is Completed and
            // nothing should be registering.
            _abortCts.Dispose();
            _stoppingCts.Dispose();
        }
    }

    /// Discarding a ValueTask silently swallows any exception thrown by the background drain.
    /// async void with a top-level try/catch ensures exceptions are observed (caught here) rather
    /// than lost to a bare discard. For now they're swallowed at this site too - once we have a
    /// logging path or an unobserved-exception hook plumbed through, this is where they get
    /// routed.
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

        // Materialize the canonical closed exception BEFORE firing any cancellation so any
        // observer that wakes on AbortToken / StoppingToken and reaches for Control.ClosedException
        // sees the same instance. PgClientClosedException wraps closeReason as InnerException so
        // callers can introspect the original cause.
        var closedException = new PgClientClosedException(closeReason);
        _closedException = closedException;

        // Anything below the first await is background work from a fire-and-forget caller's
        // perspective. Parked-flow propagation is heartbeat-driven for both paths:
        // ExecutionControl.OnHeartbeat observes AbortToken and fails the activation source within
        // HeartbeatInterval. Forceful disposal accepts that latency too - "abort is forceful, if
        // flows are observing StoppingToken activation comes soon for pending ones anyway."
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

        try
        {
            // Drain remaining items. closedException is delivered to each via policy.CompleteItem.
            await _pipeline.CompleteAsync(closedException).ConfigureAwait(false);
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
            // OnCompleted (protocol bookkeeping: ActivatedFlow release, read-state recycle)
            // must run BEFORE Complete (user-visible completion): Complete fires the flow's
            // completion action, which may Reset() and re-enqueue the SAME instance
            // (MaintenanceFlow does, pooled CommandFlows will). If that next tenure's
            // Activate lands before OnCompleted's depth-0 CAS, the CAS comparand matches the
            // new activation of the same reference (ABA) and severs a live binding - the
            // decoder's next read aborts on a null ActivatedFlow. Ordering the release first
            // closes this by causality: same-instance reuse cannot begin until after it.
            // Reduction: ActivatedFlowReductionTests.TenureReuse_Cas_{CompleteBeforeRelease_Hazardous,ReleaseBeforeComplete}.
            // Recovery items take the hardened path (capture + try/finally); keeping the EH out
            // of this method preserves its inlineability for the common case.
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
                // A recovery flow's completion ends its supplanted item's extended lifetime (see
                // RecoveryDrainFlow.FailedFlow): the wire is resynced (or definitively dead) and
                // no protocol machinery references the failed tenure anymore. The supplanted flow
                // completes on EVERY exit - including the recovery's own fault (e.g. I/O mid-
                // drain) and a throwing completion action - or its caller strands forever. A
                // flow's completion exception carries every failure that terminated its position:
                // usually just its own (the original failure), but a recovery that also died adds
                // a second terminal fact the caller has a claim on (the wire was never resynced,
                // session state is gone), attached behind the original as the primary.
                // Single-level by construction: TryRecoverItemFailure refuses RecoveryDrainFlow
                // items, so a bound failed flow is never itself a recovery with a binding.
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
                return ExecuteCore(_protocol.FlowControl, item, cancellationToken);
            }
            finally
            {
                PromiseAsyncValueTaskMethodBuilder<PipelineItemResult>.Promise = null;
            }

            [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder<>))]
            static async ValueTask<PipelineItemResult> ExecuteCore(
                Control flowControl, PgClientFlow item, CancellationToken cancellationToken)
            {
                var tasks = await flowControl.Execute(item, cancellationToken).ConfigureAwait(false);
                return new PipelineItemResult(tasks.TrailingExecutionTask, tasks.PipelineTask);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ActivateHeadItem(PgClientFlow item, bool preferAsync = true)
        {
            // Inline-activate when the framework allows it (preferAsync=false) OR when the flow
            // is sync. Sync flows park their caller via Task.AsTask().GetAwaiter().GetResult(),
            // so the activation continuation is just a kernel wait-handle signal, bounded cost,
            // safe to run under the advancer latch. Async flows can attach arbitrary continuation
            // work via await, so they MUST go through TP to keep the latch hold bounded.
            if (preferAsync && item.IsAsyncAtBind)
            {
                // The flow itself is the work item: an immutable (flow, control) pairing per
                // queued activation, zero-alloc. A single cached mutable work item here was a
                // lost-update box - two activations in flight (TP latency under load) let the
                // second Initialize overwrite the first before its Execute read the item, so
                // both executions activated the later flow and the earlier one never
                // activated (committed-never-activated hang, dump-diagnosed June 2026). One
                // pending activation per flow tenure makes the per-flow field safe.
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
            // Recovery-on-recovery does not exist, and the framework guarantees it structurally:
            // a committed recovery's late fault travels as the framework's marker exception and
            // is completed directly, never consulted here (pre-commit recovery failures complete
            // directly too). The assert keeps that contract loud; the refusal backstops release
            // builds (a dead-wire drain cannot be re-drained) and keeps the CompleteItem binding
            // discharge single-level by construction.
            System.Diagnostics.Debug.Assert(failedItem is not RecoveryDrainFlow,
                "Recovery item routed back into TryRecoverItemFailure - recovery-on-recovery must not exist.");

            // Pipeline is shutting down: skip recovery and let the framework propagate the failure.
            if (cancellationToken.IsCancellationRequested || failedItem is RecoveryDrainFlow)
            {
                recoveryItem = null;
                return false;
            }

            var control = _protocol.FlowControl;
            var failedControl = failedItem.GetExecutionControl(control);
            var inheritedRfqCount = failedControl.RfqCount;
            var unflushedBytes = _protocol.UnflushedBytes;

            // Inject Sync when the failed flow's last buffered message didn't induce RFQ - wire
            // is mid-sequence and the server is waiting for a terminator. Skip if last was already
            // self-terminating (Sync/Query), or if nothing is buffered (no bytes to send).
            var injectSync = !failedControl.LastMessageInducesRfq && unflushedBytes > 0;
            var flushPendingBytes = unflushedBytes > 0;
            var drainCount = inheritedRfqCount + (injectSync ? 1 : 0);

            var recovery = new RecoveryDrainFlow(
                async: failedItem.IsAsyncAtBind,
                drainCount: drainCount,
                injectSync: injectSync,
                flushPendingBytes: flushPendingBytes);
            recovery.GetExecutionControl(control).TransferInheritedRfqCount(inheritedRfqCount);

            // Per the policy contract, the framework will NOT complete a supplanted item -
            // that is this policy's job, and it happens when the recovery completes (see
            // CompleteItem): the failed item's lifetime extends as far as the recovery does.
            // Complete(exception) also faults the activation rendezvous, so a parked dispatch
            // resumes, observes the failure, and releases the shared pipelined-read promise
            // tenure it holds. That unwind resolves inline against the faulted source; a
            // queued sibling dispatching against the same promise in that instant hits the
            // already-started canary and recovers - a visible transient failure, not a hang.
            recovery.BindFailedFlow(failedItem, context.Exception);

            recoveryItem = recovery;
            return true;
        }

    }

    internal sealed class Control(PgClientProtocol protocol) : IProtocolStatic<CommandFlow.ReadState>
    {
        public PgClientFlow? ExecutorFlow { get; private set; }
        PgClientFlow? _activatedFlow;
        public PgClientFlow? ActivatedFlow
        {
            get => Volatile.Read(ref _activatedFlow);
            private set => Volatile.Write(ref _activatedFlow, value);
        }

        public PgProtocolDataWriter Writer => protocol._protocolDataWriter;

        // Tokens live on the protocol; per-flow storage is unnecessary since the protocol's
        // tokens are stable across the flow's tenure. Surface them through Control so
        // ExecutionControl (and through it, Context for the body and OnHeartbeat for the
        // framework) can read without each flow paying the storage.
        public CancellationToken AbortToken => protocol._abortToken;
        public CancellationToken StoppingToken => protocol._stoppingToken;

        /// The canonical <see cref="PgClientClosedException"/> for this protocol if Shutdown has
        /// been entered, null otherwise. Single instance per protocol lifetime, materialized
        /// before any cancellation fires so any observer that wakes on AbortToken/StoppingToken
        /// and reads ClosedException sees a non-null value. Use this anywhere the framework
        /// needs to "the protocol is closed" + the canonical exception, rather than re-deriving
        /// it at each callsite.
        public PgClientClosedException? ClosedException => protocol._closedException;

        /// Sync throw helper. Throws <see cref="PgClientClosedException"/> if the protocol is
        /// closed, no-op otherwise. Intended for use inside existing async I/O frames
        /// (PgDecoder, PgProtocolDataWriter) on the OCE catch path - converting OCE-from-our-
        /// abort-token to the typed closed exception without paying for an extra wrapping
        /// frame.
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
        internal async ValueTask<FlowTasks> Execute(PgClientFlow flow, CancellationToken abortToken)
        {
            ExecutorFlow = flow;
            var writer = protocol._protocolDataWriter;
            if (writer.UnflushedBytes > 1000)
                await writer.FlushAsync(abortToken).ConfigureAwait(false);
            var flowTasks = await flow.GetExecutionControl(this).ExecuteAuto().ConfigureAwait(false);
            // TODO only null after flowTasks.TrailingExecutionTask is completed.
            ExecutorFlow = null;
            return new(flowTasks.TrailingExecutionTask, flowTasks.PipelineTask);
        }

        internal void Activate(PgClientFlow flow)
        {
            ActivatedFlow = flow;
            var control = flow.GetExecutionControl(this);
            if (!control.IsPipelined)
                Interlocked.Decrement(ref protocol._pipelineStalls);
            var decoder = protocol._pgDecoder;
            decoder.Initialize(this);
            control.Activate(decoder);
        }

        internal void OnCompleted(PgClientFlow flow, int remainingDepth)
        {
            // ActivatedFlow is the decoder's current-reader handle. The framework dispatches
            // Activate(next) and Complete(prev) across threads (Policy.ActivateHeadItem TP-
            // queues; OnCommittedTaskCompleted/DrainSlotInline run on whichever thread
            // completed the prior waiter task - typically the socket-completion thread). A
            // brief null window between Complete(A) and Activate(B) means a continuation
            // resuming on the socket thread can deref a null binding. Holding the stale
            // reference between completions is safe: in-flight reads only happen during an
            // activated flow's drain and the framework's per-item Activate-before-Complete
            // invariant ensures the binding has been republished by the time those reads
            // execute. The next Activate overwrites the stale slot.
            //
            // At pipeline park (remainingDepth == 0) we want to release the flow reference
            // so GC can collect command parameters / descriptors / buffers it roots. Use CAS
            // so a concurrent Enqueue's Activate(B) that landed first stays untouched: the
            // depth==0 read could be stale relative to a user-thread Enqueue that the
            // framework already processed into an Activate. Same-instance re-activation
            // (which the CAS cannot distinguish - ABA) is excluded by ordering instead:
            // Policy.CompleteItem runs this before Complete(), and instance reuse is only
            // legal from the completion action onward.
            //
            // Recycle the read state and signal idle only when the slot is actually clear
            // (we cleared it, or it already was). A failed CAS against a live successor
            // means that flow may be mid-drain on this state right now - swapping it then
            // would yank the shared read promise out from under its pending dispatch, and
            // the connection is not idle either.
            if (remainingDepth is 0)
            {
                var observed = Interlocked.CompareExchange(ref _activatedFlow, null, flow);
                if (observed is null || ReferenceEquals(observed, flow))
                {
                    // Get rid of any rooted buffers, refs and so on.
                    _commandFlowReadState = new();
                    protocol._poolConnectionIdleSignal?.Invoke();
                }
            }
        }

        CommandFlow.ReadState _commandFlowReadState = new();
        ref readonly CommandFlow.ReadState IProtocolStatic<CommandFlow.ReadState>.Value
            => ref _commandFlowReadState;
    }
}

