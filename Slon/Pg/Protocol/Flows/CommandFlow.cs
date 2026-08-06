using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;
// A unique result type distinguishes the caller gate from this flow's other IValueTaskSource faces.
using FlowCallerInteractionCoreResult = System.ValueTuple;

namespace Slon.Pg.Protocol.Flows;

interface ICommandFlowObserver
{
    void OnFlowStarted(CommandFlow flow);
    void OnCommandResult(CommandFlow flow, CommandResult result);
    void OnFlowEnded(CommandFlow flow);
}

readonly struct CommandFlowOptions
{
    public ICommandFlowObserver? Observer { get; init; }
    public CommandList Commands { get; init; }
}

sealed class CommandFlow : PgClientFlow, IValueTaskSource<bool>, IValueTaskSource<FlowCallerInteractionCoreResult>, IValueTaskSource
{
    internal override bool DefersSyncHandoff => true;

    enum CancellationScope : byte
    {
        None,
        CurrentWindow = 1,
        RemainingFlow = 2
    }

    // Flow state
    CommandFlowOptions _options;
    FlowCallerInteractionCore<FlowCallerInteractionCoreResult> _callerInteractionCore;
    // Per-read caller token: overlaid by MoveNextAsync(ct) for a single read.
    CancellationToken _callerCancellationToken;
    // Flow-scoped token, bound at submission or enumeration and retained across results.
    CancellationToken _flowCancellationToken;
    CancellationTokenRegistration _callerCancellationTokenRegistration;
    CancellationTokenRegistration _flowCancellationTokenRegistration;
    // Callbacks only latch cancellation and wake the body. The body delivers cancellation at its
    // terminal, after releasing pipeline promise tenure.
    bool _cancelRequested;
    int _cancellationScope;
    // The token to carry on the terminal OCE, captured when the body observes the cancel.
    CancellationToken _cancelDeliverToken;
    TaskCompletionSource? _cancelDelivery;
    // Set by the body's cancel drain transition; the terminal SetResult delivers OCE (not a clean end).
    bool _deliverCancelOce;
    // Errors encountered while draining are surfaced by a waiting DisposeAsync. Live consumers observe
    // their errors directly, and the list remains unallocated on the successful path.
    List<Exception>? _drainErrors;
    // Set only by ConsumeNonQueryAsync, after enqueue but before any consumer-side gate release.
    // The body cannot reach first publication until such a release, and every release publishes
    // this write, so plain accesses suffice and the body observes the mode at first wake.
    bool _consumeNonQuery;
    bool IsConsumingNonQuery => _consumeNonQuery;
    bool IsConsumingAutonomously => IsDraining || IsConsumingNonQuery;
    long _nonQueryRecordsAffected;

    // Once draining, the body bypasses result handoffs and reads autonomously to RFQ. This is state, not
    // an I/O cancellation token: canceling the I/O would prevent restoration of a clean wire boundary.
    bool _draining;
    internal bool IsDraining => Volatile.Read(ref _draining);
    // Body-thread-only guard: later commands must not change the drive mode chosen on drain entry.
    bool _drainModeEntered;


    // Distinguishes consumer disposal from a body-initiated drain; disposal suppresses terminal OCE delivery.
    bool _consumerDisposed;
    // When true, DisposeAsync awaits the body's drain to RFQ. Otherwise it returns while the body drains;
    // pipeline retirement still prevents the next flow from observing a dirty wire.
    internal bool WaitForDrainOnDispose { get; set; } = true;

    // Consumer disposal without waiting for the autonomous drain.
    void MarkConsumerGone()
    {
        Volatile.Write(ref _consumerDisposed, true);
        Volatile.Write(ref _draining, true);
    }

    // Consumer disposal while waiting for the autonomous drain.
    void MarkConsumerWaitForDrain()
    {
        Volatile.Write(ref _consumerDisposed, true);
        Volatile.Write(ref _draining, true);
    }

    // A body-initiated drain keeps the consumer attached for terminal cancellation or close delivery.
    void MarkConsumerGoneByBody() => Volatile.Write(ref _draining, true);

    // Result state
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _enumeratorMoveNextTaskSource;
    // Serializes move-next rearming against body termination. Otherwise Reset can replace the generation
    // just before terminal completion and strand the consumer (see MoveNextRearm.tla). Never hold it while
    // dispatching the gate, which may run the body inline.
    Slon.Threading.SpinLock _rearmLock;
    CommandResult<ResultMessageEnumerator>? _enumeratorCurrent;
    RowDescription? _requestedRowDescription;
    PgError? _pgError;
    int _commandIndex = -1;
    bool _enumeratorCompleted;
    bool _isResultReady;
    PgDecoder? _decoder;
    bool _readFlowRfq;

    // Pipelined dispatch state. Lives here (not on PgClientFlow base) because the shared-promise
    // optimization that needs these fields is CommandFlow-specific (see DispatchPipelinedRead).
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _executePipelinedCore;
    ValueTaskSourcePromise<bool>? _pipelinePromise;
    Context _context;
    ValueTask _task;
    // Once the body starts, its catch paths own caller-facing fault propagation.
    bool _bodyStarted;
    // Consumer-thread-only. The first call uses the initial source generation; later calls rearm it.
    // Body start is not a substitute because an executor-driven body may finish before first consumption.
    bool _consumerAdvanced;

    ValueTask<bool> EnumeratorMoveNextTask => new(this, _enumeratorMoveNextTaskSource.Version);

    CommandFlow() : base(supportsDeferredFlush: true)
    {
        _callerInteractionCore.Initialize();
    }

    // Interactive: a command carries a caller's patience (ConnectionTimeout), so arm the activation timeout.
    protected override bool EnableActivationTimeout => true;

    internal static CommandFlow CreateUninitialized() => new();

    public CommandFlow(bool async, params ReadOnlySpan<Command> commands) : this()
        => Initialize(async, commands);
    public CommandFlow(bool async, in CommandFlowOptions options) : this()
        => Initialize(async, options);

    public CommandFlow Initialize(bool async, params ReadOnlySpan<Command> commands)
        => Initialize(async, options: new() { Commands = new(commands) });

    public CommandFlow Initialize(bool async, in CommandFlowOptions options)
    {
        IsAsync = async;
        _options = options;
        options.Observer?.OnFlowStarted(this);
        // Arm before publication: teardown may complete the source concurrently even before enumeration.
        _enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
        return this;
    }

    // Declares internal non-query consumption and runs it to completion. The single entry point
    // makes the ownership rule structural: no enumerator is exposed on this path, so mixing
    // enumeration with internal consumption is unrepresentable. The declaration precedes any
    // consumer-side gate release, so the body observes it at first wake and never publishes.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<long> ConsumeNonQueryAsync(CancellationToken cancellationToken = default)
    {
        _consumeNonQuery = true;
        var enumerator = GetAsyncEnumerator(cancellationToken);
        try
        {
            // Release a body parked pre-publication and wake the flow.
            _callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
            _callerInteractionCore.RequestWake();
            _ = await EnumeratorMoveNextTask.ConfigureAwait(false);
            // Internal consumption owns error delivery. Errors collected during the internal
            // drain have no other outlet: DisposeAsync deliberately skips a completed flow.
            if (_drainErrors is { Count: > 0 } errors)
                throw errors.Count == 1 ? errors[0] : new AggregateException(errors);
            return _nonQueryRecordsAffected;
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    // The token in force for the current read: the flow token once fired (whole-flow cancel), else the
    // per-read token if cancelable, else the flow token.
    CancellationToken EffectiveCancellationToken
        => _flowCancellationToken.IsCancellationRequested ? _flowCancellationToken
            : _callerCancellationToken.CanBeCanceled ? _callerCancellationToken
            : _flowCancellationToken;

    public int CommandCount => _options.Commands.Count;
    internal int VisibleCommandCount => _options.Commands.VisibleCount;
    public bool IsResultReady => _isResultReady;

    public Enumerator GetEnumerator()
    {
        return new Enumerator(this);
    }

    // Bind at submission because eager writing precedes the first MoveNextAsync.
    internal override void BindCallerToken(CancellationToken cancellationToken) => _flowCancellationToken = cancellationToken;

    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        // Async enumeration always needs thread-safe completion: the body completes the move-next
        // source from its own thread, AND the teardown (OnAbort/OnStopping via CancelPendingWait) is
        // an independent concurrent completer that can fault the caller-interaction gate out from under
        // a consumer mid-MoveNextAsync. A caller token adds the cancellation registration as a third
        // completer; bind it only when cancelable so a no-token enumerator doesn't clobber a submit-
        // bound token with None. Never disarm mid-tenure - a fired registration may still be in flight.
        _enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
        if (cancellationToken.CanBeCanceled)
            _flowCancellationToken = cancellationToken;
        return new(this);
    }

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        if (!IsAsync && _callerInteractionCore.IsWaiting)
            return ExecuteAfterHandoff(context);

        return new(ExecuteAutoCore(context));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<FlowTasks> ExecuteAfterHandoff(Context context)
    {
        try
        {
            await SetContinuationAndUnblockWaiter();
        }
        catch (Exception ex)
        {
            HandleException(ex);
            throw;
        }

        return ExecuteAutoCore(context);
    }

    FlowTasks ExecuteAutoCore(Context context)
    {
        if (_flowCancellationToken.IsCancellationRequested)
            RequestCancel(_flowCancellationToken, CancellationScope.RemainingFlow);
        else if (Volatile.Read(ref _cancelRequested))
            RequestBackendCancellation(delivery: Volatile.Read(ref _cancelDelivery));
        ValueTask writeTask;
        try
        {
            // Async flows write EAGERLY - no pre-write consumer gate. The old `await GetGateTask`
            // here fused the write to the first MoveNextAsync, which stranded a prior flow's deferred
            // flush whenever the executor sat on this gate waiting for a consumer that hadn't engaged
            // (batch-pipelining deadlock). The caller token is now bound at submit, so the write needs
            // nothing from the consumer. The inter-result gate (the real backpressure rendezvous)
            // stays. Sync flows still unwind so the caller's thread does its own writes:
            //   small window: an executor on a separate thread can reach here before the caller sets
            //   IsWaiting, so we always do the unwind later if we didn't here, to not stall past writing.
            // All writes captured as a single task. If they back-pressure (TCP send buffer full) the
            // task is pending and the framework awaits it inline between iterations so the next item's
            // writes don't race past. If they fit in the buffer it's sync-completed and the await is a
            // no-op. Async flows take the *Async path (TP completion). Sync flows take the *Resumable
            // path (sync syscalls on a non-blocking socket, WouldBlock suspends the coroutine, the
            // readiness driver resumes it). The SignalScope puts the encoder's cached writable signal in
            // the transport's TLS slot for the call, captured into the coroutine on WouldBlock so the
            // scope can close after the initial call returns.
            // See https://www.postgresql.org/message-id/CADT4RqAH2nuVwM6cEugFL2z6apwXfP3OJb=zxR6jRgWEpx_2Ww@mail.gmail.com
            // https://github.com/pgjdbc/pgjdbc/issues/194
            // https://github.com/npgsql/npgsql/issues/641
            var encoder = IsAsync ? default : context.GetEncoder();
            var appendSync = !_options.Commands[CommandCount - 1].WithSync;
            _readFlowRfq = appendSync;
            if (IsAsync)
            {
                // Caller cancellation never cancels wire I/O. A partially cancelled write requires
                // protocol recovery and can strand already-pipelined successors; the body instead
                // observes the latched intent and drains every written command to RFQ.
                writeTask = _options.Commands.WriteCommandsAsync(context.GetEncoder(), appendSync, default);
            }
            else
            {
                using (encoder.BeginResumableScope())
                    writeTask = _options.Commands.WriteCommandsResumable(encoder, appendSync);
            }

            // Observe an already-completed write here so a synchronously-faulted task throws inside
            // this try and routes through HandleException; otherwise it would surface only later via
            // the framework's trailing-task await, outside this scope. A still-pending write stays
            // unawaited and rides the trailing slot (read-concurrent-with-write, see the return). The
            // sync path wraps it in the resumable driver; the async path returns the raw flush task.
            if (writeTask.IsCompleted)
                writeTask.GetAwaiter().GetResult();
            else if (!IsAsync)
                writeTask = encoder.RunResumableTask(writeTask);
        }
        catch (Exception ex)
        {
            HandleException(ex);
            throw;
        }

        // Return the write as the trailing slot. The read is dispatched as the pipeline task and
        // parks concurrently, so a back-pressured flush is drained by the outstanding read off the
        // same socket (there is no background read pump). This is the TCP-window-deadlock guard. The
        // framework's between-iterations trailing await provides single-producer writer hygiene:
        // CompleteItem fires only after trailing is observed, regardless of when the read settles.
        // Write failures are transport death (ASP.NET-style); the next operation discovers the broken
        // connection, so no per-flow error bubbling here.
        return new FlowTasks(
            trailingExecutionTask: writeTask,
            pipelineTask: DispatchPipelinedRead(context, context.GetProtocolStatic<ReadState>().ReadPromise));
    }

    // Why this exists rather than the simple `using var _ = BeginCallScope(...); return ReadPhase();`
    // form: CommandFlow shares one ValueTaskSourcePromise per protocol (ReadState.ReadPromise). The
    // simple local-function form eagerly Creates a state machine per flow, each capturing the promise -
    // two state machines on one promise is a state conflict. The deferred dispatch here doesn't Create
    // until activation fires, so only ONE flow constructs at a time while others queue, saving N-1
    // promise allocations per protocol under pipelined load.
    ValueTask DispatchPipelinedRead(Context context, ValueTaskSourcePromise<bool> promise)
    {
        // Gate on ACTUAL activation (IsDecoderSettled via the non-auto awaitable), not GetDecoderAuto's
        // sync-unconditional IsCompleted: a sync successor must not Start ExecutePipelined (tenuring the
        // shared promise) before it's activated, or it collides with a predecessor still holding the
        // promise. The sync block stays inside ExecutePipelined's own GetDecoderAuto.
        var waiter = context.GetDecoderAsync().ConfigureAwait(false);
        if (waiter.IsCompleted)
        {
            // Gate the inline Start on activation SUCCEEDED, not merely SETTLED - the same guard the
            // deferred callback below already has. A faulted-settled activation (a racing dispose/teardown
            // faulted us before we took the wire) is IsDecoderSettled=true but IsDecoderReady=false; we
            // never claimed the wire, so Starting ExecutePipelined here would tenure the shared ReadPromise
            // a legitimately-activated flow still holds -> "already executing". Bail to our OWN promise
            // instead, exactly like the deferred path - the framework observes the close on the pipeline task.
            if (!waiter.IsCompletedSuccessfully)
            {
                // Activation settled NOT-ready: surface its ACTUAL fault - a real close, an activation
                // TIMEOUT, or caller CANCELLATION (the three completers in PgClientFlow.OnHeartbeat /
                // RegisterActivationCancellation). The old `?? new PgClientClosedException(null)` fallback
                // both MASKED the real error (a TimeoutException / OCE surfaced as "connection closed") AND
                // MANUFACTURED a protocol close out of a timeout/cancel - cascading a shutdown that took down
                // the wire + sibling flows. GetResult on a settled-not-ready source rethrows the real one.
                // We bail to our OWN _executePipelinedCore (never the shared promise - we never claimed the
                // wire), so there's no TryStart-over-tenured conflict and a timed-out flow just faults clean.
                try { waiter.GetAwaiter().GetResult(); }
                catch (Exception ex) { _executePipelinedCore.SetException(ex); }
                return new ValueTask(this, _executePipelinedCore.Version);
            }
            // Handing the shared-promise-backed task to the framework is safe: the contract guarantees
            // the waiter is consumed (releasing the promise tenure via GetResult's Reset) before the
            // item's position is republished, so a successor's dispatch always finds the tenure released.
            PromiseAsyncValueTaskMethodBuilder.Promise = promise;
            try
            {
                return ExecutePipelined(context);
            }
            finally
            {
                PromiseAsyncValueTaskMethodBuilder.Promise = null;
            }
        }

        _context = context;
        _pipelinePromise = promise;
        // Static continuation: a bridge into framework state, so no captured scheduling context is needed.
        waiter.OnCompleted(static state =>
        {
            var flow = (CommandFlow)state!;
            var ctx = flow._context;
            // We only have a claim on the shared promise if we actually activated (got a decoder). The
            // wake can also come from teardown faulting the activation source to unstrand us; we never
            // took the wire then, so Starting ExecutePipelined would tenure a promise a successor may
            // already hold (TryStart -> "already executing"). Surface the close to our own source instead.
            var activation = ctx.GetDecoderAsync().GetAwaiter();
            if (!activation.IsCompletedSuccessfully)
            {
                // Same as the fast path: surface the activation's real fault (close / timeout / cancel),
                // never a synthesized close. See the fast-path comment for why the old fallback was wrong.
                try { activation.GetResult(); }
                catch (Exception ex) { flow._executePipelinedCore.SetException(ex); }
                return;
            }
            var promise = flow._pipelinePromise!;
            PromiseAsyncValueTaskMethodBuilder.Promise = promise;
            ValueTask task = flow.ExecutePipelined(ctx);
            try
            {
                if (!task.IsCompleted)
                {
                    flow._task = task;
                    ((IValueTaskSource)promise).OnCompleted(static state =>
                    {
                        var flow = (CommandFlow)state!;
                        try
                        {
                            flow._task.GetAwaiter().GetResult();
                            flow._executePipelinedCore.SetResult(true);
                        }
                        catch (Exception ex)
                        {
                            flow._executePipelinedCore.SetException(ex);
                        }
                        // ENGINE-INTERNAL registration: the continuation is our own bridge code
                        // (consume + complete), no user code runs under it, and downstream user-facing
                        // cores carry their own per-registration captured contexts - so no EC flow.
                        // (The correctly-scoped version of the reverted strand-wide suppression: this
                        // kills the bridge hop's RunInternal/barrier share without touching the
                        // ambient semantics of any thread users resume on.)
                    }, flow, promise.Token, ValueTaskSourceOnCompletedFlags.None);
                }
                else
                {
                    try
                    {
                        task.GetAwaiter().GetResult();
                        flow._executePipelinedCore.SetResult(true);
                    }
                    catch (Exception ex)
                    {
                        flow._executePipelinedCore.SetException(ex);
                    }
                }
            }
            finally
            {
                PromiseAsyncValueTaskMethodBuilder.Promise = null;
            }
        }, this);

        return new ValueTask(this, _executePipelinedCore.Version);
    }

    [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder))]
    async ValueTask ExecutePipelined(Context context)
    {
        _bodyStarted = true;
        // If we have a continuation stored we must already be on the caller thread,
        // otherwise we must make sure to unblock the executor (see comment in the write phase).
        if (!IsAsync && !_callerInteractionCore.HasContinuation)
            await SetContinuationAndUnblockWaiter();

        try
        {
            // User cancellation must not cancel activation or wire I/O. The flow observes it after
            // activation, drains itself to RFQ, then delivers OCE without invoking pipeline recovery.
            _decoder = await context.GetDecoderAuto().ConfigureAwait(false);
            var publishedResult = false;
            while (++_commandIndex < CommandCount)
            {
                _isResultReady = false;
                bool hasPreparedDescription;
                bool suppressEnumeration;
                {
                    ref readonly var command = ref _options.Commands.ItemRef(_commandIndex);
                    _decoder.ReadTimeout = command.Timeout;
                    suppressEnumeration = command.SuppressEnumeration;
                    hasPreparedDescription = command.Descriptor is { IsPrepared: true, PreparedRowDescription: not null }
                        && !command.DescribeOnly;
                }

                // Registrations only latch and wake; terminal delivery remains body-owned. Dispose them
                // before promise tenure ends so callbacks cannot reach the next flow. Do not rearm after
                // consumer disposal, where a persistent cancellation could escape its intended wait.
                if (_callerCancellationToken.CanBeCanceled && !IsDraining)
                {
                    Debug.Assert(IsAsync);
                    _callerCancellationTokenRegistration = _callerCancellationToken.UnsafeRegister(static (state, token)
                        => ((CommandFlow)state!).RequestCancel(token, CancellationScope.CurrentWindow), this);
                }
                if (_flowCancellationToken.CanBeCanceled && !IsDraining)
                {
                    _flowCancellationTokenRegistration = _flowCancellationToken.UnsafeRegister(static (state, token)
                        => ((CommandFlow)state!).RequestCancel(token, CancellationScope.RemainingFlow), this);
                }
                // After close, a fresh command must not consume bytes left by its predecessor. A draining
                // flow may continue reading its own response to restore RFQ.
                if (!IsDraining && context.IsProtocolClosed)
                    throw context.FlowTerminationException;

                if (IsAsync && hasPreparedDescription)
                {
                    // Prepared commands with a known description have the compact BindComplete ->
                    // DataRow/CommandComplete prelude. Await the decoder directly so a read wake resumes
                    // this outer body rather than a nested parser coroutine; the second message normally
                    // comes from the same batch and is consumed synchronously.
                    if (!_decoder.TryGetNext(out var message))
                    {
                        if (!await _decoder.MoveNextAsync().ConfigureAwait(false))
                            throw PgProtocolException.UnexpectedEof();
                        message = _decoder.Current;
                    }

                    if (message.EnsureExpectedOrError(PgTypes.BackendType.BindComplete) is { } bindError)
                    {
                        _pgError = bindError;
                        _requestedRowDescription = null;
                    }
                    else
                    {
                        if (!_decoder.TryGetNext(out message))
                        {
                            if (!await _decoder.MoveNextAsync().ConfigureAwait(false))
                                throw PgProtocolException.UnexpectedEof();
                            message = _decoder.Current;
                        }
                        message.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
                        _pgError = null;
                        _requestedRowDescription = null;
                    }
                }
                else if (IsAsync)
                {
                    var rowDescription = context.GetProtocolStatic<ReadState>().RowDescription;
                    var read = _options.Commands.ItemRef(_commandIndex).ReadUntilExecuteAsync(_decoder, rowDescription);
                    (_pgError, _requestedRowDescription) = await read.ConfigureAwait(false);
                }
                else
                {
                    var rowDescription = context.GetProtocolStatic<ReadState>().RowDescription;
                    (_pgError, _requestedRowDescription) = _options.Commands.ItemRef(_commandIndex)
                        .ReadUntilExecute(_decoder, rowDescription);
                }

                // A draining consumer cannot observe CommandResult, so retain read errors for disposal.
                var capturedThisCommand = false;
                if (IsConsumingAutonomously && _pgError is { } readError && !IsOwnCancellation(readError))
                {
                    (_drainErrors ??= new()).Add(PgErrorException.Create(readError));
                    capturedThisCommand = true;
                }

                // Await in-flight callbacks before releasing shared promise tenure.
                if (_callerCancellationToken.CanBeCanceled)
                    await _callerCancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
                if (_flowCancellationToken.CanBeCanceled)
                    await _flowCancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
                // Cancellation switches to autonomous drain; terminal delivery follows RFQ and tenure release.
                if ((Volatile.Read(ref _cancelRequested) || EffectiveCancellationToken.IsCancellationRequested)
                    && !_enumeratorCompleted)
                {
                    _cancelDeliverToken = EffectiveCancellationToken.IsCancellationRequested ? EffectiveCancellationToken : _cancelDeliverToken;
                    _deliverCancelOce = true;
                    _enumeratorCompleted = true;
                    if (!IsDraining)
                        MarkConsumerGoneByBody();
                }

                CommandResult<ResultMessageEnumerator> result;
                {
                    ref readonly var readState = ref context.GetProtocolStatic<ReadState>();
                    readState.ResultMessageEnumerator.Initialize(this, _decoder);
                    result = _enumeratorCurrent ?? readState.CommandResult;

                    ref readonly var resultCommand = ref _options.Commands.ItemRef(_commandIndex);
                    var descriptor = resultCommand.Descriptor;
                    // We were preparing and we have no error from parse, make a prepared descriptor.
                    if (!descriptor.IsPrepared && !descriptor.CommandName.IsDefault
                        && (_pgError is not { } err || !err.Expected.Contains(PgTypes.BackendType.ParseComplete)))
                    {
                        descriptor = CommandDescriptor.CreatePrepared(
                            descriptor.CommandName, descriptor.ParameterTypes, _requestedRowDescription?.Preserve());
                    }
                    result.Initialize(_commandIndex, descriptor, _requestedRowDescription,
                        !resultCommand.DescribeOnly, resultCommand.IsSimple());
                }
                _options.Observer?.OnCommandResult(this, result);

                // Disposal drains without another result handoff. Graceful close instead faults the
                // attached consumer, then uses the same autonomous drain. Command errors remain results.
                if (context.StoppingToken.IsCancellationRequested && !IsDraining && !_enumeratorCompleted)
                {
                    // Latch the close (a consumer that Resets past this point self-delivers it), wake a
                    // parked consumer, then drain.
                    var close = context.FlowTerminationException;
                    _callerInteractionCore.SetCloseLatch(close);
                    DeliverClose(close);
                    MarkConsumerGoneByBody();
                }
                var consumeInternally = IsConsumingNonQuery || suppressEnumeration;
                if (!IsDraining && !consumeInternally)
                {
                    // Eager async execution must wait for the consumer to arm generation zero before
                    // publishing its first result. Synchronous execution already runs on that caller.
                    if (!publishedResult && IsAsync)
                    {
                        await _callerInteractionCore.GetGateTask(this).ConfigureAwait(false);
                        HandleStoppingGate(context);
                    }

                    if (!IsDraining && !IsConsumingNonQuery)
                    {
                        _isResultReady = true;
                        publishedResult = true;
                        // Result continuations run asynchronously so the body can reach the next gate
                        // before user code asks for the next result. Buffered batches then advance inline
                        // from MoveNextAsync instead of suspending one Task state machine per result.
                        SetResult(result);

                        if (!IsDraining && !IsConsumingNonQuery)
                        {
                            if (IsAsync)
                            {
                                await _callerInteractionCore.GetGateTask(this).ConfigureAwait(false);
                                HandleStoppingGate(context);
                            }
                            else
                                await SetContinuationAndUnblockWaiter();

                            /* The next MoveNext or MoveNextAsync call resumes here. */
                        }
                    }
                }
                else if (!_drainModeEntered && IsAsyncAtBind && !IsAsync)
                {
                    // An async I/O wake raced a synchronous disposer before the body reached its handoff.
                    if (WaitForDrainOnDispose)
                    {
                        // Hand the continuation to the disposer, which waits on the rendezvous rather than
                        // this task and can therefore drive the remaining drain without sync-over-async.
                        await SetContinuationAndUnblockWaiter();
                    }
                    else
                    {
                        // No disposer is waiting to drive; retain asynchronous background draining.
                        IsAsync = IsAsyncAtBind;
                    }
                }
                // Preserve the drive mode chosen on first drain entry.
                _drainModeEntered = _drainModeEntered || IsDraining;

                // We check IsAsync again as it can change after every resumption.
                // Current is disposed here, something the user might have done, but if not we'll do it here.
                // This also causes us to pick up any I/O exception thrown during user code that was stored on the resultmessage enumerator.
                // In drain mode this dispose IS the drain: it reads remaining DataRows + CommandComplete for the current command.
                (PgError Error, TransactionStatus TransactionStatus)? completeError;
                // Current mode, not the iteration snapshot: a body that raced past the snapshot
                // before the caller declared non-query consumption published a result no consumer
                // will read. Its decoder state is identical to the internal path's entry (rows
                // unread, the consume caller never touches Current), so it takes the internal
                // path wholesale, error collection and accumulation included.
                if (consumeInternally || IsConsumingNonQuery)
                {
                    while (_decoder.Current.Header.Type is PgTypes.BackendType.DataRow)
                    {
                        if (!_decoder.TryGetNext(out _))
                            await _decoder.GetNextAsync().ConfigureAwait(false);
                    }
                    result.CompleteNonQuery(_decoder.Current);
                    var completion = _options.Commands.ItemRef(_commandIndex).CompleteAsync(_decoder);
                    completeError = await completion.ConfigureAwait(false);
                    if (_pgError is null && completeError is null)
                        _nonQueryRecordsAffected = checked(_nonQueryRecordsAffected + result.RecordsAffected);
                }
                else if (IsAsync)
                {
                    var resultEnumerator = context.GetProtocolStatic<ReadState>().ResultMessageEnumerator;
                    await resultEnumerator.DisposeAsync().ConfigureAwait(false);
                    completeError = resultEnumerator.CompleteError;
                }
                else
                {
                    var resultEnumerator = context.GetProtocolStatic<ReadState>().ResultMessageEnumerator;
                    resultEnumerator.Dispose();
                    completeError = resultEnumerator.CompleteError;
                }

                if (suppressEnumeration && result.Error is { } suppressedError && !IsOwnCancellation(suppressedError))
                {
                    (_drainErrors ??= new()).Add(PgErrorException.Create(suppressedError));
                    capturedThisCommand = true;
                    if (!IsDraining)
                        MarkConsumerGoneByBody();
                }

                {
                    // CompleteError is non-null only for a FRESHLY-faulted command (a real ErrorResponse on
                    // this command's segment); a command skipped-to-sync by a prior fault returns null. On a
                    // consumer-gone DRAIN, accumulate each fresh error so the terminal can surface them
                    // (the active-consumer path instead observes errors per CommandResult, so skip it there).
                    // The loop keeps draining remaining commands/segments to the final RFQ regardless, so the
                    // wire is left clean; we only collect, never throw mid-drain. A WithSync=false batch yields
                    // at most one such error (one trailing sync); per-command-sync flows can yield several.
                    // Capture a completion-phase error only if the read phase did not already capture this
                    // command's error (a single command's ErrorResponse can appear in both _pgError and
                    // CompleteError - dedupe so one fault is one entry). A drain skips errors already owned
                    // by a live consumer. Direct nonquery consumption routes through the internal path
                    // above (current-mode check), so its errors are collected there.
                    if ((consumeInternally || IsConsumingNonQuery || IsDraining && !_isResultReady)
                        && !capturedThisCommand && completeError is { } err && !IsOwnCancellation(err.Error))
                        (_drainErrors ??= new()).Add(PgErrorException.Create(err.Error));
                }

                // Extended-query errors discard every following command through the next Sync. Skip
                // those commands locally and consume the RFQ which is their only wire response.
                if (completeError is { TransactionStatus: TransactionStatus.Unknown })
                {
                    while (++_commandIndex < CommandCount && !_options.Commands[_commandIndex].WithSync) { }

                    if (IsAsync)
                        await ReadRfqAsync(_decoder).ConfigureAwait(false);
                    else
                        ReadRfq(_decoder);

                    // Reaching the end means the discarded segment terminated at our appended Sync.
                    if (_commandIndex == CommandCount)
                        _readFlowRfq = false;
                }
            }

            // Write failures are framework-observed via the trailing-task slot. The framework
            // catches the exception, the structural routing ensures CompleteItem only fires
            // after trailing is observed, and the no-recovery path lets the pipeline-task
            // outcome stand. ASP.NET treats transport writes the same way. A failing write is
            // connection death, the next operation discovers it. We don't bubble it up as a
            // per-flow error here.
            if (_readFlowRfq)
            {
                if (_decoder.TryGetNext(out var message))
                {
                    if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                        PgErrorException.Throw(rfqError);
                }
                else if (IsAsync)
                {
                    await ReadRfqAsync(_decoder).ConfigureAwait(false);
                }
                else
                {
                    ReadRfq(_decoder);
                }
            }

            SetResult(null);
        }
        catch (PgClientClosedException) when (context.IsProtocolClosed)
        {
            // Scope to our own closure so a nested protocol's close doesn't get treated as ours.
            // Latch the close so a consumer that Resets after this point self-delivers it.
            _callerInteractionCore.SetCloseLatch(context.FlowTerminationException);
            // Consumer-gone means a dead wire is the expected terminal: complete the move-next source
            // (false) and RETURN cleanly, the same shape as the graceful StoppingToken transition above
            // (SetResult on close, fall through to a clean body completion - no throw). This keeps the
            // close from propagating as an exception through the pipeline drain only to be swallowed by
            // DisposeAsyncCore / the sync Dispose loop. A live consumer instead gets the close on its
            // move-next source and we rethrow so it surfaces (and the framework runs recovery).
            if (IsDraining)
            {
                if (!_enumeratorCompleted)
                    SetResult(null);
                return;
            }
            HandleException(context.FlowTerminationException);
            throw;
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _callerCancellationToken || ex.CancellationToken == _flowCancellationToken)
        {
            HandleException(ex);
            throw;
        }
        catch (TimeoutException ex)
        {
            RequestBackendCancellation(BackendCancellationTiming.Immediate);
            HandleException(ex);
            throw;
        }
        catch (Exception ex)
        {
            HandleException(ex);
            throw;
        }
        finally
        {
            // The body is the sole owner of protocol-static row metadata. Recovery consumes only
            // decoder/wire state, so a faulted body can release oversized storage while recovery
            // retains the failed flow's framework tenure.
            context.GetProtocolStatic<ReadState>().RowDescription.PrepareForReuse();
        }
        void SetResult(CommandResult<ResultMessageEnumerator>? next)
        {
            var completed = next is null;
            if (completed)
            {
                _enumeratorCurrent = null;
            }
            else
            {
                if (_callerCancellationToken.CanBeCanceled)
                    _callerCancellationToken = CancellationToken.None;

                if (!ReferenceEquals(_enumeratorCurrent, next))
                    _enumeratorCurrent = next;

            }

            // Under close, the consumer owns delivery to whatever generation it armed (it self-delivers
            // the latch on rearm). The body must NOT land a result or a clean completion - but it MUST
            // wake a consumer currently PARKED on its generation. Deliver the close to the current
            // generation: idempotent against the consumer's self-deliver, wakes a parked consumer, never
            // lands a stale result.
            if (_callerInteractionCore.CloseException is not null)
            {
                DeliverClose(_callerInteractionCore.CloseException);
                return;
            }
            if (completed)
            {
                // Version-aware terminal, sequenced with the body's return (tenure released right after).
                // _enumeratorCompleted is version-less: a DisposeAsync drain (or any rearm) Resets the
                // source onto a fresh PENDING generation after a prior generation went terminal, and parks
                // awaiting THAT generation. TrySet lands on a pending generation (waking a parked drain)
                // and no-ops an already-completed one. runContinuationsAsynchronously keeps the consumer
                // continuation / pipeline advance off this stack. When the body observed a user-cancel,
                // THIS is the sole tenure-safe point to deliver the OCE; otherwise a clean end. A
                // consumer-gone DisposeAsync drain gets the clean end, never an OCE (DisposeAsyncCore only
                // swallows close), so the cancel OCE is suppressed once the consumer itself disposed.
                //
                // The _enumeratorCompleted set and the move-next completion are the body's half of the
                // guard-decide-rearm straddle (see _rearmLock): held together so the consumer can't read
                // _enumeratorCompleted=false and then Reset away THIS completion. DeliverTerminal completes
                // the source with runContinuationsAsynchronously:true, so the dispatch stays off this
                // locked stack - no inline re-entry of the non-reentrant lock.
                using (_rearmLock.EnterScope())
                {
                    _enumeratorCompleted = true;
                    // INVARIANT: the terminal completes here directly, even when this runs on the inline
                    // dispatch frame that still tenures the shared ReadPromise. Safe ONLY because every
                    // terminal completer (DeliverTerminal / DeliverClose / RepairLostTerminal / HandleException)
                    // uses runContinuationsAsynchronously:true, so the consumer wake - and the pipeline advance
                    // it can drive - is scheduled off THIS stack, never re-entering a TryStart over the still-
                    // tenured promise. (This replaced a _pendingInlineTerminal defer that moved the completion
                    // off-frame; the defer became redundant once completion went async, and its bare-bool
                    // frame-guard had a cross-thread strand bug. If a terminal completer is ever made inline,
                    // the shared-promise TryStart throws "already executing" - a loud regression, not silent.)
                    DeliverTerminal();
                }
                return;
            }
            _enumeratorMoveNextTaskSource.SetResult(true, runContinuationsAsynchronously: true);
        }

        async ValueTask ReadRfqAsync(PgDecoder decoder)
        {
            var message = await decoder.GetNextAsync().ConfigureAwait(false);
            if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                PgErrorException.Throw(rfqError);
        }

        static void ReadRfq(PgDecoder decoder)
        {
            var message = decoder.GetNext();
            if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                PgErrorException.Throw(rfqError);
        }
    }

    /// Faults the caller-facing sources. Reached from the body's catch paths for in-body
    /// faults, and from release teardown for failures the body never observed (dispatch-time throw,
    /// pre-body protocol fault). Callers are structurally sequential with each other - the
    /// framework releases an item only after its pipeline task settles, and teardown
    /// defers to a parked dispatch's unwind - so the _enumeratorCompleted guard suffices for
    /// double-call idempotency. The cancellation registration is the one genuinely concurrent
    /// completer; when a cancelable token armed it, the source is in thread-safe completion
    /// mode and we must use TrySet semantics to absorb a lost race. Continuations run
    /// asynchronously: this can fire from the pipeline's completion chain and must not run
    /// caller code under it.
    // Deliver the close to the consumer's CURRENT generation (wakes a parked consumer; idempotent
    // against the consumer's own latch self-deliver). The source is in concurrent mode for every live
    // flow (Initialize, sync and async), so TrySet is the safe completer for both - teardown faults from
    // a different thread than the consumer regardless of mode. Marks terminal; signals the sync rendezvous.
    // Latch a user-cancel WITHOUT completing the move-next source. Called from the (non-completing)
    // cancellation registration callbacks and the eager-write catch. Completing the source here would do
    // it from a context that may hold (or race) the shared pipeline promise, driving ExecuteSource into a
    // TryStart over a tenured promise. Instead we latch intent + the token to carry, and RequestWake so a
    // parked body advances to its terminal, where it delivers the OCE tenure-safely (after its return).
    internal Task CancelAsync()
    {
        var delivery = Volatile.Read(ref _cancelDelivery);
        if (delivery is null)
        {
            var created = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            delivery = Interlocked.CompareExchange(ref _cancelDelivery, created, null) ?? created;
        }
        if (IsCompleted)
        {
            delivery.TrySetResult();
            return delivery.Task;
        }
        RequestCancel(default, CancellationScope.RemainingFlow);
        if (IsCompleted)
            delivery.TrySetResult();
        return delivery.Task;
    }

    void RequestCancel(CancellationToken token, CancellationScope scope)
    {
        _cancelDeliverToken = token;
        var observedScope = Volatile.Read(ref _cancellationScope);
        while ((int)scope > observedScope)
        {
            var priorScope = Interlocked.CompareExchange(ref _cancellationScope, (int)scope, observedScope);
            if (priorScope == observedScope)
                break;
            observedScope = priorScope;
        }
        Volatile.Write(ref _cancelRequested, true);
        Volatile.Write(ref _draining, true);
        RequestBackendCancellation(delivery: Volatile.Read(ref _cancelDelivery));
        _callerInteractionCore.OpenGate(runContinuationsAsynchronously: true);
        _callerInteractionCore.RequestWake(useDedicatedDriver: !IsAsync && Volatile.Read(ref _cancelDelivery) is not null);
    }

    protected override void OnCancellationWindowCompleted(int completedWindow, int remainingWindowCount)
    {
        if (remainingWindowCount != 0
            && Volatile.Read(ref _cancellationScope) == (int)CancellationScope.RemainingFlow)
            RequestBackendCancellation(delivery: Volatile.Read(ref _cancelDelivery));
    }

    bool IsOwnCancellation(PgError error)
        => Volatile.Read(ref _cancelRequested) && error.SqlState == PgErrorCodes.QueryCanceled;

    // The terminal move-next completion (clean end, or user-cancel OCE), factored so it can run either
    // inline (when the body suspended at least once - off the dispatch frame) or be deferred to the
    // dispatch site (when the body ran fully synchronously on the inline frame, where the promise is
    // tenured). Every completer here is runContinuationsAsynchronously: true so the consumer continuation
    // / pipeline advance never runs inline on the completing stack.
    void DeliverTerminal()
    {
        // Drained ErrorResponses win: a Postgres error hit while draining is connection-state truth that
        // must surface (Npgsql parity), over a clean end or a user-cancel OCE. One error => a bare
        // PgErrorException (the common case); several (a per-command-sync batch that faulted in multiple
        // segments) => an AggregateException, lossless - the ADO layer can take InnerExceptions[0] if it
        // only wants the first. An await-drain DisposeAsync (WaitForDrainOnDispose) rethrows this via
        // WaitForComplete; a fault-and-return dispose returns before this, so the error is observed by the
        // flow's completion rather than out of dispose.
        if (_drainErrors is { Count: > 0 } errors)
        {
            Exception fault = errors.Count == 1 ? errors[0] : new AggregateException(errors);
            _enumeratorMoveNextTaskSource.TrySetException(fault, runContinuationsAsynchronously: true);
        }
        else if (_deliverCancelOce && !_consumerDisposed)
            _enumeratorMoveNextTaskSource.TrySetException(new OperationCanceledException(_cancelDeliverToken), runContinuationsAsynchronously: true);
        else
            _enumeratorMoveNextTaskSource.TrySetResult(false, runContinuationsAsynchronously: true);
        // _enumeratorCompleted was set by the caller (SetResult's completed branch) before this runs.
        WakePumpOnCompletion();
    }

    // Token-bounded await-drain for WaitForDrainOnDispose. Parks on the body's completion until it
    // drains to RFQ, OR the flow/enumerator token fires - then unwind fast (the body stays consumer-gone
    // and finishes draining on its own; we just stop waiting). The cancel is swallowed: DisposeAsync must
    // not throw the caller's own token out of a dispose.
    async ValueTask AwaitDrainOnDispose()
    {
        try
        {
            await WaitForComplete(_flowCancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_flowCancellationToken.IsCancellationRequested)
        {
            // Caller cancelled the wait; unwind. The body drains autonomously in the background.
        }
        catch (PgClientClosedException)
        {
            // The flow completed via a protocol close; that is a clean terminal for a disposing consumer.
            return;
        }
        // The drain reached RFQ. Surface any ErrorResponses hit while draining (Npgsql parity): one => a
        // bare PgErrorException, several (multi-sync) => an AggregateException (ADO takes InnerExceptions[0]
        // if it only wants the first). WaitForComplete keys off the completion signal, which resolves
        // successfully even when the drain saw command errors - those are accumulated separately, surfaced
        // here so they escape await DisposeAsync().
        if (_drainErrors is { Count: > 0 } errors)
            throw errors.Count == 1 ? errors[0] : new AggregateException(errors);
    }

    void AwaitDrainOnDisposeSynchronously()
    {
        try
        {
            WaitForCompleteSynchronously(_flowCancellationToken);
        }
        catch (OperationCanceledException) when (_flowCancellationToken.IsCancellationRequested)
        {
            // Caller cancelled the wait; the body continues draining autonomously.
        }
        catch (PgClientClosedException)
        {
            return;
        }

        if (_drainErrors is { Count: > 0 } errors)
            throw errors.Count == 1 ? errors[0] : new AggregateException(errors);
    }

    void DeliverClose(Exception closeException)
    {
        _enumeratorMoveNextTaskSource.TrySetException(closeException, runContinuationsAsynchronously: true);
        _enumeratorCompleted = true;
        WakePumpOnCompletion();
    }

    void HandleStoppingGate(Context context)
    {
        if (_callerInteractionCore.CloseException is { } close && context.StoppingToken.IsCancellationRequested
            && !IsDraining && !_enumeratorCompleted)
        {
            HandleException(close);
            MarkConsumerGoneByBody();
        }
    }

    // Publish terminal progress unconditionally. A terminal delivery can lose its task-source CAS to an
    // already-completed generation and leave _enumeratorCompleted false. A later synchronous disposer
    // must still observe this sticky level rather than park for a completer that has already gone away.
    void WakePumpOnCompletion()
        => _callerInteractionCore.SignalProgress();

    // Repair a version-less terminal (_enumeratorCompleted) that survived onto a fresh PENDING generation
    // with no completer (the lost-completion shape), shared by the sync and async MoveNext short-circuits.
    // Two distinct causes, close-first precedence:
    //   1. CLOSE - the protocol close-latch (sticky, monotone, set out-of-band by another thread). If set
    //      it wins: a closed protocol is terminal and authoritative, the user cancel is moot.
    //   2. USER CANCEL - the caller's per-read token fired (e.g. a pre-fired token whose OCE delivery
    //      landed on a prior generation, or the body drained past it). Read live on THIS (consumer)
    //      thread - the token is the consumer's own, so no latch is needed; deliver an OCE carrying the
    //      bound token (per-read, NOT sticky - the flow/connection is fine, the next read works).
    // Only acts while the current generation is Pending (skips churn on an already-completed gen).
    void RepairLostTerminal()
    {
        if (_enumeratorMoveNextTaskSource.GetStatus(_enumeratorMoveNextTaskSource.Version) is not ValueTaskSourceStatus.Pending)
            return;
        if (_callerInteractionCore.CloseException is { } latched)
            _enumeratorMoveNextTaskSource.TrySetException(latched, runContinuationsAsynchronously: true);
        else if (Volatile.Read(ref _cancelRequested) || EffectiveCancellationToken.IsCancellationRequested)
            _enumeratorMoveNextTaskSource.TrySetException(
                new OperationCanceledException(EffectiveCancellationToken.IsCancellationRequested ? EffectiveCancellationToken : _cancelDeliverToken),
                runContinuationsAsynchronously: true);
        // Clean-end fallback. The body ran its ONE terminal onto a prior generation and returned; a
        // disposer (or any rearm) then Reset onto this fresh PENDING generation with no remaining
        // completer. With _enumeratorCompleted set and neither a close nor a live cancel, the terminal was
        // a clean end (false) - deliver it here so the parked drain always completes. Guarded by
        // _enumeratorCompleted so it never fabricates a false on a still-running flow.
        else if (_enumeratorCompleted)
            _enumeratorMoveNextTaskSource.TrySetResult(false, runContinuationsAsynchronously: true);
    }

    void HandleException(Exception ex)
    {
        // A close fault must latch so a consumer that Resets past this delivery self-delivers it - the
        // never-started path (OnStopping -> HandleException, body never ran so no body-side latch set)
        // is the one that strands an orphaned generation otherwise.
        if (ex is PgClientClosedException or PgCollateralException)
            _callerInteractionCore.SetCloseLatch(ex);
        if (_enumeratorCompleted)
            return;
        // Mark terminal only when the fault actually lands. A concurrent teardown TrySetException
        // no-ops if it targets an already-completed generation - e.g. the abort faulted the inter-
        // result gate and this fault hits the already-CONSUMED prior generation. Setting
        // _enumeratorCompleted on that no-op would make the consumer's next MoveNext short-circuit onto
        // the stale consumed result; leaving it false lets that call fall through to its own faulted-
        // gate self-delivery, which completes the consumer's actual generation.
        // HandleException can fire from teardown (OnAbort/OnStopping) on the heartbeat or
        // shutdown thread - NOT the consumer's thread - concurrently with the consumer's own completer.
        // The source is in concurrent mode for every live flow (armed in Initialize, sync and async
        // alike), so this is an atomic, idempotent CAS. A plain SetException would throw on an already-
        // completed source and a status-check guard is TOCTOU. (Recovery used to swallow the resulting
        // InvalidOperationException, masking the race.)
        //
        // Deliberately UNLOCKED, even though the body's clean-end terminal writes _enumeratorCompleted
        // under _rearmLock. _rearmLock orders ONLY consumer-rearm vs the body's clean-end terminal; it
        // does not - and need not - cover teardown. This write is safe unlocked because _enumeratorCompleted
        // is a FOLLOWER of the CAS above (set only when TrySetException landed), never the authority: a
        // torn/stale read just degrades to "do the CAS and let it decide". The authority against teardown
        // is the CAS (single-completer) plus the close-latch (SetCloseLatch, Interlocked + Volatile,
        // independent of _rearmLock), which the consumer self-delivers on its next rearm. So do NOT treat
        // _enumeratorCompleted as lock-consistent across the body/teardown boundary. Verified safe with the
        // unlocked teardown racing the locked consumer/body in MoveNextRearm.tla (Lock + WithTeardown).
        if (_enumeratorMoveNextTaskSource.TrySetException(ex, runContinuationsAsynchronously: true))
            _enumeratorCompleted = true;
        // Wake a sync caller / a sync disposer pumping the rendezvous (the WakeHandshake - see
        // WakePumpOnCompletion). The body just faulted in-flight, so no continuation will register.
        WakePumpOnCompletion();
        // The remainder (leftover RFQs) is NOT consumed here. When a consumer is truly gone the body
        // rethrows and the framework's ResyncRecoveryFlow inherits this flow's live RfqCount and blind-
        // drains it. When a consumer is present (live or wait-for-drain) the body drains it ITSELF
        // (MarkConsumerGoneByBody + the _drainErrors / per-result surfacing) - recovery must not take it,
        // it is a blind read-drain and cannot publish ErrorResponses to a waiting consumer.
    }


    // A sync CommandFlow's caller parks on this for the source handoff (WaitForExecutor). Reuses the
    // caller-core MRES - safe because the handoff turn-handshake completes BEFORE the body's first
    // WaitForContinuation. GetMres ensures non-null (lazy field); a null here on the sync path is a bug.
    protected override ManualResetEventSlim? HandoffEvent => _callerInteractionCore.GetMres();

    // Return the rendezvous directly. An async wrapper could signal the disposer before registering the
    // body's continuation, causing a late ThreadPool dispatch instead of caller-thread drain execution.
    FlowCallerInteractionCore<FlowCallerInteractionCoreResult>.ContinuationCapturingAwaitable SetContinuationAndUnblockWaiter()
    {
        FieldRef<FlowCallerInteractionCore<FlowCallerInteractionCoreResult>> fieldRef;
        unsafe
        {
            fieldRef = FieldRef<FlowCallerInteractionCore<FlowCallerInteractionCoreResult>>.Create(&GetCallerInteractionCore, this);
        }
        return _callerInteractionCore.SetContinuationAndUnblockWaiter(fieldRef);
    }

    static ref FlowCallerInteractionCore<FlowCallerInteractionCoreResult> GetCallerInteractionCore(CommandFlow instance)
        => ref instance._callerInteractionCore;

    protected override void OnAbort(Exception exception) => FaultCaller(exception);

    // Graceful stopping is the early wire-close wake and is idempotent across heartbeat ticks.
    protected override void OnStopping(Exception exception)
    {
        if (!_bodyStarted || !IsAsync)
        {
            FaultCaller(exception);
            return;
        }

        // Resume normally so the body observes the close latch and drains; abort faults the gate.
        _callerInteractionCore.SetCloseLatch(exception);
        _callerInteractionCore.OpenGate(runContinuationsAsynchronously: true);
    }

    // Wake a running body so it owns fault delivery; directly fault a flow whose body never started.
    void FaultCaller(Exception exception)
    {
        if (_bodyStarted)
            _callerInteractionCore.CancelPendingWait(exception);
        else
            HandleException(exception);
    }

    protected override void OnReleasing(Exception? exception)
    {
        Volatile.Read(ref _cancelDelivery)?.TrySetResult();
        _options.Observer?.OnFlowEnded(this);
        _options.Commands.Return();
    }

    protected override void OnDiscarded()
    {
        _options.Observer?.OnFlowEnded(this);
        _options.Commands.Return();
    }

    protected override void OnReset()
    {
        Debug.Assert(IsPending || IsCompleted);
        _commandIndex = -1;
        _executePipelinedCore.Reset();
        _enumeratorMoveNextTaskSource.Reset();
        // Disarm while idle in the pool (no teardown can target a non-live flow). Initialize re-arms it
        // before the flow is queued, so a live flow is always in concurrent-completion mode.
        _enumeratorMoveNextTaskSource.CanCompleteConcurrently = false;
        _enumeratorCurrent = default;
        _isResultReady = false;
        _callerInteractionCore.Reset();
        _callerCancellationToken = default;
        _flowCancellationToken = default;
        _callerCancellationTokenRegistration.Dispose();
        _callerCancellationTokenRegistration = default;
        _flowCancellationTokenRegistration.Dispose();
        _flowCancellationTokenRegistration = default;
        _cancelRequested = false;
        _cancellationScope = (int)CancellationScope.None;
        _cancelDeliverToken = default;
        _cancelDelivery = null;
        _deliverCancelOce = false;
        _drainErrors = null;
        _consumeNonQuery = false;
        _nonQueryRecordsAffected = 0;
        _consumerDisposed = false;
        _draining = false;
        _drainModeEntered = false;
        WaitForDrainOnDispose = true;
        // Dispatch state is per-tenure.
        _pipelinePromise = null;
        _context = default;
        _task = default;
        _bodyStarted = false;
        _consumerAdvanced = false;
    }

    FlowCallerInteractionCoreResult IValueTaskSource<FlowCallerInteractionCoreResult>.GetResult(short token)
        => _callerInteractionCore.ConsumeGateResult(token);

    ValueTaskSourceStatus IValueTaskSource<FlowCallerInteractionCoreResult>.GetStatus(short token)
        => _callerInteractionCore.GateStatus(token);

    void IValueTaskSource<FlowCallerInteractionCoreResult>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        _callerInteractionCore.OnGateCompleted(continuation, state, token, flags);
        // Recheck-after-register. The drain signal is a sticky LEVEL (IsDraining); the gate open is a
        // one-shot EDGE (the consumer's TrySetResult, fired OUTSIDE _rearmLock). If the consumer set
        // draining and opened the gate before we registered here, that edge was buffered and then wiped by
        // the body's next gate Reset: the open-before-park lost-wake (the body parks forever and pins the
        // pipeline's activated slot, see ConsumerDispose_MidBatch). Re-read the level now the continuation
        // is registered and complete the gate ourselves if draining, so the parked body drains. Force
        // runContinuationsAsynchronously because we are on the body's own suspending stack and must not run
        // the continuation inline. Idempotent against the consumer's own TrySetResult. The move-next source
        // avoids the same hazard with a _rearmLock-serialized !IsDraining rearm skip, but the gate is
        // unlocked (opening it runs the body inline) so it recovers via this level recheck instead.
        if (IsDraining)
        {
            // Driver identity, decided on the body side at the hand-off: we are only NOW parking, so a
            // synchronous disposer (which presented IsAsync=false for an inline takeover) could not have
            // caught us - the takeover did not engage. Recover on async reads: restore IsAsync=true before
            // the async wake, ordered before the schedule so the resumed body reads true and does NOT do
            // sync socket I/O on this pool thread (the double-block). A real inline takeover never reaches
            // here - it resumes the already-parked body directly, leaving IsAsync=false for sync drain.
            IsAsync = true;
            _callerInteractionCore.OpenGate(runContinuationsAsynchronously: true);
        }
    }

    bool IValueTaskSource<bool>.GetResult(short token) => _enumeratorMoveNextTaskSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _enumeratorMoveNextTaskSource.GetStatus(token);
    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _enumeratorMoveNextTaskSource.OnCompleted(continuation, state, token, flags);

    // Backing for the pipelined-dispatch ValueTask. Returned to the framework when activation
    // hasn't fired yet. Nested callback completes it when ExecutePipelined finishes.
    void IValueTaskSource.GetResult(short token) => _executePipelinedCore.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _executePipelinedCore.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _executePipelinedCore.OnCompleted(continuation, state, token, flags);

    public readonly struct Enumerator(CommandFlow flow) : IEnumerator<CommandResult>, IAsyncEnumerator<CommandResult>
    {
        // Here so we can pass the cancellation token and enumerate without boxing the struct (which WithCancellation must do).
        /// <inheritdoc cref="IAsyncEnumerable{T}.GetAsyncEnumerator" />
        public Enumerator GetAsyncEnumerator() => this;

        // Dispose always calls MoveNext to confirm the enumerator is done without tracking additional state.
        // So this method should be resilient to multiple fetches of *at least* the final result.
        /// <inheritdoc />
        public bool MoveNext()
        {
            if (flow is null)
                return false;

            // Queueing only established FIFO position. This is the first point at which the caller
            // is ready to take the source pump and drive the synchronous body.
            if (!flow._consumerAdvanced && !flow.IsAsyncAtBind)
                flow.WaitForSyncHandoff();

            var takeOverAsyncGate = false;

            // Guard-decide-rearm serialized against the body's terminal (see _rearmLock). The using scope
            // ends before the WaitForContinuation drive below (which runs the body inline and would
            // re-enter this non-reentrant lock); try/finally release keeps it lock-safe on any throw.
            using (flow._rearmLock.EnterScope())
            {
                // See MoveNextAsync: a version-less terminal that survived a Reset onto a fresh pending
                // generation needs repair for the close and user-cancel causes (see RepairLostTerminal);
                // otherwise return the terminal task as before.
                if (flow._enumeratorCompleted)
                {
                    flow.RepairLostTerminal();
                    return flow.EnumeratorMoveNextTask.Result;
                }

                if (flow.IsAsync)
                {
                    if (flow._enumeratorCurrent is null)
                        ThrowHelper.ThrowInvalidOperation("No immediate sync/async mixing is allowed, the first MoveNext{Async} call has to match the async argument passed during initialize.");
                    flow.IsAsync = false;
                    takeOverAsyncGate = true;
                }

                // See MoveNextAsync: rearm only on a non-first call; the first-call source is fresh and the
                // body's first delivery lands on it.
                if (flow._consumerAdvanced)
                    flow._enumeratorMoveNextTaskSource.Reset();
                flow._consumerAdvanced = true;
            }
            // Close-latch self-deliver (sync): under close this call completes the generation it just
            // armed, on its own thread.
            if (flow._callerInteractionCore.CloseException is { } syncClosed)
            {
                flow.DeliverClose(syncClosed);
                return flow.EnumeratorMoveNextTask.Result;
            }
            // The body may already be parked on the async inter-result gate. Once this caller changes
            // the flow to synchronous driving, open that gate inline so the body can hand its continuation
            // to the rendezvous below. The edge also covers an in-flight body that has not parked yet.
            if (takeOverAsyncGate)
            {
                flow._callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
                var delivered = flow.EnumeratorMoveNextTask;
                if (delivered.IsCompleted)
                    return delivered.Result;
            }
            // Two wake reasons: a continuation was registered (drive the body forward inline)
            // or the body signaled progress (a result, completion, or fault landed on the
            // move-next task source while we were parked). In the progress-only case there is
            // no continuation to invoke. The task is already complete.
            var continuation = flow._callerInteractionCore.WaitForContinuation();
            var task = flow.EnumeratorMoveNextTask;
            if (task.IsCompleted)
            {
                if (continuation is not null)
                    flow._callerInteractionCore.DeferContinuation(continuation);
                return task.Result;
            }
            continuation?.Invoke();
            task = flow.EnumeratorMoveNextTask;
            return task.IsCompleted ? task.Result : task.AsTask().GetAwaiter().GetResult();
        }

        // DisposeAsync always calls MoveNextAsync to confirm the enumerator is done without tracking additional state.
        // So this method should be resilient to multiple fetches of the final result.
        /// <inheritdoc />
        public ValueTask<bool> MoveNextAsync() => MoveNextAsync(null);

        /// <summary>Advances the enumerator asynchronously to the next element of the collection.</summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous operation.</param>
        /// <returns>A <see cref="T:System.Threading.Tasks.ValueTask`1" /> that will complete with a result of <see langword="true" /> if the enumerator was successfully advanced to the next element, or <see langword="false" /> if the enumerator has passed the end of the collection.</returns>
        public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken) => MoveNextAsync(new CancellationToken?(cancellationToken));
        ValueTask<bool> MoveNextAsync(CancellationToken? cancellationToken)
        {
            if (flow is null)
                return new(false);

            if (cancellationToken is { IsCancellationRequested: true } preCanceledToken)
            {
                flow._callerCancellationToken = preCanceledToken;
                flow.RequestCancel(preCanceledToken, CancellationScope.CurrentWindow);
                flow._callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
                return ValueTask.FromException<bool>(new OperationCanceledException(preCanceledToken));
            }

            // The guard-decide-rearm is serialized against the body's terminal (see _rearmLock): the using
            // scope covers the _enumeratorCompleted read through the move-next Reset, and ends before the
            // gate dispatch below (which runs the body inline and would re-enter this non-reentrant lock).
            // try/finally release keeps it lock-safe on any throw.
            using (flow._rearmLock.EnterScope())
            {
                // Publish the per-read token before the terminal repair below: a flow may already have
                // completed before its first consumer call, and RepairLostTerminal needs this token to
                // distinguish a pre-fired cancellation from a clean end.
                if (cancellationToken is { } terminalToken && terminalToken.CanBeCanceled)
                {
                    flow._callerCancellationToken = terminalToken;
                    flow._enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
                }

                // A version-less terminal may outlive its completed source generation. Repair the newly
                // armed generation for close or cancellation before returning it.
                if (flow._enumeratorCompleted)
                {
                    flow.RepairLostTerminal();
                    return flow.EnumeratorMoveNextTask;
                }

                if (!flow.IsAsync)
                {
                    if (flow._enumeratorCurrent is null)
                        ThrowHelper.ThrowInvalidOperation("No immediate sync/async mixing is allowed, the first MoveNext{Async} call has to match the async argument passed during initialize.");
                    flow.IsAsync = true;
                }

                // The first delivery targets the initial generation. After consumer disposal, keep the
                // current generation for the body's one-shot terminal. Body-initiated drain retains a live
                // consumer and therefore continues rearming.
                if (flow._consumerAdvanced && !Volatile.Read(ref flow._consumerDisposed))
                    flow._enumeratorMoveNextTaskSource.Reset();
                flow._consumerAdvanced = true;
            }
            // Drive the body; teardown may already have faulted the gate.
            flow._callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
            // Read the close latch after Reset and complete the generation just armed.
            if (flow._callerInteractionCore.CloseException is { } closed)
                flow.DeliverClose(closed);
            return flow.EnumeratorMoveNextTask;
        }

        public CommandResult Current => flow?._enumeratorCurrent ?? default!;

        /// <inheritdoc />
        public void Dispose()
        {
            if (flow is null)
                return;

            // If the flow already terminally completed, there's nothing to drain. Skipping
            // also avoids re-throwing a previously-observed fault from EnumeratorMoveNextTask.Result
            // during foreach's exception unwind.
            if (flow._enumeratorCompleted)
                return;

            // Synchronous disposal takes over an async body through a two-way rendezvous. A gate-parked
            // body resumes inline; an in-flight body later hands over its continuation. Waiting on this
            // rendezvous rather than MoveNext avoids blocking on the task the body itself must complete.
            if (flow.IsAsync)
            {
                flow.IsAsync = false;
                if (flow.WaitForDrainOnDispose)
                    flow.MarkConsumerWaitForDrain();
                else
                    flow.MarkConsumerGone();
                // Resume a gate-parked body inline; otherwise buffer the edge.
                flow._callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
                if (flow.WaitForDrainOnDispose)
                {
                    // Pump handed-off continuations inline until sticky terminal progress wakes us
                    // without one.
                    while (!Volatile.Read(ref flow._enumeratorCompleted))
                    {
                        var continuation = flow._callerInteractionCore.WaitForContinuation();
                        if (continuation is null)
                            break;
                        continuation.Invoke();
                    }
                    // Drain ran on this thread; surface accumulated drain errors (completes immediately).
                    flow.AwaitDrainOnDisposeSynchronously();
                }
                else
                {
                    // Fast-return: wake the body to drain autonomously in the background, then return.
                    flow._callerInteractionCore.RequestWake();
                }
                return;
            }

            // A synchronous consumer drives the autonomous drain through the same rendezvous.
            flow.MarkConsumerGone();
            while (MoveNext())
                Current.Dispose();
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            if (flow is null)
                return new();

            if (flow._enumeratorCompleted)
                // A completed awaited drain may still have errors to surface.
                return flow.WaitForDrainOnDispose ? flow.AwaitDrainOnDispose() : new();

            // Mark autonomous drain, open the async result gate, and wake any synchronous rendezvous.
            // Pipeline tenure keeps successors behind the body until it reaches RFQ.
            if (flow.WaitForDrainOnDispose) flow.MarkConsumerWaitForDrain(); else flow.MarkConsumerGone();
            flow._callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
            flow._callerInteractionCore.RequestWake();
            // Optionally await the body's bounded completion; cancellation stops waiting, not draining.
            if (flow.WaitForDrainOnDispose)
                return flow.AwaitDrainOnDispose();
            return new();
        }

        /// <inheritdoc />
        void IEnumerator.Reset() => throw new NotSupportedException();
        /// <inheritdoc />
        object? IEnumerator.Current => Current;
    }

    internal struct ReadState
    {
        public ResultMessageEnumerator ResultMessageEnumerator { get; }
        public CommandResult<ResultMessageEnumerator> CommandResult { get; }
        public ValueTaskSourcePromise<bool> ReadPromise { get; }
        public RowDescription RowDescription { get; }

        public ReadState()
        {
            ResultMessageEnumerator = new();
            CommandResult = new(ResultMessageEnumerator);
            ReadPromise = new();
            RowDescription = new();
        }
    }

    // This is a struct to make CommandResult<T> specialize.
    public readonly struct ResultMessageEnumerator() : IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
    {
        readonly MessageEnumerator _messageEnumerator = new();
        public bool MoveNext() => _messageEnumerator.MoveNext();
        public ValueTask<bool> MoveNextAsync() => _messageEnumerator.MoveNextAsync();
        public BackendMessage Current => _messageEnumerator.Current;

        public void Dispose() => _messageEnumerator.Dispose();
        public ValueTask DisposeAsync() => _messageEnumerator.DisposeAsync();

        void IEnumerator.Reset() => ((IEnumerator)_messageEnumerator).Reset();
        BackendMessage IAsyncEnumerator<BackendMessage>.Current => _messageEnumerator.Current;
        BackendMessage IEnumerator<BackendMessage>.Current => _messageEnumerator.Current;
        object? IEnumerator.Current => ((IEnumerator)_messageEnumerator).Current;

        public void Initialize(CommandFlow flow, PgDecoder decoder)
            => _messageEnumerator.Initialize(flow, decoder);

        public (PgError Error, TransactionStatus TransactionStatus)? CompleteError
            => _messageEnumerator.CompleteError;

        sealed class MessageEnumerator : IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
        {
            CommandFlow _flow = null!;
            PgDecoder _decoder = null!;
            bool _disposed;
            bool _first;
            bool _done;
            ExceptionDispatchInfo? _exceptionDispatchInfo;
            (PgError, TransactionStatus)? _completeError;

            Command Command => _flow._options.Commands[_flow._commandIndex];

            // Additional debugging for figuring out where we lose protocol sync.
            // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
            // "Therefore, an Execute phase is always terminated by the appearance of exactly one of these messages:
            // CommandComplete, EmptyQueryResponse (if the portal was created from an empty query string), ErrorResponse, or PortalSuspended"
            [Conditional("DEBUG")]
            static void DebugEnsureExpected(BackendMessage message)
                => message.DebugEnsureExpected(PgTypes.BackendType.DataRow,
                    PgTypes.BackendType.CommandComplete, PgTypes.BackendType.EmptyQueryResponse,
                    PgTypes.BackendType.ErrorResponse, PgTypes.BackendType.PortalSuspended);

            [MethodImpl(MethodImplOptions.NoInlining)]
            bool EnumerateFirst()
            {
                _first = false;
                DebugEnsureExpected(_decoder.Current);
                if (_decoder.Current.Header.Type is not PgTypes.BackendType.DataRow)
                    _done = true;
                return true;
            }

            public bool MoveNext()
            {
                if (_first)
                    return EnumerateFirst();

                _exceptionDispatchInfo?.Throw();
                if (_done)
                    return false;

                try
                {
                    if (_decoder.TryGetNext(out var message))
                    {
                        DebugEnsureExpected(message);
                        if (message.Header.Type is not PgTypes.BackendType.DataRow)
                            _done = true;
                        return true;
                    }

                    message = _decoder.GetNext();
                    DebugEnsureExpected(message);
                    if (message.Header.Type is not PgTypes.BackendType.DataRow)
                        _done = true;
                    return true;
                }
                catch (Exception ex)
                {
                    _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                    throw;
                }
            }

            public ValueTask<bool> MoveNextAsync()
            {
                if (_first)
                    return new(EnumerateFirst());

                _exceptionDispatchInfo?.Throw();
                if (_done)
                    return new(false);

                // We don't capture TryGetNext errors, we assume no IO will happen and it prevents inlining of this method.
                if (_decoder.TryGetNext(out var message))
                {
                    DebugEnsureExpected(message);
                    if (message.Header.Type is not PgTypes.BackendType.DataRow)
                        _done = true;
                    return new(true);
                }

                return Core();

                [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
                async ValueTask<bool> Core()
                {
                    try
                    {
                        var message = await _decoder.GetNextAsync().ConfigureAwait(false);
                        DebugEnsureExpected(message);
                        if (message.Header.Type is not PgTypes.BackendType.DataRow)
                            _done = true;
                        return true;
                    }
                    catch (Exception ex)
                    {
                        _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                        throw;
                    }
                }
            }

            public BackendMessage Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _decoder.Current;
            }

            public void Dispose()
            {
                _exceptionDispatchInfo?.Throw();
                if (_disposed)
                    return;
                _disposed = true;
                try
                {
                    var decoder = _decoder;
                    if (!decoder.TryGetCurrent(out var current)
                        || current.Header.Type is PgTypes.BackendType.DataRow)
                    {
                        while (decoder.GetNext().Header.Type is PgTypes.BackendType.DataRow) {}
                    }
                    _completeError = Command.Complete(_decoder);
                }
                catch (Exception ex)
                {
                    _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                    throw;
                }
            }

            public ValueTask DisposeAsync()
            {
                _exceptionDispatchInfo?.Throw();
                if (_disposed)
                    return new();

                return DisposeAsyncCore();
            }

            ValueTask DisposeAsyncCore()
            {
                _disposed = true;
                try
                {
                    var decoder = _decoder;
                    if (decoder.TryGetCurrent(out var current)
                        && current.Header.Type is not PgTypes.BackendType.DataRow)
                    {
                        var completion = Command.CompleteAsync(decoder);
                        if (completion.IsCompletedSuccessfully)
                        {
                            _completeError = completion.Result;
                            return new();
                        }
                        return AwaitCompletion(completion);
                    }
                    return DrainRowsAndComplete(decoder);
                }
                catch (Exception ex)
                {
                    _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                    return ValueTask.FromException(ex);
                }

                [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder))]
                async ValueTask AwaitCompletion(ValueTask<(PgError, TransactionStatus)?> completion)
                {
                    try
                    {
                        _completeError = await completion.ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                        throw;
                    }
                }

                [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder))]
                async ValueTask DrainRowsAndComplete(PgDecoder decoder)
                {
                    try
                    {
                        while (true)
                        {
                            if (decoder.TryGetNext(out var message) && message.Header.Type is not PgTypes.BackendType.DataRow)
                                break;

                            message = await decoder.GetNextAsync().ConfigureAwait(false);
                            if (message.Header.Type is not PgTypes.BackendType.DataRow)
                                break;
                        }
                        _completeError = await Command.CompleteAsync(decoder).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                        throw;
                    }
                }
            }

            public void Initialize(CommandFlow flow, PgDecoder decoder)
            {
                if (!ReferenceEquals(_flow, flow))
                    _flow = flow;
                if (!ReferenceEquals(_decoder, decoder))
                    _decoder = decoder;

                _exceptionDispatchInfo = null;
                _disposed = false;
                _completeError = null;

                // A command is immediately done if we haven't submitted an execute.
                _done = Command.DescribeOnly;
                _first = !_done;
            }

            public (PgError Error, TransactionStatus TransactionStatus)? CompleteError
            {
                get
                {
                    if (!_disposed)
                        ThrowHelper.ThrowInvalidOperation("Command was not completed yet.");

                    return _completeError;
                }
            }

            void IEnumerator.Reset() => throw new NotSupportedException();
            object? IEnumerator.Current => Current;
        }
    }
}
