using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;
// A unique result type distinguishes the caller gate from this flow's other IValueTaskSource faces.
using FlowCallerInteractionCoreResult = System.ValueTuple;

namespace Slon.Pg.Protocol.Flows;

readonly struct CommandFlowOptions
{
    public Action<CommandResult, object?>? OnCommandResultAction { get; init; }
    public object? OnCommandResultActionState { get; init; }
    public CommandList Commands { get; init; }
}

sealed class CommandFlow : PgClientFlow, IValueTaskSource<bool>, IValueTaskSource<FlowCallerInteractionCoreResult>, IValueTaskSource
{
    // Flow state
    CommandFlowOptions _options;
    FlowCallerInteractionCore<FlowCallerInteractionCoreResult> _callerInteractionCore;
    // Per-read caller token: overlaid by MoveNextAsync(ct) for a single read.
    CancellationToken _callerCancellationToken;
    // Flow-scoped caller token: bound at submit (BindCallerToken) or GetAsyncEnumerator; cancels the
    // ENTIRE flow. Bound at submit it is honored from the eager write (whole-flow cancel is then
    // deterministic); bound at GetAsyncEnumerator it is bound after the eager dispatch, so the first
    // result may race. Not cleared per result.
    CancellationToken _flowCancellationToken;
    CancellationTokenRegistration _callerCancellationTokenRegistration;
    CancellationTokenRegistration _flowCancellationTokenRegistration;
    // Cancel intent latches set by the (non-completing) registration callbacks. The callback NEVER
    // completes the move-next source: completing it mid-body, while this flow holds the shared pipeline
    // promise (or a concurrent pipelined flow does), drives ExecuteSource into a TryStart over a tenured
    // promise. The callback only latches intent + RequestWake; the body delivers the OCE at its OWN
    // TERMINAL (sequenced with the body's return, after tenure is released). Consumer-thread reads, so
    // Volatile.
    bool _cancelRequested;
    // The token to carry on the terminal OCE, captured when the body observes the cancel.
    CancellationToken _cancelDeliverToken;
    // Set by the body's cancel drain transition; the terminal SetResult delivers OCE (not a clean end).
    bool _deliverCancelOce;
    // (Removed: the _onInlineDispatchStack / _pendingInlineTerminal terminal-defer. It guarded an INLINE
    // move-next completion driving ExecuteSource into a TryStart over the tenured ReadPromise - a conflict
    // that ceased to exist once DeliverTerminal moved to runContinuationsAsynchronously:true, so the
    // completion's advance is always off-stack. The defer outlived its reason; the terminal now completes
    // directly on any frame. Verified: full suite 205/0 + ~15M mixed-flow stress, no "already executing".)
    // Fresh ErrorResponses captured while draining a consumer-gone flow (one per faulting sync segment).
    // Surfaced at the terminal so an await-drain DisposeAsync (WaitForDrainOnDispose) rethrows them - Npgsql
    // parity: a Postgres error hit while draining a disposed reader must not be swallowed. Lazily allocated;
    // null in the common no-error path (zero alloc). Only populated when IsDraining (the drain); on the
    // live path the active consumer observes errors per-result, so we do not double-surface.
    System.Collections.Generic.List<PostgresException>? _drainErrors;
    bool _consumeNonQuery;
    bool IsConsumingNonQuery => Volatile.Read(ref _consumeNonQuery);
    bool IsConsumingAutonomously => IsDraining || IsConsumingNonQuery;
    long _nonQueryRecordsAffected;

    // The body's drain-mode switch: once set, the body skips the user-handoff gates and drains the wire to
    // RFQ autonomously (it no longer waits to be driven by MoveNextAsync). Named for the body MODE, not a
    // consumer event, because it is set by ALL three departure paths below - including the body opting the
    // consumer out on its own cancel/stopping drain, where the consumer is still live and did NOT depart
    // ("consumer-gone" / "detached" would be false for that path; _consumerDisposed is the narrower
    // consumer-only fact). NOT the caller's CancellationToken (which may just mean "skip this result"), and
    // not exposed AS a CancellationToken: that would invite I/O ops to register and throw OCE, breaking the
    // drain-to-clean-state guarantee.
    bool _draining;
    internal bool IsDraining => Volatile.Read(ref _draining);
    // One-shot guard for the FIRST drain transition. Body-thread only (serialized, no Volatile). Records
    // that the drain's drive mode is settled, so a sync takeover's later commands don't re-run the skip-gate
    // IsAsync restore and flip a genuine sync drain back to async mid-batch.
    bool _drainModeEntered;


    // The consumer DISPOSED the enumerator (the two consumer departure paths below), as opposed to the
    // body opting the consumer out on its own cancel/close drain. The terminal suppresses a user-cancel
    // OCE once the consumer itself disposed (it asked to stop; deliver a clean end instead).
    bool _consumerDisposed;
    // DEFAULT: of the two consumer departure paths, pick WAIT-FOR-DRAIN. DisposeAsync parks on the body's
    // drain (WaitForComplete) before returning, so the wire is at RFQ on return (next ExecuteReader
    // immediate; ADO semantics). The body drains either way - this only controls whether DisposeAsync WAITS
    // (parked on a TCS), never whether the drain happens, never by driving it (a polling drive busy-spins).
    // Hence "WaitForDrain", not "Drain". The wait is bounded for everyone by the read timeout / AbortToken /
    // CompletionTimeout (the same bounds every read already has); a flow/enumerator token just makes it
    // PROMPTLY bounded (fired => unwind fast). Set false to skip the wait (fault + return; the body drains in
    // the background and item retirement hands the next flow a clean wire).
    internal bool WaitForDrainOnDispose { get; set; } = true;

    // ---- The three departure paths that flip the body to autonomous-drain mode ----

    // Consumer departure path 1 of 2: GONE. The consumer disposed and does NOT want to wait - DisposeAsync
    // faults + wakes the body and returns immediately; the body drains in the background and the pipeline's
    // item retirement gives the next flow a clean wire.
    void MarkConsumerGone()
    {
        Volatile.Write(ref _consumerDisposed, true);
        // Full fence, not a release: this store pairs with the body's register-then-recheck on the
        // inter-result gate. The gate open that follows can no-op fence-free against an already
        // consumed generation, so a release store can stay unpublished past the body's recheck -
        // the body then parks on the fresh generation with both the edge and the level missed and
        // pins the pipeline's activated slot.
        Interlocked.Exchange(ref _draining, true);
    }

    // Consumer departure path 2 of 2: WAIT-FOR-DRAIN. The consumer disposed and wants DisposeAsync to park
    // on the drain before returning. Records the wait intent; the dispose path reads it to route through
    // AwaitDrainOnDispose.
    void MarkConsumerWaitForDrain()
    {
        Volatile.Write(ref _consumerDisposed, true);
        // Full fence, same pairing as MarkConsumerGone.
        Interlocked.Exchange(ref _draining, true);
    }

    // NOT a consumer departure: the BODY itself opts the consumer out during its own cancel/StoppingToken
    // drain (the consumer is still live and gets the OCE/close at the terminal - so this does NOT set
    // _consumerDisposed, which would suppress that OCE).
    void MarkConsumerGoneByBody() => Volatile.Write(ref _draining, true);

    // Result state
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _enumeratorMoveNextTaskSource;
    // Serializes the consumer's guard-decide-rearm (the _enumeratorCompleted read through the move-next
    // Reset) against the body's terminal (its _enumeratorCompleted set). Under pipelined escalation the
    // body's terminal can run on a wire-I/O completion thread, off the consumer thread, so the two
    // straddle the move-next source's generation: without exclusion the consumer can Reset onto a fresh
    // generation in the window after reading _enumeratorCompleted=false but before the body's terminal
    // lands, wiping the body's completion and stranding the consumer (spec: MoveNextRearm.tla). NOT held
    // over the gate dispatch (it runs the body inline and would re-enter this non-reentrant lock).
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
    // TRUE once ExecutePipelined's state machine has begun running; from that point the
    // body's catch paths own caller-facing fault propagation.
    bool _bodyStarted;
    // True once the consumer has made a prior MoveNext{Async} call. Gates the per-call source Reset:
    // the FIRST call must never Reset (the body's first delivery - result, completion, or a teardown
    // fault that bypasses the gate ordering - lands on the initial generation the consumer awaits).
    // _bodyStarted is the wrong proxy here: the body can start and complete BEFORE the consumer's
    // first call (executor-dispatched), so gating Reset on it rearms past a delivery the consumer
    // never consumed. Consumer-thread-only, so no Volatile needed.
    bool _consumerAdvanced;

    ValueTask<bool> EnumeratorMoveNextTask => new(this, _enumeratorMoveNextTaskSource.Version);

    CommandFlow() : base(supportsPipelining: true)
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
        // Arm thread-safe completion NOW (before queue, before any teardown can race), not lazily at
        // GetAsyncEnumerator: a flow torn down before the consumer ever obtains its enumerator faults
        // via OnComplete -> HandleException on another thread, which must complete the move-next source
        // by CAS. This holds for SYNC flows too: teardown (OnAbort/OnStopping/OnComplete) fires from the
        // heartbeat/shutdown thread, NOT the consumer thread the sync body is hogging - so the source has
        // two concurrent writers regardless of flow mode. Re-armed per tenure here (OnReset disarms).
        _enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
        return this;
    }

    // The token in force for the current read: the flow token once fired (whole-flow cancel), else the
    // per-read token if cancelable, else the flow token.
    CancellationToken EffectiveCancellationToken
        => _flowCancellationToken.IsCancellationRequested ? _flowCancellationToken
            : _callerCancellationToken.CanBeCanceled ? _callerCancellationToken
            : _flowCancellationToken;

    public int CommandCount => _options.Commands.Count;
    public bool IsResultReady => _isResultReady;

    public Enumerator GetEnumerator()
    {
        return new Enumerator(this);
    }

    // Bind the caller token at submit so the eager write honors it (the pre-write consumer gate is
    // gone, so the write no longer borrows the token from the first MoveNextAsync).
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
                writeTask = _options.Commands.WriteCommandsAsync(context.GetEncoder(), appendSync, _flowCancellationToken);
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
        catch (OperationCanceledException ex) when (ex.CancellationToken == _flowCancellationToken)
        {
            // Flow-scoped cancel observed by the EAGER WRITE (a pre-fired submit-bound token). The command
            // bytes may already be on the wire, so the read must still drain to RFQ. Do NOT route through
            // HandleException: that both faults the move-next source from this (write-phase) stack and
            // tears the connection. Latch the cancel; the read body observes it, drains consumer-gone, and
            // delivers the OCE at its terminal - tenure-safe. The write happens before any in-body
            // registration arms, so this catch is the only place to capture a write-phase flow-cancel.
            RequestCancel(_flowCancellationToken);
            // The write faulted; no trailing write task to observe. The read drains to RFQ.
            writeTask = default;
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
            // GetDecoderAuto adapts to flow mode internally. Activation has already been gated
            // by DispatchPipelinedRead's waiter.IsCompleted check, so the fast path returns
            // sync-immediately in both modes. The await is free when sync-completed.
            //
            // Do NOT pass the user cancel token here. With the token at the gate, a cancel fired before
            // activation makes GetResult throw OCE out of the body, faulting the pipeline task - so the
            // framework RESYNCS this flow via a recovery substitute (inject Sync, drain inherited RFQs).
            // That is the wrong tool: a user cancel is not a wire fault, and the flow can resync itself.
            // Activate unconditionally; the cancel-drain transition below observes the latched cancel,
            // MarkConsumerGoneByBody, drains to RFQ, and delivers the OCE at the terminal - the same
            // self-drain a consumer-gone dispose uses, no recovery flow. I/O is never cancelled by the
            // user token (only the read timeout / wire abort), so activation must not be either.
            _decoder = await context.GetDecoderAuto().ConfigureAwait(false);
            while (++_commandIndex < CommandCount)
            {
                _isResultReady = false;
                bool hasPreparedDescription;
                {
                    ref readonly var command = ref _options.Commands.ItemRef(_commandIndex);
                    _decoder.ReadTimeout = command.Timeout;
                    hasPreparedDescription = command.Descriptor is { IsPrepared: true, PreparedRowDescription: not null }
                        && !command.DescribeOnly;
                }

                // CancellationTokens from callers just cancels their enumerator task, we don't cancel I/O unless the timeout hits.
                // As we execute in a pipeline we must make sure harmless cancellations don't unnecessarily abort the protocol (and already pipelined flows with it).
                // The only way we can successfully process those flows is by consuming all data meant for the current flow, which means waiting for I/O.
                // As long as the server is sufficiently responsive we'll handle all consumption without caller interaction and complete the current flow.
                // Only arm while there is a consumer to deliver OCE to. Once consumer-gone, arming a
                // still-cancelled token would re-fire TrySetException into the move-next source
                // DisposeAsync just reset, leaking the OCE past its one wait.
                // Arm NON-COMPLETING cancel registrations for this read. The callbacks never touch the
                // move-next source (completing it mid-body races ExecuteSource into a TryStart over a
                // tenured promise); they latch intent + RequestWake, and the body delivers the OCE at its
                // terminal. Both tokens are honored: the per-read token (MoveNextAsync(ct)) and the
                // flow-scoped token (submit / GetAsyncEnumerator). A pre-fired token fires the callback
                // INLINE here, which is now harmless (it only latches). Both registrations are disposed
                // below via the awaited DisposeAsync BEFORE the body returns and releases the promise, so
                // no callback can fire after the next flow takes the promise.
                if (_callerCancellationToken.CanBeCanceled && !IsDraining)
                {
                    Debug.Assert(IsAsync);
                    _callerCancellationTokenRegistration = _callerCancellationToken.UnsafeRegister(static (state, token)
                        => ((CommandFlow)state!).RequestCancel(token), this);
                }
                if (_flowCancellationToken.CanBeCanceled && !IsDraining)
                {
                    Debug.Assert(IsAsync);
                    _flowCancellationTokenRegistration = _flowCancellationToken.UnsafeRegister(static (state, token)
                        => ((CommandFlow)state!).RequestCancel(token), this);
                }
                // A flow reading a FRESH command response after the protocol has already CLOSED would consume
                // a prior flow's leftover wire bytes (a desync: "Unexpected backend message: DataRow, expected
                // ParseComplete"). When a flow faults under a graceful close it retires WITHOUT draining (an
                // expected dirty retire); the next flow legitimately activates (tenure is intact - it IS the
                // ActivatedFlow), but its own command can no longer be answered, so it must NOT read - it must
                // fault with the close. Draining flows are exempt: they consume their OWN already-received
                // bytes to leave the wire clean. The per-CommandResult StoppingToken check below is too late -
                // it runs AFTER this read - so the close must be observed HERE, before the read.
                if (!IsDraining && context.IsProtocolClosed && context.ClosedException is { } preReadClose)
                    throw preReadClose;

                if (IsAsync && hasPreparedDescription)
                {
                    // Prepared commands with a known description have the compact BindComplete ->
                    // DataRow/CommandComplete prelude. Await the decoder directly so a read wake resumes
                    // this outer body rather than a nested parser coroutine; the second message normally
                    // comes from the same batch and is consumed synchronously.
                    BackendMessage message;
                    if (!_decoder.TryGetNext(out message))
                    {
                        if (!await _decoder.MoveNextAsync().ConfigureAwait(false))
                            ThrowHelper.ThrowInvalidOperation("No more messages");
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
                                ThrowHelper.ThrowInvalidOperation("No more messages");
                            message = _decoder.Current;
                        }
                        message.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
                        _pgError = null;
                        _requestedRowDescription = null;
                    }
                }
                else if (IsAsync)
                {
                    var read = _options.Commands.ItemRef(_commandIndex).ReadUntilExecuteAsync(_decoder);
                    (_pgError, _requestedRowDescription) = await read.ConfigureAwait(false);
                }
                else
                    (_pgError, _requestedRowDescription) = _options.Commands.ItemRef(_commandIndex).ReadUntilExecute(_decoder);

                // On a consumer-gone drain, a command's ErrorResponse surfaces here (the response read hit
                // an ErrorResponse instead of the expected message) rather than via a CommandResult the
                // gone consumer would have enumerated. Capture it (Preserve()d) so the terminal surfaces it.
                // A command error lands in EITHER _pgError (read phase) OR CompleteError (completion phase),
                // never both, so capturing in both places does not double-count a single command's error.
                var capturedThisCommand = false;
                if (IsConsumingAutonomously && _pgError is { } readError)
                {
                    (_drainErrors ??= new()).Add(new PostgresException(readError));
                    capturedThisCommand = true;
                }

                // Dispose the registrations BEFORE the body returns (the awaited DisposeAsync sequences
                // this strictly before tenure release; it also waits out any in-flight callback). After
                // this, no callback can fire, so the next flow taking the shared promise never races one.
                if (_callerCancellationToken.CanBeCanceled)
                    await _callerCancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
                if (_flowCancellationToken.CanBeCanceled)
                    await _flowCancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
                // Cancel drain transition. The cancel was latched (by a registration callback, the eager-
                // write catch, or a token already fired at this point). Do NOT throw and do NOT complete
                // the source here (mid-body, tenured). Mirror the consumer-gone / StoppingToken drain:
                // mark terminal + consumer-gone, latch the OCE for terminal delivery, and fall through to
                // the wire drain to RFQ so the protocol stays usable. The terminal SetResult delivers the
                // OCE, sequenced with the body's return.
                if ((Volatile.Read(ref _cancelRequested) || EffectiveCancellationToken.IsCancellationRequested)
                    && !IsDraining && !_enumeratorCompleted)
                {
                    _cancelDeliverToken = EffectiveCancellationToken.IsCancellationRequested ? EffectiveCancellationToken : _cancelDeliverToken;
                    _deliverCancelOce = true;
                    _enumeratorCompleted = true;
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
                        descriptor = CommandDescriptor.CreatePrepared(descriptor.CommandName, descriptor.ParameterTypes, _requestedRowDescription);
                    }
                    result.Initialize(_commandIndex, descriptor, _requestedRowDescription, !resultCommand.DescribeOnly, resultCommand.IsSimple());
                }
                if (IsConsumingNonQuery)
                {
                    try
                    {
                        _options.OnCommandResultAction?.Invoke(result, _options.OnCommandResultActionState);
                    }
                    catch
                    {
                        // Result observers are advisory and must not interrupt protocol progress.
                    }
                }

                // Drain transition. IsDraining (Enumerator.Dispose) is the consumer's opt-out:
                // skip the user-handoff and drain the wire to RFQ ourselves, no fault on the source.
                // StoppingToken (graceful shutdown) is the protocol-close signal: the input-commands-
                // equals-output-results rule means we can't silently drop CommandResults while a
                // consumer is watching, so we fault the move-next source with the canonical
                // PgClientClosedException (OCE is reserved for the caller's own token), then fall
                // through to the same wire drain. No throw in the body. ErrorResponse failures are
                // NOT handled here; they flow through as failing CommandResults normally.
                if (context.StoppingToken.IsCancellationRequested && !IsDraining && !_enumeratorCompleted)
                {
                    // Latch the close (a consumer that Resets past this point self-delivers it), wake a
                    // parked consumer, then drain.
                    _callerInteractionCore.SetCloseLatch(context.ClosedException!);
                    DeliverClose(context.ClosedException!);
                    MarkConsumerGoneByBody();
                }
                if (!IsDraining && !IsConsumingNonQuery)
                {
                    // First result, ASYNC ONLY: re-establish the gate-ordered rendezvous the eager
                    // write removed. The write is now eager (so a batch's deferred flush is not stranded
                    // behind a consumer gate), but an async body runs CONCURRENTLY and would complete the
                    // move-next source before the consumer's first MoveNextAsync has Reset it and opened
                    // the gate - that Reset then clobbers the delivered result (silent drop) and races
                    // the non-concurrent SetResult (torn / lost-wake). Parking on the gate here restores
                    // the Reset(0)->gate-open(0)->SetResult(0) edge that rounds 2..N already get below.
                    // SYNC is exempt: its body runs on the caller's own MoveNext thread (no eager
                    // delivery, no concurrent Reset), and a pre-delivery unwind would hand control back
                    // to the caller before result1 exists (Sync_Completes hang). A consumer that disposes
                    // without consuming drives this gate via MoveNextAsync, so the body still drains.
                    if (_commandIndex is 0 && IsAsync)
                    {
                        await _callerInteractionCore.GetGateTask(this).ConfigureAwait(false);
                        HandleStoppingGate(context);
                    }

                    if (!IsDraining && !IsConsumingNonQuery)
                    {
                        _isResultReady = true;
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
                    // Reached the drain already draining without parking this transition = an in-flight body
                    // (resumed on a pool thread via async I/O) under a SYNC disposer (it set IsAsync=false).
                    if (WaitForDrainOnDispose)
                    {
                        // The disposer is PUMPING the rendezvous (await-drain). Hand off our continuation so it
                        // resumes us on ITS thread and the rest of the drain runs synchronously there - one
                        // thread. Safe: the disposer waits on the rendezvous mres, not the move-next task, so
                        // this suspend can't sync-over-async deadlock it. One-shot (_drainModeEntered below),
                        // so later commands drain straight through on the disposer's thread.
                        await SetContinuationAndUnblockWaiter();
                    }
                    else
                    {
                        // Fast-return dispose: no disposer waiting to take us over. Restore the bound async mode
                        // so we drain AUTONOMOUSLY in the background, reading async (releasing the pool thread
                        // between reads) instead of blocking it on sync socket I/O (the double-block).
                        IsAsync = IsAsyncAtBind;
                    }
                }
                // One-shot: record the drain transition so a sync takeover's later commands keep their
                // sync (IsAsync=false) drain instead of re-running the restore above and flipping to async.
                _drainModeEntered = _drainModeEntered || IsDraining;

                // We check IsAsync again as it can change after every resumption.
                // Current is disposed here, something the user might have done, but if not we'll do it here.
                // This also causes us to pick up any I/O exception thrown during user code that was stored on the resultmessage enumerator.
                // In drain mode this dispose IS the drain: it reads remaining DataRows + CommandComplete for the current command.
                (PgError Error, TransactionStatus TransactionStatus)? completeError;
                if (IsConsumingNonQuery)
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
                    // by a live consumer; direct nonquery consumption has taken that ownership even if it
                    // raced a result publication.
                    if ((IsConsumingNonQuery || IsDraining && !_isResultReady)
                        && !capturedThisCommand && completeError is { } err)
                        (_drainErrors ??= new()).Add(new PostgresException(err.Error));
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
                        PostgresException.Throw(rfqError);
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
        catch (PgClientClosedException ex) when (context.IsProtocolClosed)
        {
            // Scope to our own closure so a nested protocol's close doesn't get treated as ours.
            // Latch the close so a consumer that Resets after this point self-delivers it.
            _callerInteractionCore.SetCloseLatch(ex);
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
            HandleException(ex);
            throw;
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _callerCancellationToken || ex.CancellationToken == _flowCancellationToken)
        {

            // All of what happens below is complicated by the fact PG cancellation is not deterministic.
            // Ideally we want a mechanism to state "cancel if you are at this message and continue until you see this other one"
            // Postgres unfortunately only supports a non-contextual cancel request, cancelling whatever is running, viewed from the Postgres side of things.
            // This means that if commands execute fast, and the TCP window allowed the response to be flushed, we might just cancel whatever is coming after this flow.
            if (_commandIndex is 0)
            {
                // Issue backend cancel(s), this is an ExecuteReaderAsync call.
                // We have to issue as many cancels as there are syncs in this flow.
            }
            else
            {
                // This is a NextResultAsync call, we choose not to do anything here.
                // If we do want to cancel something we have to take into account a cancellation actually cancels a *transaction* and not just a single command.
            }

            HandleException(ex);
            throw;
        }
        catch (TimeoutException ex)
        {
            // Issue backend cancel(s), this is a timeout on I/O, luckily this means we have a fairly high certainty we cancel our own commands.
            // We have to issue as many cancels as there are syncs in this flow.
            HandleException(ex);
            throw;
        }
        catch (Exception ex)
        {
            HandleException(ex);
            throw;
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

                try
                {
                    _options.OnCommandResultAction?.Invoke(next!, _options.OnCommandResultActionState);
                }
                catch
                {
                    // TODO log.
                }
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
                PostgresException.Throw(rfqError);
        }

        static void ReadRfq(PgDecoder decoder)
        {
            var message = decoder.GetNext();
            if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                PostgresException.Throw(rfqError);
        }
    }

    /// Faults the caller-facing sources. Reached from the body's catch paths for in-body
    /// faults, and from OnComplete for failures the body never observed (dispatch-time throw,
    /// pre-body protocol fault). Callers are structurally sequential with each other - the
    /// framework completes an item only after its pipeline task settles, and OnComplete
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
    void RequestCancel(CancellationToken token)
    {
        _cancelDeliverToken = token;
        Volatile.Write(ref _cancelRequested, true);
        _callerInteractionCore.RequestWake();
    }

    // The terminal move-next completion (clean end, or user-cancel OCE), factored so it can run either
    // inline (when the body suspended at least once - off the dispatch frame) or be deferred to the
    // dispatch site (when the body ran fully synchronously on the inline frame, where the promise is
    // tenured). Every completer here is runContinuationsAsynchronously: true so the consumer continuation
    // / pipeline advance never runs inline on the completing stack.
    void DeliverTerminal()
    {
        // Drained ErrorResponses win: a Postgres error hit while draining is connection-state truth that
        // must surface (Npgsql parity), over a clean end or a user-cancel OCE. One error => a bare
        // PostgresException (the common case); several (a per-command-sync batch that faulted in multiple
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
        // bare PostgresException, several (multi-sync) => an AggregateException (ADO takes InnerExceptions[0]
        // if it only wants the first). WaitForComplete keys off the completion signal, which resolves
        // successfully even when the drain saw command errors - those are accumulated separately, surfaced
        // here so they escape await DisposeAsync().
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

    // Wake a sync caller parked in WaitForContinuation once the body's completion is recorded
    // (_enumeratorCompleted set by the caller). TWO waiters: a genuine sync flow (!IsAsync), and a sync
    // DISPOSER PUMPING the rendezvous for an async flow - it set _consumerDisposed before it parked. The
    // full fence pairs with the disposer's fence before it reads _enumeratorCompleted (the WakeHandshake,
    // machine-checked in WakeHandshake.tla): if it didn't see our completion it WILL see its own arm here,
    // so neither misses the other. Reached cross-thread when the body COMPLETES on its own pool thread
    // without handing off - an in-flight read that faults (HandleException) or, under a concurrent protocol
    // close, takes the IsProtocolClosed catch -> SetResult(null) -> DeliverTerminal. LOAD-BEARING and
    // effectively UNTESTABLE: the trigger is a sub-microsecond visibility window (a stale IsAsync read
    // racing the disposer's IsAsync=false), so no test reliably reddens if the fence is dropped - do NOT
    // remove it.
    void WakePumpOnCompletion()
    {
        Interlocked.MemoryBarrier();
        if (!IsAsync || Volatile.Read(ref _consumerDisposed))
            _callerInteractionCore.SignalProgress();
    }

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
        // never-started path (OnComplete -> HandleException, body never ran so no body-side latch set)
        // is the one that strands an orphaned generation otherwise.
        if (ex is PgClientClosedException closeEx)
            _callerInteractionCore.SetCloseLatch(closeEx);
        if (_enumeratorCompleted)
            return;
        // Mark terminal only when the fault actually lands. A concurrent teardown TrySetException
        // no-ops if it targets an already-completed generation - e.g. the abort faulted the inter-
        // result gate and this fault hits the already-CONSUMED prior generation. Setting
        // _enumeratorCompleted on that no-op would make the consumer's next MoveNext short-circuit onto
        // the stale consumed result; leaving it false lets that call fall through to its own faulted-
        // gate self-delivery, which completes the consumer's actual generation.
        // HandleException can fire from teardown (OnComplete/OnAbort/OnStopping) on the heartbeat or
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
    protected override ManualResetEventSlim? GetHandoffMres() => _callerInteractionCore.GetMres();

    // Return the rendezvous awaitable DIRECTLY (NOT via an async ValueTask wrapper). The wrapper added a
    // second await level whose inner OnCompleted set _mres (unblocking the disposer) BEFORE the body had
    // registered its continuation on the wrapper's ValueTask. So under TP contention the disposer could
    // complete that ValueTask before the body registered, and the body's late UnsafeOnCompleted on the
    // already-completed source was dispatched to the ThreadPool - the body bounced off the disposer's thread
    // and ran its SYNC drain on a foreign pool thread (the double-block). Awaiting the awaitable directly
    // captures the BODY's MoveNext as _continuation in the SAME OnCompleted that sets _mres, so the disposer
    // always finds it and invokes the body's MoveNext INLINE on its own thread. No intermediate ValueTask.
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

    protected override void OnAbort(PgClientClosedException exception) => FaultCaller(exception);

    // Same wire-death verdict as OnAbort (graceful-stopping fires first in the graceful Shutdown path, so
    // this is the earlier wake). Idempotent across ticks.
    protected override void OnStopping(PgClientClosedException exception)
    {
        if (!_bodyStarted || !IsAsync)
        {
            FaultCaller(exception);
            return;
        }

        // A graceful stop resumes the gate normally. The body observes the close latch immediately after
        // the await and switches to drain; abort still faults the gate and escapes through the body catch.
        _callerInteractionCore.SetCloseLatch(exception);
        _callerInteractionCore.OpenGate(runContinuationsAsynchronously: true);
    }

    // The close verdict, both reach-paths in one place. A RUNNING body is woken (TrySet on the gate source)
    // so it exits through its own HandleException - the single writer once the body started. A NEVER-RAN
    // flow (drained from the backlog, body never started) has no body to wake, so deliver to the caller
    // directly. This is the never-ran fault delivery that used to live in OnComplete; it is the same event.
    void FaultCaller(PgClientClosedException exception)
    {
        if (_bodyStarted)
            _callerInteractionCore.CancelPendingWait(exception);
        else
            HandleException(exception);
    }

    protected override void OnExecutionCompleted(Exception? exception)
        => _options.Commands.Return();

    protected override void OnDiscarded()
        => _options.Commands.Return();

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
        _cancelDeliverToken = default;
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

            // The guard-decide-rearm is serialized against the body's terminal (see _rearmLock): the using
            // scope covers the _enumeratorCompleted read through the move-next Reset, and ends before the
            // gate dispatch below (which runs the body inline and would re-enter this non-reentrant lock).
            // try/finally release keeps it lock-safe on any throw.
            using (flow._rearmLock.EnterScope())
            {
                // Terminal short-circuit. _enumeratorCompleted is version-less and survives a Reset, so a
                // terminal landed on a PRIOR generation leaves it set while the consumer is parked on a
                // fresh, pending generation with no completer (the lost-completion). Repair it for the close
                // and user-cancel causes (see RepairLostTerminal); otherwise this returns the terminal task
                // unchanged (its GetResult replays the result/fault).
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

                // Override only with a cancelable per-read token: a no-token / None MoveNext keeps the
                // token bound at submit (or GetAsyncEnumerator) for this read, never clobbering it with
                // None. A cancelable token also arms the concurrent completer for this tenure.
                if (cancellationToken is { } ct && ct.CanBeCanceled)
                {
                    flow._callerCancellationToken = ct;
                    flow._enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
                }

                // Rearm only on a non-first call. The first-call source is fresh from OnReset and the body's
                // first delivery lands on it; Resetting here would rearm past that delivery. ALSO skip the
                // rearm once the CONSUMER DISPOSED (DisposeAsync drives the drain): the body's terminal is
                // ONE-SHOT and completes the CURRENT generation; resetting to a fresh one parks the disposer on
                // a generation the terminal can never reach (the disposer-reset-vs-one-shot-terminal hang). The
                // drain only needs to drive the body (gate-open below) and observe its terminal on the
                // generation it is already awaiting. Gate on _consumerDisposed, NOT the broader IsDraining: a
                // BODY-initiated drain (MarkConsumerGoneByBody, e.g. a graceful gate-fault) leaves the consumer
                // LIVE and looping - it MUST rearm to a fresh generation so the close-latch self-deliver below
                // has a pending generation to complete, else it re-yields the consumed generation (hang).
                if (flow._consumerAdvanced && !Volatile.Read(ref flow._consumerDisposed))
                    flow._enumeratorMoveNextTaskSource.Reset();
                flow._consumerAdvanced = true;
            }
            // Open the gate to drive the body forward, OR self-deliver the close under close. Under close
            // the body parked on the inter-result gate is woken by the consumer's gate-open (the always-
            // present, heartbeat-independent waker - the heartbeat's CancelPendingWait is an optimization
            // on top). A false TrySetResult means the gate was already faulted by a teardown.
            flow._callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
            // Close-latch self-deliver: under close THIS call is the sole completer of the generation it
            // just armed - self-deliver here, ordered after our Reset, so the live generation always has
            // a completer. Idempotent against a racing body writer (CAS). Read the latch AFTER the Reset.
            if (flow._callerInteractionCore.CloseException is { } closed)
                flow.DeliverClose(closed);
            return flow.EnumeratorMoveNextTask;
        }

        internal ValueTask<long> ConsumeNonQueryAsync()
        {
            if (flow is null)
                return new(0);

            Volatile.Write(ref flow._consumeNonQuery, true);
            // Cover both sides of the first-result race: release a body already parked on the gate;
            // a body observing the mode first skips publication and the gate altogether.
            flow._callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
            flow._callerInteractionCore.RequestWake();
            return AwaitCompletion(flow);

            [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
            static async ValueTask<long> AwaitCompletion(CommandFlow flow)
            {
                _ = await flow.EnumeratorMoveNextTask.ConfigureAwait(false);
                return flow._nonQueryRecordsAffected;
            }
        }

        /// <inheritdoc cref="ISyncAsyncEnumerator{T}.Current" />
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

            // Async-mode body, sync teardown - a TWO-WAY RENDEZVOUS that drains on THIS (the disposer's)
            // thread. Present as a synchronous driver (IsAsync=false), then:
            //   - park-before-open: a body PARKED at the inter-result gate is taken over INLINE by the gate's
            //     synchronous completion below; it re-reads IsAsync=false and drains straight through, here.
            //   - open-before-park / in-flight: the body hands off its continuation at a drain rendezvous
            //     point (the sync-rendezvous park, or the skip-gate once its in-flight async read completes);
            //     we pump those handoffs below and run each INLINE, on this thread.
            // The pump waits on the RENDEZVOUS mres (WaitForContinuation), NOT the move-next task. That is the
            // whole trick: a MoveNext drive blocks on task.GetResult (sync-over-async) and deadlocks against
            // the body's own in-flight await - the body completes the rendezvous, not the task, so the disposer
            // must wait on the same signal the body raises. A fast-return (non-await-drain) dispose has no
            // pump; it wakes the body to drain autonomously in the background and returns.
            if (flow.IsAsync)
            {
                flow.IsAsync = false;
                if (flow.WaitForDrainOnDispose)
                    flow.MarkConsumerWaitForDrain();
                else
                    flow.MarkConsumerGone();
                // INLINE (runContinuationsAsynchronously: false): a gate-parked body resumes + drains here.
                // Buffered (no-op) if the body isn't gate-parked.
                flow._callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
                if (flow.WaitForDrainOnDispose)
                {
                    // Pump the rendezvous: take each continuation the body hands off and run it inline, until
                    // the body completes (terminal sets _enumeratorCompleted inline during the Invoke below;
                    // the null return is the cross-thread safety - a terminal/fault SignalProgress wakes us
                    // with no continuation). No task.GetResult anywhere = no sync-over-async deadlock.
                    // Fence the consumer-gone arm (MarkConsumerWaitForDrain above) against the completion read,
                    // paired with HandleException's fence: if the body already faulted before we got here we
                    // see _enumeratorCompleted and never park; otherwise the body sees our arm and signals us.
                    Interlocked.MemoryBarrier();
                    while (!Volatile.Read(ref flow._enumeratorCompleted))
                    {
                        var continuation = flow._callerInteractionCore.WaitForContinuation();
                        if (continuation is null)
                            break;
                        continuation.Invoke();
                    }
                    // Drain ran on this thread; surface accumulated drain errors (completes immediately).
                    flow.AwaitDrainOnDispose().AsTask().GetAwaiter().GetResult();
                }
                else
                {
                    // Fast-return: wake the body to drain autonomously in the background, then return.
                    flow._callerInteractionCore.RequestWake();
                }
                return;
            }

            // Consumer-gone: the body stops yielding and drains the wire itself. The sync rendezvous
            // via WaitForContinuation drives it forward - MoveNext invokes the body's stored
            // continuation, the body observes IsDraining and drains, and its terminal
            // SetResult(null) wakes the final WaitForContinuation via SignalProgress.
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
                // Already terminal. Under WaitForDrainOnDispose, the drain may have completed WITH errors
                // before the consumer got here; surface them (WaitForComplete returns immediately, then
                // AwaitDrainOnDispose throws the accumulated error). Otherwise nothing to do.
                return flow.WaitForDrainOnDispose ? flow.AwaitDrainOnDispose() : new();

            // Fault and RETURN - do NOT drive or await the wire drain. The body drains autonomously:
            // a read-parked body resumes on bytes; a gate-parked body, once woken, sees IsDraining
            // and skips the handoff to drain to RFQ. The pipeline retires the item only after the body's
            // read settles, so the next flow still finds RFQ - that guarantee comes from item lifetime,
            // not from Dispose blocking on it.
            //   1. mark consumer-gone (the body's !IsDraining guards then skip the handoff gates),
            //   2. open the async gate - the SOLE async-gate waker; RequestWake cannot reach the gate,
            //      so a body parked on the inter-result gate hangs without this,
            //   3. RequestWake to wake a sync-suspended body (TP-queues its stored continuation).
            // The body's autonomous terminal completes the move-next source (clean end), waking any
            // consumer still mid-await. No drive loop, no DisposeAsyncCore await.
            if (flow.WaitForDrainOnDispose) flow.MarkConsumerWaitForDrain(); else flow.MarkConsumerGone();
            flow._callerInteractionCore.OpenGate(runContinuationsAsynchronously: false);
            flow._callerInteractionCore.RequestWake();
            // Opt-in await-drain: PARK on the body's completion signal (TCS-backed WaitForComplete), so
            // the wire is drained to RFQ before this returns. This WAITS on the body, it does not DRIVE
            // it - no poll loop, so no busy-spin. Bounded by the flow/enumerator token: if the caller's
            // token has fired they no longer want to wait, so we unwind FAST. The body stays consumer-gone
            // and keeps draining autonomously in the background regardless - we just stop waiting on it.
            // Default (false) returns immediately and lets the body drain in the background.
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

        public ReadState()
        {
            ResultMessageEnumerator = new();
            CommandResult = new(ResultMessageEnumerator);
            ReadPromise = new();
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
                    if (decoder.Current.Header.Type is PgTypes.BackendType.DataRow)
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
                _disposed = true;
                try
                {
                    var decoder = _decoder;
                    if (decoder.Current.Header.Type is not PgTypes.BackendType.DataRow)
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
