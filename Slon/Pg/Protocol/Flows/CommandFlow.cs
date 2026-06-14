using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;
// We use a type that is already used in Execute.Read so we can share the field.
using FlowCallerInteractionCoreResult = (Slon.Pg.Protocol.PgError, Slon.Pg.RowDescription);

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
    CancellationToken _callerCancellationToken;
    CancellationTokenRegistration _callerCancellationTokenRegistration;

    // Consumer-gone signal: set only by the enumerator's Dispose/DisposeAsync - those are the
    // sole "I'm abandoning the result stream" assertions. The caller's CancellationToken is NOT
    // a consumer-gone signal: a fired CT may just mean "skip this result" or "stop waiting for
    // this row" with the consumer still interested in subsequent results (NextResult, the next
    // command's output). The body reads this at handoff boundaries (next commit will wire the
    // drain transition). Deliberately not exposed as a CancellationToken - a public CT would
    // invite I/O ops to register on it and any wire read could throw OCE, breaking the "drain
    // wire to clean state" guarantee the signal exists to enable.
    bool _consumerGone;
    internal bool IsConsumerGone => Volatile.Read(ref _consumerGone);
    void MarkConsumerGone() => Volatile.Write(ref _consumerGone, true);

    // Result state
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _enumeratorMoveNextTaskSource;
    CommandResult<ResultMessageEnumerator>? _enumeratorCurrent;
    RowDescription? _requestedRowDescription;
    PgError? _pgError;
    int _commandIndex = -1;
    bool _enumeratorCompleted;
    bool _isResultReady;
    PgDecoder? _decoder;
    bool _readFlowRfq;

    // Pipelined dispatch state. Lives here (not on PgClientFlow base) because the shared-promise
    // optimization that needs these fields is CommandFlow-specific. See DispatchPipelinedRead
    // for why. Other flows that pipeline use the local-function form with their own per-instance
    // promise via PromiseAsyncValueTaskMethodBuilder.BeginCallScope.
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _executePipelinedCore;
    ValueTaskSourcePromise<bool>? _pipelinePromise;
    Context _context;
    ValueTask _task;
    // TRUE once ExecutePipelined's state machine has begun running; from that point the
    // body's catch paths own caller-facing fault propagation (see OnComplete).
    bool _bodyStarted;

    ValueTask<bool> EnumeratorMoveNextTask => new(this, _enumeratorMoveNextTaskSource.Version);

    CommandFlow() : base(supportsPipelining: true)
    {
        _callerInteractionCore.Initialize();
    }

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
        return this;
    }

    public int CommandCount => _options.Commands.Count;
    public bool IsResultReady => _isResultReady;

    public Enumerator GetEnumerator()
    {
        return new Enumerator(this);
    }

    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        _callerCancellationToken = cancellationToken;
        // A cancelable token arms the only truly concurrent completer of the move-next source
        // (the cancellation registration completes it from an arbitrary thread while the body
        // keeps reading). Pay for thread-safe completion only then; never disarm mid-tenure -
        // a fired registration may still be in flight. Reset clears it.
        if (cancellationToken.CanBeCanceled)
            _enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
        return new(this);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    protected override async ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        ValueTask writeTask;
        try
        {
            if (IsAsync)
            {
                await _callerInteractionCore.GetGateTask(this).ConfigureAwait(false);
            }
            else
            {
                // There is a small window where an executor already on a separate thread races to this point before the caller can set IsWaiting.
                // So we'll always do an unwind later on *if we didn't do it here* to make sure we won't stall the executor past writing.
                // (another approach is a thread static to identify whether the executor is on the caller thread, but it's messy).
                if (_callerInteractionCore.IsWaiting)
                {
                    // We're on the executor thread, unwind to let the caller do its own writes.
                    // The executor task will properly await, freeing up its thread to do other work.
                    await SetContinuationAndUnblockWaiter().ConfigureAwait(false);
                }
            }

            // All writes captured as a single task. If the writes back-pressure (TCP send
            // buffer full, large batch saturating local+peer windows), this task is pending
            // and the framework awaits it inline between iterations to prevent the next
            // item's writes from racing past. If writes fit in the buffer, the task is
            // sync-completed and the framework's await is a free no-op.
            //
            // Async flows take the *Async path (TP completion via SocketAsyncEngine plus the
            // adaptive batching scheduler). Sync flows take the *Resumable path (sync
            // syscalls on a non-blocking socket, WouldBlock suspends the coroutine, the
            // readiness driver fires the writable signal to resume). The SignalScope places
            // the encoder's cached writable signal in the transport's TLS slot for the
            // duration of the call. The transport captures the signal into its coroutine on
            // WouldBlock, so the scope can close after the initial call returns without
            // disturbing the suspended state machine.
            // See https://www.postgresql.org/message-id/CADT4RqAH2nuVwM6cEugFL2z6apwXfP3OJb=zxR6jRgWEpx_2Ww@mail.gmail.com
            // https://github.com/pgjdbc/pgjdbc/issues/194
            // https://github.com/npgsql/npgsql/issues/641
            var encoder = context.GetEncoder();
            if (IsAsync)
            {
                writeTask = WriteAllCommandsAsync();
            }
            else
            {
                using (encoder.BeginResumableScope())
                    writeTask = WriteAllCommandsResumable();
            }

            // If the writes already completed observe them here so a synchronously-faulted
            // task throws inside this try block and goes through HandleException. Without
            // this, a sync-thrown error would only be observed later via the framework's
            // trailing-task await, missing this scope's error handling. Same concern in both
            // async and sync paths.
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

        // Return writeTask as the trailing slot so the framework awaits it inline between
        // iterations for transport-level backpressure (TCP-window-deadlock prevention).
        // Ordering between pipeline-task completion and trailing observation is enforced by
        // the framework's tail-waiter routing. CompleteItem fires only after trailing has
        // been observed regardless of when the pipeline task completes. Write failures are
        // treated as transport death (ASP.NET-style). The next operation discovers a broken
        // connection, no per-flow error bubbling needed.
        return new FlowTasks(
            trailingExecutionTask: writeTask,
            pipelineTask: DispatchPipelinedRead(context, context.GetProtocolStatic<ReadState>().ReadPromise));

        async ValueTask WriteAllCommandsAsync()
        {
            var encoder = context.GetEncoder();
            for (var i = 0; i < CommandCount; i++)
                await _options.Commands[i].WriteAsync(encoder, _callerCancellationToken).ConfigureAwait(false);

            if (!encoder.LastMessageInducesRfq)
            {
                _readFlowRfq = true;
                encoder.WriteSync();
            }
            await encoder.FlushAsync(_callerCancellationToken).ConfigureAwait(false);
        }

        async ValueTask WriteAllCommandsResumable()
        {
            var encoder = context.GetEncoder();
            for (var i = 0; i < CommandCount; i++)
                await _options.Commands[i].WriteResumable(encoder).ConfigureAwait(false);

            if (!encoder.LastMessageInducesRfq)
            {
                _readFlowRfq = true;
                encoder.WriteSync();
            }
            await encoder.FlushResumable().ConfigureAwait(false);
        }
    }

    // The reason this whole mechanism exists rather than just `using var _ = BeginCallScope(...);
    // return ReadPhase();` like other pipelined flows:
    //
    // CommandFlow uses a per-protocol SHARED ValueTaskSourcePromise (via ReadState.ReadPromise).
    // One promise instance serves every CommandFlow queued on this protocol. The natural local-
    // function form would eagerly Create the state machine in each flow, each capturing the
    // promise. Two state machines pointing at the same promise is a state conflict, so the
    // sharing breaks.
    //
    // The deferred dispatch here doesn't Create the state machine until activation fires, so only
    // ONE flow at a time pulls the promise from TLS and constructs. Multiple flows can queue up
    // referencing the same promise. Each one Creates only at its turn. N-1 promise allocations
    // saved per protocol under pipelined load.
    //
    // External flow authors can't easily replicate this (would need a CWT or similar hosting slot
    // and pay the lookup cost on every dispatch), so the optimization is structurally protocol-
    // package-internal. Hence "live here" rather than as a framework helper on Context.
    ValueTask DispatchPipelinedRead(Context context, ValueTaskSourcePromise<bool> promise)
    {
        var waiter = context.GetDecoderAuto().ConfigureAwait(false);
        if (waiter.IsCompleted)
        {
            // Handing the shared-promise-backed task to the framework is safe: the framework
            // contract guarantees the waiter task is consumed (releasing the promise tenure
            // via GetResult's Reset) before the item's pipeline position is republished to
            // the executor's inline-activation gate. A successor's dispatch therefore always
            // finds the tenure released.
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
        waiter.OnCompleted(static state =>  // ConfigureAwait(false): the continuation is a static bridge into framework state, no scheduling context needed

        {
            var flow = (CommandFlow)state!;
            var promise = flow._pipelinePromise!;
            var ctx = flow._context;
            PromiseAsyncValueTaskMethodBuilder.Promise = promise;
            try
            {
                var task = flow.ExecutePipelined(ctx);
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
                    }, flow, promise.Token, ValueTaskSourceOnCompletedFlags.FlowExecutionContext);
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
            await SetContinuationAndUnblockWaiter().ConfigureAwait(false);

        try
        {
            // GetDecoderAuto adapts to flow mode internally. Activation has already been gated
            // by DispatchPipelinedRead's waiter.IsCompleted check, so the fast path returns
            // sync-immediately in both modes. The await is free when sync-completed.
            _decoder = await context.GetDecoderAuto(_callerCancellationToken).ConfigureAwait(false);
            while (++_commandIndex < CommandCount)
            {
                _isResultReady = false;
                ref readonly var command = ref _options.Commands.ItemRef(_commandIndex);
                _decoder.ReadTimeout = command.Timeout;

                // CancellationTokens from callers just cancels their enumerator task, we don't cancel I/O unless the timeout hits.
                // As we execute in a pipeline we must make sure harmless cancellations don't unnecessarily abort the protocol (and already pipelined flows with it).
                // The only way we can successfully process those flows is by consuming all data meant for the current flow, which means waiting for I/O.
                // As long as the server is sufficiently responsive we'll handle all consumption without caller interaction and complete the current flow.
                if (_callerCancellationToken.CanBeCanceled)
                {
                    Debug.Assert(IsAsync);
                    _callerCancellationTokenRegistration = _callerCancellationToken.UnsafeRegister(static (state, token)
                        => ((CommandFlow)state!)._enumeratorMoveNextTaskSource.TrySetException(new OperationCanceledException(token)), this);
                }

                command = ref _options.Commands.ItemRef(_commandIndex);
                if (IsAsync)
                    (_pgError, _requestedRowDescription) = await command.ReadUntilExecuteAsync(_decoder).ConfigureAwait(false);
                else
                    (_pgError, _requestedRowDescription) = command.ReadUntilExecute(_decoder);

                if (_callerCancellationToken.CanBeCanceled)
                {
                    // Afterwards we check whether the token was canceled so we can cleanup as well.
                    _callerCancellationToken.ThrowIfCancellationRequested();
                    await _callerCancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
                }

                ref readonly var state = ref context.GetProtocolStatic<ReadState>();
                state.ResultMessageEnumerator.Initialize(this, _decoder);
                var result = _enumeratorCurrent ?? state.CommandResult;

                command = ref _options.Commands.ItemRef(_commandIndex);
                var descriptor = command.Descriptor;
                {
                    // We were preparing and we have no error from parse, make a prepared descriptor.
                    if (!descriptor.IsPrepared && !descriptor.CommandName.IsDefault
                        && (_pgError is not { } err || !err.Expected.Contains(PgTypes.BackendType.ParseComplete)))
                    {
                        descriptor = CommandDescriptor.CreatePrepared(descriptor.CommandName, descriptor.ParameterTypes, _requestedRowDescription);
                    }
                }
                result.Initialize(_commandIndex, descriptor, _requestedRowDescription, !command.DescribeOnly, command.IsSimple());

                _isResultReady = true;
                SetResult(result);

                if (IsAsync)
                    await _callerInteractionCore.GetGateTask(this).ConfigureAwait(false);
                else
                    await SetContinuationAndUnblockWaiter().ConfigureAwait(false);

                /* The next MoveNext or MoveNextAsync call resumes here. */

                // TODO if it's not disposed yet we should capture any exceptions and set it on MoveNext task source
                // TODO e.g. with the message "Previous command result was not disposed and completed with an exception, see inner exception for more details.".
                // TODO we can just await _callerInteractionCore as the next movenext should be clean.
                // TODO unless we conclude there is no exception that could come from current that we don't consider critical (e.g. flow abort).

                // We check IsAsync again as it can change after every resumption.
                // Current is disposed here, something the user might have done, but if not we'll do it here.
                // This also causes us to pick up any I/O exception thrown during user code that was stored on the resultmessage enumerator.
                state = ref context.GetProtocolStatic<ReadState>();
                if (IsAsync)
                    await state.ResultMessageEnumerator.DisposeAsync().ConfigureAwait(false);
                else
                    state.ResultMessageEnumerator.Dispose();

                {
                    state = ref context.GetProtocolStatic<ReadState>();
                    if (state.ResultMessageEnumerator.CompleteError is { } err)
                    {
                        if (err.TransactionStatus is TransactionStatus.Error)
                        {
                            // Complete commands with the error until we reach a sync.
                            // If we still read TransactionStatus.Error we complete until the next sync, and so on.
                            // If we end up at TransactionStatus.Idle we reached a rollback and can resume from there.
                            // If not we completed our flow, the protocol will see the last transaction status and handle it from there.
                        }
                        else
                        {
                            // Here we just have to complete commands with the error until we reach a sync, we can continue from there.
                        }
                    }
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
            // Scope the catch to our own closure so a nested protocol's closed exception
            // bubbling through doesn't get treated as ours. HandleException signals the
            // consumer's pending MoveNextTask before rethrow, otherwise the consumer
            // stays parked forever even after the executor's recovery path runs.
            HandleException(ex);
            throw;
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _callerCancellationToken)
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

            _enumeratorCompleted = completed;
            _enumeratorMoveNextTaskSource.SetResult(!completed, runContinuationsAsynchronously: completed);
            // For completion (completed=true) the body is done and will not register a follow-up
            // continuation, so we must wake any parked sync MoveNext directly. For result
            // delivery (completed=false) the body's next statement is
            // await SetContinuationAndUnblockWaiter, which stores the next continuation and
            // signals the mres. Signaling here would race ahead of that and the caller's MoveNext
            // would observe a "no continuation" wake even though one is about to be stored.
            if (!IsAsync && completed)
                _callerInteractionCore.SignalProgress();
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
    void HandleException(Exception ex)
    {
        if (_enumeratorCompleted)
            return;
        // Mark the flow terminally completed so the caller's next MoveNext returns the cached
        // task result (which throws the exception) instead of resetting the source and parking.
        _enumeratorCompleted = true;
        // We have to make sure to unblock the caller if the flow failed to (could be a cancellation, io error, etc).
        if (_enumeratorMoveNextTaskSource.CanCompleteConcurrently)
            _enumeratorMoveNextTaskSource.TrySetException(ex, runContinuationsAsynchronously: true);
        else
            _enumeratorMoveNextTaskSource.SetException(ex, runContinuationsAsynchronously: true);
        // Wake any sync MoveNext parked in WaitForContinuation. The body just faulted, so no
        // continuation will be registered. The caller needs to observe the exception via the
        // move-next task source.
        if (!IsAsync)
            _callerInteractionCore.SignalProgress();
        // TODO use tryrecover to consume as many rfqs as we have left at the time of throwing.
        // TODO This essentially replaces manually having to consume the flow remainder on unexpected errors (e.g. unhandled protocol errors).
    }

    protected override void OnComplete(Exception? exception)
    {
        if (exception is null)
            return;
        // Once the body has started, its own catch paths own the caller-facing fault -
        // faulting here too would be a concurrent writer pair on the move-next source.
        // When the flow failed without the body ever running (parked deferred dispatch,
        // dispatch-time throw, pre-body protocol fault), no body will ever fault the caller:
        // do it here, as the single writer (the parked bridge is never invoked for completed
        // flows; Reset clears its registration on reuse).
        if (_bodyStarted)
            return;
        HandleException(exception);
    }

    // Set-up field ref on demand, most flows will be asynchronous and never need this.
    // Also wrap this in another async method so the awaiter field can be shared given this is a sync only thing.
    async ValueTask SetContinuationAndUnblockWaiter()
    {
        FieldRef<FlowCallerInteractionCore<FlowCallerInteractionCoreResult>> fieldRef;
        unsafe
        {
            fieldRef = FieldRef<FlowCallerInteractionCore<FlowCallerInteractionCoreResult>>.Create(&GetCallerInteractionCore, this);
        }
        await _callerInteractionCore.SetContinuationAndUnblockWaiter(fieldRef);
    }

    static ref FlowCallerInteractionCore<FlowCallerInteractionCoreResult> GetCallerInteractionCore(CommandFlow instance)
        => ref instance._callerInteractionCore;

    protected override void OnAbort(PgClientClosedException exception)
        => _callerInteractionCore.CancelPendingWait(exception);

    protected override void OnReset()
    {
        Debug.Assert(IsPending || IsCompleted);
        _commandIndex = -1;
        _executePipelinedCore.Reset();
        _enumeratorMoveNextTaskSource.Reset();
        // Back to single-threaded completion; the next tenure re-arms it if it binds a
        // cancelable token.
        _enumeratorMoveNextTaskSource.CanCompleteConcurrently = false;
        _enumeratorCurrent = default;
        _isResultReady = false;
        _callerInteractionCore.Reset();
        _callerCancellationToken = default;
        _callerCancellationTokenRegistration.Dispose();
        _callerCancellationTokenRegistration = default;
        _consumerGone = false;
        // Dispatch state is per-tenure.
        _pipelinePromise = null;
        _context = default;
        _task = default;
        _bodyStarted = false;
    }

    FlowCallerInteractionCoreResult IValueTaskSource<FlowCallerInteractionCoreResult>.GetResult(short token)
    {
        var result = _callerInteractionCore.GateTaskSource.GetResult(token);
        _callerInteractionCore.GateTaskSource.Reset();
        return result;
    }

    ValueTaskSourceStatus IValueTaskSource<FlowCallerInteractionCoreResult>.GetStatus(short token)
        => _callerInteractionCore.GateTaskSource.GetStatus(token);

    void IValueTaskSource<FlowCallerInteractionCoreResult>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _callerInteractionCore.GateTaskSource.OnCompleted(continuation, state, token, flags);

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
            // Default value.
            if (flow is null)
                return false;

            // This may also throw any recorded exception.
            if (flow._enumeratorCompleted)
                return flow.EnumeratorMoveNextTask.Result;

            if (flow.IsAsync)
            {
                if (flow._enumeratorCurrent is null)
                    ThrowHelper.ThrowInvalidOperation("No immediate sync/async mixing is allowed, the first MoveNext{Async} call has to match the async argument passed during initialize.");
                flow.IsAsync = false;
            }

            // We reset the source here to mirror the async side.
            flow._enumeratorMoveNextTaskSource.Reset();
            // Two wake reasons: a continuation was registered (drive the body forward inline)
            // or the body signaled progress (a result, completion, or fault landed on the
            // move-next task source while we were parked). In the progress-only case there is
            // no continuation to invoke. The task is already complete.
            var continuation = flow._callerInteractionCore.WaitForContinuation();
            continuation?.Invoke();
            var task = flow.EnumeratorMoveNextTask;
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
            // Default value.
            if (flow is null)
                return new(false);

            // This may also throw any recorded exception.
            if (flow._enumeratorCompleted)
                return flow.EnumeratorMoveNextTask;

            if (!flow.IsAsync)
            {
                if (flow._enumeratorCurrent is null)
                    ThrowHelper.ThrowInvalidOperation("No immediate sync/async mixing is allowed, the first MoveNext{Async} call has to match the async argument passed during initialize.");
                flow.IsAsync = true;
            }

            // We only set it if it's not null, as we don't want to overwrite the initial GetEnumerator token until the first result is returned.
            if (cancellationToken is not null)
            {
                flow._callerCancellationToken = cancellationToken.GetValueOrDefault();
                // Same as GetAsyncEnumerator: a cancelable token arms the concurrent
                // completer, so completion must become thread-safe for this tenure.
                if (flow._callerCancellationToken.CanBeCanceled)
                    flow._enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
            }

            // We reset the source ourselves in case the flow isn't waiting yet, as we must return a fresh task.
            flow._enumeratorMoveNextTaskSource.Reset();
            flow._callerInteractionCore.GateTaskSource.SetResult(default);
            return flow.EnumeratorMoveNextTask;
        }

        /// <inheritdoc cref="ISyncAsyncEnumerator{T}.Current" />
        public CommandResult Current => flow?._enumeratorCurrent ?? default!;

        /// <inheritdoc />
        public void Dispose()
        {
            // Default value.
            if (flow is null)
                return;

            // If the flow already terminally completed, there's nothing to drain. Skipping
            // also avoids re-throwing a previously-observed fault from EnumeratorMoveNextTask.Result
            // during foreach's exception unwind.
            if (flow._enumeratorCompleted)
                return;

            // Consumer-gone: tells the body to stop yielding new results and drain the wire on
            // its own (next commit will wire the body observation). Until then this is just a
            // signal recorded for the body to read at its next boundary.
            flow.MarkConsumerGone();
            while (MoveNext())
                Current.Dispose();
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            // Default value.
            if (flow is null)
                return new();

            flow.MarkConsumerGone();
            ValueTask<bool> task;
            while ((task = MoveNextAsync()).IsCompletedSuccessfully)
            {
                if (!task.Result)
                    return new();
            }

            return DisposeAsyncCore(task);
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask DisposeAsyncCore(ValueTask<bool> task)
        {
            await task.ConfigureAwait(false);

            // Flow cleans up the result every time, we do the absolute minimum here.
            while (await MoveNextAsync().ConfigureAwait(false))
            {
            }
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

            [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder))]
            public async ValueTask DisposeAsync()
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
                        while (true)
                        {
                            if (decoder.TryGetNext(out var message) && message.Header.Type is not PgTypes.BackendType.DataRow)
                                break;

                            message = await decoder.GetNextAsync().ConfigureAwait(false);
                            if (message.Header.Type is not PgTypes.BackendType.DataRow)
                                break;
                        }
                    }
                    _completeError = await Command.CompleteAsync(_decoder).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                    throw;
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
