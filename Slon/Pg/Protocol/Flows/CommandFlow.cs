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
        return new(this);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    protected override async ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
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

            var encoder = context.GetEncoder();
            for (var i = 0; i < CommandCount; i++)
                await _options.Commands[i].WriteAuto(encoder, _callerCancellationToken).ConfigureAwait(false);

            if (!encoder.LastMessageInducesRfq)
            {
                _readFlowRfq = true;
                encoder.WriteSync();
            }
            await encoder.FlushAuto(_callerCancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            HandleException(ex);
            throw;
        }

        // TODO put writing past some point on a write task handled by the executor so it won't deadlock.
        // See https://www.postgresql.org/message-id/CADT4RqAH2nuVwM6cEugFL2z6apwXfP3OJb=zxR6jRgWEpx_2Ww@mail.gmail.com
        // https://www.postgresql.org/message-id/E1YPrLl-0001qZ-5g%40gemulon.postgresql.org
        // https://github.com/postgres/postgres/commit/2a3f6e368babdac7b586a7d43105af60fc08b1a3#diff-f841acf02862af937cc11c55df1c8ae3e8db81dd16ea0c0e0c7ead5f404e796cR915
        // https://github.com/pgjdbc/pgjdbc/issues/194
        // https://github.com/npgsql/npgsql/issues/641
        _writeTask = new ValueTask();
        return DispatchPipelinedRead(context, context.GetProtocolStatic<ReadState>().ReadPromise);
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
        var waiter = context.GetDecoderAsync().ConfigureAwait(false);
        if (waiter.IsCompleted)
        {
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
        // If we have a continuation stored we must already be on the caller thread,
        // otherwise we must make sure to unblock the executor (see comment in the write phase).
        if (!IsAsync && !_callerInteractionCore.HasContinuation)
            await SetContinuationAndUnblockWaiter().ConfigureAwait(false);

        try
        {
            _decoder = await context.GetDecoderAsync(_callerCancellationToken).ConfigureAwait(false);
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
                var task = command.ReadUntilExecuteAuto(_decoder);
                Debug.Assert(task.IsCompleted || IsAsync);
                (_pgError, _requestedRowDescription) = await task.ConfigureAwait(false);

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

            // Make sure any asynchronous writes are also completed and any exceptions are observed.
            await _writeTask.ConfigureAwait(false);

            if (_readFlowRfq)
            {
                if (_decoder.TryGetNext(out var message))
                {
                    if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                        PostgresException.Throw(rfqError);
                }
                else
                {
                    await ReadRfq(_decoder).ConfigureAwait(false);
                }
            }

            SetResult(null);
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == AbortToken || ex.CancellationToken == _callerCancellationToken)
        {
            if (ex.CancellationToken == AbortToken)
            {
                // Protocol will terminate the backend connection, nothing more for us to do.
                throw;
            }

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

            throw;
        }
        catch (TimeoutException)
        {
            Console.WriteLine("Timeout exception?");
            throw;
            // Issue backend cancel(s), this is a timeout on I/O, luckily this means we have a fairly high certainty we cancel our own commands.
            // We have to issue as many cancels as there are syncs in this flow.
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception?: " + ex);
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
                catch (Exception ex)
                {
                    Console.WriteLine("Exception from command result action: " + ex);
                    // TODO log.
                }
            }

            _enumeratorCompleted = completed;
            _enumeratorMoveNextTaskSource.SetResult(!completed, runContinuationsAsynchronously: completed);
        }

        async ValueTask ReadRfq(PgDecoder decoder)
        {
            var message = await decoder.GetNextAsync().ConfigureAwait(false);
            if (message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery) is { } rfqError)
                PostgresException.Throw(rfqError);
        }
    }

    void HandleException(Exception ex)
    {
        // We have to make sure to unblock the caller if the flow failed to (could be a cancellation, io error, etc).
        _enumeratorMoveNextTaskSource.SetException(ex);
        // TODO use tryrecover to consume as many rfqs as we have left at the time of throwing.
        // TODO This essentially replaces manually having to consume the flow remainder on unexpected errors (e.g. unhandled protocol errors).
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

    protected override void OnAbort(OperationCanceledException exception)
        => _callerInteractionCore.CancelPendingWait(exception);

    protected override void OnReset()
    {
        Debug.Assert(IsPending || IsCompleted);
        _commandIndex = -1;
        _enumeratorMoveNextTaskSource.Reset();
        _enumeratorCurrent = default;
        _isResultReady = false;
        _callerInteractionCore.Reset();
        _callerCancellationToken = default;
        _callerCancellationTokenRegistration.Dispose();
        _callerCancellationTokenRegistration = default;
    }

    FlowCallerInteractionCoreResult IValueTaskSource<FlowCallerInteractionCoreResult>.GetResult(short token)
    {
        var result = _callerInteractionCore.GateTaskSource.GetResult(token);
        _callerInteractionCore.GateTaskSource.Reset();
        return result;
    }

    ValueTask _writeTask;

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
            flow._callerInteractionCore.WaitForContinuation().Invoke();
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
                flow._callerCancellationToken = cancellationToken.GetValueOrDefault();

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

            while (MoveNext())
                Current.Dispose();
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            // Default value.
            if (flow is null)
                return new();

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
