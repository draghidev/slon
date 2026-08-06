namespace Slon.Pg.Protocol.Flows;

// Pins the outer pipeline while a user drives subflows through a nested pipeline. The inner pipeline
// owns wire I/O and composes the same ordering, handoff, and recovery rules recursively. Scope state is
// reused because an open ADO connection commonly holds one scope across many commands.
sealed class ExclusiveAccessFlow : PgClientFlow
{
    // Stable state shared across reusable scope tenures.
    readonly PgClientProtocol _protocol;
    readonly PgClientProtocol.Control _innerControl;
    readonly PgClientProtocol.ExclusiveScopeState _state;
    readonly Func<Exception?, ValueTask> _completeInner;
    // Acquired only after activation, when the inner executor is started.
    PgClientFlowSource? _innerSource;
    bool _acquired;
    TimeSpan _activationTimeout;
    TaskCompletionSource _handoffReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
    TaskCompletionSource _scopeEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Cancellation detaches the caller but cannot remove an already-issued flow. Before activation the
    // flow retires without acquiring; after handoff it ends the abandoned scope.
    TaskCompletionSource _consumerGone = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Idempotence guards for the cascade hooks (fire once per tenure). Reset in OnReset.
    bool _innerStopping;
    bool _innerAborting;
    // Lazily allocated and reused rendezvous for caller-thread execution of synchronous scopes.
    ManualResetEventSlim? _handoffEvent;

    internal ExclusiveAccessFlow(PgClientProtocol protocol, PgClientProtocol.Control innerControl, PgClientProtocol.ExclusiveScopeState state, Func<Exception?, ValueTask> completeInner)
    {
        _protocol = protocol;
        _innerControl = innerControl;
        _state = state;
        _completeInner = completeInner;
    }

    // TODO: enable after activation timeouts identify pooled tenures; otherwise a late timeout can
    // complete a reused flow generation.
    // protected override bool EnableActivationTimeout => true;

    // The inner pipeline starts only after this scope wins activation.
    internal void PrepareScope(bool async, TimeSpan activationTimeout)
    {
        _innerSource = null;
        _acquired = false;
        _activationTimeout = activationTimeout;
        IsAsync = async;
        // Release the reusable flow only after terminal state is visible. A close may complete a flow
        // dispatched across the source cutoff without starting it, so also resolve its caller gates here.
        SetCompletionAction(static (f, ex, s) =>
        {
            var flow = (ExclusiveAccessFlow)f;
            if (ex is PgClientClosedException closed)
            {
                flow._handoffReady.TrySetException(closed);
                flow._scopeEnded.TrySetResult();
            }
            ((PgClientProtocol.ExclusiveScopeState)s!).ReleaseFlow(flow);
        }, _state);
        // Async scopes never allocate the synchronous handoff.
        if (!async)
            _handoffEvent ??= new(false);
    }

    // Non-null only for caller-thread execution of a synchronous scope.
    protected override ManualResetEventSlim? HandoffEvent => _handoffEvent;

    protected override void OnReset()
    {
        _handoffReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _scopeEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _consumerGone = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _innerStopping = false;
        _innerAborting = false;
        _handoffEvent?.Reset();
    }

    /// Resolves once this flow is activated and owns the wire - the caller awaits it to gain exclusive
    /// access before submitting any subflow. Faults if the flow is torn down before activation.
    public Task HandoffReady => _handoffReady.Task;

    /// Acquires exclusive access. Cancellation before handoff detaches the caller; the issued flow still
    /// takes its turn and retires. Cancellation after handoff does not revoke ownership.
    internal async Task WaitForHandoffAsync(CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
        {
            await _handoffReady.Task.ConfigureAwait(false);
            return;
        }
        var cancelTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var reg = cancellationToken.UnsafeRegister(static (s, ct) => ((TaskCompletionSource)s!).TrySetCanceled(ct), cancelTcs);
        var winner = await Task.WhenAny(_handoffReady.Task, cancelTcs.Task).ConfigureAwait(false);
        if (winner == _handoffReady.Task)
        {
            await _handoffReady.Task.ConfigureAwait(false); // observe a pre-activation fault, if any
            return;
        }
        // The issued flow still takes its turn, but no longer acquires the scope.
        _consumerGone.TrySetResult();
        cancellationToken.ThrowIfCancellationRequested();
    }

    /// Queues a subflow in FIFO order. Synchronous subflows recursively hand execution to the caller.
    public T Queue<T>(T subflow, CancellationToken cancellationToken = default) where T : PgClientFlow
    {
        // Queue is valid only after handoff has acquired the source.
        var innerSource = _innerSource ?? throw new InvalidOperationException("Cannot submit a subflow before the scope is acquired (await HandoffReady first).");
        if (cancellationToken.CanBeCanceled)
            subflow.BindCallerToken(cancellationToken);
        subflow.GetExecutionControl(_innerControl).Bind(_activationTimeout);
        _protocol.AssignCancellationBoundary(_innerControl, subflow);
        // Reserve handoff only for a synchronous caller already waiting to drive it.
        if (!subflow.NeedsSyncHandoff)
        {
            var inlineEligible = _state.IsPipelineEmpty(innerSource);
            innerSource.Enqueue(subflow, inlineEligible).Execute(runContinuationsAsynchronously: false);
        }
        else
        {
            innerSource.EnqueueSyncWaiter(subflow);
            if (subflow.DefersSyncHandoff)
                innerSource.SignalExecutor();
            else
                innerSource.WaitForExecutor(subflow);
        }
        return subflow;
    }

    /// End the exclusive scope: drain its pipeline and await session reset plus outer retirement.
    public async ValueTask CompleteScopeAsync()
    {
        // Gracefully drain submitted subflows before releasing the outer flow.
        await _completeInner(null).ConfigureAwait(false);
        // Capture completion before retirement can release and reset the reusable flow.
        var completion = WaitForComplete();
        _scopeEnded.TrySetResult();
        // Completion follows teardown, making the flow safe to reuse on return.
        await completion.ConfigureAwait(false);
    }

    internal static bool WriteScopeReset(PgEncoder encoder, string? command)
    {
        if (command is null)
            return false;
        encoder.WriteQuery(command);
        return true;
    }

    protected override async ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        // Activation grants exclusive tenure; subflows perform the wire I/O.
        _ = await context.GetDecoderAsync().ConfigureAwait(false);
        // A detached caller leaves the issued flow to retire without starting an inner executor.
        if (_consumerGone.Task.IsCompleted)
            return ValueTask.CompletedTask;
        // Shutdown may retire the prior holder before the source cutoff is observed. Do not hand the
        // resulting activation a dying wire.
        if (_protocol.CloseReason is { } closeReason)
        {
            _handoffReady.TrySetException(closeReason);
            return ValueTask.CompletedTask;
        }
        // Start the inner executor only after winning the turn.
        _innerSource = _state.AcquireForTurn(_protocol);
        Volatile.Write(ref _acquired, true);
        // If close won the acquire/handoff race, drain the newly started inner pipeline here.
        if (!_handoffReady.TrySetResult())
        {
            await _completeInner(null).ConfigureAwait(false);
            return ValueTask.CompletedTask;
        }
        // An abandoned held scope must still drain before the reusable flow retires.
        var ended = await Task.WhenAny(_scopeEnded.Task, _consumerGone.Task).ConfigureAwait(false);
        if (ended == _consumerGone.Task && Volatile.Read(ref _acquired))
            await _completeInner(null).ConfigureAwait(false);
        if (_protocol.TransactionStatus is not TransactionStatus.Idle)
            throw new InvalidOperationException(
                $"The exclusive scope completed leaving the connection in transaction status '{_protocol.TransactionStatus}'. " +
                "The transaction must be committed or rolled back before completing the scope.");
        if (_protocol.ScopeResetCommand is { } resetCommand)
        {
            // An inner flow's CancelRequest is connection-wide. Do not extend its exposed prefix
            // with the outer scope reset while delivery is unresolved.
            await context.WaitForCancellationAttempt().ConfigureAwait(false);
            await ResetSession(context, resetCommand).ConfigureAwait(false);
        }
        return ValueTask.CompletedTask;
    }

    static async ValueTask ResetSession(Context context, string command)
    {
        var encoder = context.GetEncoder();
        var decoder = await context.GetDecoderAsync().ConfigureAwait(false);
        await ResetSession(encoder, decoder, command).ConfigureAwait(false);
    }

    static async ValueTask ResetSession(PgEncoder encoder, PgDecoder decoder, string? command)
    {
        if (!WriteScopeReset(encoder, command))
            return;
        await encoder.FlushAsync().ConfigureAwait(false);

        PgError? error = null;
        while (true)
        {
            var message = await decoder.GetNextAsync().ConfigureAwait(false);
            if (message.TryCreateError(out var currentError))
                error ??= currentError.Preserve();
            if (message.Header.Type is PgTypes.BackendType.ReadyForQuery)
                break;
        }

        ThrowSessionResetError(error);
    }

    internal static void ThrowSessionResetError(PgError? error)
    {
        if (error is not null)
            PgErrorException.Throw(error);
    }

    // Close cascades must stop the inner executor and release both body and pre-activation caller gates.
    // Graceful close releases the body only after the inner pipeline drains.
    protected override void OnStopping(PgClientClosedException exception)
    {
        if (_innerStopping)
            return;
        _innerStopping = true;
        if (Volatile.Read(ref _acquired))
        {
            CompleteInnerThenEndScope(_completeInner(null));
        }
        else
        {
            _handoffReady.TrySetException(exception);
            _scopeEnded.TrySetResult();
        }
    }

    // Abort completes inner items with the close reason. The outer flow may retire only after the
    // inner pipeline stops; otherwise both source pumps can reach the shared writer concurrently.
    protected override void OnAbort(PgClientClosedException exception)
    {
        if (_innerAborting)
            return;
        _innerAborting = true;
        if (Volatile.Read(ref _acquired))
            CompleteInnerThenEndScope(_completeInner(exception));
        else
        {
            _handoffReady.TrySetException(exception);
            _scopeEnded.TrySetResult();
        }
    }

    // Observe completion faults without throwing into the heartbeat thread.
    async void CompleteInnerThenEndScope(ValueTask completion)
    {
        try { await completion.ConfigureAwait(false); }
        catch { /* TODO route to an unobserved-exception hook once one exists */ }
        finally { _scopeEnded.TrySetResult(); }
    }
}
