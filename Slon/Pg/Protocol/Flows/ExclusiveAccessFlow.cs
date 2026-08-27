namespace Slon.Pg.Protocol.Flows;

// Pins the outer pipeline while a user drives subflows through a nested pipeline. The inner pipeline
// owns wire I/O and composes the same ordering, handoff, and recovery rules recursively. Scope state is
// reused because an open ADO connection commonly holds one scope across many commands.
sealed class ExclusiveAccessFlow : PgClientFlow
{
    sealed class ObserverImpl : PgClientFlowObserver
    {
        internal static readonly ObserverImpl Instance = new();

        protected internal override void OnCompleted(PgClientFlow completed, Exception? exception, object? state)
        {
            var flow = (ExclusiveAccessFlow)completed;
            if (!Volatile.Read(ref flow._acquired) && !flow._consumerGone.Task.IsCompleted)
            {
                flow._handoffReady.TrySetException(exception
                    ?? new InvalidOperationException("The exclusive scope retired before acquiring the protocol."));
                flow._scopeEnded.TrySetResult();
            }
            else if (exception is PgClientClosedException)
                flow._scopeEnded.TrySetResult();
            ((PgClientProtocol.ExclusiveScopeState)state!).ReleaseFlow(flow);
        }
    }

    // Stable state shared across reusable scope tenures.
    readonly PgClientProtocol _protocol;
    readonly PgClientProtocol.ExclusiveScopeState _state;
    readonly Func<Exception?, ValueTask> _completeInner;
    // Acquired only after activation, when the inner executor is started.
    PgClientFlowSource? _innerSource;
    bool _acquired;
    long _tenure;
    TimeSpan _activationTimeout;
    TaskCompletionSource _handoffReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
    TaskCompletionSource _scopeEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Cancellation detaches the caller but cannot remove an already-issued flow. Before activation the
    // flow retires without acquiring; after handoff it ends the abandoned scope.
    TaskCompletionSource _consumerGone = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Idempotence guards for the cascade hooks (fire once per tenure). Reset in OnReset.
    bool _innerStopping;
    int _innerAborting;
    // Lazily allocated and reused rendezvous for caller-thread execution of synchronous scopes.
    FlowHandoffEvent? _handoffEvent;

    internal ExclusiveAccessFlow(PgClientProtocol protocol, PgClientProtocol.Control innerControl, PgClientProtocol.ExclusiveScopeState state, Func<Exception?, ValueTask> completeInner)
    {
        _protocol = protocol;
        _state = state;
        _completeInner = completeInner;
    }

    // TODO: enable after activation timeouts identify pooled tenures; otherwise a late timeout can
    // complete a reused flow generation.
    // protected override bool EnableActivationTimeout => true;

    // The inner pipeline starts only after this scope wins activation.
    internal void PrepareScope(bool async, TimeSpan activationTimeout)
    {
        Interlocked.Increment(ref _tenure);
        _innerSource = null;
        _acquired = false;
        _activationTimeout = activationTimeout;
        IsAsync = async;
        SetObserver(ObserverImpl.Instance, _state);
        // The completed observer releases the reusable flow only after terminal state is visible. A close
        // may complete a flow dispatched across the source cutoff without starting it, so it also resolves
        // the caller gates.
        // Async scopes never allocate the synchronous handoff.
        if (!async)
            _handoffEvent ??= new(false);
    }

    // Non-null only for caller-thread execution of a synchronous scope.
    private protected override FlowHandoffEvent? HandoffEvent => _handoffEvent;

    protected override void OnReset()
    {
        _handoffReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _scopeEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _consumerGone = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _innerStopping = false;
        _innerAborting = 0;
        _handoffEvent?.Reset();
    }

    /// Resolves once this flow is activated and owns the wire - the caller awaits it to gain exclusive
    /// access before submitting any subflow. Faults if the flow is torn down before activation.
    public Task HandoffReady => _handoffReady.Task;

    internal ExclusiveScopeLease CreateLease() => new(this, Volatile.Read(ref _tenure));

    internal PgClientFlow? ExecutingFlow => _state.ExecutingFlow;
    internal PgClientFlow? ActivatedFlow => _state.ActivatedFlow;

    void EnsureTenure(long tenure)
    {
        if (tenure != Volatile.Read(ref _tenure))
            throw new InvalidOperationException("The exclusive-scope lease no longer owns this scope tenure.");
    }

    /// Acquires exclusive access. Cancellation before handoff detaches the caller; the issued flow still
    /// takes its turn and retires. Cancellation after handoff does not revoke ownership.
    internal async Task WaitForHandoffAsync(long tenure, CancellationToken cancellationToken)
    {
        EnsureTenure(tenure);
        if (!cancellationToken.CanBeCanceled)
        {
            await _handoffReady.Task.ConfigureAwait(false);
            return;
        }
        var cancelTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var reg = cancellationToken.UnsafeRegister(static (s, ct) => ((TaskCompletionSource)s!).TrySetCanceled(ct), cancelTcs).ConfigureAwait(false);
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

    internal void WaitForHandoffSynchronously(long tenure)
    {
        EnsureTenure(tenure);
        _protocol.DriveSyncHandoff(this);
        _handoffReady.Task.GetAwaiter().GetResult();
    }

    /// Queues a subflow in FIFO order. Synchronous subflows recursively hand execution to the caller.
    internal T Queue<T>(long tenure, T subflow, CancellationToken cancellationToken = default) where T : PgClientFlow
    {
        EnsureTenure(tenure);
        // Queue is valid only after handoff has acquired the source.
        var innerSource = _innerSource ?? throw new InvalidOperationException("Cannot submit a subflow before the scope is acquired (await HandoffReady first).");
        if (cancellationToken.CanBeCanceled)
            subflow.BindCallerToken(cancellationToken);
        // Reserve handoff only for a synchronous caller already waiting to drive it.
        if (!subflow.NeedsSyncHandoff)
        {
            var inlineEligible = _state.IsPipelineEmpty(innerSource);
            innerSource.Enqueue(subflow, inlineEligible, _activationTimeout)
                .Execute(runContinuationsAsynchronously: false);
        }
        else
        {
            innerSource.EnqueueSyncWaiter(subflow, _activationTimeout);
            if (subflow.DefersSyncHandoff)
                innerSource.SignalExecutor();
            else
                innerSource.WaitForExecutor(subflow);
        }
        return subflow;
    }

    /// End the exclusive scope: drain its pipeline and await session reset plus outer retirement.
    internal async ValueTask CompleteScopeAsync(long tenure)
    {
        EnsureTenure(tenure);
        var completion = WaitForComplete();
        // Gracefully drain submitted subflows before releasing the outer flow.
        await _completeInner(null).ConfigureAwait(false);
        // Completion follows teardown, making the flow safe to reuse on return.
        await completion.ConfigureAwait(false);
    }

    internal static bool WriteSessionReset(PgEncoder encoder, string? command)
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
        // A failed private pipeline resyncs its own wire obligations, but closes admission instead of
        // recovering availability. Its clean completion means the resync item has fully retired; the
        // hosting scope can then perform its ordinary reset and return the physical connection.
        var innerCompletion = _state.Completion;
        var ended = await Task.WhenAny(_scopeEnded.Task, _consumerGone.Task, innerCompletion).ConfigureAwait(false);
        if (ended == innerCompletion)
        {
            try
            {
                await innerCompletion.ConfigureAwait(false);
            }
            finally
            {
                // Inner completion can resume inside its source-driver callback. Suspend the outer
                // scope until a queued edge runs after that callback relinquishes its driver turn.
                _state.SignalScopeEnded(_scopeEnded);
                await _scopeEnded.Task.ConfigureAwait(false);
            }
        }
        if (ended == _consumerGone.Task && Volatile.Read(ref _acquired))
            await _completeInner(null).ConfigureAwait(false);
        if (_protocol.TransactionStatus is not TransactionStatus.Idle)
            throw new InvalidOperationException(
                $"The exclusive scope completed leaving the connection in transaction status '{_protocol.TransactionStatus}'. " +
                "The transaction must be committed or rolled back before completing the scope.");
        if (_protocol.SessionResetCommand is { } resetCommand)
        {
            // An inner flow's CancelRequest is connection-wide. Do not extend its exposed prefix
            // with the outer scope reset while delivery is unresolved.
            await context.WaitForCancellationAttempt().ConfigureAwait(false);
            // The cancellation coordinator is wire-wide. The inner owner has retired, so make this
            // outer cleanup command the next attribution boundary before it touches the same wire.
            _protocol.ResumeCancellationOwner(this);
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
        while (true)
        {
            if (!WriteSessionReset(encoder, command))
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

            if (error is null)
                return;
            // One CancelRequest can produce two SIGINTs. If either reaches this internal successor,
            // its retained wire-level exposure marks the reset error collateral. Retry after RFQ;
            // once that bounded exposure is exhausted, an unrelated 57014 is no longer suppressed.
            if (!error.IsCollateralCancellation)
                ThrowSessionResetError(error);
        }
    }

    internal static void ThrowSessionResetError(PgError? error)
    {
        if (error is not null)
            PgErrorException.Throw(error);
    }

    // Close cascades must stop the inner executor and release both body and pre-activation caller gates.
    // Graceful close releases the body only after the inner pipeline drains.
    protected override void OnStopping(Exception exception)
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
    protected override void OnAbort(Exception exception)
    {
        // Forceful shutdown performs an immediate propagation pass while the periodic heartbeat may
        // already be visiting this flow. Claim the cascade once across both callers.
        if (Interlocked.Exchange(ref _innerAborting, 1) is not 0)
            return;
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
        catch (Exception ex) when (ex is not PgClientClosedException)
        {
            _protocol.ReportUnobservedCallback(ex, "exclusive-scope shutdown");
        }
        catch (PgClientClosedException) { }
        finally { _scopeEnded.TrySetResult(); }
    }
}
