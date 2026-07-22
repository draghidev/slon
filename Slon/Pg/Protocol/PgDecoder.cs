using System.Collections;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Slon.Runtime.CompilerServices;
using Slon.Pipelines;

namespace Slon.Pg.Protocol;

// Thin, poolable read-side shell over a shared ReadChannel. Carries the token-bearing concerns:
// the scope/protocol abort token, its linked CTS (+ recycle), TranslateReadCancellation, the
// read-timeout countdown + OnHeartbeat, CurrentExecutionControl, and the read/handler loops that
// drive the channel against this shell's CTS. The physical wire state lives in the channel; each
// exclusive scope gets its own shell with the SCOPE token over the one shared channel, and the
// single-pump invariant keeps only one shell active at a time.
sealed class PgDecoder: IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
{
    readonly ReadChannel _channel;
    readonly CancellationToken _abortToken;
    readonly TimeSpan _defaultReadTimeout;
    readonly Action<TimeSpan> _onHeartbeatAction;
    CancellationTokenSource _cancellationTokenSource;

    PgClientProtocol.Control _control = null!;
    const long ClaimedTimeoutTicks = long.MinValue;
    const long ExpiringTimeoutTicks = long.MinValue + 1;
    long _remainingTimeoutTicks;
    int _cancellationReadFrontierWindow = -1;
    PgClientFlow? _cancellationReadFrontierFlow;

    PgClientFlow.ExecutionControl CurrentExecutionControl
    {
        get
        {
            Debug.Assert(_control is not null);
            var activated = _control.ActivatedFlow;
            Debug.Assert(activated is not null);
            // Read-side substitution permit (inverse of ThrowIfCannotWrite): while a recovery holds
            // the ActivatedFlow but its failed flow still has an in-flight read, resolve to the failed
            // flow until that read finishes. Otherwise the failed read decodes against the recovery's
            // read-state and its late fault re-enters nonexistent recovery-of-recovery.
            if (activated is Flows.ResyncRecoveryFlow { FailedReadOutstanding: true } recovery)
                return recovery.FailedFlow!.GetExecutionControl(_control);
            return activated.GetExecutionControl(_control);
        }
    }

    // The heartbeat claims the scalar with a sentinel while decrementing it; arm/disarm waits out
    // that short ownership window. Expiry publishes a second sentinel: re-entrant cleanup may
    // disarm it during Cancel, while a new finite tenure cannot arm the old CTS before delivery.
    void SetRemainingTimeout(TimeSpan timeout)
    {
        var spin = new SpinWait();
        var disarming = timeout == Timeout.InfiniteTimeSpan || timeout == TimeSpan.Zero;
        while (true)
        {
            var current = Volatile.Read(ref _remainingTimeoutTicks);
            if (current == ClaimedTimeoutTicks || (current == ExpiringTimeoutTicks && !disarming))
            {
                spin.SpinOnce();
                continue;
            }
            if (Interlocked.CompareExchange(ref _remainingTimeoutTicks, timeout.Ticks, current) == current)
                return;
        }
    }

    TimeSpan GetRemainingTimeout()
    {
        var spin = new SpinWait();
        while (true)
        {
            var ticks = Volatile.Read(ref _remainingTimeoutTicks);
            if (ticks != ClaimedTimeoutTicks)
                return ticks == ExpiringTimeoutTicks ? TimeSpan.Zero : TimeSpan.FromTicks(ticks);
            spin.SpinOnce();
        }
    }

    PgDecoder(ReadChannel channel, CancellationToken abortToken, TimeSpan defaultReadTimeout)
    {
        _channel = channel;
        _abortToken = abortToken;
        _defaultReadTimeout = defaultReadTimeout;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(abortToken);
        _onHeartbeatAction = OnHeartbeat;
        SetRemainingTimeout(Timeout.InfiniteTimeSpan);
    }

    internal PgDecoder(PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> messageBatchEnumerator, CancellationToken abortToken, TimeSpan defaultReadTimeout)
        : this(new ReadChannel(messageBatchEnumerator), abortToken, defaultReadTimeout)
    {
    }

    internal ReadChannel Channel => _channel;

    // Builds a scope-bound shell over an existing channel, carrying the scope's abort token.
    internal static PgDecoder CreateScopeShell(PgDecoder baseShell, CancellationToken abortToken, TimeSpan defaultReadTimeout)
        => new(baseShell._channel, abortToken, defaultReadTimeout);

    internal void Initialize(PgClientProtocol.Control control)
    {
        // A read disarms its own timeout in its finally, but the read task's SetResult drives the next
        // flow's activation (BindDecoder -> here) on the SAME stack (the inline completion -> advancer ->
        // ActivateHeadItem cascade), so that disarm can lag this re-init. The single-reader gate guarantees
        // the prior read has fully completed - no in-flight read owns this timeout - so a lingering armed
        // value is a benign leftover; reset it rather than let it ride into (or the heartbeat fire it on)
        // the new flow's reads.
        if (GetRemainingTimeout() != Timeout.InfiniteTimeSpan)
            SetRemainingTimeout(Timeout.InfiniteTimeSpan);
        ReadTimeout = _defaultReadTimeout;
        if (!ReferenceEquals(_control, control))
            _control = control;
        // TODO we want a heartbeat setup directly through the protocol on construction.
        CurrentExecutionControl.RegisterDecoderOnHeartbeat(_onHeartbeatAction);
    }

    void OnHeartbeat(TimeSpan elapsed)
    {
        var ticks = Interlocked.Exchange(ref _remainingTimeoutTicks, ClaimedTimeoutTicks);
        if (ticks == ClaimedTimeoutTicks)
            return;
        if (ticks == ExpiringTimeoutTicks)
        {
            Interlocked.CompareExchange(ref _remainingTimeoutTicks, ExpiringTimeoutTicks, ClaimedTimeoutTicks);
            return;
        }

        var active = ticks != Timeout.InfiniteTimeSpan.Ticks && ticks != 0;
        var remaining = active ? ticks - elapsed.Ticks : ticks;
        // A concurrent arm/disarm replaced the sentinel and owns the next tenure. Never write the
        // old tick into it or cancel on its behalf.
        if (Interlocked.CompareExchange(ref _remainingTimeoutTicks, remaining, ClaimedTimeoutTicks) != ClaimedTimeoutTicks)
            return;

        if (active && remaining <= 0
            && Interlocked.CompareExchange(ref _remainingTimeoutTicks, ExpiringTimeoutTicks, remaining) == remaining)
        {
            try
            {
                _cancellationTokenSource.Cancel();
            }
            finally
            {
                // A cancellation callback may have disarmed this tenure inline. Do not restore
                // the expired budget over that cleanup (or over a subsequently recycled tenure).
                Interlocked.CompareExchange(ref _remainingTimeoutTicks, remaining, ExpiringTimeoutTicks);
            }
        }
    }

    /// Applies not just to {Get,Move}Next but also {Get,Move}NextAsync, fully cancels I/O.
    public TimeSpan ReadTimeout { get; set; }

    ValueTask<bool> IAsyncEnumerator<BackendMessage>.MoveNextAsync() => MoveNextAsync(CancellationToken.None);

    // Recycle a CTS cancelled by timeout or user-CT from the previous call. Abort is terminal,
    // never recycle past it. Single recycle site so the heartbeat thread and the flow's own
    // teardown can't race it.
    void EnsureUsableCts()
    {
        if (_cancellationTokenSource.IsCancellationRequested && !_abortToken.IsCancellationRequested)
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_abortToken);
    }

    // Translate a read cancellation to the protocol's typed surface, shared by sync and async paths.
    // The cause is an OCE when the cancel landed before/at the read's start, or an IOException /
    // SocketException / ObjectDisposedException when our CTS aborted (or Abort closed the socket under)
    // an in-flight receive. The CTS also fires on read-timeout, hence the timeout branch. Returns rather
    // than throws so a sync caller's throw keeps definite assignment. _abortToken is this shell's token
    // (the scope token for a scope shell), so a scope-only abort breaks a parked read here.
    Exception TranslateReadCancellation(Exception cause, CancellationToken cancellationToken)
    {
        if (_abortToken.IsCancellationRequested && _control.ClosedException is { } closed)
            return closed;
        if (cancellationToken.IsCancellationRequested)
            return new OperationCanceledException(cancellationToken);
        return new TimeoutException("Read timed out.", cause);
    }

    PgClientFlow EnterCancellationReadFrontier()
    {
        var execution = CurrentExecutionControl;
        var flow = execution.Flow;
        var window = flow.CancellationWindow;
        _control.EnterCancellationReadFrontier(flow, window);
        return flow;
    }

    void LeaveCancellationReadFrontier(PgClientFlow flow)
        => _control.LeaveCancellationReadFrontier();

    internal void SetCancellationReadFrontier(PgClientFlow flow, int window)
    {
        _cancellationReadFrontierFlow = flow;
        // Full-fence publication before the caller probes for cancellation intents. The intent side
        // atomically publishes its level before probing this frontier, closing the two-sided skip race.
        Interlocked.Exchange(ref _cancellationReadFrontierWindow, window);
    }

    internal void ClearCancellationReadFrontier()
    {
        Volatile.Write(ref _cancellationReadFrontierWindow, -1);
        _cancellationReadFrontierFlow = null;
    }

    internal bool IsAtCancellationReadFrontier(PgClientFlow flow, int window)
    {
        var observedWindow = Volatile.Read(ref _cancellationReadFrontierWindow);
        return observedWindow == window
            && ReferenceEquals(_cancellationReadFrontierFlow, flow)
            && Volatile.Read(ref _cancellationReadFrontierWindow) == observedWindow;
    }

    /// Flow-owned escape hatch from a parked read. Without it the only break-out is protocol
    /// abort. An uncaught firing triggers the protocol's recovery path, so prefer a
    /// coordination-boundary check in connection-preserving flows.
    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        EnsureUsableCts();
        while (true)
        {
            var channel = _channel;
            while (channel.TryMoveNext())
            {
                var handleTask = CurrentExecutionControl.HandleMessageAuto(channel.Current);
                if (!handleTask.IsCompletedSuccessfully)
                    return MoveNextAsyncCore(null, null, handleTask, cancellationToken);
                if (!handleTask.Result)
                    return new(true);
            }

            if (channel.TryMoveNextBatch(out var completed))
                continue;
            if (completed)
                return new(false);

            var readToken = _cancellationTokenSource.Token;
            var frontierFlow = EnterCancellationReadFrontier();
            try
            {
                if (channel.TryBeginDirectRead(readToken, out var directReadTask))
                {
                    try
                    {
                        while (true)
                        {
                            if (!directReadTask.IsCompletedSuccessfully)
                                return MoveNextAsyncCore(null, directReadTask, null, cancellationToken, frontierFlow);
                            if (channel.CompleteDirectRead(directReadTask.Result, readToken, out directReadTask, out var readFinished, out var directReadCompleted))
                                break;
                            if (!readFinished)
                                continue;
                            if (directReadCompleted)
                            {
                                LeaveCancellationReadFrontier(frontierFlow);
                                return new(false);
                            }
                            goto nextRead;
                        }
                        LeaveCancellationReadFrontier(frontierFlow);
                        continue;
                    }
                    catch (Exception ex)
                    {
                        channel.AbortDirectRead();
                        LeaveCancellationReadFrontier(frontierFlow);
                        if (_cancellationTokenSource.IsCancellationRequested)
                            throw TranslateReadCancellation(ex, cancellationToken);
                        throw;
                    }
                }

                var readTask = channel.ReadAsync(readToken);
                if (!readTask.IsCompletedSuccessfully)
                    return MoveNextAsyncCore(readTask, null, null, cancellationToken, frontierFlow);
                LeaveCancellationReadFrontier(frontierFlow);
                if (channel.TryMoveNextBatch(readTask.Result, _cancellationTokenSource.Token, out var readCompleted))
                    continue;
                if (readCompleted)
                    return new(false);
            }
            catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
            {
                LeaveCancellationReadFrontier(frontierFlow);
                throw TranslateReadCancellation(ex, cancellationToken);
            }
            catch
            {
                LeaveCancellationReadFrontier(frontierFlow);
                throw;
            }
            nextRead:;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        async ValueTask<bool> MoveNextAsyncCore(ValueTask<ReadResult>? readTask, ValueTask<int>? directReadTask, ValueTask<bool>? messageHandledTask, CancellationToken cancellationToken, PgClientFlow? frontierFlow = null)
        {
            var timeoutSet = false;
            var registration = cancellationToken.UnsafeRegister(static (state, _) => ((CancellationTokenSource)state!).Cancel(), _cancellationTokenSource);
            try
            {
                while (true)
                {
                    if (messageHandledTask is { } t)
                    {
                        if (!await t.ConfigureAwait(false))
                            return true;
                        messageHandledTask = null;
                    }

                    if (readTask is { } pendingRead)
                    {
                        try
                        {
                            if (!timeoutSet)
                            {
                                SetRemainingTimeout(ReadTimeout);
                                timeoutSet = true;
                            }
                            var result = await pendingRead.ConfigureAwait(false);
                            LeaveCancellationReadFrontier(frontierFlow!);
                            frontierFlow = null;
                            readTask = null;
                            if (_channel.TryMoveNextBatch(result, _cancellationTokenSource.Token, out var readCompleted))
                                continue;
                            if (readCompleted)
                                return false;
                        }
                        catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
                        {
                            throw TranslateReadCancellation(ex, cancellationToken);
                        }
                    }

                    if (directReadTask is { } pendingDirectRead)
                    {
                        try
                        {
                            if (!timeoutSet)
                            {
                                SetRemainingTimeout(ReadTimeout);
                                timeoutSet = true;
                            }
                            var length = await pendingDirectRead.ConfigureAwait(false);
                            if (_channel.CompleteDirectRead(length, _cancellationTokenSource.Token, out var nextDirectRead, out var readFinished, out var readCompleted))
                            {
                                LeaveCancellationReadFrontier(frontierFlow!);
                                frontierFlow = null;
                                directReadTask = null;
                                continue;
                            }
                            if (!readFinished)
                            {
                                directReadTask = nextDirectRead;
                                continue;
                            }
                            directReadTask = null;
                            LeaveCancellationReadFrontier(frontierFlow!);
                            frontierFlow = null;
                            if (readCompleted)
                                return false;
                        }
                        catch (Exception ex)
                        {
                            _channel.AbortDirectRead();
                            if (frontierFlow is not null)
                            {
                                LeaveCancellationReadFrontier(frontierFlow);
                                frontierFlow = null;
                            }
                            if (_cancellationTokenSource.IsCancellationRequested)
                                throw TranslateReadCancellation(ex, cancellationToken);
                            throw;
                        }
                    }

                    while (_channel.TryMoveNext())
                    {
                        var handleTask = CurrentExecutionControl.HandleMessageAuto(_channel.Current);
                        if (!handleTask.IsCompletedSuccessfully)
                        {
                            messageHandledTask = handleTask;
                            break;
                        }
                        if (!handleTask.Result)
                            return true;
                    }
                    if (messageHandledTask.HasValue)
                        continue;

                    if (_channel.TryMoveNextBatch(out var completed))
                        continue;
                    if (completed)
                        return false;

                    try
                    {
                        var token = _cancellationTokenSource.Token;
                        frontierFlow = EnterCancellationReadFrontier();
                        if (_channel.TryBeginDirectRead(token, out var nextDirectRead))
                            directReadTask = nextDirectRead;
                        else
                            readTask = _channel.ReadAsync(token);
                    }
                    catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
                    { throw TranslateReadCancellation(ex, cancellationToken); }
                }
            }
            finally
            {
                if (frontierFlow is not null)
                    LeaveCancellationReadFrontier(frontierFlow);
                registration.Dispose();
                if (timeoutSet)
                    SetRemainingTimeout(Timeout.InfiniteTimeSpan);
            }
        }
    }


    public BackendMessage Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => EnrichError(_channel.Current);
    }

    BackendMessage EnrichError(BackendMessage message)
    {
        if (message.Header.Type is not PgTypes.BackendType.ErrorResponse)
            return message;

        var execution = CurrentExecutionControl;
        var flow = execution.Flow;
        if (_control.HasPriorCancellationExposure(flow, flow.CancellationWindow))
            message.MarkPriorCancellationExposure();
        return message;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetNext(out BackendMessage message)
    {
        // Peek - try - commit, mirroring MoveNext's auto-handled skip + RFQ accounting on the sync-fast
        // path. Run the handler here: a body reading its terminating RFQ via TryGetNext would otherwise
        // leave _rfqCount stale and route the wrong count to recovery. TryHandleMessage returns false
        // only when the handler needs I/O, where we bail and the caller falls back to MoveNextAsync.
        while (true)
        {
            while (_channel.TryPeekNext(out var header))
            {
                var handled = false;
                if (header.Type
                    is PgTypes.BackendType.ReadyForQuery
                    or PgTypes.BackendType.NoticeResponse
                    or PgTypes.BackendType.NotificationResponse
                    or PgTypes.BackendType.ParameterStatus
                    && !CurrentExecutionControl.TryHandleMessage(_channel.Peeked, out handled))
                {
                    goto unavailable;
                }
                _channel.TryMoveNext();
                if (handled)
                    continue;
                message = Current;
                return true;
            }

            // The current batch is exhausted. Descend through any bytes the PipeReader already owns
            // before reporting unavailable; only a genuinely pending physical read should make the
            // async caller install its continuation tree.
            if (!_channel.TryMoveNextBatch(out _))
                break;
        }

        unavailable:
        message = default;
        return false;
    }

    // Auto-switch read, mirroring the encoder's FlushAuto: a sync flow takes the BLOCKING read path
    // (GetNext -> MoveNext -> channel.MoveNext, a real blocking syscall - the BCL does the waiting), an
    // async flow takes GetNextAsync. Using GetNextAsync unconditionally for a sync flow leaves the read on
    // the non-blocking/emulated path, so the body completes on a TP thread instead of inline.
    public ValueTask<BackendMessage> GetNextAuto()
        => CurrentExecutionControl.IsAsync ? GetNextAsync() : new(GetNext());

    public ValueTask<BackendMessage> GetNextAsync()
    {
        var task = MoveNextAsync();
        if (!task.IsCompletedSuccessfully)
            return GetNextAsyncCore(task);

        if (task.Result)
            return new(Current);

        ThrowHelper.ThrowInvalidOperation("No more messages");
        return default;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<BackendMessage> GetNextAsyncCore(ValueTask<bool> task)
    {
        if (await task.ConfigureAwait(false))
            return Current;

        ThrowHelper.ThrowInvalidOperation("No more messages");
        return default;
    }

    public bool MoveNext()
    {
        var timeoutSet = false;
        try
        {
            while (true)
            {
                var channel = _channel;
                if (!channel.TryMoveNext())
                {
                    if (!timeoutSet)
                    {
                        SetRemainingTimeout(ReadTimeout);
                        timeoutSet = true;
                    }

                    bool success;
                    var frontierFlow = EnterCancellationReadFrontier();
                    try
                    {
                        success = channel.MoveNext(GetRemainingTimeout());
                    }
                    catch (Exception) when (_abortToken.IsCancellationRequested && _control.ClosedException is { } closed)
                    {
                        // Sync reads block in a syscall no token reaches; a forceful abort breaks them
                        // by closing the socket, surfacing as ObjectDisposedException / IOException /
                        // TimeoutException rather than an OCE. Translate any of them to the typed closed
                        // exception, mirroring the async path's TranslateReadCancellation.
                        throw closed;
                    }
                    finally
                    {
                        LeaveCancellationReadFrontier(frontierFlow);
                    }
                    channel.CommitBatch();
                    if (!success)
                        return false;

                    if (!channel.TryMoveNext())
                        ThrowHelper.ThrowInvalidOperation("No message in a new batch");
                }

                // HandleMessageAuto is always sync-completing (every branch returns a
                // synchronously-constructed ValueTask). Reading .Result inline is safe.
                if (CurrentExecutionControl.HandleMessageAuto(channel.Current).Result)
                    continue;

                return true;
            }
        }
        finally
        {
            if (timeoutSet)
                SetRemainingTimeout(Timeout.InfiniteTimeSpan);
        }
    }

    public BackendMessage GetNext()
    {
        if (!MoveNext())
            ThrowHelper.ThrowInvalidOperation("No more messages");
        return Current;
    }

    void IDisposable.Dispose() => _channel.Dispose();
    ValueTask IAsyncDisposable.DisposeAsync() => _channel.DisposeAsync();

    object? IEnumerator.Current => Current;
    void IEnumerator.Reset() => throw new NotSupportedException();
}
