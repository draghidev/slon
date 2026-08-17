using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Runtime.CompilerServices;
using Slon.Pipelines;

namespace Slon.Pg.Protocol;

// Thin, poolable read-side shell over a shared ProtocolReadPipe. Carries the token-bearing concerns:
// the scope/protocol abort token, its linked CTS (+ recycle), TranslateReadCancellation, the
// read-timeout countdown + OnHeartbeat, CurrentExecutionControl, and the read/handler loops that
// drive the pipe against this shell's CTS. The physical wire state lives in the pipe; each
// exclusive scope gets its own shell with the scope token over the shared pipe, and the
// single-pump invariant keeps only one shell active at a time.
sealed class PgDecoder: IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
{
    readonly ProtocolReadPipe _pipe;
    readonly CancellationToken _abortToken;
    readonly TimeSpan _defaultReadTimeout;
    readonly Action? _readTimeoutArmed;
    readonly Action<TimeSpan> _onHeartbeatAction;
    CancellationTokenSource _cancellationTokenSource;
    TimeSpan _readTimeout;

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

    PgDecoder(ProtocolReadPipe pipe, CancellationToken abortToken, TimeSpan defaultReadTimeout,
        Action? readTimeoutArmed)
    {
        _pipe = pipe;
        _abortToken = abortToken;
        _defaultReadTimeout = defaultReadTimeout;
        _readTimeout = defaultReadTimeout;
        _readTimeoutArmed = readTimeoutArmed;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(abortToken);
        _onHeartbeatAction = OnHeartbeat;
        SetRemainingTimeout(Timeout.InfiniteTimeSpan);
    }

    internal PgDecoder(PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> messageBatchEnumerator,
        CancellationToken abortToken, TimeSpan defaultReadTimeout, Action? readTimeoutArmed = null)
        : this(new ProtocolReadPipe(messageBatchEnumerator), abortToken, defaultReadTimeout, readTimeoutArmed)
    {
    }

    internal ProtocolReadPipe Pipe => _pipe;
    internal Encoding ClientEncoding => _control.ClientEncoding;

    // Builds a scope-bound shell over the shared pipe with the scope's abort token.
    internal static PgDecoder CreateScopeShell(PgDecoder baseShell, CancellationToken abortToken, TimeSpan defaultReadTimeout)
        => new(baseShell._pipe, abortToken, defaultReadTimeout, baseShell._readTimeoutArmed);

    void ArmReadTimeout()
    {
        SetRemainingTimeout(_readTimeout);
        _readTimeoutArmed?.Invoke();
    }

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
        RestoreDefaultReadTimeout();
        if (!ReferenceEquals(_control, control))
            _control = control;
        _pipe.BindDecoder(this);
        // TODO we want a heartbeat setup directly through the protocol on construction.
        CurrentExecutionControl.RegisterDecoderOnHeartbeat(_onHeartbeatAction);
    }

    /// <summary>
    /// Selects the timeout for reads belonging to the current operation. The decoder restores the protocol
    /// default when it observes ErrorResponse or ReadyForQuery, and whenever it is rebound to a flow. A flow
    /// spanning multiple Sync groups must therefore select the timeout for each group.
    /// </summary>
    public void UseReadTimeout(TimeSpan timeout)
        => _readTimeout = timeout;

    void RestoreDefaultReadTimeout()
        => _readTimeout = _defaultReadTimeout;

    internal bool TryContinueCurrentMessage(SequencePosition consumed, long consumedLength, out CurrentSegmentBuffer result)
        => _pipe.TryContinueCurrentMessage(consumed, consumedLength, out result);

    internal ValueTask<CurrentSegmentBuffer> ContinueCurrentMessageAsync(
        SequencePosition consumed, long consumedLength, CancellationToken cancellationToken)
    {
        EnsureUsableCts();
        if (_pipe.TryContinueCurrentMessage(consumed, consumedLength, out var result))
            return new(result);

        return Core(cancellationToken);

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        async ValueTask<CurrentSegmentBuffer> Core(CancellationToken cancellationToken)
        {
            var timeoutSet = false;
            var frontierFlow = EnterCancellationReadFrontier();
            var registration = cancellationToken.UnsafeRegister(
                static (state, _) => ((CancellationTokenSource)state!).Cancel(), _cancellationTokenSource);
            try
            {
                ArmReadTimeout();
                timeoutSet = true;
                return await _pipe.ContinueCurrentMessageAsync(
                    consumed, consumedLength, _cancellationTokenSource.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
            {
                throw TranslateReadCancellation(ex, cancellationToken);
            }
            catch (EndOfStreamException ex)
            {
                throw TranslateEof(ex);
            }
            finally
            {
                LeaveCancellationReadFrontier(frontierFlow);
                registration.Dispose();
                if (timeoutSet)
                    SetRemainingTimeout(Timeout.InfiniteTimeSpan);
            }
        }
    }

    internal CurrentSegmentBuffer ContinueCurrentMessage(
        SequencePosition consumed, long consumedLength)
    {
        var timeoutSet = false;
        try
        {
            ArmReadTimeout();
            timeoutSet = true;
            return _pipe.ContinueCurrentMessage(consumed, consumedLength, GetRemainingTimeout());
        }
        catch (Exception) when (_abortToken.IsCancellationRequested && _control.ClosedException is not null)
        {
            throw _control.FlowTerminationException;
        }
        catch (EndOfStreamException ex)
        {
            throw TranslateEof(ex);
        }
        finally
        {
            if (timeoutSet)
                SetRemainingTimeout(Timeout.InfiniteTimeSpan);
        }
    }

    internal bool TryExtendCurrentMessage(out CurrentSegmentBuffer result)
        => _pipe.TryExtendCurrentMessage(out result);

    internal ValueTask<CurrentSegmentBuffer> ExtendCurrentMessageAsync(CancellationToken cancellationToken)
    {
        EnsureUsableCts();
        if (_pipe.TryExtendCurrentMessage(out var result))
            return new(result);

        return Core(cancellationToken);

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        async ValueTask<CurrentSegmentBuffer> Core(CancellationToken cancellationToken)
        {
            var timeoutSet = false;
            var frontierFlow = EnterCancellationReadFrontier();
            var registration = cancellationToken.UnsafeRegister(
                static (state, _) => ((CancellationTokenSource)state!).Cancel(), _cancellationTokenSource);
            try
            {
                ArmReadTimeout();
                timeoutSet = true;
                return await _pipe.ExtendCurrentMessageAsync(
                    _cancellationTokenSource.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
            {
                throw TranslateReadCancellation(ex, cancellationToken);
            }
            catch (EndOfStreamException ex)
            {
                throw TranslateEof(ex);
            }
            finally
            {
                LeaveCancellationReadFrontier(frontierFlow);
                registration.Dispose();
                if (timeoutSet)
                    SetRemainingTimeout(Timeout.InfiniteTimeSpan);
            }
        }
    }

    internal CurrentSegmentBuffer ExtendCurrentMessage()
    {
        var timeoutSet = false;
        try
        {
            ArmReadTimeout();
            timeoutSet = true;
            return _pipe.ExtendCurrentMessage(GetRemainingTimeout());
        }
        catch (Exception) when (_abortToken.IsCancellationRequested && _control.ClosedException is not null)
        {
            throw _control.FlowTerminationException;
        }
        catch (EndOfStreamException ex)
        {
            throw TranslateEof(ex);
        }
        finally
        {
            if (timeoutSet)
                SetRemainingTimeout(Timeout.InfiniteTimeSpan);
        }
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
        if (_abortToken.IsCancellationRequested && _control.ClosedException is not null)
            return _control.FlowTerminationException;
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
        => _control.LeaveCancellationReadFrontier(flow);

    internal void SetCancellationReadFrontier(PgClientFlow flow, int window)
    {
        _cancellationReadFrontierFlow = flow;
        // Full-fence publication before the caller probes for cancellation intents. The intent side
        // atomically publishes its level before probing this frontier, closing the two-sided skip race.
        Interlocked.Exchange(ref _cancellationReadFrontierWindow, window);
    }

    internal int ClearCancellationReadFrontier(PgClientFlow expectedFlow)
    {
        // Decoder frontier writers run on the single protocol reader. Validate ownership before
        // clearing so a stale leave cannot erase a newer flow's frontier.
        if (!ReferenceEquals(_cancellationReadFrontierFlow, expectedFlow))
            return -1;
        var window = Interlocked.Exchange(ref _cancellationReadFrontierWindow, -1);
        _cancellationReadFrontierFlow = null;
        return window;
    }

    internal bool IsAtCancellationReadFrontier(PgClientFlow flow, int window)
    {
        var observedWindow = Volatile.Read(ref _cancellationReadFrontierWindow);
        return observedWindow == window
            && ReferenceEquals(_cancellationReadFrontierFlow, flow)
            && Volatile.Read(ref _cancellationReadFrontierWindow) == observedWindow;
    }

    /// Flow-owned cancellation path for a parked read. Without it the only break-out is protocol
    /// abort. An uncaught firing triggers the protocol's recovery path, so prefer a
    /// coordination-boundary check in connection-preserving flows.
    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        EnsureUsableCts();
        while (true)
        {
            var pipe = _pipe;
            while (pipe.TryMoveNext())
            {
                var handleTask = CurrentExecutionControl.HandleMessageAuto(pipe.Current);
                if (!handleTask.IsCompletedSuccessfully)
                    return MoveNextAsyncCore(null, null, handleTask, cancellationToken);
                if (!handleTask.Result)
                    return new(true);
            }

            if (pipe.TryMoveNextBatch(out var completed))
                continue;
            if (completed)
                return new(ReadCompleted());

            var readToken = _cancellationTokenSource.Token;
            var frontierFlow = EnterCancellationReadFrontier();
            try
            {
                retryRead:
                if (pipe.TryBeginDirectRead(readToken, out var directReadTask))
                {
                    try
                    {
                        while (true)
                        {
                            if (!directReadTask.IsCompletedSuccessfully)
                                return MoveNextAsyncCore(null, directReadTask, null, cancellationToken, frontierFlow);
                            if (pipe.CompleteDirectRead(directReadTask.Result, readToken, out directReadTask, out var readFinished, out var directReadCompleted))
                                break;
                            if (!readFinished)
                                continue;
                            if (directReadCompleted)
                            {
                                LeaveCancellationReadFrontier(frontierFlow);
                                return new(ReadCompleted());
                            }
                            goto retryRead;
                        }
                        LeaveCancellationReadFrontier(frontierFlow);
                        continue;
                    }
                    catch
                    {
                        pipe.AbortDirectRead();
                        throw;
                    }
                }

                var readTask = pipe.ReadAsync(readToken);
                if (!readTask.IsCompletedSuccessfully)
                    return MoveNextAsyncCore(readTask, null, null, cancellationToken, frontierFlow);
                LeaveCancellationReadFrontier(frontierFlow);
                if (pipe.TryMoveNextBatch(readTask.Result, _cancellationTokenSource.Token, out var readCompleted))
                    continue;
                if (readCompleted)
                    return new(ReadCompleted());
            }
            catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
            {
                LeaveCancellationReadFrontier(frontierFlow);
                throw TranslateReadCancellation(ex, cancellationToken);
            }
            catch (EndOfStreamException ex)
            {
                LeaveCancellationReadFrontier(frontierFlow);
                throw TranslateEof(ex);
            }
            catch
            {
                LeaveCancellationReadFrontier(frontierFlow);
                throw;
            }
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
                                ArmReadTimeout();
                                timeoutSet = true;
                            }
                            var result = await pendingRead.ConfigureAwait(false);
                            LeaveCancellationReadFrontier(frontierFlow!);
                            frontierFlow = null;
                            readTask = null;
                            if (_pipe.TryMoveNextBatch(result, _cancellationTokenSource.Token, out var readCompleted))
                                continue;
                            if (readCompleted)
                                return ReadCompleted();
                        }
                        catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
                        {
                            throw TranslateReadCancellation(ex, cancellationToken);
                        }
                        catch (EndOfStreamException ex)
                        {
                            throw TranslateEof(ex);
                        }
                    }

                    if (directReadTask is { } pendingDirectRead)
                    {
                        try
                        {
                            if (!timeoutSet)
                            {
                                ArmReadTimeout();
                                timeoutSet = true;
                            }
                            var length = await pendingDirectRead.ConfigureAwait(false);
                            if (_pipe.CompleteDirectRead(length, _cancellationTokenSource.Token, out var nextDirectRead, out var readFinished, out var readCompleted))
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
                                return ReadCompleted();
                        }
                        catch (Exception ex)
                        {
                            _pipe.AbortDirectRead();
                            if (frontierFlow is not null)
                            {
                                LeaveCancellationReadFrontier(frontierFlow);
                                frontierFlow = null;
                            }
                            if (_cancellationTokenSource.IsCancellationRequested)
                                throw TranslateReadCancellation(ex, cancellationToken);
                            if (ex is EndOfStreamException eof)
                                throw TranslateEof(eof);
                            throw;
                        }
                    }

                    while (_pipe.TryMoveNext())
                    {
                        var handleTask = CurrentExecutionControl.HandleMessageAuto(_pipe.Current);
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

                    if (_pipe.TryMoveNextBatch(out var completed))
                        continue;
                    if (completed)
                        return ReadCompleted();

                    try
                    {
                        var token = _cancellationTokenSource.Token;
                        frontierFlow = EnterCancellationReadFrontier();
                        if (_pipe.TryBeginDirectRead(token, out var nextDirectRead))
                            directReadTask = nextDirectRead;
                        else
                            readTask = _pipe.ReadAsync(token);
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

    bool ReadCompleted()
    {
        // Closing a socket may settle a pending read as EOF instead of an exception. Once shutdown has
        // published its reason, EOF is the same terminal event and must use the flow's termination
        // verdict. The protocol completion itself retains the canonical close.
        // EOF on an open PostgreSQL session is itself a terminal wire failure. Publish that fact here,
        // before returning control to whichever pipelined flow happened to own the read. Deferring the
        // verdict to item recovery lets a successor win the race and expose its locally manufactured EOF
        // (or a secondary parsing error) instead of the protocol-wide collateral failure.
        throw TranslateTerminalRead(PgProtocolException.UnexpectedEof());
    }

    Exception TranslateEof(EndOfStreamException exception)
        => TranslateTerminalRead(PgProtocolException.UnexpectedEof(exception));

    Exception TranslateTerminalRead(PgProtocolException exception)
    {
        if (_control.ClosedException is not null)
            return _control.FlowTerminationException;

        _control.FailProtocol(exception);
        return _control.FlowTerminationException;
    }

    [DoesNotReturn]
    internal void ThrowUnexpectedEof()
        => throw TranslateTerminalRead(PgProtocolException.UnexpectedEof());


    public BackendMessage Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => EnrichError(_pipe.Current);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetCurrent(out BackendMessage message)
    {
        if (!_pipe.TryGetCurrent(out message))
            return false;
        message = EnrichError(message);
        return true;
    }

    BackendMessage EnrichError(BackendMessage message)
    {
        if (message.Header.Type is not PgTypes.BackendType.ErrorResponse)
            return message;

        // Current may be inspected repeatedly while parsing and completing a command. Cancellation
        // acknowledgement is an arrival event: replaying it after the logical command window advances
        // would let one ErrorResponse satisfy a successor window which still needs its own request.
        if (!message.TryObserveError())
            return message;

        // ErrorResponse ends the current execution window. Any following read belongs to RFQ drain or
        // recovery, so it must use the protocol timeout rather than the command-specific timeout. A later
        // command installs its own timeout before reading its response.
        RestoreDefaultReadTimeout();

        var execution = CurrentExecutionControl;
        var flow = execution.Flow;
        if (_control.HasPriorCancellationExposure(flow, flow.CancellationWindow))
            message.MarkPriorCancellationExposure();
        if (message.TryCreateError(out var error))
        {
            if (_control.QueryProtocolEstablished && error.TerminatesSession)
            {
                // FATAL/PANIC is a wire event, not a command result. Mark the live message before
                // preserving the canonical cause so the observing flow and every successor receive
                // the same collateral classification. Shutdown publication then makes a following
                // EOF/RST translate to this verdict instead of replacing it with a framing failure.
                message.MarkBackendTermination();
                message.TryCreateError(out error);
                _control.FailBackendTermination(error!);
                return message;
            }
            if (error.SqlState == PgErrorCodes.QueryCanceled)
                _control.OnBackendCancellationObserved(flow, flow.CancellationWindow);
        }
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
            while (_pipe.TryPeekNext(out var header))
            {
                var handled = false;
                if (header.Type is PgTypes.BackendType.ReadyForQuery)
                    RestoreDefaultReadTimeout();
                if (header.Type
                    is PgTypes.BackendType.ReadyForQuery
                    or PgTypes.BackendType.NoticeResponse
                    or PgTypes.BackendType.NotificationResponse
                    or PgTypes.BackendType.ParameterStatus
                    && !CurrentExecutionControl.TryHandleMessage(_pipe.Peeked, out handled))
                {
                    goto unavailable;
                }
                _pipe.TryMoveNext();
                if (handled)
                    continue;
                message = Current;
                return true;
            }

            // The current batch is exhausted. Descend through any bytes the PipeReader already owns
            // before reporting unavailable; only a genuinely pending physical read should make the
            // async caller install its continuation tree.
            if (!_pipe.TryMoveNextBatch(out _))
                break;
        }

        unavailable:
        message = default;
        return false;
    }

    // Auto-switch read, mirroring the encoder's FlushAuto: a sync flow takes the BLOCKING read path
    // (GetNext -> MoveNext -> pipe.MoveNext, a real blocking syscall—the BCL does the waiting), an
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

        ThrowUnexpectedEof();
        return default;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    async ValueTask<BackendMessage> GetNextAsyncCore(ValueTask<bool> task)
    {
        if (await task.ConfigureAwait(false))
            return Current;

        ThrowUnexpectedEof();
        return default;
    }

    public bool MoveNext()
    {
        var timeoutSet = false;
        try
        {
            while (true)
            {
                var pipe = _pipe;
                if (!pipe.TryMoveNext())
                {
                    if (!timeoutSet)
                    {
                        ArmReadTimeout();
                        timeoutSet = true;
                    }

                    bool success;
                    var frontierFlow = EnterCancellationReadFrontier();
                    try
                    {
                        success = pipe.MoveNext(GetRemainingTimeout());
                    }
                    catch (Exception) when (_abortToken.IsCancellationRequested && _control.ClosedException is not null)
                    {
                        // Sync reads block in a syscall no token reaches; a forceful abort breaks them
                        // by closing the socket, surfacing as ObjectDisposedException / IOException /
                        // TimeoutException rather than an OCE. Translate any of them to the typed closed
                        // exception, mirroring the async path's TranslateReadCancellation.
                        throw _control.FlowTerminationException;
                    }
                    catch (EndOfStreamException ex)
                    {
                        throw TranslateEof(ex);
                    }
                    finally
                    {
                        LeaveCancellationReadFrontier(frontierFlow);
                    }
                    pipe.CommitBatch();
                    if (!success)
                        return ReadCompleted();

                    if (!pipe.TryMoveNext())
                        ThrowHelper.ThrowInvalidOperation("No message in a new batch");
                }

                // HandleMessageAuto is always sync-completing (every branch returns a
                // synchronously-constructed ValueTask). Reading .Result inline is safe.
                var current = pipe.Current;
                if (current.Header.Type is PgTypes.BackendType.ReadyForQuery)
                    RestoreDefaultReadTimeout();
                if (CurrentExecutionControl.HandleMessageAuto(current).Result)
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
            ThrowUnexpectedEof();
        return Current;
    }

    void IDisposable.Dispose() => _pipe.Dispose();
    ValueTask IAsyncDisposable.DisposeAsync() => _pipe.DisposeAsync();

    object? IEnumerator.Current => Current;
    void IEnumerator.Reset() => throw new NotSupportedException();
}
