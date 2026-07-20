using System.Buffers;
using System.Buffers.Text;
using System.Collections;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
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
            try
            {
                if (channel.TryBeginDirectRead(readToken, out var directReadTask))
                {
                    try
                    {
                        while (true)
                        {
                            if (!directReadTask.IsCompletedSuccessfully)
                                return MoveNextAsyncCore(null, directReadTask, null, cancellationToken);
                            if (channel.CompleteDirectRead(directReadTask.Result, readToken, out directReadTask, out var readFinished, out var directReadCompleted))
                                break;
                            if (!readFinished)
                                continue;
                            if (directReadCompleted)
                                return new(false);
                            goto nextRead;
                        }
                        continue;
                    }
                    catch (Exception ex)
                    {
                        channel.AbortDirectRead();
                        if (_cancellationTokenSource.IsCancellationRequested)
                            throw TranslateReadCancellation(ex, cancellationToken);
                        throw;
                    }
                }

                var readTask = channel.ReadAsync(readToken);
                if (!readTask.IsCompletedSuccessfully)
                    return MoveNextAsyncCore(readTask, null, null, cancellationToken);
                if (channel.TryMoveNextBatch(readTask.Result, _cancellationTokenSource.Token, out var readCompleted))
                    continue;
                if (readCompleted)
                    return new(false);
            }
            catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
            {
                throw TranslateReadCancellation(ex, cancellationToken);
            }
            nextRead:;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        async ValueTask<bool> MoveNextAsyncCore(ValueTask<ReadResult>? readTask, ValueTask<int>? directReadTask, ValueTask<bool>? messageHandledTask, CancellationToken cancellationToken)
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
                                directReadTask = null;
                                continue;
                            }
                            if (!readFinished)
                            {
                                directReadTask = nextDirectRead;
                                continue;
                            }
                            directReadTask = null;
                            if (readCompleted)
                                return false;
                        }
                        catch (Exception ex)
                        {
                            _channel.AbortDirectRead();
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
                registration.Dispose();
                if (timeoutSet)
                    SetRemainingTimeout(Timeout.InfiniteTimeSpan);
            }
        }
    }


    public BackendMessage Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _channel.Current;
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
                message = _channel.Current;
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

// Context to manage message streaming, and to limit incurred write barriers per message to the minimum.
sealed class BackendMessageContext
{
    BackendMessageBatch _remainingBatch;
    BackendMessage _current;
    short _version;

    // Peek slot: TryPeekNext advances the real batch cursor into here, so the header parse
    // happens at peek time and a follow-up TryMoveNext can publish without re-parsing. _hasPeeked
    // alone owns validity; leaving the inactive buffer populated avoids a redundant clear and lets
    // the next peek usually reuse the same backing objects without write barriers.
    bool _hasPeeked;
    BackendHeader _peekedHeader;
    ReadOnlySequence<byte> _peekedBuffer;

    public BackendMessage Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _current;
    }

    public BackendMessage GetCurrent(short token)
    {
        if (_version != token)
            ThrowHelper.ThrowInvalidOperation("Backend message has been invalidated by moving to the next message.");
        return _current;
    }

    public bool TryMoveNext()
    {
        if (_hasPeeked)
        {
            _hasPeeked = false;
            BackendMessage.Initialize(ref _current, _peekedHeader, _peekedBuffer, this, ++_version,
                _peekedBuffer.Length >= _peekedHeader.Length);
            return true;
        }
        return BackendMessage.TryCreateFromBatch(ref _remainingBatch, this, ++_version, out _current);
    }

    // Reads the next message WITHOUT publishing it as Current. The remaining batch cursor
    // really advances past the header, but the parsed (header, buffer) lands in the peek
    // slot and the follow-up TryMoveNext picks it up without re-parsing. The returned
    // BackendMessage is valid until the next TryMoveNext (which bumps the version token);
    // use it immediately, don't store it.
    public bool TryPeekNext(out BackendHeader header)
    {
        if (_hasPeeked)
        {
            header = _peekedHeader;
            return true;
        }
        if (!_remainingBatch.TryReadNextInPlace(out _peekedHeader, out var buffer, out _))
        {
            header = default;
            return false;
        }
        BackendMessage.SetSequence(ref _peekedBuffer, in buffer);
        _hasPeeked = true;
        header = _peekedHeader;
        return true;
    }

    public BackendMessage Peeked => new(_peekedHeader, _peekedBuffer, this, _version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetBatch(BackendMessageBatch batch)
    {
        // A fresh batch retires the prior peek. The inactive buffer may stay populated because
        // _hasPeeked owns validity and the next peek overwrites it.
        _hasPeeked = false;
        _remainingBatch = batch;
    }
}

readonly struct ErrorOrNoticeMessage
{
    readonly PgTypes.BackendType[] _expected;
    // A VIEW into the live message buffer by default - valid only while the error is handled inline,
    // before the next read advances the buffer. Preserve() copies it for errors that escape that window.
    readonly ReadOnlySequence<byte> _body;
    public bool IsNotice { get; }
    /// <summary>
    /// Specifies whether the exception is considered transient, that is, whether retrying the operation could
    /// succeed (e.g. a network error). Check <see cref="SqlState"/>.
    /// </summary>
    public bool IsTransientError
    {
        get
        {
            switch (SqlState)
            {
                case PgErrorCodes.InsufficientResources:
                case PgErrorCodes.DiskFull:
                case PgErrorCodes.OutOfMemory:
                case PgErrorCodes.TooManyConnections:
                case PgErrorCodes.ConfigurationLimitExceeded:
                case PgErrorCodes.CannotConnectNow:
                case PgErrorCodes.SystemError:
                case PgErrorCodes.IoError:
                case PgErrorCodes.SerializationFailure:
                case PgErrorCodes.DeadlockDetected:
                case PgErrorCodes.LockNotAvailable:
                case PgErrorCodes.ObjectInUse:
                case PgErrorCodes.ObjectNotInPrerequisiteState:
                case PgErrorCodes.ConnectionException:
                case PgErrorCodes.ConnectionDoesNotExist:
                case PgErrorCodes.ConnectionFailure:
                case PgErrorCodes.SqlClientUnableToEstablishSqlConnection:
                case PgErrorCodes.SqlServerRejectedEstablishmentOfSqlConnection:
                case PgErrorCodes.TransactionResolutionUnknown:
                case PgErrorCodes.AdminShutdown:
                case PgErrorCodes.CrashShutdown:
                case PgErrorCodes.IdleSessionTimeout:
                    return true;
                default:
                    return false;
            }
        }
    }
    // Eagerly parsed: the hot field (transient detection, recovery decisions, and ADO catch filters
    // all read it). Captured as a string, so it stays valid even on a view-only error that was never
    // Preserve()d. The rest stay lazy.
    public string SqlState { get; }

    // Lazily decoded ErrorResponse / NoticeResponse fields. The wire body is a sequence of
    // <field-type byte><null-terminated string> pairs, terminated by a zero field-type byte; we keep
    // the raw body and decode a field only when it is read (errors are rare, most fields go unread).
    // The (byte)'x' argument is the PG field identifier.
    // https://www.postgresql.org/docs/current/protocol-error-fields.html
    public string Severity => GetAscii((byte)'S');

    public string InvariantSeverity
    {
        get
        {
            var v = GetAscii((byte)'V');
            return v.Length is 0 ? Severity : v;
        }
    }

    public string MessageText => GetText((byte)'M');
    public string? Detail => GetTextOrNull((byte)'D');
    public string? Hint => GetTextOrNull((byte)'H');
    public int Position => GetInt((byte)'P');
    public int InternalPosition => GetInt((byte)'p');
    public string? InternalQuery => GetTextOrNull((byte)'q');
    public string? Where => GetTextOrNull((byte)'W');
    public string? SchemaName => GetTextOrNull((byte)'s');
    public string? TableName => GetTextOrNull((byte)'t');
    public string? ColumnName => GetTextOrNull((byte)'c');
    public string? DataTypeName => GetTextOrNull((byte)'d');
    public string? ConstraintName => GetTextOrNull((byte)'n');
    public string? File => GetTextOrNull((byte)'F');
    public string? Line => GetAsciiOrNull((byte)'L');
    public string? Routine => GetAsciiOrNull((byte)'R');

    public bool Unhandled { get; }

    public ReadOnlySpan<PgTypes.BackendType> Expected => _expected;

    ErrorOrNoticeMessage(ReadOnlySequence<byte> body, PgTypes.BackendType[] expected, bool isNotice, bool unhandled)
    {
        _body = body;
        _expected = expected;
        IsNotice = isNotice;
        Unhandled = unhandled;
        SqlState = GetAscii((byte)'C');
    }

    /// Copies the underlying field bytes so the error can outlive the transient message buffer it was
    /// read from. By default the body is a view into that buffer (zero copy), valid only while the
    /// error is handled inline; holders that let it escape the read cycle must Preserve first. The
    /// human-readable fields decode as UTF8 (TODO: thread ClientEncoding for non-UTF8 connections);
    /// the protocol-defined fields (C/S/V/P/p/L/R) are always ASCII.
    public ErrorOrNoticeMessage Preserve()
        => new(new ReadOnlySequence<byte>(_body.ToArray()), _expected, IsNotice, Unhandled);

    // Scan the field block for fieldType, returning its value bytes. One pass per access; the body is
    // small and the common path reads only a couple of fields.
    bool TryGetField(byte fieldType, out ReadOnlySequence<byte> value)
    {
        var reader = new SequenceReader<byte>(_body);
        while (reader.TryRead(out var type) && type is not 0)
        {
            if (!reader.TryReadTo(out ReadOnlySequence<byte> v, (byte)0))
                break;
            if (type == fieldType)
            {
                value = v;
                return true;
            }
        }
        value = default;
        return false;
    }

    string GetAscii(byte fieldType) => TryGetField(fieldType, out var v) ? Encoding.ASCII.GetString(v) : "";
    string? GetAsciiOrNull(byte fieldType) => TryGetField(fieldType, out var v) ? Encoding.ASCII.GetString(v) : null;
    string GetText(byte fieldType) => TryGetField(fieldType, out var v) ? Encoding.UTF8.GetString(v) : "";
    string? GetTextOrNull(byte fieldType) => TryGetField(fieldType, out var v) ? Encoding.UTF8.GetString(v) : null;
    int GetInt(byte fieldType) => TryGetField(fieldType, out var v) && int.TryParse(Encoding.ASCII.GetString(v), out var n) ? n : 0;

    public static ErrorOrNoticeMessage Create(BackendMessage message, ReadOnlySpan<PgTypes.BackendType> expected, bool unhandled = true)
    {
        message.EnsureExpected(PgTypes.BackendType.ErrorResponse, PgTypes.BackendType.NoticeResponse);
        message.EnsureBuffered();

        return new(message.GetSequence(), expected.ToArray(), message.Header.Type is PgTypes.BackendType.NoticeResponse, unhandled);
    }

    // Test seam: build directly from a raw error/notice field block, bypassing the BackendMessage
    // wrapper, so the field parser can be exercised without a live connection. Exposed via
    // InternalsVisibleTo (Slon.Tests).
    internal static ErrorOrNoticeMessage FromFieldBlock(ReadOnlySequence<byte> fieldBlock, bool isNotice = false)
        => new(fieldBlock, [], isNotice, unhandled: true);
}

sealed class PgError
{
    readonly ErrorOrNoticeMessage _message;

    public PgError(ErrorOrNoticeMessage message)
    {
        if (message.IsNotice)
            throw new ArgumentException("Cannot be constructed from a notice message.", nameof(message));
        _message = message;
    }

    public ReadOnlySpan<PgTypes.BackendType> Expected => _message.Expected;

    public string Severity => _message.Severity;
    public string InvariantSeverity => _message.InvariantSeverity;
    public string SqlState => _message.SqlState;
    public string MessageText => _message.MessageText;
    public string? Detail => _message.Detail;
    public string? Hint => _message.Hint;
    public int Position => _message.Position;
    public int InternalPosition => _message.InternalPosition;
    public string? InternalQuery => _message.InternalQuery;
    public string? Where => _message.Where;
    public string? SchemaName => _message.SchemaName;
    public string? TableName => _message.TableName;
    public string? ColumnName => _message.ColumnName;
    public string? DataTypeName => _message.DataTypeName;
    public string? ConstraintName => _message.ConstraintName;
    public string? File => _message.File;
    public string? Line => _message.Line;
    public string? Routine => _message.Routine;
    public bool IsTransientError => _message.IsTransientError;

    /// Copies the underlying field bytes so the error can outlive the transient message buffer.
    /// See <see cref="ErrorOrNoticeMessage.Preserve"/>.
    public PgError Preserve() => new(_message.Preserve());

    public static implicit operator PgError(ErrorOrNoticeMessage message) => new(message);
}

sealed class PgNotice
{
    readonly ErrorOrNoticeMessage _message;

    public PgNotice(ErrorOrNoticeMessage message)
    {
        if (!message.IsNotice)
            throw new ArgumentException("Cannot be constructed from an error message.", nameof(message));
        _message = message;
    }

    public static implicit operator PgNotice(ErrorOrNoticeMessage message) => new(message);
}

readonly struct RowDescriptionMessage
{

}

// The command tag of a successfully-executed command. https://www.postgresql.org/docs/current/protocol-message-formats.html
enum StatementType : byte
{
    Unknown = 0,
    Empty,          // EmptyQueryResponse - the portal came from an empty query string, no tag at all.
    Select,
    Insert,
    Update,
    Delete,
    Merge,
    Copy,
    Call,
    Move,
    Fetch,
    CreateTableAs,
    Other,          // DDL / BEGIN / SET / ... - no row count.
}

// CommandComplete (or EmptyQueryResponse). The body is a single null-terminated command tag
// ("INSERT oid rows", "UPDATE rows", "SELECT rows", a no-count tag like "BEGIN", ...). Following
// npgsql's parse: anchor on the leading keyword to find where the numeric arguments start, then
// Utf8Parser the OID (INSERT only) and row count straight off the bytes - no string allocation.
//
// Unlike ErrorResponse (many string fields, usually unread, reference types needing the buffer) this
// is three value-type scalars, parsed eagerly into inline fields - so it needs no body view and no
// Preserve: the values survive any buffer recycle for free. (RecordsAffected forces the parse to be
// eager-while-on-message anyway, since it's read after the command when the view could be stale.)
readonly struct CommandCompleteMessage
{
    public StatementType StatementType { get; }
    public uint Oid { get; }
    public ulong Rows { get; }
    public long RecordsAffected => StatementType is
        StatementType.Insert or StatementType.Update or StatementType.Delete or StatementType.Merge
        or StatementType.Copy or StatementType.Move or StatementType.Fetch or StatementType.CreateTableAs
            ? (long)Rows
            : 0;

    CommandCompleteMessage(StatementType statementType, uint oid, ulong rows)
    {
        StatementType = statementType;
        Oid = oid;
        Rows = rows;
    }

    /// EmptyQueryResponse: the portal was created from an empty query string. No tag, no rows.
    public bool IsEmptyQuery => StatementType is StatementType.Empty;

    public static CommandCompleteMessage Create(in BackendMessage message)
    {
        message.EnsureExpected(PgTypes.BackendType.EmptyQueryResponse, PgTypes.BackendType.CommandComplete);
        message.EnsureBuffered();
        if (message.Header.Type is PgTypes.BackendType.EmptyQueryResponse)
            return new(StatementType.Empty, 0, 0);

        Span<byte> scratch = stackalloc byte[64];
        var bodyLength = message.Header.BodyLength;
        var bytes = message.TryGetFirstSpan(0, out var first) && first.Length >= bodyLength
            ? first[..bodyLength]
            : CopyToScratch(message.GetSequence(), scratch);
        return Parse(bytes);
    }

    // Test seam: build directly from a raw command-tag body (the null-terminated tag bytes), bypassing
    // the BackendMessage wrapper, so the tag parser can be exercised without a live connection. Exposed
    // via InternalsVisibleTo (Slon.Tests).
    internal static CommandCompleteMessage FromTag(ReadOnlySequence<byte> tagBody)
    {
        Span<byte> scratch = stackalloc byte[64];
        return Parse(tagBody.IsSingleSegment ? tagBody.FirstSpan : CopyToScratch(tagBody, scratch));
    }

    static CommandCompleteMessage Parse(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
            return new(StatementType.Other, 0, 0);
        if (bytes[^1] is 0)
            bytes = bytes[..^1];   // strip the null terminator

        var (type, argsStart) = bytes[0] switch
        {
            (byte)'S' when bytes.StartsWith("SELECT "u8) => (StatementType.Select, "SELECT ".Length),
            (byte)'I' when bytes.StartsWith("INSERT "u8) => (StatementType.Insert, "INSERT ".Length),
            (byte)'U' when bytes.StartsWith("UPDATE "u8) => (StatementType.Update, "UPDATE ".Length),
            (byte)'D' when bytes.StartsWith("DELETE "u8) => (StatementType.Delete, "DELETE ".Length),
            (byte)'M' when bytes.StartsWith("MERGE "u8) => (StatementType.Merge, "MERGE ".Length),
            (byte)'C' when bytes.StartsWith("COPY "u8) => (StatementType.Copy, "COPY ".Length),
            (byte)'C' when bytes.StartsWith("CALL"u8) => (StatementType.Call, "CALL".Length),
            (byte)'M' when bytes.StartsWith("MOVE "u8) => (StatementType.Move, "MOVE ".Length),
            (byte)'F' when bytes.StartsWith("FETCH "u8) => (StatementType.Fetch, "FETCH ".Length),
            (byte)'C' when bytes.StartsWith("CREATE TABLE AS "u8) => (StatementType.CreateTableAs, "CREATE TABLE AS ".Length),
            _ => (StatementType.Other, 0),
        };

        // Call and Other carry no numeric arguments.
        if (type is StatementType.Other or StatementType.Call)
            return new(type, 0, 0);

        var args = bytes[argsStart..];
        uint oid = 0;
        if (type is StatementType.Insert)
        {
            // "INSERT oid rows" - oid first, then a space, then the row count.
            Utf8Parser.TryParse(args, out oid, out var consumed);
            args = consumed + 1 <= args.Length ? args[(consumed + 1)..] : default;
        }
        Utf8Parser.TryParse(args, out ulong rows, out _);
        return new(type, oid, rows);
    }

    static ReadOnlySpan<byte> CopyToScratch(ReadOnlySequence<byte> tag, Span<byte> scratch)
    {
        var len = (int)Math.Min(tag.Length, scratch.Length);
        tag.Slice(0, len).CopyTo(scratch);
        return scratch[..len];
    }
}

// https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-READYFORQUERY
readonly struct ReadyForQueryMessage
{
    public TransactionStatus TransactionStatus { get; }

    ReadyForQueryMessage(TransactionStatus transactionStatus)
    {
        TransactionStatus = transactionStatus;
    }

    public static ReadyForQueryMessage Create(in BackendMessage message)
    {
        message.EnsureExpected(PgTypes.BackendType.ReadyForQuery);
        message.EnsureBuffered();

        byte status;
        if (message.TryGetFirstSpan(0, out var body) && !body.IsEmpty)
        {
            status = body[0];
        }
        else
        {
            status = 0;
            message.BodyReader.TryCopyTo(new Span<byte>(ref status));
        }
        var transactionStatus = (TransactionStatus)status;
        switch (transactionStatus)
        {
            case TransactionStatus.Idle:
            case TransactionStatus.Transaction:
            case TransactionStatus.Error:
            case TransactionStatus.Pending:
                break;
            default:
                ThrowHelper.ThrowUnhandledCase(transactionStatus);
                break;
        }
        return new(transactionStatus);
    }
}

enum TransactionStatus : byte
{
    /// <summary>
    /// Unknown status
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Currently not in a transaction block
    /// </summary>
    Idle = (byte)'I',

    /// <summary>
    /// Currently in a transaction block
    /// </summary>
    Transaction = (byte)'T',

    /// <summary>
    /// Currently in a failed transaction block (queries will be rejected until block is ended)
    /// </summary>
    Error = (byte)'E',

    /// <summary>
    /// A new transaction has been requested but not yet transmitted to the backend.
    /// This is a client-side state option only, and is never transmitted from the backend.
    /// </summary>
    Pending = byte.MaxValue,
}
