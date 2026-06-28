using System.Buffers;
using System.Buffers.Text;
using System.Collections;
using System.Diagnostics;
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
    TimeSpan _remainingTimeout;

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

    void SetRemainingTimeout(TimeSpan timeout)
    {
        _remainingTimeout = timeout;
        Interlocked.MemoryBarrier();
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
        Debug.Assert(_remainingTimeout == Timeout.InfiniteTimeSpan);
        ReadTimeout = _defaultReadTimeout;
        if (!ReferenceEquals(_control, control))
            _control = control;
        // TODO we want a heartbeat setup directly through the protocol on construction.
        CurrentExecutionControl.RegisterDecoderOnHeartbeat(_onHeartbeatAction);
    }

    void OnHeartbeat(TimeSpan elapsed)
    {
        // Both InfiniteTimeSpan and Zero are treated as "no timeout" (Zero is the default(TimeSpan)
        // "no timeout set" case). Without the Zero guard the first heartbeat tick would fire the
        // cancel immediately and abort any read parked on I/O.
        if (_remainingTimeout != Timeout.InfiniteTimeSpan && _remainingTimeout != TimeSpan.Zero
            && (_remainingTimeout -= elapsed) <= TimeSpan.Zero)
            _cancellationTokenSource.Cancel();
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
            if (!channel.TryMoveNext())
            {
                // The read can throw OCE synchronously: if the CTS (linked to AbortToken) is already
                // cancelled at entry, it throws before returning a task, bypassing MoveNextAsyncCore's
                // catch (which only runs when the read parks). A pre-check is a TOCTOU, so catch the
                // synchronous throw and run it through the same translation as the async path.
                ValueTask<bool> task;
                try
                {
                    task = channel.MoveNextAsync(_cancellationTokenSource.Token);
                }
                catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
                {
                    // Key on the cancel state - which the protocol owns - NOT the exception type. A
                    // cancelled read surfaces an OCE when the token is tripped at entry, but whatever the
                    // transport throws when an in-flight read is aborted otherwise (today a wrapped socket
                    // exception / ObjectDisposedException; another transport would throw its own). Keying
                    // on the type would couple us to one transport's exception vocabulary and silently
                    // fail to translate another's. Translate whatever it is; mirrors the sync MoveNext.
                    throw TranslateReadCancellation(ex, cancellationToken);
                }
                if (!task.IsCompletedSuccessfully)
                    return MoveNextAsyncCore(task, null, cancellationToken);

                var success = task.Result;
                channel.CommitBatch();
                if (!success)
                    return new(false);

                if (!channel.TryMoveNext())
                    ThrowHelper.ThrowInvalidOperation("No message in a new batch");
            }

            var handleTask = CurrentExecutionControl.HandleMessageAuto(channel.Current);
            if (!handleTask.IsCompletedSuccessfully)
                return MoveNextAsyncCore(default, handleTask, cancellationToken);

            // We have to try to fetch another message if this one was handled by the protocol.
            if (handleTask.Result)
                continue;

            return new(true);
        }

        // Implemented in an somewhat convoluted fashion to handle either task without needing another async frame.
        [MethodImpl(MethodImplOptions.NoInlining)]
        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        async ValueTask<bool> MoveNextAsyncCore(ValueTask<bool> task, ValueTask<bool>? messageHandledTask, CancellationToken cancellationToken)
        {
            var firstMessageHandled = messageHandledTask.HasValue;
            var timeoutSet = false;
            var registration = cancellationToken.UnsafeRegister(static (state, _) => ((CancellationTokenSource)state!).Cancel(), _cancellationTokenSource);
            try
            {
                while (true)
                {
                    if (messageHandledTask is { } t)
                    {
                        // If message wasn't handled we can surface it to the caller.
                        if (!await t.ConfigureAwait(false))
                            return true;

                        while (_channel.TryMoveNext())
                        {
                            if (!await CurrentExecutionControl.HandleMessageAuto(_channel.Current).ConfigureAwait(false))
                                return true;
                        }
                    }

                    try
                    {
                        if (!timeoutSet)
                        {
                            SetRemainingTimeout(ReadTimeout);
                            timeoutSet = true;
                            if (firstMessageHandled)
                                task = _channel.MoveNextAsync(_cancellationTokenSource.Token);
                            firstMessageHandled = false;
                        }
                        else
                        {
                            task = _channel.MoveNextAsync(_cancellationTokenSource.Token);
                        }

                        var success = await task.ConfigureAwait(false);
                        _channel.CommitBatch();
                        if (!success)
                            return false;

                        if (!_channel.TryMoveNext())
                            ThrowHelper.ThrowInvalidOperation("No message in a new batch");
                    }
                    catch (Exception ex) when (_cancellationTokenSource.IsCancellationRequested)
                    {
                        // Key on cancel state, not exception type, so the translation stays independent
                        // of the transport's exception vocabulary (see the entry-catch note above).
                        throw TranslateReadCancellation(ex, cancellationToken);
                    }

                    messageHandledTask = CurrentExecutionControl.HandleMessageAuto(_channel.Current);
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
        while (_channel.TryPeekNext(out var peeked))
        {
            if (!CurrentExecutionControl.TryHandleMessage(peeked, out var handled))
                break;
            _channel.TryMoveNext();
            if (handled)
                continue;
            message = _channel.Current;
            return true;
        }

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
                        success = channel.MoveNext(_remainingTimeout);
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
    // happens at peek time and a follow-up TryMoveNext can publish without re-parsing. Cleared
    // by TryMoveNext on commit or by SetBatch (a fresh batch retires any prior peek).
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
            _current = new BackendMessage(_peekedHeader, _peekedBuffer, this, ++_version);
            _peekedBuffer = default;
            return true;
        }
        return BackendMessage.TryCreateFromBatch(ref _remainingBatch, this, ++_version, out _current);
    }

    // Reads the next message WITHOUT publishing it as Current. The remaining batch cursor
    // really advances past the header, but the parsed (header, buffer) lands in the peek
    // slot and the follow-up TryMoveNext picks it up without re-parsing. The returned
    // BackendMessage is valid until the next TryMoveNext (which bumps the version token);
    // use it immediately, don't store it.
    public bool TryPeekNext(out BackendMessage message)
    {
        if (_hasPeeked)
        {
            message = new BackendMessage(_peekedHeader, _peekedBuffer, this, _version);
            return true;
        }
        if (!_remainingBatch.TryReadNextInPlace(out _peekedHeader, out _peekedBuffer, out _))
        {
            message = default;
            return false;
        }
        _hasPeeked = true;
        message = new BackendMessage(_peekedHeader, _peekedBuffer, this, _version);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetBatch(BackendMessageBatch batch)
    {
        // A fresh batch retires the peeked-from-previous-batch state.
        _hasPeeked = false;
        _peekedBuffer = default;
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

    CommandCompleteMessage(StatementType statementType, uint oid, ulong rows)
    {
        StatementType = statementType;
        Oid = oid;
        Rows = rows;
    }

    /// EmptyQueryResponse: the portal was created from an empty query string. No tag, no rows.
    public bool IsEmptyQuery => StatementType is StatementType.Empty;

    public static CommandCompleteMessage Create(BackendMessage message)
    {
        message.EnsureExpected(PgTypes.BackendType.EmptyQueryResponse, PgTypes.BackendType.CommandComplete);
        message.EnsureBuffered();
        return message.Header.Type is PgTypes.BackendType.EmptyQueryResponse
            ? new(StatementType.Empty, 0, 0)
            : Parse(message.GetSequence());
    }

    // Test seam: build directly from a raw command-tag body (the null-terminated tag bytes), bypassing
    // the BackendMessage wrapper, so the tag parser can be exercised without a live connection. Exposed
    // via InternalsVisibleTo (Slon.Tests).
    internal static CommandCompleteMessage FromTag(ReadOnlySequence<byte> tagBody) => Parse(tagBody);

    static CommandCompleteMessage Parse(ReadOnlySequence<byte> body)
    {
        // Tags are tiny (a keyword + a couple of decimals); single-segment is the norm. Copy the rare
        // multi-segment tag to the stack.
        Span<byte> scratch = stackalloc byte[64];
        var bytes = body.IsSingleSegment ? body.FirstSpan : CopyToScratch(body, scratch);
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

    public static ReadyForQueryMessage Create(BackendMessage message)
    {
        message.EnsureExpected(PgTypes.BackendType.ReadyForQuery);
        message.EnsureBuffered();

        byte status = 0;
        message.BodyReader.TryCopyTo(new Span<byte>(ref status));
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
