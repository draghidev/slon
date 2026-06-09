using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Slon.Runtime.CompilerServices;
using Slon.Pipelines;

namespace Slon.Pg.Protocol;

sealed class PgDecoder: IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
{
    readonly PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> _messageBatchEnumerator;
    readonly BackendMessageContext _messageContext;
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
            Debug.Assert(_control.ActivatedFlow is not null);
            return _control.ActivatedFlow.GetExecutionControl(_control);
        }
    }

    void SetRemainingTimeout(TimeSpan timeout)
    {
        _remainingTimeout = timeout;
        Interlocked.MemoryBarrier();
    }

    internal PgDecoder(PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> messageBatchEnumerator, CancellationToken abortToken, TimeSpan defaultReadTimeout)
    {
        _messageBatchEnumerator = messageBatchEnumerator;
        _messageContext = new();
        _abortToken = abortToken;
        _defaultReadTimeout = defaultReadTimeout;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(abortToken);
        _onHeartbeatAction = OnHeartbeat;
        SetRemainingTimeout(Timeout.InfiniteTimeSpan);
    }

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
        if (_remainingTimeout != Timeout.InfiniteTimeSpan && (_remainingTimeout -= elapsed) <= TimeSpan.Zero)
            _cancellationTokenSource.Cancel();
    }

    /// Applies not just to {Get,Move}Next but also {Get,Move}NextAsync, fully cancels I/O.
    public TimeSpan ReadTimeout { get; set; }

    ValueTask<bool> IAsyncEnumerator<BackendMessage>.MoveNextAsync() => MoveNextAsync(CancellationToken.None);

    // Recycle a CTS cancelled by timeout or user-CT from the previous call. Abort is terminal,
    // never recycle past it. Single recycle site (here, not OnHeartbeat / Core's finally) so
    // there's no race between heartbeat thread and the flow's own teardown.
    void EnsureUsableCts()
    {
        if (_cancellationTokenSource.IsCancellationRequested && !_abortToken.IsCancellationRequested)
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_abortToken);
    }

    /// Flow-owned escape hatch from a parked read. Without it the only break-out is protocol
    /// abort. An uncaught firing triggers the protocol's recovery path, so prefer a
    /// coordination-boundary check in connection-preserving flows.
    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        EnsureUsableCts();
        while (true)
        {
            var context = _messageContext;
            if (!context.TryMoveNext())
            {
                var task = _messageBatchEnumerator.MoveNextAsync(_cancellationTokenSource.Token);
                if (!task.IsCompletedSuccessfully)
                    return MoveNextAsyncCore(task, null, cancellationToken);

                var success = task.Result;
                context.SetBatch(_messageBatchEnumerator.Current);
                if (!success)
                    return new(false);

                if (!context.TryMoveNext())
                    ThrowHelper.ThrowInvalidOperation("No message in a new batch");
            }

            var handleTask = CurrentExecutionControl.HandleMessageAuto(context.Current);
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

                        while (_messageContext.TryMoveNext())
                        {
                            if (!await CurrentExecutionControl.HandleMessageAuto(_messageContext.Current).ConfigureAwait(false))
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
                                task = _messageBatchEnumerator.MoveNextAsync(_cancellationTokenSource.Token);
                            firstMessageHandled = false;
                        }
                        else
                        {
                            task = _messageBatchEnumerator.MoveNextAsync(_cancellationTokenSource.Token);
                        }

                        var success = await task.ConfigureAwait(false);
                        _messageContext.SetBatch(_messageBatchEnumerator.Current);
                        if (!success)
                            return false;

                        if (!_messageContext.TryMoveNext())
                            ThrowHelper.ThrowInvalidOperation("No message in a new batch");
                    }
                    catch (OperationCanceledException oce) when
                        (oce.CancellationToken == _cancellationTokenSource.Token && _cancellationTokenSource.IsCancellationRequested)
                    {
                        // Gate on this protocol's own abort token so a different protocol's
                        // closure can't be mistaken for ours; the CTS would otherwise also
                        // observe cancellation via the read-timeout heartbeat path.
                        if (_abortToken.IsCancellationRequested)
                            _control.ThrowIfClosed();
                        if (cancellationToken.IsCancellationRequested)
                            throw new OperationCanceledException(cancellationToken);
                        throw new TimeoutException("Read timed out.", oce);
                    }

                    messageHandledTask = CurrentExecutionControl.HandleMessageAuto(_messageContext.Current);
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
        get => _messageContext.Current;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetNext(out BackendMessage message)
    {
        if (_messageContext.TryMoveNext())
        {
            message = _messageContext.Current;
            return true;
        }

        message = default;
        return false;
    }

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
                var context = _messageContext;
                if (!context.TryMoveNext())
                {
                    if (!timeoutSet)
                    {
                        SetRemainingTimeout(ReadTimeout);
                        timeoutSet = true;
                    }

                    var success = _messageBatchEnumerator.MoveNext(_remainingTimeout);
                    context.SetBatch(_messageBatchEnumerator.Current);
                    if (!success)
                        return false;

                    if (!context.TryMoveNext())
                        ThrowHelper.ThrowInvalidOperation("No message in a new batch");
                }

                // HandleMessageAuto is always sync-completing (every branch returns a
                // synchronously-constructed ValueTask). Reading .Result inline is safe.
                if (CurrentExecutionControl.HandleMessageAuto(context.Current).Result)
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

    void IDisposable.Dispose() => _messageBatchEnumerator.Dispose();
    ValueTask IAsyncDisposable.DisposeAsync() => _messageBatchEnumerator.DisposeAsync();

    object? IEnumerator.Current => Current;
    void IEnumerator.Reset() => throw new NotSupportedException();
}

// Context to manage message streaming, and to limit incurred write barriers per message to the minimum.
sealed class BackendMessageContext
{
    BackendMessageBatch _remainingBatch;
    BackendMessage _current;
    short _version;

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
        => BackendMessage.TryCreateFromBatch(ref _remainingBatch, this, ++_version, out _current);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetBatch(BackendMessageBatch batch) => _remainingBatch = batch;
}

readonly struct ErrorOrNoticeMessage
{
    readonly PgTypes.BackendType[] _expected;
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
    public string SqlState { get; }

    public bool Unhandled { get; }

    public ReadOnlySpan<PgTypes.BackendType> Expected => _expected;

    ErrorOrNoticeMessage(PgTypes.BackendType[] expected, bool isNotice, bool unhandled = false)
    {
        _expected = expected;
        IsNotice = isNotice;
        Unhandled = unhandled;
        SqlState = "";
    }

    public static ErrorOrNoticeMessage Create(BackendMessage message, ReadOnlySpan<PgTypes.BackendType> expected, bool unhandled = true)
    {
        message.EnsureExpected(PgTypes.BackendType.ErrorResponse, PgTypes.BackendType.NoticeResponse);
        message.EnsureBuffered();

        return new(expected.ToArray(), message.Header.Type is PgTypes.BackendType.NoticeResponse, unhandled);
    }
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

readonly struct CommandCompleteMessage
{
    public static CommandCompleteMessage Create(BackendMessage message)
    {
        message.EnsureExpected(PgTypes.BackendType.EmptyQueryResponse, PgTypes.BackendType.CommandComplete);
        message.EnsureBuffered();

        // TODO actually parse it.
        return new();
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
