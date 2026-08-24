using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Pg;

sealed class CommandResult(CommandFlow.ResultMessageEnumerator messageEnumerator)
    : IDisposable, IAsyncDisposable, IEnumerable<Row>, IAsyncEnumerable<Row>
{
    public enum RowBuffering : byte
    {
        Buffered,
        Streaming
    }

    readonly Row _row = new();
    RowDescription? _rowDescription;
    int _index;
    CommandDescriptor _descriptor;
    bool _requestedExecution;
    bool _simpleProtocol;
    bool _firstRowEnumerated;

    long _recordsAffected;
    CommandCompleteMessage? _commandCompleteMessage;
    PgError? _errorMessage;
    Action<CommandResult, object?>? _completionAction;
    object? _completionActionState;
    CommandFlow _flow = null!;

    // The requested row description is what was returned for this exact command (i.e. commands that requested a describe).
    internal void Initialize(CommandFlow flow, int index, CommandDescriptor descriptor,
        RowDescription? requestedRowDescription, bool requestedExecution, bool simpleProtocol, PgError? error = null)
    {
        if (!ReferenceEquals(_flow, flow))
            _flow = flow;
        _index = index;
        _descriptor = descriptor;

        // If the command wasn't redescribed, and the prepared description is valid use it instead.
        var rowDescription = requestedRowDescription;
        if (rowDescription is null && descriptor.IsPrepared && descriptor.PreparedRowDescription is { } descriptorRowDescription)
        {
            rowDescription = descriptorRowDescription;
        }

        if (!ReferenceEquals(_rowDescription, rowDescription))
        {
            _rowDescription = rowDescription;
            if (rowDescription is not null)
                _row.Initialize(rowDescription);
        }
        else if (rowDescription is not null)
        {
            _row.Initialize(rowDescription);
        }
        _requestedExecution = requestedExecution;
        _simpleProtocol = simpleProtocol;

        // Enumeration state.
        _firstRowEnumerated = false;
        _recordsAffected = default;
        _commandCompleteMessage = null;
        _errorMessage = error;
        _completionAction = null;
        _completionActionState = null;
    }

    /// Returns all metadata known about the command after execution has taken place.
    public CommandMetadata GetMetadata()
    {
        var descriptor = _descriptor;
        return new()
        {
            CommandIndex = _index,
            CommandName = descriptor.CommandName,
            RowDescription = _rowDescription,
            ParameterTypes = descriptor.ParameterTypes,
            IsPrepared = descriptor.IsPrepared
        };
    }

    public RowEnumerator GetEnumerator(RowBuffering buffering = RowBuffering.Buffered) => new(this, buffering);
    public RowEnumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
        => GetAsyncEnumerator(RowBuffering.Buffered, cancellationToken);

    public RowEnumerator GetAsyncEnumerator(RowBuffering buffering, CancellationToken cancellationToken = default)
    {
        // The IAsyncEnumerable/Enumerator api was designed with serious LINQ and generator method tunnel vision...
        // if (cancellationToken.CanBeCanceled)
        //     throw new NotSupportedException("Cancelable CancellationTokens are not supported by this implementation.");

        return new(this, buffering);
    }

    public bool TryGetCommandComplete([NotNullWhen(true)]out CommandCompleteMessage? value)
    {
        // For commands without rows we enumerate once ourselves.
        if (_rowDescription is null && _commandCompleteMessage is null && _errorMessage is null)
        {
            using var rowEnumerator = GetEnumerator();
            _ = rowEnumerator.MoveNext();
            Debug.Assert(_commandCompleteMessage is not null || _errorMessage is not null);
        }

        if (_commandCompleteMessage is not null)
        {
            value = _commandCompleteMessage;
            return true;
        }
        if (_errorMessage is not null)
            PgErrorException.Throw(_errorMessage);

        value = null;
        return false;
    }

    // Non-nullable: it never returns null - it throws when the result isn't complete. Pair with IsComplete
    // (or TryGetCommandComplete) to check first. Consistent with RecordsAffected's throw-on-incomplete.
    public CommandCompleteMessage GetCommandComplete()
    {
        if (TryGetCommandComplete(out var value))
            return value.Value;

        ThrowHelper.ThrowInvalidOperation("CommandResult rows are not enumerated yet (check IsComplete first).");
        return default;
    }

    // If we have an indeterminate (null) row description here we will find out the error when completing the result.
    // We report this as CanHaveRows true as we don't know, which means we want to enumerate it.
    public bool CanHaveRows => _rowDescription is null || !_rowDescription.IsNoData;
    // We have rows if we requested execution, can have rows and read one, or the command isn't yet completed (this means rows must be coming).
    // This distinction is important for result-set based readers (e.g. DbDataReader) which must always enumerate commands that have a row description.
    public bool HasRows
    {
        get
        {
            if (_errorMessage is not null)
                PgErrorException.Throw(_errorMessage);
            return _requestedExecution && CanHaveRows && (_firstRowEnumerated || !TryGetCommandComplete(out _));
        }
    }
    // A description-only result is published after its RowDescription/NoData or ErrorResponse has
    // already been read, and has no Execute terminal of its own. Executing results become complete
    // when their CommandComplete / EmptyQueryResponse / ErrorResponse is consumed.
    public bool IsComplete => !_requestedExecution || _commandCompleteMessage is not null || _errorMessage is not null;

    internal PgError? Error => _errorMessage;

    internal void OnCompleted(Action<CommandResult, object?> action, object? state)
    {
        if (IsComplete)
        {
            action(this, state);
            return;
        }
        Debug.Assert(_completionAction is null);
        _completionAction = action;
        _completionActionState = state;
    }

    internal void Reset()
    {
        _flow = null!;
        _completionAction = null;
        _completionActionState = null;
    }

    // RecordsAffected is only known once the command has run to its CommandComplete / ErrorResponse.
    // Reading it on a not-yet-drained result is a consumer bug - surface it loudly instead of silently
    // handing back 0 (which is what hid the un-drained ExecuteNonQuery path). Gate with IsComplete.
    public long RecordsAffected
    {
        get
        {
            if (!IsComplete)
                ThrowHelper.ThrowInvalidOperation("RecordsAffected is unavailable until the command result has been read to its CommandComplete (check IsComplete first).");
            // A failed command is complete (terminal ErrorResponse) but has no valid count: surface the
            // failure as a PgErrorException rather than silently reporting 0, consistent with
            // GetCommandComplete. IsComplete keys on _errorMessage too, so the guard above doesn't cover it.
            if (_errorMessage is not null)
                PgErrorException.Throw(_errorMessage);
            return _recordsAffected;
        }
    }

    internal void CompleteNonQuery(BackendMessage message)
    {
        if (message.Header.Type is PgTypes.BackendType.DataRow)
            ThrowHelper.ThrowInvalidOperation("Cannot complete a command result on a DataRow.");
        CompleteCommand(message);
    }


    public int FieldCount => _rowDescription?.FieldCount ?? 0;

    internal void Complete()
    {
        if (IsComplete)
            return;

        _row.RevokeColumnLease();
        while (MoveNextMessage())
        {
            var message = GetCurrentMessage();
            if (message.Header.Type is PgTypes.BackendType.DataRow)
                continue;

            CompleteTerminal(message);
            return;
        }

        EnsureComplete();
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask CompleteAsync()
    {
        if (IsComplete)
            return;

        await _row.RevokeColumnLeaseAsync().ConfigureAwait(false);
        while (await MoveNextMessageAsync().ConfigureAwait(false))
        {
            var message = GetCurrentMessage();
            if (message.Header.Type is PgTypes.BackendType.DataRow)
                continue;

            CompleteTerminal(message);
            return;
        }

        EnsureComplete();
    }

    void EnsureComplete()
    {
        if (_requestedExecution && !IsComplete)
            ThrowHelper.ThrowInvalidOperation("Underlying message enumerator completed before CommandComplete was returned.");
    }

    void CompleteTerminal(in BackendMessage message)
    {
        switch (message.Header.Type)
        {
            case PgTypes.BackendType.EmptyQueryResponse:
            case PgTypes.BackendType.CommandComplete:
            case PgTypes.BackendType.ErrorResponse:
                CompleteCommand(message);
                return;
            case PgTypes.BackendType.PortalSuspended when !_simpleProtocol:
            default:
                ThrowHelper.ThrowUnhandledCase(message.Header.Type);
                return;
        }
    }

    // Disposing the CommandResult skips going through our enumerator, the results won't be accessed anyway.
    // We do expose disposal methods so any I/O can easily be done the way the user expects it (sync or async)
    public void Dispose() => messageEnumerator.Dispose();
    public ValueTask DisposeAsync() => messageEnumerator.DisposeAsync();

    void CompleteCommand(BackendMessage message)
    {
        Debug.Assert(_commandCompleteMessage is null
            && (_errorMessage is null || message.Header.Type is PgTypes.BackendType.ErrorResponse));
        switch (message.Header.Type)
        {
            case PgTypes.BackendType.EmptyQueryResponse:
            case PgTypes.BackendType.CommandComplete:
                // Create parses the tag into value scalars while we're on this message (zero alloc, no
                // buffer view kept). Only data-modifying statements count toward RecordsAffected;
                // SELECT/Call/Other/EmptyQuery don't.
                var ccm = CommandCompleteMessage.Create(message);
                _commandCompleteMessage = ccm;
                _recordsAffected = ccm.RecordsAffected;
                break;
            case PgTypes.BackendType.ErrorResponse:
                // TODO fill out expected types.
                _errorMessage = ErrorOrNoticeMessage.Create(message, []);
                break;
        }

        InvokeCompletionAction();
    }

    void InvokeCompletionAction()
    {
        if (_completionAction is { } action)
        {
            var state = _completionActionState;
            _completionAction = null;
            _completionActionState = null;
            try { action(this, state); }
            catch (Exception ex)
            {
                _flow.Fail(ex);
            }
        }
    }

    Row GetRow()
    {
        _firstRowEnumerated = true;
        return _row;
    }

    BackendMessage GetCurrentMessage() => messageEnumerator.Current;
    bool MoveNextMessage() => messageEnumerator.MoveNext();
    CommandFlow.MoveNextStatus TryMoveNextMessage() => messageEnumerator.TryMoveNext();
    ValueTask<bool> MoveNextMessageAsync() => messageEnumerator.MoveNextAsync();

    public struct RowEnumerator(CommandResult instance, RowBuffering buffering) : IEnumerator<Row>, IAsyncEnumerator<Row>
    {
        Row? _row;

        BackendMessage PrepareRow(BackendMessage message)
        {
            if (buffering is RowBuffering.Buffered && !message.Buffered)
            {
                message.BufferBody();
                message = instance.GetCurrentMessage();
            }
            return message;
        }

        bool PublishRow(in BackendMessage message)
        {
            (_row ??= instance.GetRow()).InitializeRow(message);
            return true;
        }

        public bool MoveNext()
        {
            // Check for null so we can use a default struct value to respresent no more rows (ADO layer uses this).
            if (instance is null)
                return false;

            _row?.RevokeColumnLease();

            if (!instance.MoveNextMessage())
            {
                if (instance._requestedExecution && instance._commandCompleteMessage is null && instance._errorMessage is null)
                    ThrowHelper.ThrowInvalidOperation("Underlying message enumerator completed before CommandComplete was returned.");
                return false;
            }

            // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
            // "Therefore, an Execute phase is always terminated by the appearance of exactly one of these messages:
            // CommandComplete, EmptyQueryResponse (if the portal was created from an empty query string), ErrorResponse, or PortalSuspended"
            var current = instance.GetCurrentMessage();
            if (current.Header.Type is PgTypes.BackendType.DataRow)
                return PublishRow(PrepareRow(current));

            return HandleUncommon(current);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
        {
            // TODO consider tracking whether the connection is exclusively holding the protocol, at that point we could terminate per row.
            // If a caller cancels we just unblock their task, the command will continue to wait until an I/O timeout is hit.
            // This produces better behavior when unrelated pipelined commands are enqueued, as the pipeline won't be frivolously aborted.
            var task = MoveNextAsync();
            return cancellationToken.CanBeCanceled && !task.IsCompleted
                ? new(task.AsTask().WaitAsync(cancellationToken))
                : task;
        }

        // TODO we must store the current task such that disposal can wait on it before disposing, we don't support concurrent reads after all.
        public ValueTask<bool> MoveNextAsync()
        {
            // Check for null so we can use a default struct value to respresent no more rows (ADO layer uses this).
            if (instance is null)
                return new(false);

            if (_row is { HasColumnLease: true } leasedRow)
                return MoveNextAfterRevokeAsync(leasedRow);

            var status = instance.TryMoveNextMessage();
            if (status is CommandFlow.MoveNextStatus.RequiresInput)
                return MoveNextAsyncCore(instance.MoveNextMessageAsync());

            if (status is CommandFlow.MoveNextStatus.EndOfSequence)
            {
                if (instance._requestedExecution && instance._commandCompleteMessage is null && instance._errorMessage is null)
                    ThrowHelper.ThrowInvalidOperation("Underlying message enumerator completed before CommandComplete was returned.");
                return new(false);
            }

            // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
            // "Therefore, an Execute phase is always terminated by the appearance of exactly one of these messages:
            // CommandComplete, EmptyQueryResponse (if the portal was created from an empty query string), ErrorResponse, or PortalSuspended"
            var current = instance.GetCurrentMessage();
            if (current.Header.Type is PgTypes.BackendType.DataRow)
            {
                if (buffering is RowBuffering.Buffered && !current.Buffered)
                    return BufferCurrentRow(in current);
                return new(PublishRow(in current));
            }

            return new(HandleUncommon(current));
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        ValueTask<bool> BufferCurrentRow(in BackendMessage current)
        {
            var bufferTask = current.BufferBodyAsync(default);
            if (!bufferTask.IsCompletedSuccessfully)
                return BufferRowAsync(bufferTask, _row ??= instance.GetRow());
            return new(PublishRow(instance.GetCurrentMessage()));
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        async ValueTask<bool> MoveNextAfterRevokeAsync(Row row)
        {
            await row.RevokeColumnLeaseAsync().ConfigureAwait(false);
            return await MoveNextAsync().ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        bool HandleUncommon(in BackendMessage current)
        {
            var type = current.Header.Type;
            switch (type)
            {
                case PgTypes.BackendType.EmptyQueryResponse:
                case PgTypes.BackendType.CommandComplete:
                case PgTypes.BackendType.ErrorResponse:
                    instance.CompleteCommand(current);
                    return false;
                case PgTypes.BackendType.PortalSuspended when !instance._simpleProtocol:
                default:
                    ThrowHelper.ThrowUnhandledCase(type);
                    return default;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        async ValueTask<bool> MoveNextAsyncCore(ValueTask<bool> task)
        {
            if (!await task.ConfigureAwait(false))
            {
                if (instance._requestedExecution && instance._commandCompleteMessage is null && instance._errorMessage is null)
                    ThrowHelper.ThrowInvalidOperation("Underlying message enumerator completed before CommandComplete was returned.");
                return false;
            }

            // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
            // "Therefore, an Execute phase is always terminated by the appearance of exactly one of these messages:
            // CommandComplete, EmptyQueryResponse (if the portal was created from an empty query string), ErrorResponse, or PortalSuspended"
            var current = instance.GetCurrentMessage();
            if (current.Header.Type is PgTypes.BackendType.DataRow)
            {
                if (buffering is RowBuffering.Buffered && !current.Buffered)
                    await current.BufferBodyAsync(default).ConfigureAwait(false);
                return PublishRow(instance.GetCurrentMessage());
            }

            return HandleUncommon(current);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        async ValueTask<bool> BufferRowAsync(ValueTask task, Row row)
        {
            await task.ConfigureAwait(false);
            row.InitializeRow(instance.GetCurrentMessage());
            return true;
        }

        public Row Current => _row!;

        internal void RevokeColumnLease() => _row?.RevokeColumnLease();

        internal ValueTask RevokeColumnLeaseAsync()
            => _row is { HasColumnLease: true } row
                ? row.RevokeColumnLeaseAsync()
                : default;

        // We enumerate all so we always get to store the error or command complete message.
        public void Dispose()
        {
            if (instance is null)
                return;
            instance.Complete();
        }

        // We enumerate all so we always get to store the error or command complete message.
        public ValueTask DisposeAsync()
            => instance?.CompleteAsync() ?? default;

        object IEnumerator.Current => Current;
        void IEnumerator.Reset() => throw new NotSupportedException();
        ValueTask<bool> IAsyncEnumerator<Row>.MoveNextAsync() => MoveNextAsync();
    }

    IEnumerator<Row> IEnumerable<Row>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    IAsyncEnumerator<Row> IAsyncEnumerable<Row>.GetAsyncEnumerator(CancellationToken cancellationToken)
        => GetAsyncEnumerator(cancellationToken);
}
