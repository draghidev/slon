using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg.Protocol.Flows;

partial class CommandFlow
{
    internal enum MoveNextStatus : byte
    {
        RequiresInput,
        EndOfSequence,
        Moved
    }

    internal struct ReadState
    {
        public ResultMessageEnumerator ResultMessageEnumerator { get; }
        public CommandResult CommandResult { get; }
        public RowDescription RowDescription { get; }

        public ReadState()
        {
            ResultMessageEnumerator = new();
            CommandResult = new(ResultMessageEnumerator);
            RowDescription = new();
        }

        public void Reset()
        {
            CommandResult.Reset();
            ResultMessageEnumerator.Reset();
            RowDescription.Reset();
        }
    }

    internal readonly struct ReadPromiseState()
    {
        public ValueTaskSourcePromise<bool> Promise { get; } = new();
    }

    // The value wrapper lets ReadState and CommandResult share one MessageEnumerator instance
    // without an interface or another adapter allocation.
    internal readonly struct ResultMessageEnumerator() : IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
    {
        readonly MessageEnumerator _messageEnumerator = new();
        public bool MoveNext() => _messageEnumerator.MoveNext();
        public ValueTask<bool> MoveNextAsync() => _messageEnumerator.MoveNextAsync();
        public BackendMessage Current => _messageEnumerator.Current;
        internal BackendMessage.Accessor CurrentAccessor => _messageEnumerator.CurrentAccessor;
        internal MoveNextStatus TryMoveNext() => _messageEnumerator.TryMoveNext();
        // RowEnumerator classifies the publication itself, avoiding a second Current copy here.
        internal MoveNextStatus TryMoveNextRow() => _messageEnumerator.TryMoveNextRow();
        internal void MarkCurrentTerminal() => _messageEnumerator.MarkCurrentTerminal();
        internal ValueTask<BackendMessage> CollectRowsAsync<TState>(
            TState state, Action<TState, CommandResult.RowView> collector,
            CancellationToken cancellationToken)
            => _messageEnumerator.CollectRowsAsync(state, collector, cancellationToken);
        internal void ThrowCollectorException() => _messageEnumerator.ThrowCollectorException();

        public void Dispose() => _messageEnumerator.Dispose();
        public ValueTask DisposeAsync() => _messageEnumerator.DisposeAsync();

        void IEnumerator.Reset() => ((IEnumerator)_messageEnumerator).Reset();
        BackendMessage IAsyncEnumerator<BackendMessage>.Current => _messageEnumerator.Current;
        BackendMessage IEnumerator<BackendMessage>.Current => _messageEnumerator.Current;
        object? IEnumerator.Current => ((IEnumerator)_messageEnumerator).Current;

        public void Initialize(in Command command, PgDecoder decoder)
            => _messageEnumerator.Initialize(command, decoder);

        public void Reset() => _messageEnumerator.Reset();

        public void EnableResultSetBuffering()
            => _messageEnumerator.EnableResultSetBuffering();

        public (PgError Error, TransactionStatus TransactionStatus)? CompleteError
            => _messageEnumerator.CompleteError;

        sealed class MessageEnumerator : IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
        {
            // Completion needs only these two command facts. Retaining them keeps the protocol-static
            // enumerator independent of the flow and avoids holding a reference-bearing command copy.
            bool _describeOnly;
            bool _withSync;
            PgDecoder _decoder = null!;
            bool _disposed;
            bool _first;
            bool _done;
            ExceptionDispatchInfo? _exceptionDispatchInfo;
            ExceptionDispatchInfo? _collectorException;
            (PgError, TransactionStatus)? _completeError;

            // An Execute response consists of DataRow messages followed by one terminal message.
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
                    BackendMessage message;
                    if (_decoder.TryMoveNext())
                    {
                        message = _decoder.Current;
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
                var status = TryMoveNext();
                if (status is not MoveNextStatus.RequiresInput)
                    return new(status is MoveNextStatus.Moved);

                return Core();

                [RuntimeAsyncMethodGeneration(false)]
                [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
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

            enum CollectRowsStatus : byte
            {
                RequiresInput,
                RequiresBuffer,
                Complete
            }

            public ValueTask<BackendMessage> CollectRowsAsync<TState>(
                TState state, Action<TState, CommandResult.RowView> collector,
                CancellationToken cancellationToken)
            {
                try
                {
                    var status = CollectAvailableRows(
                        state, collector, currentReady: false, out var pending, out var terminal);
                    return status is CollectRowsStatus.Complete
                        ? new(terminal)
                        : Core(this, state, collector, status, pending, cancellationToken);
                }
                catch (Exception ex)
                {
                    _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                    return ValueTask.FromException<BackendMessage>(ex);
                }

                static async ValueTask<BackendMessage> Core(
                    MessageEnumerator enumerator,
                    TState state, Action<TState, CommandResult.RowView> collector,
                    CollectRowsStatus status, BackendMessage.Accessor pending,
                    CancellationToken cancellationToken)
                {
                    try
                    {
                        while (true)
                        {
                            if (status is CollectRowsStatus.RequiresInput)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                _ = await enumerator._decoder.GetNextAsync().ConfigureAwait(false);
                            }
                            else
                            {
                                await pending.BufferBodyAsync(cancellationToken).ConfigureAwait(false);
                            }

                            status = enumerator.CollectAvailableRows(
                                state, collector, currentReady: true, out pending, out var terminal);
                            if (status is CollectRowsStatus.Complete)
                                return terminal;
                        }
                    }
                    catch (Exception ex)
                    {
                        enumerator._exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                        throw;
                    }
                }
            }

            CollectRowsStatus CollectAvailableRows<TState>(
                TState state, Action<TState, CommandResult.RowView> collector,
                bool currentReady,
                out BackendMessage.Accessor pending,
                out BackendMessage terminal)
            {
                var status = CollectAvailableRowsCore(state, collector, currentReady);
                if (status is CollectRowsStatus.RequiresInput)
                {
                    pending = default;
                    terminal = default;
                }
                else if (status is CollectRowsStatus.RequiresBuffer)
                {
                    pending = _decoder.CurrentAccessor;
                    terminal = default;
                }
                else
                {
                    _done = true;
                    pending = default;
                    terminal = _decoder.Current;
                }
                return status;
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            CollectRowsStatus CollectAvailableRowsCore<TState>(
                TState state, Action<TState, CommandResult.RowView> collector,
                bool currentReady)
            {
                var decoder = _decoder;
                var collect = _collectorException is null;
                if (currentReady)
                    goto ProcessCurrent;
                if (_first)
                {
                    _first = false;
                    goto ProcessCurrent;
                }

                _exceptionDispatchInfo?.Throw();
                if (_done)
                    ThrowHelper.ThrowInvalidOperation(
                        "Underlying message enumerator completed before a terminal message was returned.");

                MoveNext:
                if (!decoder.TryMoveNext())
                    return CollectRowsStatus.RequiresInput;

                ProcessCurrent:
                DebugEnsureExpected(decoder.Current);
                if (decoder.CurrentType is not PgTypes.BackendType.DataRow)
                    return CollectRowsStatus.Complete;
                if (!decoder.CurrentBuffered)
                    return CollectRowsStatus.RequiresBuffer;

                if (collect)
                {
                    try
                    {
                        collector(state, new CommandResult.RowView(decoder.CurrentBufferedBody));
                    }
                    catch (Exception ex)
                    {
                        _collectorException = ExceptionDispatchInfo.Capture(ex);
                        collect = false;
                    }
                }
                goto MoveNext;
            }

            public void ThrowCollectorException()
            {
                var exception = _collectorException;
                _collectorException = null;
                exception?.Throw();
            }

            public MoveNextStatus TryMoveNext()
            {
                if (_first)
                {
                    _ = EnumerateFirst();
                    return MoveNextStatus.Moved;
                }

                _exceptionDispatchInfo?.Throw();
                if (_done)
                    return MoveNextStatus.EndOfSequence;

                if (_decoder.TryMoveNext())
                {
                    var message = _decoder.Current;
                    DebugEnsureExpected(message);
                    if (message.Header.Type is not PgTypes.BackendType.DataRow)
                        _done = true;
                    return MoveNextStatus.Moved;
                }

                return MoveNextStatus.RequiresInput;
            }

            public MoveNextStatus TryMoveNextRow()
            {
                if (_first)
                {
                    _first = false;
                    return MoveNextStatus.Moved;
                }

                _exceptionDispatchInfo?.Throw();
                if (_done)
                    return MoveNextStatus.EndOfSequence;

                return _decoder.TryMoveNext()
                    ? MoveNextStatus.Moved
                    : MoveNextStatus.RequiresInput;
            }

            public void MarkCurrentTerminal() => _done = true;
            public BackendMessage Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _decoder.Current;
            }

            internal BackendMessage.Accessor CurrentAccessor => _decoder.CurrentAccessor;

            public void Dispose()
            {
                _exceptionDispatchInfo?.Throw();
                if (_disposed)
                    return;
                _disposed = true;
                try
                {
                    var decoder = _decoder;
                    if (!decoder.TryGetCurrent(out var current)
                        || current.Header.Type is PgTypes.BackendType.DataRow)
                    {
                        while (decoder.GetNext().Header.Type is PgTypes.BackendType.DataRow) {}
                    }
                    _completeError = CommandExtensions.Complete(_describeOnly, _withSync, _decoder);
                }
                catch (Exception ex)
                {
                    _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                    throw;
                }
            }

            public ValueTask DisposeAsync()
            {
                _exceptionDispatchInfo?.Throw();
                if (_disposed)
                    return new();

                return DisposeAsyncCore();
            }

            ValueTask DisposeAsyncCore()
            {
                _disposed = true;
                try
                {
                    var decoder = _decoder;
                    if (decoder.TryGetCurrent(out var current)
                        && current.Header.Type is not PgTypes.BackendType.DataRow)
                    {
                        var completion = CommandExtensions.CompleteAsync(_describeOnly, _withSync, decoder);
                        if (completion.IsCompletedSuccessfully)
                        {
                            _completeError = completion.Result;
                            return new();
                        }
                        return AwaitCompletion(completion);
                    }
                    return DrainRowsAndComplete(decoder);
                }
                catch (Exception ex)
                {
                    _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                    return ValueTask.FromException(ex);
                }

                async ValueTask AwaitCompletion(ValueTask<(PgError, TransactionStatus)?> completion)
                {
                    try
                    {
                        _completeError = await completion.ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                        throw;
                    }
                }

                async ValueTask DrainRowsAndComplete(PgDecoder decoder)
                {
                    try
                    {
                        while (true)
                        {
                            if (decoder.TryMoveNext())
                            {
                                if (decoder.Current.Header.Type is not PgTypes.BackendType.DataRow)
                                    break;
                                continue;
                            }

                            var message = await decoder.GetNextAsync().ConfigureAwait(false);
                            if (message.Header.Type is not PgTypes.BackendType.DataRow)
                                break;
                        }
                        _completeError = await CommandExtensions.CompleteAsync(_describeOnly, _withSync, decoder).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                        throw;
                    }
                }
            }

            public void Initialize(in Command command, PgDecoder decoder)
            {
                if (_decoder is not null)
                    _decoder.ResultSetBuffering = false;
                _describeOnly = command.DescribeOnly;
                _withSync = command.WithSync;
                if (!ReferenceEquals(_decoder, decoder))
                    _decoder = decoder;

                _exceptionDispatchInfo = null;
                if (_collectorException is not null)
                    _collectorException = null;
                _disposed = false;
                _completeError = null;

                // A command is immediately done if we haven't submitted an execute.
                _done = _describeOnly;
                _first = !_done;
            }

            public void Reset()
            {
                if (_decoder is not null)
                    _decoder.ResultSetBuffering = false;
                _describeOnly = false;
                _withSync = false;
                _decoder = null!;
                _exceptionDispatchInfo = null;
                if (_collectorException is not null)
                    _collectorException = null;
                _completeError = null;
                _disposed = true;
                _first = false;
                _done = true;
            }

            public void EnableResultSetBuffering()
            {
                if (_disposed)
                    ThrowHelper.ThrowInvalidOperation(
                        "The command result has already been released.");
                _decoder.ResultSetBuffering = true;
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
    }}
