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
        public ValueTaskSourcePromise<bool> ReadPromise { get; }
        // The reader-driven collect body's promise: one per wire, single-pumped by decoder ownership.
        public ValueTaskSourcePromise<bool> CollectPromise { get; }
        public RowDescription RowDescription { get; }

        public ReadState()
        {
            ResultMessageEnumerator = new();
            CommandResult = new(ResultMessageEnumerator);
            ReadPromise = new();
            CollectPromise = new();
            RowDescription = new();
        }

        public void Reset()
        {
            CommandResult.Reset();
            ResultMessageEnumerator.Reset();
            RowDescription.Reset();
        }
    }

    // The value wrapper lets ReadState and CommandResult share one MessageEnumerator instance
    // without an interface or another adapter allocation.
    internal readonly struct ResultMessageEnumerator() : IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
    {
        readonly MessageEnumerator _messageEnumerator = new();
        public bool MoveNext() => _messageEnumerator.MoveNext();
        public ValueTask<bool> MoveNextAsync() => _messageEnumerator.MoveNextAsync();
        public BackendMessage Current => _messageEnumerator.Current;
        internal MoveNextStatus TryMoveNext() => _messageEnumerator.TryMoveNext();
        // Fused read for a consumer that owns the message loop: parks on the decoder itself instead
        // of the MoveNextAsync/GetNextAsync frames. The caller records the move with MarkMoved.
        internal ValueTask<bool> ReadNextAsync() => _messageEnumerator.ReadNextAsync();
        internal void MarkMoved() => _messageEnumerator.MarkMoved();

        public void Dispose() => _messageEnumerator.Dispose();
        public ValueTask DisposeAsync() => _messageEnumerator.DisposeAsync();

        void IEnumerator.Reset() => ((IEnumerator)_messageEnumerator).Reset();
        BackendMessage IAsyncEnumerator<BackendMessage>.Current => _messageEnumerator.Current;
        BackendMessage IEnumerator<BackendMessage>.Current => _messageEnumerator.Current;
        object? IEnumerator.Current => ((IEnumerator)_messageEnumerator).Current;

        public void Initialize(in Command command, PgDecoder decoder)
            => _messageEnumerator.Initialize(command, decoder);

        public void Reset() => _messageEnumerator.Reset();

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

            public ValueTask<bool> ReadNextAsync() => _decoder.MoveNextAsync();

            public void MarkMoved()
            {
                var message = _decoder.Current;
                DebugEnsureExpected(message);
                if (message.Header.Type is not PgTypes.BackendType.DataRow)
                    _done = true;
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
                _describeOnly = command.DescribeOnly;
                _withSync = command.WithSync;
                if (!ReferenceEquals(_decoder, decoder))
                    _decoder = decoder;

                _exceptionDispatchInfo = null;
                _disposed = false;
                _completeError = null;

                // A command is immediately done if we haven't submitted an execute.
                _done = _describeOnly;
                _first = !_done;
            }

            public void Reset()
            {
                _describeOnly = false;
                _withSync = false;
                _decoder = null!;
                _exceptionDispatchInfo = null;
                _completeError = null;
                _disposed = true;
                _first = false;
                _done = true;
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
