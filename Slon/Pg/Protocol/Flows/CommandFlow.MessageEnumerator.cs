using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg.Protocol.Flows;

partial class CommandFlow
{
    internal struct ReadState
    {
        public ResultMessageEnumerator ResultMessageEnumerator { get; }
        public CommandResult<ResultMessageEnumerator> CommandResult { get; }
        public ValueTaskSourcePromise<bool> ReadPromise { get; }
        public RowDescription RowDescription { get; }

        public ReadState()
        {
            ResultMessageEnumerator = new();
            CommandResult = new(ResultMessageEnumerator);
            ReadPromise = new();
            RowDescription = new();
        }

        public void Reset()
        {
            CommandResult.Reset();
            ResultMessageEnumerator.Reset();
            RowDescription.Reset();
        }
    }

    // This is a struct to make CommandResult<T> specialize.
    public readonly struct ResultMessageEnumerator() : IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
    {
        readonly MessageEnumerator _messageEnumerator = new();
        public bool MoveNext() => _messageEnumerator.MoveNext();
        public ValueTask<bool> MoveNextAsync() => _messageEnumerator.MoveNextAsync();
        public BackendMessage Current => _messageEnumerator.Current;

        public void Dispose() => _messageEnumerator.Dispose();
        public ValueTask DisposeAsync() => _messageEnumerator.DisposeAsync();

        void IEnumerator.Reset() => ((IEnumerator)_messageEnumerator).Reset();
        BackendMessage IAsyncEnumerator<BackendMessage>.Current => _messageEnumerator.Current;
        BackendMessage IEnumerator<BackendMessage>.Current => _messageEnumerator.Current;
        object? IEnumerator.Current => ((IEnumerator)_messageEnumerator).Current;

        public void Initialize(CommandFlow flow, PgDecoder decoder)
            => _messageEnumerator.Initialize(flow, decoder);

        public void Reset() => _messageEnumerator.Reset();

        public (PgError Error, TransactionStatus TransactionStatus)? CompleteError
            => _messageEnumerator.CompleteError;

        sealed class MessageEnumerator : IEnumerator<BackendMessage>, IAsyncEnumerator<BackendMessage>
        {
            CommandFlow _flow = null!;
            PgDecoder _decoder = null!;
            bool _disposed;
            bool _first;
            bool _done;
            ExceptionDispatchInfo? _exceptionDispatchInfo;
            (PgError, TransactionStatus)? _completeError;

            Command Command => _flow._commands[_flow._commandIndex];

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
                    if (_decoder.TryGetNext(out var message))
                    {
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
                if (_first)
                    return new(EnumerateFirst());

                _exceptionDispatchInfo?.Throw();
                if (_done)
                    return new(false);

                // We don't capture TryGetNext errors, we assume no IO will happen and it prevents inlining of this method.
                if (_decoder.TryGetNext(out var message))
                {
                    DebugEnsureExpected(message);
                    if (message.Header.Type is not PgTypes.BackendType.DataRow)
                        _done = true;
                    return new(true);
                }

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
                    _completeError = Command.Complete(_decoder);
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
                        var completion = Command.CompleteAsync(decoder);
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
                            if (decoder.TryGetNext(out var message) && message.Header.Type is not PgTypes.BackendType.DataRow)
                                break;

                            message = await decoder.GetNextAsync().ConfigureAwait(false);
                            if (message.Header.Type is not PgTypes.BackendType.DataRow)
                                break;
                        }
                        _completeError = await Command.CompleteAsync(decoder).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                        throw;
                    }
                }
            }

            public void Initialize(CommandFlow flow, PgDecoder decoder)
            {
                if (!ReferenceEquals(_flow, flow))
                    _flow = flow;
                if (!ReferenceEquals(_decoder, decoder))
                    _decoder = decoder;

                _exceptionDispatchInfo = null;
                _disposed = false;
                _completeError = null;

                // A command is immediately done if we haven't submitted an execute.
                _done = Command.DescribeOnly;
                _first = !_done;
            }

            public void Reset()
            {
                _flow = null!;
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
