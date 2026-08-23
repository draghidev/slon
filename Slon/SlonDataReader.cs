using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Serialization;

namespace Slon;

// Implementation
public sealed partial class SlonDataReader
{
    const CommandBehavior EnumerateCommandResultsBehavior = (CommandBehavior)int.MinValue;

    int _state;
    ReaderState State
    {
        get => (ReaderState)Volatile.Read(ref _state);
        set => Volatile.Write(ref _state, (int)value);
    }

    SlonConnection? _connectionToClose;

    bool _singleRowBehavior;
    CommandResult.RowBuffering _rowBuffering;
    bool EnumerateCommandResults { get; set; }

    // Serializes enumerator retirement across exhaustion, Close, and Dispose.
    bool _enumeratorDisposalActive;

    // Completion may surface when rows end before NextResult applies that result.
    ResultCompletionState _currentCompletion;
    RowPresence _rowPresence;

    CommandResult.RowEnumerator _rowEnumerator;
    PgSerializerFieldReader _fieldReader;
    int _remainingResults;
    CommandFlow.Enumerator _enumerator;
    long? _recordsAffected;

    SlonDataReader() { }

    void Initialize(CommandFlow.Enumerator enumerator, CommandBehavior behavior, int remainingResults,
        PgSerializerOptions serializerOptions, SlonConnection? connectionToClose,
        long? recordsAffected, bool hasCurrent)
    {
        if (Interlocked.CompareExchange(ref _state, (int)ReaderState.Initializing,
                (int)ReaderState.Uninitialized) is not (int)ReaderState.Uninitialized)
            ThrowHelper.ThrowInvalidOperation("Reader is already initialized.");

        _enumerator = enumerator;
        _connectionToClose = connectionToClose;
        _singleRowBehavior = behavior.HasFlag(CommandBehavior.SingleRow);
        EnumerateCommandResults = ShouldEnumerateCommandResults(behavior);
        _remainingResults = remainingResults;
        _fieldReader = new(serializerOptions);
        _rowBuffering = behavior.HasFlag(CommandBehavior.SequentialAccess)
            ? CommandResult.RowBuffering.Streaming
            : CommandResult.RowBuffering.Buffered;
        _recordsAffected = recordsAffected;

        _enumeratorDisposalActive = false;
        _currentCompletion = ResultCompletionState.None;
        _rowPresence = RowPresence.Unknown;
        _rowEnumerator = default;

        if (hasCurrent)
        {
            var processed = InitializeCurrentResult();
            Debug.Assert(processed);
        }
        State = ReaderState.Active;
    }

    static int GetResultLimit(CommandBehavior behavior, int commandCount)
        => behavior.HasFlag(CommandBehavior.SingleRow) || behavior.HasFlag(CommandBehavior.SingleResult)
            ? 1
            : commandCount;

    static bool ShouldEnumerateCommandResults(CommandBehavior behavior)
        => behavior.HasFlag(EnumerateCommandResultsBehavior);

    static SlonDataReader CreateReader(CommandFlow.Enumerator enumerator, CommandBehavior behavior,
        int remainingResults, PgSerializerOptions serializerOptions,
        SlonConnection? connectionToClose, long? recordsAffected, bool hasCurrent)
    {
        var reader = new SlonDataReader();
        reader.Initialize(enumerator, behavior, remainingResults, serializerOptions,
            connectionToClose, recordsAffected, hasCurrent);
        return reader;
    }

    int FieldCountCore => Current?.FieldCount ?? 0;
    ref PgSerializerFieldReader FieldReader => ref _fieldReader;
    CommandResult? Current => _enumerator.Current;
    bool IsSequential => _rowBuffering is CommandResult.RowBuffering.Streaming;

    internal static SlonDataReader Create(CommandBehavior behavior, CommandFlow flow,
        PgSerializerOptions serializerOptions,
        SlonConnection? connectionToClose = null)
    {
        var enumerator = flow.GetEnumerator();
        try
        {
            var reader = CreateReader(enumerator, behavior,
                GetResultLimit(behavior, flow.VisibleCommandCount), serializerOptions,
                connectionToClose, recordsAffected: null, hasCurrent: false);
            reader.NextResultCore();
            return reader;
        }
        catch (Exception)
        {
            try
            {
                enumerator.Dispose();
            }
            finally
            {
                connectionToClose?.Close();
            }
            throw;
        }
    }

    internal static async ValueTask<TReader> CreateAsync<TReader>(CommandBehavior behavior,
        ValueTask<CommandFlow> flowTask, PgSerializerOptions serializerOptions,
        CancellationToken cancellationToken = default,
        SlonConnection? connectionToClose = null, Activity? activity = null)
        where TReader : DbDataReader
    {
        Debug.Assert(typeof(TReader) == typeof(SlonDataReader) || typeof(TReader) == typeof(DbDataReader));
        CommandFlow.Enumerator enumerator = default;
        try
        {
            var flow = await flowTask.ConfigureAwait(false);
            enumerator = flow.GetEnumerator();
            var remainingResults = GetResultLimit(behavior, flow.VisibleCommandCount);
            long? recordsAffected = null;

            // Advance to the first row-bearing result before allocating the reader.
            while (remainingResults > 0)
            {
                if (!await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    break;
                if (enumerator.Current.CanHaveRows || ShouldEnumerateCommandResults(behavior))
                    return (TReader)(object)CreateReader(enumerator, behavior, remainingResults,
                        serializerOptions, connectionToClose, recordsAffected, hasCurrent: true);

                remainingResults--;
                if (!enumerator.Current.IsComplete)
                    await enumerator.Current.CompleteAsync().ConfigureAwait(false);
                ApplyCompletion(enumerator.Current, ref recordsAffected);
            }

            // Dispose the enumerator right away to allow the pipeline to handle next commands.
            // This also has the benefit Close/Dispose doesn't have to go async if the user exhausted the reader properly.
            var enumeratorToDispose = enumerator;
            enumerator = default;
            await enumeratorToDispose.DisposeAsync().ConfigureAwait(false);
            return (TReader)(object)CreateReader(default, behavior, remainingResults,
                serializerOptions, connectionToClose, recordsAffected, hasCurrent: false);
        }
        catch (Exception ex)
        {
            try
            {
                try
                {
                    await enumerator.DisposeAsync().ConfigureAwait(false);
                }
                finally
                {
                    if (connectionToClose is not null)
                        await connectionToClose.CloseAsync().ConfigureAwait(false);
                }
            }
            catch (Exception cleanupException)
            {
                ex = cleanupException;
            }

            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default!;
        }
        finally
        {
            activity?.Dispose();
        }

        static void ApplyCompletion(CommandResult current, ref long? recordsAffected)
        {
            current.TryGetCommandComplete(out _);
            if (current.Error is null)
                recordsAffected += current.RecordsAffected;
        }
    }

    SlonConnection? TakeConnectionToClose()
    {
        var connection = _connectionToClose;
        _connectionToClose = null;
        return connection;
    }

    // Initializes row and serializer state for the current flow result. Results without rows are
    // exposed only when command-result enumeration was requested.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool InitializeCurrentResult()
    {
        Debug.Assert(_remainingResults is > 0);
        if (Current is not { } current)
            return false;
        _fieldReader.Initialize(current);

        _remainingResults--;
        _currentCompletion = ResultCompletionState.None;
        _rowPresence = RowPresence.Unknown;
        if (!current.CanHaveRows)
        {
            _rowEnumerator = default;
            return EnumerateCommandResults;
        }

        _rowEnumerator = current.GetEnumerator(_rowBuffering);
        return true;
    }

    bool NextResultCore()
    {
        try
        {
            if (_currentCompletion is not ResultCompletionState.Applied && Current is { } current)
            {
                if (!current.IsComplete)
                    current.Complete();
                ApplyCompletion(current);
            }

            var next = false;
            while (_remainingResults > 0
                && (next = _enumerator.MoveNext())
                && !InitializeCurrentResult())
            {
                current = Current!;
                if (!current.IsComplete)
                    current.Complete();
                ApplyCompletion(current);
            }
            if (!next)
            {
                // Release the flow as soon as its results end.
                DisposeEnumerator(out _);
            }
            return next;
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
            return default;
        }
    }

    async Task<bool> NextResultAsyncCore(CancellationToken cancellationToken)
    {
        try
        {
            if (_currentCompletion is not ResultCompletionState.Applied && Current is { } current)
            {
                if (!current.IsComplete)
                    await current.CompleteAsync().ConfigureAwait(false);
                ApplyCompletion(current);
            }

            var next = false;
            while (_remainingResults > 0
                && (next = await _enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                && !InitializeCurrentResult())
            {
                current = Current!;
                if (!current.IsComplete)
                    await current.CompleteAsync().ConfigureAwait(false);
                ApplyCompletion(current);
            }
            if (!next)
                await DisposeEnumeratorAsync().ConfigureAwait(false);
            return next;
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
            return default;
        }
    }

    void SurfaceCompletion(CommandResult current)
    {
        if (_currentCompletion is ResultCompletionState.None)
        {
            if (current.Error is not null)
                _currentCompletion = ResultCompletionState.Surfaced;
            current.TryGetCommandComplete(out _);
            _currentCompletion = ResultCompletionState.Surfaced;
        }
    }

    void ApplyCompletion(CommandResult current)
    {
        SurfaceCompletion(current);
        if (current.Error is null)
            _recordsAffected += current.RecordsAffected;
        _currentCompletion = ResultCompletionState.Applied;
    }

    bool ReadCore()
    {
        try
        {
            Debug.Assert(_singleRowBehavior && _remainingResults is 0 || !_singleRowBehavior);
            if (_rowPresence is RowPresence.Prefetched)
            {
                _rowPresence = RowPresence.Present;
                return true;
            }

            // After one SingleRow result, normal result disposal drains the remainder.
            var hasRow = (!_singleRowBehavior || _rowPresence is not RowPresence.Present)
                && _rowEnumerator.MoveNext();
            return ProcessReadResult(hasRow);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
            return default;
        }
    }

    async Task<bool> ReadAsyncCore(CancellationToken cancellationToken)
    {
        try
        {
            Debug.Assert(_singleRowBehavior && _remainingResults is 0 || !_singleRowBehavior);
            bool hasRow;
            if (_rowPresence is RowPresence.Prefetched)
            {
                _rowPresence = RowPresence.Present;
                return true;
            }
            else if (_singleRowBehavior && _rowPresence is RowPresence.Present)
            {
                hasRow = false;
            }
            else
            {
                hasRow = await _rowEnumerator.MoveNextAsync(cancellationToken)
                    .ConfigureAwait(false);
            }

            if (hasRow)
                return ProcessReadResult(hasRow: true);

            if (Current is { IsComplete: false } current)
                await current.CompleteAsync().ConfigureAwait(false);
            return ProcessReadResult(hasRow: false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
            return default;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool ProcessReadResult(bool hasRow)
    {
        if (hasRow)
        {
            _rowPresence = RowPresence.Present;
            return true;
        }

        if (_rowPresence is RowPresence.Unknown)
            _rowPresence = RowPresence.Empty;
        CompleteRowEnumeration();
        return false;
    }

    bool HasAnyRows
    {
        get
        {
            if (_rowPresence is not RowPresence.Unknown)
                return _rowPresence is RowPresence.Prefetched or RowPresence.Present;
            if (Current is null)
                return false;

            var hasRows = _rowEnumerator.MoveNext();
            _rowPresence = hasRows ? RowPresence.Prefetched : RowPresence.Empty;
            if (!hasRows)
                CompleteRowEnumeration();
            return hasRows;
        }
    }

    void CompleteRowEnumeration()
    {
        var current = Current;
        var finalResult = _remainingResults is 0;
        try
        {
            if (current is not null)
            {
                if (!current.IsComplete)
                    current.Complete();
                if (finalResult)
                    ApplyCompletion(current);
                else
                    SurfaceCompletion(current);
            }
        }
        finally
        {
            if (finalResult)
                DisposeEnumerator(out _);
        }
    }

    void DisposeEnumerator(out bool ownsCleanup)
    {
        ownsCleanup = false;
        var (rowEnumerator, enumerator) = BeginEnumeratorDisposal();
        ownsCleanup = true;
        try
        {
            try
            {
                rowEnumerator.RevokeColumnLease();
            }
            finally
            {
                enumerator.Dispose();
            }
        }
        finally
        {
            EndEnumeratorDisposal();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    async ValueTask DisposeEnumeratorAsync()
    {
        var (rowEnumerator, enumerator) = BeginEnumeratorDisposal();
        try
        {
            try
            {
                await rowEnumerator.RevokeColumnLeaseAsync().ConfigureAwait(false);
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            EndEnumeratorDisposal();
        }
    }

    (CommandResult.RowEnumerator Rows, CommandFlow.Enumerator Results) BeginEnumeratorDisposal()
    {
        if (_enumeratorDisposalActive)
            ThrowHelper.ThrowInvalidOperation("Invalid concurrent call.");
        _enumeratorDisposalActive = true;

        var cleanup = (_rowEnumerator, _enumerator);
        _rowEnumerator = default;
        _enumerator = default;
        return cleanup;
    }

    void EndEnumeratorDisposal() => _enumeratorDisposalActive = false;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ThrowIfClosedOrDisposed()
    {
        var state = State;
        if (state is not ReaderState.Active)
            ThrowInvalidState(state);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    Exception? GetExceptionIfClosedOrDisposed()
    {
        var state = State;
        if (state is not ReaderState.Active)
            return GetInvalidStateException(state);

        return null;
    }

    Row CurrentRow
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            var row = _rowEnumerator.Current;
            if (row is null)
                ThrowInvalidState(State);

            return row!;
        }
    }

    RowDescription CurrentRowDescription
    {
        get
        {
            if (Current is null)
                throw new InvalidOperationException("Reader is not on a result.");

            var description = FieldReader.RowDescription;
            return description.FieldCount is 0
                ? throw new InvalidOperationException("The current result has no columns.")
                : description;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    Exception GetInvalidStateException(ReaderState readerState)
    {
        var exception = readerState switch
        {
            ReaderState.Uninitialized or ReaderState.Disposed => new ObjectDisposedException(nameof(SlonDataReader)),
            ReaderState.Closed => new InvalidOperationException("Reader is closed."),
            _ => new InvalidOperationException("Reader is not on a row.")
        };
        return ExceptionDispatchInfo.SetCurrentStackTrace(exception);
    }

    [DoesNotReturn, MethodImpl(MethodImplOptions.NoInlining)]
    void ThrowInvalidState(ReaderState readerState)
        => throw GetInvalidStateException(readerState);

    void CloseCore(bool resetForReuse)
    {
        var ownsCleanup = false;
        try
        {
            try
            {
                DisposeEnumerator(out ownsCleanup);
            }
            finally
            {
                TakeConnectionToClose()?.Close();
            }
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
        finally
        {
            if (resetForReuse && ownsCleanup)
                Reset();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    async ValueTask CloseAsyncCore(bool resetForReuse)
    {
        var ownsCleanup = false;
        try
        {
            try
            {
                var (rowEnumerator, enumerator) = BeginEnumeratorDisposal();
                ownsCleanup = true;
                try
                {
                    try
                    {
                        await rowEnumerator.RevokeColumnLeaseAsync().ConfigureAwait(false);
                    }
                    finally
                    {
                        await enumerator.DisposeAsync().ConfigureAwait(false);
                    }
                }
                finally
                {
                    EndEnumeratorDisposal();
                }
            }
            finally
            {
                if (TakeConnectionToClose() is { } connection)
                    await connection.CloseAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
        finally
        {
            if (resetForReuse && ownsCleanup)
                Reset();
        }
    }

    void Reset()
    {
        _connectionToClose = null;
        _singleRowBehavior = false;
        _rowBuffering = default;
        EnumerateCommandResults = false;
        _enumeratorDisposalActive = false;
        _currentCompletion = ResultCompletionState.None;
        _rowPresence = RowPresence.Unknown;
        _rowEnumerator = default;
        _fieldReader = default;
        _remainingResults = 0;
        _enumerator = default;
        _recordsAffected = null;
        State = ReaderState.Uninitialized;
    }

    async Task<bool> IsDBNullAsyncCore(int ordinal, CancellationToken cancellationToken)
    {
        try
        {
            _ = CurrentRowDescription[ordinal];
            return await CurrentRow.IsDBNullAsync(ordinal, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
            return default;
        }
    }

    // Non-gvm helper to make inlining GetBoolean GetString etc possible.
    T GetFieldValueCore<T>(int ordinal)
    {
        var row = CurrentRow;
        if (typeof(T) == typeof(object))
            return ReadObject<T>(this, row, ordinal);

        return FieldReader.Read<T>(row, ordinal);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static TResult ReadObject<TResult>(SlonDataReader reader, Row row, int ordinal)
            => (TResult)reader.FieldReader.ReadObject(row, ordinal);
    }

    ValueTask<T> GetFieldValueCoreAsync<T>(int ordinal, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<T>(cancellationToken);

        var row = _rowEnumerator.Current;
        if (row is null)
            return ValueTask.FromException<T>(GetInvalidStateException(State));

        if (typeof(T) == typeof(object))
            return ReadObjectAsync<T>(FieldReader.ReadObjectAsync(row, ordinal, cancellationToken));

        return FieldReader.ReadAsync<T>(row, ordinal, cancellationToken);

        static async ValueTask<TResult> ReadObjectAsync<TResult>(ValueTask<object> task)
            => (TResult)await task
                .ConfigureAwait(false);
    }

    enum RowPresence : byte
    {
        Unknown,
        Empty,
        Prefetched,
        Present
    }

    enum ResultCompletionState : byte
    {
        None,
        Surfaced,
        Applied
    }

    enum ReaderState
    {
        Uninitialized = 0,
        Initializing,
        Active,
        Closed,
        Disposed
    }
}

// Public surface & ADO.NET
/// <inheritdoc cref="System.Data.Common.DbDataReader" />
public sealed partial class SlonDataReader : DbDataReader, IDbColumnSchemaGenerator
{
    /// <inheritdoc/>
    public override int Depth => 0;
    /// <inheritdoc/>
    public override int FieldCount
    {
        get
        {
            ThrowIfClosedOrDisposed();
            return FieldCountCore;
        }
    }

    /// <inheritdoc/>
    public override object this[int ordinal] => GetValue(ordinal);
    /// <inheritdoc/>
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <summary>Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.</summary>
    /// <returns>The number of rows changed, inserted, or deleted. -1 for SELECT statements. 0 if no rows were affected or the statement failed.</returns>
    /// <remarks>When the value is too large to be represented by an Int32, int.MinValue is returned and LongRecordsAffected should be consulted instead.</remarks>
    public override int RecordsAffected
        => _recordsAffected is null
            ? -1 : _recordsAffected > int.MaxValue
                ? int.MinValue : (int)_recordsAffected;

    /// <summary>Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.</summary>
    /// <returns>The number of rows changed, inserted, or deleted. -1 for SELECT statements. 0 if no rows were affected or the statement failed.</returns>
    public long LongRecordsAffected => _recordsAffected ?? -1;

    /// <inheritdoc/>
    public override bool HasRows
    {
        get
        {
            ThrowIfClosedOrDisposed();
            return HasAnyRows;
        }
    }

    /// <inheritdoc/>
    public override bool IsClosed => State is not ReaderState.Active;

    /// <inheritdoc/>
    public override bool NextResult()
    {
        ThrowIfClosedOrDisposed();
        return NextResultCore();
    }

    /// <inheritdoc/>
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<bool>(exception);

        return NextResultAsyncCore(cancellationToken);
    }

    /// <inheritdoc/>
    public override bool Read()
    {
        ThrowIfClosedOrDisposed();
        return ReadCore();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<bool>(exception);

        return ReadAsyncCore(cancellationToken);
    }

    /// <inheritdoc/>
    public override IEnumerator GetEnumerator()
    {
        ThrowIfClosedOrDisposed();
        return new DbEnumerator(this, closeReader: false);
    }

    /// <inheritdoc/>
    public override string GetDataTypeName(int ordinal)
    {
        ThrowIfClosedOrDisposed();
        _ = CurrentRowDescription[ordinal];
        return FieldReader.GetDataTypeName(ordinal);
    }

    /// <inheritdoc/>
    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
    public override Type GetFieldType(int ordinal)
    {
        ThrowIfClosedOrDisposed();
        _ = CurrentRowDescription[ordinal];
        return FieldReader.GetFieldType(ordinal);
    }

    /// <inheritdoc/>
    public override string GetName(int ordinal)
    {
        ThrowIfClosedOrDisposed();
        return CurrentRowDescription[ordinal].Name;
    }

    /// <inheritdoc/>
    public override int GetOrdinal(string name)
    {
        ThrowIfClosedOrDisposed();
        return CurrentRowDescription.GetFieldIndex(name);
    }

    /// <inheritdoc/>
    public override bool IsDBNull(int ordinal)
    {
        ThrowIfClosedOrDisposed();
        _ = CurrentRowDescription[ordinal];
        return CurrentRow.IsDBNull(ordinal);
    }

    /// <inheritdoc/>
    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<bool>(exception);

        return cancellationToken.IsCancellationRequested
            ? Task.FromCanceled<bool>(cancellationToken)
            : IsDBNullAsyncCore(ordinal, cancellationToken);
    }

    /// <summary>Returns a nested data reader for the requested column.</summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <exception cref="T:System.NotSupportedException">Nested data readers are not supported.</exception>
    /// <returns>A data reader.</returns>
    public new SlonDataReader GetData(int ordinal)
        => throw new NotSupportedException("Nested data readers are not supported.");

    /// <inheritdoc/>
    protected override DbDataReader GetDbDataReader(int ordinal)
        => GetData(ordinal);

    /// <summary>Reads the complete field at the specified ordinal as a byte array.</summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The field contents.</returns>
    public byte[] GetBytes(int ordinal)
        => GetFieldValueCore<byte[]>(ordinal);

    /// <inheritdoc/>
    public override T GetFieldValue<T>(int ordinal)
        => GetFieldValueCore<T>(ordinal);

    /// <inheritdoc/>
    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<T>(exception);

        return GetFieldValueCoreAsync<T>(ordinal, cancellationToken).AsTask();
    }

    /// <inheritdoc/>
    public override bool GetBoolean(int ordinal)
        => GetFieldValueCore<bool>(ordinal);

    /// <inheritdoc/>
    public override byte GetByte(int ordinal)
        => GetFieldValueCore<byte>(ordinal);

    /// <inheritdoc/>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        if (dataOffset is < 0 or > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(dataOffset));
        if (buffer is not null)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(bufferOffset);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(bufferOffset, buffer.Length);
            ArgumentOutOfRangeException.ThrowIfNegative(length);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(length, buffer.Length - bufferOffset);
        }

        var row = CurrentRow;
        var lease = FieldReader.Read<ByteColumnLease>(row, ordinal, IsSequential);

        if (buffer is null)
            return lease.Length;
        if (dataOffset >= lease.Length)
            return 0;
        return lease.Read(checked((int)dataOffset), buffer.AsSpan(bufferOffset, length));
    }

    /// <inheritdoc/>
    public override char GetChar(int ordinal)
        => GetFieldValueCore<char>(ordinal);

    /// <inheritdoc/>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        if (dataOffset is < 0 or > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(dataOffset));
        if (buffer is not null)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(bufferOffset);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(bufferOffset, buffer.Length);
            ArgumentOutOfRangeException.ThrowIfNegative(length);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(length, buffer.Length - bufferOffset);
        }

        var lease = FieldReader.Read<CharsColumnLease>(CurrentRow, ordinal,
            IsSequential);
        return lease.Read(buffer is null ? 0 : checked((int)dataOffset),
            buffer is null ? default : buffer.AsSpan(bufferOffset, length), buffer is null);
    }

    /// <inheritdoc/>
    public override DateTime GetDateTime(int ordinal)
        => GetFieldValueCore<DateTime>(ordinal);

    /// <inheritdoc/>
    public override decimal GetDecimal(int ordinal)
        => GetFieldValueCore<decimal>(ordinal);

    /// <inheritdoc/>
    public override double GetDouble(int ordinal)
        => GetFieldValueCore<double>(ordinal);

    /// <inheritdoc/>
    public override float GetFloat(int ordinal)
        => GetFieldValueCore<float>(ordinal);

    /// <inheritdoc/>
    public override Guid GetGuid(int ordinal)
        => GetFieldValueCore<Guid>(ordinal);

    /// <inheritdoc/>
    public override short GetInt16(int ordinal)
        => GetFieldValueCore<short>(ordinal);

    /// <inheritdoc/>
    public override int GetInt32(int ordinal)
        => GetFieldValueCore<int>(ordinal);

    /// <inheritdoc/>
    public override long GetInt64(int ordinal)
        => GetFieldValueCore<long>(ordinal);

    /// <inheritdoc/>
    public override Stream GetStream(int ordinal)
        => GetFieldValueCore<Stream>(ordinal);

    /// <inheritdoc/>
    public override string GetString(int ordinal)
        => GetFieldValueCore<string>(ordinal);

    /// <inheritdoc/>
    public override TextReader GetTextReader(int ordinal)
        => GetFieldValueCore<TextReader>(ordinal);

    /// <inheritdoc/>
    public override object GetValue(int ordinal)
        => FieldReader.ReadObject(CurrentRow, ordinal);

    /// <inheritdoc/>
    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);

        var row = CurrentRow;
        var count = Math.Min(FieldCountCore, values.Length);
        for (var i = 0; i < count; i++)
            values[i] = FieldReader.ReadObject(row, i);
        return count;
    }

    /// <inheritdoc/>
    public override void Close()
    {
        if (State is not ReaderState.Active)
            return;

        State = ReaderState.Closed;
        CloseCore(resetForReuse: false);
    }

    /// <inheritdoc/>
    public override Task CloseAsync()
    {
        if (State is not ReaderState.Active)
            return Task.CompletedTask;

        State = ReaderState.Closed;
        return CloseAsyncCore(resetForReuse: false).AsTask();
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (State is ReaderState.Disposed or ReaderState.Uninitialized)
            return;

        State = ReaderState.Disposed;
        CloseCore(resetForReuse: true);
    }

    /// <inheritdoc/>
    public override ValueTask DisposeAsync()
    {
        if (State is ReaderState.Disposed or ReaderState.Uninitialized)
            return new();

        State = ReaderState.Disposed;
        return CloseAsyncCore(resetForReuse: true);
    }
}
