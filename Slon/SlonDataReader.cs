using System.Collections;
using System.Collections.ObjectModel;
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
    const CommandBehavior EnumerateCommandResultsBehavior = (CommandBehavior)64;

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

    // We use this to prevent users doing any concurrent Dispose calls, at which point we throw.
    bool _closing;

    bool _singleRowConsumed;
    bool _hasPrefetchedRow;
    bool _currentCompletionApplied;
    bool _currentErrorObserved;
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

        _closing = false;
        _singleRowConsumed = false;
        _hasPrefetchedRow = false;
        _currentCompletionApplied = false;
        _currentErrorObserved = false;
        _rowPresence = RowPresence.Unknown;
        _rowEnumerator = default;

        if (hasCurrent)
        {
            var processed = ProcessCurrent();
            Debug.Assert(processed);
        }
        State = ReaderState.Active;
    }

    static int GetResultLimit(CommandBehavior behavior, int commandCount)
        => behavior.HasFlag(CommandBehavior.SingleRow) || behavior.HasFlag(CommandBehavior.SingleResult)
            ? 1
            : commandCount;

    static bool ShouldEnumerateCommandResults(CommandBehavior behavior)
        => (behavior & EnumerateCommandResultsBehavior) is not 0;

    static SlonDataReader CreateReader(CommandFlow.Enumerator enumerator, CommandBehavior behavior,
        int remainingResults, PgSerializerOptions serializerOptions,
        SlonConnection? connectionToClose, long? recordsAffected, bool hasCurrent)
    {
        var reader = new SlonDataReader();
        reader.Initialize(enumerator, behavior, remainingResults, serializerOptions,
            connectionToClose, recordsAffected, hasCurrent);
        return reader;
    }

    static void ApplyPendingCompletion(CommandResult current, ref long? recordsAffected)
    {
        current.TryGetCommandComplete(out _);
        if (current.Error is null)
            recordsAffected += current.RecordsAffected;
    }

    int FieldCountCore => Current?.FieldCount ?? 0;
    bool HasRowsCore => HasAnyRows;
    long? RecordsAffectedCore => _recordsAffected;

    ref PgSerializerFieldReader FieldReader => ref _fieldReader;
    CommandResult? Current => _enumerator.Current;
    bool CompletionApplied => _currentCompletionApplied;
    bool IsSequential => _rowBuffering is CommandResult.RowBuffering.Streaming;

    SlonConnection? TakeConnectionToClose()
        => Interlocked.Exchange(ref _connectionToClose, null);

    Row? CurrentRow => _rowEnumerator.Current;

    /// <summary>
    /// Processes the current enumerator result and updates relevant information on this instance.
    /// </summary>
    /// <returns>True if the current enumerator result is a suitable target for NextResult{Async}, false if it should be stepped over.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool ProcessCurrent()
    {
        Debug.Assert(_remainingResults is > 0);
        if (Current is not { } current)
            return false;
        _fieldReader.Initialize(current);

        _remainingResults--;
        _singleRowConsumed = false;
        _hasPrefetchedRow = false;
        _currentCompletionApplied = false;
        _currentErrorObserved = false;
        _rowPresence = RowPresence.Unknown;
        if (!current.CanHaveRows)
        {
            _rowEnumerator = default;
            return EnumerateCommandResults;
        }

        _rowEnumerator = current.GetEnumerator(_rowBuffering);
        return true;
    }

    bool MoveToNextResult()
    {
        CompleteAndApplyCurrent();

        var next = false;
        while (_remainingResults > 0 && (next = _enumerator.MoveNext()) && !ProcessCurrent())
            CompleteAndApplyCurrent();
        if (!next)
        {
            // Release the flow as soon as its results end.
            DisposeEnumerator();
        }
        return next;
    }

    void CompleteAndApplyCurrent()
    {
        if (_currentCompletionApplied || Current is not { } current)
            return;
        if (!current.IsComplete)
            current.Complete();
        ApplyCompletion(current);
    }

    void ApplyCompletion(CommandResult current)
    {
        if (!_currentErrorObserved)
            current.TryGetCommandComplete(out _);
        if (current.Error is null)
            _recordsAffected += current.RecordsAffected;
        _currentCompletionApplied = true;
    }

    void ObserveCurrentError()
    {
        if (Current?.Error is not null)
            _currentErrorObserved = true;
    }

    bool ReadRow()
    {
        Debug.Assert(_singleRowBehavior && _remainingResults is 0 || !_singleRowBehavior);
        if (_hasPrefetchedRow)
        {
            _hasPrefetchedRow = false;
            if (_singleRowBehavior)
                _singleRowConsumed = true;
            return true;
        }

        if (_singleRowBehavior)
        {
            // After one row, normal result disposal drains the remainder.
            if (!_singleRowConsumed && _rowEnumerator.MoveNext())
            {
                _singleRowConsumed = true;
                _rowPresence = RowPresence.Present;
                return true;
            }
        }
        else if (_rowEnumerator.MoveNext())
        {
            _rowPresence = RowPresence.Present;
            return true;
        }

        CompleteRowEnumeration();
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<bool> ReadRowAsync(CancellationToken cancellationToken)
    {
        if (_hasPrefetchedRow)
        {
            _hasPrefetchedRow = false;
            if (_singleRowBehavior)
                _singleRowConsumed = true;
            return ValueTask.FromResult(true);
        }
        if (_singleRowBehavior && _singleRowConsumed)
            return ValueTask.FromResult(false);

        return _rowEnumerator.MoveNextAsync(cancellationToken);
    }

    bool ProcessReadResult(bool hasRow)
    {
        if (hasRow)
        {
            _rowPresence = RowPresence.Present;
            if (_singleRowBehavior)
                _singleRowConsumed = true;
            return true;
        }

        CompleteRowEnumeration();
        return false;
    }

    bool HasAnyRows
    {
        get
        {
            if (_rowPresence is not RowPresence.Unknown)
                return _rowPresence is RowPresence.Present;
            if (Current is null)
                return false;

            var hasRows = _rowEnumerator.MoveNext();
            _rowPresence = hasRows ? RowPresence.Present : RowPresence.Empty;
            _hasPrefetchedRow = hasRows;
            if (!hasRows)
                CompleteRowEnumeration();
            return hasRows;
        }
    }

    void CompleteRowEnumeration()
    {
        var current = Current;
        if (_remainingResults is not 0)
        {
            if (current is not null)
            {
                if (!current.IsComplete)
                    current.Complete();
                current.GetCommandComplete();
            }
            return;
        }

        try
        {
            if (current is not null)
            {
                if (!current.IsComplete)
                    current.Complete();
                else
                    current.GetCommandComplete();
                ApplyCompletion(current);
            }
        }
        finally
        {
            DisposeEnumerator();
        }
    }

    void DisposeEnumerator()
    {
        if (Interlocked.CompareExchange(ref _closing, true, false))
            ThrowHelper.ThrowInvalidOperation("Invalid concurrent call.");

        try
        {
            var rowEnumerator = _rowEnumerator;
            var enumerator = _enumerator;
            _enumerator = default;
            _rowEnumerator = default;
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
            Volatile.Write(ref _closing, false);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    async ValueTask DisposeEnumeratorAsync()
    {
        if (Interlocked.CompareExchange(ref _closing, true, false))
            ThrowHelper.ThrowInvalidOperation("Invalid concurrent call.");

        var rowEnumerator = _rowEnumerator;
        var enumerator = _enumerator;
        _enumerator = default;
        _rowEnumerator = default;

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
            Volatile.Write(ref _closing, false);
        }
    }

    enum RowPresence : byte
    {
        Unknown,
        Empty,
        Present
    }

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
            reader.MoveToNextResult();
            return reader;
        }
        catch (Exception)
        {
            enumerator.Dispose();
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
                ApplyPendingCompletion(enumerator.Current, ref recordsAffected);
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
                await enumerator.DisposeAsync().ConfigureAwait(false);
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
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ThrowIfClosedOrDisposed()
    {
        var state = State;
        if (state is not ReaderState.Active)
            ThrowInvalidState(state, returnException: false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    Exception? GetExceptionIfClosedOrDisposed()
    {
        var state = State;
        if (state is not ReaderState.Active)
            return ThrowInvalidState(state, returnException: true);

        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    Row GetRowOrThrow()
    {
        var row = CurrentRow;
        if (row is null)
            ThrowInvalidState(State, returnException: false);

        return row!;
    }

    RowDescription GetRowDescriptionOrThrow()
    {
        if (Current is null)
            throw new InvalidOperationException("Reader is not on a result.");

        var description = FieldReader.RowDescription;
        return description.FieldCount is 0
            ? throw new InvalidOperationException("The current result has no columns.")
            : description;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    Row GetRowOrException(out Exception? exception)
    {
        var row = CurrentRow;
        if (row is null)
        {
            exception = ThrowInvalidState(State, returnException: true);
        }
        else
        {
            exception = null;
        }

        return row!;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    Exception? ThrowInvalidState(ReaderState readerState, bool returnException)
    {
        var exception = readerState switch
        {
            ReaderState.Uninitialized or ReaderState.Disposed => new ObjectDisposedException(nameof(SlonDataReader)),
            ReaderState.Closed => new InvalidOperationException("Reader is closed."),
            _ when CurrentRow is null => new InvalidOperationException("Reader is not on a row."),
            _ => null
        };

        if (exception is null)
            return null;

        return returnException ? ExceptionDispatchInfo.SetCurrentStackTrace(exception) : throw exception;
    }

    void CloseCore()
    {
        try
        {
            DisposeEnumerator();
        }
        finally
        {
            TakeConnectionToClose()?.Close();
        }
    }

    async ValueTask CloseAsyncCore()
    {
        try
        {
            await DisposeEnumeratorAsync().ConfigureAwait(false);
        }
        finally
        {
            if (TakeConnectionToClose() is { } connection)
                await connection.CloseAsync().ConfigureAwait(false);
        }
    }

    void Reset()
    {
        _connectionToClose = null;
        _singleRowBehavior = false;
        _rowBuffering = default;
        EnumerateCommandResults = false;
        _closing = false;
        _singleRowConsumed = false;
        _hasPrefetchedRow = false;
        _currentCompletionApplied = false;
        _currentErrorObserved = false;
        _rowPresence = RowPresence.Unknown;
        _rowEnumerator = default;
        _fieldReader = default;
        _remainingResults = 0;
        _enumerator = default;
        _recordsAffected = null;
        State = ReaderState.Uninitialized;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void DisposeCore()
    {
        try
        {
            CloseCore();
        }
        finally
        {
            Reset();
        }
    }

    async ValueTask<DataTable?> GetSchemaTableCore(bool async, CancellationToken cancellationToken = default)
    {
        if (FieldCount == 0) // No resultset
            return null;

        var table = new DataTable("SchemaTable");

        // Important to match SqlClient's column order, certain ADO.NET libraries naively assume identical ordering.
        // See: https://github.com/npgsql/npgsql/issues/1671
        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("ColumnSize", typeof(int));
        table.Columns.Add("NumericPrecision", typeof(int));
        table.Columns.Add("NumericScale", typeof(int));
        table.Columns.Add("IsUnique", typeof(bool));
        table.Columns.Add("IsKey", typeof(bool));
        table.Columns.Add("BaseServerName", typeof(string));
        table.Columns.Add("BaseCatalogName", typeof(string));
        table.Columns.Add("BaseColumnName", typeof(string));
        table.Columns.Add("BaseSchemaName", typeof(string));
        table.Columns.Add("BaseTableName", typeof(string));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("AllowDBNull", typeof(bool));
        table.Columns.Add("ProviderType", typeof(SlonDbType));
        table.Columns.Add("IsAliased", typeof(bool));
        table.Columns.Add("IsExpression", typeof(bool));
        table.Columns.Add("IsIdentity", typeof(bool));
        table.Columns.Add("IsAutoIncrement", typeof(bool));
        table.Columns.Add("IsRowVersion", typeof(bool));
        table.Columns.Add("IsHidden", typeof(bool));
        table.Columns.Add("IsLong", typeof(bool));
        table.Columns.Add("IsReadOnly", typeof(bool));
        table.Columns.Add("ProviderSpecificDataType", typeof(Type));
        table.Columns.Add("DataTypeName", typeof(string));

        foreach (var column in await GetColumnSchemaCore<SlonDbColumn>(async, cancellationToken).ConfigureAwait(false))
        {
            var row = table.NewRow();

            row["ColumnName"] = column.ColumnName;
            row["ColumnOrdinal"] = column.ColumnOrdinal ?? -1;
            row["ColumnSize"] = column.ColumnSize ?? -1;
            row["NumericPrecision"] = column.NumericPrecision ?? 0;
            row["NumericScale"] = column.NumericScale ?? 0;
            row["IsUnique"] = column.IsUnique == true;
            row["IsKey"] = column.IsKey == true;
            row["BaseServerName"] = "";
            row["BaseCatalogName"] = column.BaseCatalogName;
            row["BaseColumnName"] = column.BaseColumnName;
            row["BaseSchemaName"] = column.BaseSchemaName;
            row["BaseTableName"] = column.BaseTableName;
            row["DataType"] = column.DataType;
            row["AllowDBNull"] = (object?)column.AllowDBNull ?? DBNull.Value;
            row["ProviderType"] = column.SlonDbType;
            row["IsAliased"] = column.IsAliased == true;
            row["IsExpression"] = column.IsExpression == true;
            row["IsIdentity"] = column.IsIdentity == true;
            row["IsAutoIncrement"] = column.IsAutoIncrement == true;
            row["IsRowVersion"] = false;
            row["IsHidden"] = column.IsHidden == true;
            row["IsLong"] = column.IsLong == true;
            row["IsReadOnly"] = column.IsReadOnly == true;
            row["ProviderSpecificDataType"] = column.DataType;
            row["DataTypeName"] = column.DataTypeName;

            table.Rows.Add(row);
        }

        return table;
    }

    ValueTask<ReadOnlyCollection<TColumn>> GetColumnSchemaCore<TColumn>(bool async, CancellationToken cancellationToken = default) where TColumn : DbColumn
    {
        Debug.Assert(typeof(TColumn) == typeof(DbColumn) || typeof(TColumn) == typeof(SlonDbColumn));
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<ReadOnlyCollection<TColumn>>(cancellationToken);

        if (Current is null || FieldCountCore is 0)
            return ValueTask.FromResult(new ReadOnlyCollection<TColumn>([]));

        var description = FieldReader.RowDescription;
        var columns = new TColumn[description.FieldCount];
        for (var i = 0; i < columns.Length; i++)
        {
            ref readonly var field = ref description[i];
            columns[i] = (TColumn)(DbColumn)new SlonDbColumn(field.Name, i,
                FieldReader.GetFieldType(i), FieldReader.GetDataTypeName(i),
                FieldReader.GetSlonDbType(i));
        }
        return ValueTask.FromResult(new ReadOnlyCollection<TColumn>(columns));
    }

    async Task<bool> NextResultAsyncCore(CancellationToken cancellationToken)
    {
        try
        {
            if (!CompletionApplied && Current is { } current)
            {
                if (!current.IsComplete)
                    await current.CompleteAsync().ConfigureAwait(false);
                ApplyCompletion(current);
            }

            var next = false;
            while (_remainingResults > 0
                && (next = await _enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                && !ProcessCurrent())
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
            ObserveCurrentError();
            AdoException.Throw(ex);
            return default;
        }
    }

    async Task<bool> ReadAsyncCore(CancellationToken cancellationToken)
    {
        try
        {
            if (await ReadRowAsync(cancellationToken).ConfigureAwait(false))
                return ProcessReadResult(hasRow: true);

            if (Current is not null && !Current.IsComplete)
                await Current.CompleteAsync().ConfigureAwait(false);
            if (_remainingResults is 0
                && !CompletionApplied
                && Current is not null)
            {
                try
                {
                    ApplyCompletion(Current);
                }
                finally
                {
                    DisposeEnumerator();
                }
                return false;
            }
            return ProcessReadResult(hasRow: false);
        }
        catch (Exception ex)
        {
            ObserveCurrentError();
            AdoException.Throw(ex);
            return default;
        }
    }

    async Task<bool> IsDBNullAsyncCore(int ordinal, CancellationToken cancellationToken)
    {
        try
        {
            _ = GetRowDescriptionOrThrow()[ordinal];
            return await GetRowOrThrow().IsDBNullAsync(ordinal, cancellationToken)
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
        var row = GetRowOrThrow();
        return typeof(T) == typeof(object)
            ? (T)FieldReader.ReadObject(row, ordinal)
            : FieldReader.Read<T>(row, ordinal);
    }

    // Non-gvm helper to make inlining GetTextReaderAsync etc possible.
    ValueTask<T> GetFieldValueCoreAsync<T>(int ordinal, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<T>(cancellationToken);

        var row = GetRowOrException(out var exception);
        if (exception is not null)
            return ValueTask.FromException<T>(exception);

        if (typeof(T) == typeof(object))
            return ReadObjectAsync<T>(FieldReader.ReadObjectAsync(row, ordinal, cancellationToken));
        return FieldReader.ReadAsync<T>(row, ordinal, cancellationToken);

        static async ValueTask<TResult> ReadObjectAsync<TResult>(ValueTask<object> task)
            => (TResult)await task
                .ConfigureAwait(false);
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
        => RecordsAffectedCore is null
            ? -1 : RecordsAffectedCore > int.MaxValue
                ? int.MinValue : (int)RecordsAffectedCore;

    /// <summary>Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.</summary>
    /// <returns>The number of rows changed, inserted, or deleted. -1 for SELECT statements. 0 if no rows were affected or the statement failed.</returns>
    public long LongRecordsAffected => RecordsAffectedCore ?? -1;

    /// <inheritdoc/>
    public override bool HasRows
    {
        get
        {
            ThrowIfClosedOrDisposed();
            return HasRowsCore;
        }
    }

    /// <inheritdoc/>
    public override bool IsClosed => State is not ReaderState.Active;

    /// <inheritdoc/>
    public override DataTable? GetSchemaTable()
    {
        ThrowIfClosedOrDisposed();
        var task = GetSchemaTableCore(async: false);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    /// <inheritdoc/>
    public override Task<DataTable?> GetSchemaTableAsync(CancellationToken cancellationToken = default)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<DataTable?>(exception);

        return GetSchemaTableCore(async: true, cancellationToken).AsTask();
    }

    /// <summary>Gets the column schema (<see cref="T:System.Data.Common.DbColumn" /> collection).</summary>
    /// <returns>The column schema (<see cref="T:System.Data.Common.DbColumn" /> collection).</returns>
    ReadOnlyCollection<DbColumn> IDbColumnSchemaGenerator.GetColumnSchema()
    {
        ThrowIfClosedOrDisposed();
        var task = GetColumnSchemaCore<DbColumn>(async: false);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    /// <summary>Gets the column schema (<see cref="T:System.Data.Common.DbColumn" /> collection).</summary>
    /// <returns>The column schema (<see cref="T:System.Data.Common.DbColumn" /> collection).</returns>
    public override Task<ReadOnlyCollection<DbColumn>> GetColumnSchemaAsync(CancellationToken cancellationToken = default)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<ReadOnlyCollection<DbColumn>>(exception);

        return GetColumnSchemaCore<DbColumn>(async: true, cancellationToken).AsTask();
    }

    /// <summary>Gets the column schema (<see cref="T:Slon.SlonDbColumn" /> collection).</summary>
    /// <returns>The column schema (<see cref="T:Slon.SlonDbColumn" /> collection).</returns>
    public ReadOnlyCollection<SlonDbColumn> GetColumnSchema()
    {
        ThrowIfClosedOrDisposed();
        var task = GetColumnSchemaCore<SlonDbColumn>(async: false);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    /// <inheritdoc/>
    public override bool NextResult()
    {
        ThrowIfClosedOrDisposed();
        try
        {
            return MoveToNextResult();
        }
        catch (Exception ex)
        {
            ObserveCurrentError();
            AdoException.Throw(ex);
            return default;
        }
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
        try
        {
            return ReadRow();
        }
        catch (Exception ex)
        {
            ObserveCurrentError();
            AdoException.Throw(ex);
            return default;
        }
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
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    /// <inheritdoc/>
    public override string GetDataTypeName(int ordinal)
    {
        _ = GetRowDescriptionOrThrow()[ordinal];
        return FieldReader.GetDataTypeName(ordinal);
    }

    /// <inheritdoc/>
    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
    public override Type GetFieldType(int ordinal)
    {
        _ = GetRowDescriptionOrThrow()[ordinal];
        return FieldReader.GetFieldType(ordinal);
    }

    /// <inheritdoc/>
    public override string GetName(int ordinal)
        => GetRowDescriptionOrThrow()[ordinal].Name;

    /// <inheritdoc/>
    public override int GetOrdinal(string name)
        => GetRowDescriptionOrThrow().GetFieldIndex(name);

    /// <inheritdoc/>
    public override bool IsDBNull(int ordinal)
    {
        _ = GetRowDescriptionOrThrow()[ordinal];
        return GetRowOrThrow().IsDBNull(ordinal);
    }

    /// <inheritdoc/>
    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        => cancellationToken.IsCancellationRequested
            ? Task.FromCanceled<bool>(cancellationToken)
            : IsDBNullAsyncCore(ordinal, cancellationToken);

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
        => GetFieldValueCoreAsync<T>(ordinal, cancellationToken).AsTask();

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

        var row = GetRowOrThrow();
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

        var lease = FieldReader.Read<CharsColumnLease>(GetRowOrThrow(), ordinal,
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
        => GetFieldValueCore<object>(ordinal);

    /// <inheritdoc/>
    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);

        var row = GetRowOrThrow();
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
        try
        {
            CloseCore();
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    /// <inheritdoc/>
    public override Task CloseAsync()
    {
        if (State is not ReaderState.Active)
            return Task.CompletedTask;

        State = ReaderState.Closed;
        return CloseAsyncProjected();
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (State is ReaderState.Disposed or ReaderState.Uninitialized)
            return;

        State = ReaderState.Disposed;
        try
        {
            DisposeCore();
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    /// <inheritdoc/>
    public override ValueTask DisposeAsync()
    {
        if (State is ReaderState.Disposed or ReaderState.Uninitialized)
            return new();

        State = ReaderState.Disposed;
        return DisposeAsyncProjected();
    }

    async Task CloseAsyncProjected()
    {
        try
        {
            await CloseAsyncCore().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    async ValueTask DisposeAsyncProjected()
    {
        var ownsCleanup = false;
        try
        {
            if (Interlocked.CompareExchange(ref _closing, true, false))
                ThrowHelper.ThrowInvalidOperation("Invalid concurrent call.");
            ownsCleanup = true;

            var rowEnumerator = _rowEnumerator;
            var enumerator = _enumerator;
            _enumerator = default;
            _rowEnumerator = default;

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
                Volatile.Write(ref _closing, false);
            }

            if (TakeConnectionToClose() is { } connection)
                await new ValueTask(connection.CloseAsync()).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
        finally
        {
            if (ownsCleanup)
                Reset();
        }
    }
}
