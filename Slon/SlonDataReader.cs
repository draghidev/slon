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

namespace Slon;

// Implementation
public sealed partial class SlonDataReader
{
    Core _core;

    ReaderState State { get; set; }

    SlonDataReader() {}

    int FieldCountCore => _core.Current?.FieldCount ?? 0;
    bool HasRowsCore => _core.Current?.HasRows ?? false;
    long? RecordsAffectedCore => _core._recordsAffected;

    struct Core
    {
        // Will be set during initialization.
        readonly bool _singleRowBehavior;
        readonly bool _remainingReflectsActual;
        readonly bool _asyncExecute;
        readonly CommandResult.RowBuffering _rowBuffering;

        // We use this to prevent users doing any concurrent Dispose calls, at which point we throw.
        bool _closing;

        bool _enumeratedSingleRow;
        CommandResult.RowEnumerator _rowEnumerator;

        // Public for CreateAsync which holds an inline copy of NextResultAsync to avoid an extra state machine.
        public int _remainingResults;
        public CommandFlow.Enumerator _enumerator;

        public Core(CommandFlow.Enumerator enumerator, CommandBehavior behavior, int commandCount, bool asyncExecute)
        {
            _enumerator = enumerator;
            var singleRow = _singleRowBehavior = behavior.HasFlag(CommandBehavior.SingleRow);
            var remaining = _remainingResults = singleRow || behavior.HasFlag(CommandBehavior.SingleResult) ? 1 : commandCount;
            _remainingReflectsActual = commandCount == remaining;
            _asyncExecute = asyncExecute;
            _rowBuffering = behavior.HasFlag(CommandBehavior.SequentialAccess)
                ? CommandResult.RowBuffering.Streaming
                : CommandResult.RowBuffering.Buffered;
        }

        public long? _recordsAffected;

        bool EnumerateCommands => false; // _behavior.HasFlag((CommandBehavior)64);

        public CommandResult? Current => _enumerator.Current;

        public Row? CurrentRow => _rowEnumerator.Current;

        /// <summary>
        /// Processes the current enumerator result and updates relevant information on this instance.
        /// </summary>
        /// <returns>True if the current enumerator result is a suitable target for NextResult{Async}, false if it should be stepped over.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ProcessCurrent()
        {
            Debug.Assert(_remainingResults is > 0);
            if (Current is not { } current)
                return false;

            _remainingResults--;
            if (!current.CanHaveRows)
            {
                // Prefer completing the reader over detecting a practically unreachable row-count overflow.
                _recordsAffected += current.RecordsAffected;
                _rowEnumerator = default;
                return EnumerateCommands;
            }

            _rowEnumerator = current.GetEnumerator(_rowBuffering);
            return true;
        }

        public bool NextResult()
        {
            if (Current is { } current && !current.TryGetCommandComplete(out var completeMessage))
                current.Dispose();

            var next = false;
            while (_remainingResults > 0 && (next = _enumerator.MoveNext()) && !ProcessCurrent());
            if (!next)
            {
                // Release the flow as soon as its results end.
                DisposeEnumerator();
            }
            return next;
        }

        public bool Read()
        {
            Debug.Assert(_singleRowBehavior && _remainingResults is 0 || !_singleRowBehavior);
            if (_singleRowBehavior)
            {
                // After one row, normal result disposal drains the remainder.
                if (!_enumeratedSingleRow && _rowEnumerator.MoveNext())
                {
                    _enumeratedSingleRow = true;
                    return true;
                }

                if (!_enumeratedSingleRow)
                    Current?.GetCommandComplete();
            }
            else
            {
                if (_rowEnumerator.MoveNext())
                    return true;

                Current?.GetCommandComplete();
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if (_singleRowBehavior)
                return SingleRowCore(cancellationToken);

            var task = cancellationToken.CanBeCanceled
                ? _rowEnumerator.MoveNextAsync(cancellationToken)
                : _rowEnumerator.MoveNextAsync();
            if (!task.IsCompletedSuccessfully)
                return CompleteReadAsync(task, Current);

            var result = task.Result;
            if (!result)
                Current?.GetCommandComplete();
            return Task.FromResult(result);

            static async Task<bool> CompleteReadAsync(ValueTask<bool> pending, CommandResult? current)
            {
                var result = await pending.ConfigureAwait(false);
                if (!result)
                    current?.GetCommandComplete();
                return result;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        async Task<bool> SingleRowCore(CancellationToken cancellationToken)
        {
            if (!_enumeratedSingleRow && await _rowEnumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                _enumeratedSingleRow = true;
                return true;
            }

            if (!_enumeratedSingleRow)
                Current?.GetCommandComplete();

            await DisposeEnumeratorAsync().ConfigureAwait(false);
            return false;
        }

        public void DisposeEnumerator()
        {
            if (Interlocked.CompareExchange(ref _closing, true, false))
                ThrowHelper.ThrowInvalidOperation("Invalid concurrent call.");

            try
            {
                var enumerator = _enumerator;
                _enumerator = default;
                _rowEnumerator = default;
                enumerator.Dispose();
            }
            finally
            {
                Volatile.Write(ref _closing, false);
            }
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        public async ValueTask DisposeEnumeratorAsync()
        {
            if (Interlocked.CompareExchange(ref _closing, true, false))
                ThrowHelper.ThrowInvalidOperation("Invalid concurrent call.");

            try
            {
                var enumerator = _enumerator;
                _enumerator = default;
                _rowEnumerator = default;
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                Volatile.Write(ref _closing, false);
            }
        }
    }

    internal static SlonDataReader Create(CommandBehavior behavior, CommandFlow flow)
    {
        var enumerator = flow.GetEnumerator();
        try
        {
            var core = new Core(enumerator, behavior, flow.VisibleCommandCount, asyncExecute: false);
            core.NextResult();
            // TODO now that we don't need a DataReader while we wait for the first result we can pool the reader on the connection.
            return new SlonDataReader { State = ReaderState.Active, _core = core };
        }
        catch (Exception)
        {
            enumerator.Dispose();
            throw;
        }
    }

    internal static async ValueTask<TReader> CreateAsync<TReader>(CommandBehavior behavior, ValueTask<CommandFlow> flowTask, CancellationToken cancellationToken = default)
        where TReader: DbDataReader
    {
        Debug.Assert(typeof(TReader) == typeof(SlonDataReader) || typeof(TReader) == typeof(DbDataReader));
        var flow = await flowTask.ConfigureAwait(false);
        var enumerator = flow.GetEnumerator();
        try
        {
            var core = new Core(enumerator, behavior, flow.VisibleCommandCount, asyncExecute: true);

            // This is an inline copy of NextResultAsync (minus the 'Current' check) to avoid an extra state machine.
            var next = false;
            while (core._remainingResults > 0
                && (next = await (cancellationToken.CanBeCanceled
                    ? core._enumerator.MoveNextAsync(cancellationToken)
                    : core._enumerator.MoveNextAsync()).ConfigureAwait(false))
                && !core.ProcessCurrent());
            if (!next)
            {
                // Dispose the enumerator right away to allow the pipeline to handle next commands.
                // This also has the benefit Close/Dispose doesn't have to go async if the user exhausted the reader properly.
                await core.DisposeEnumeratorAsync().ConfigureAwait(false);
            }
            // TODO now that we don't need a DataReader while we wait for the first result we can pool the reader on the connection.
            return (TReader)(object)new SlonDataReader { State = ReaderState.Active, _core = core };
        }
        catch (Exception)
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
            throw;
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
        var row = _core.CurrentRow;
        if (row is null)
            ThrowInvalidState(State, returnException: false);

        return row!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    Row GetRowOrException(out Exception? exception)
    {
        var row = _core.CurrentRow;
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
            _ when _core.CurrentRow is null => new InvalidOperationException("Reader is not on a row."),
            _ => null
        };

        if (exception is null)
            return null;

        return returnException ? ExceptionDispatchInfo.SetCurrentStackTrace(exception) : throw exception;
    }

    void CloseCore()
    {
        _core.DisposeEnumerator();
    }

    ValueTask CloseAsyncCore()
    {
        return _core.DisposeEnumeratorAsync();
    }

    void Reset()
    {
        // TODO make pooling work again.
        // _core = default;
        // State = ReaderState.Uninitialized;
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask DisposeAsyncCore()
    {
        if (CloseAsyncCore() is var closeTask && closeTask.IsCompletedSuccessfully)
        {
            closeTask.GetAwaiter().GetResult();
            Reset();
            return new();
        }

        return Core(closeTask);

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask Core(ValueTask closeTask)
        {
            try
            {
                await closeTask.ConfigureAwait(false);
            }
            finally
            {
                Reset();
            }
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
            row["DataTypeName"] = column.DataTypeName;

            table.Rows.Add(row);
        }

        return table;
    }

    ValueTask<ReadOnlyCollection<TColumn>> GetColumnSchemaCore<TColumn>(bool async, CancellationToken cancellationToken = default) where TColumn : DbColumn
    {
        Debug.Assert(typeof(TColumn) == typeof(DbColumn) || typeof(TColumn) == typeof(SlonDbColumn));
        throw new NotImplementedException();
    }

    enum ReaderState
    {
        Uninitialized = 0,
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
    {
        get
        {
            ThrowIfClosedOrDisposed();
            return RecordsAffectedCore is null ? -1 : RecordsAffectedCore > int.MaxValue ? int.MinValue : (int)RecordsAffectedCore;
        }
    }

    /// <summary>Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.</summary>
    /// <returns>The number of rows changed, inserted, or deleted. -1 for SELECT statements. 0 if no rows were affected or the statement failed.</returns>
    public long LongRecordsAffected
    {
        get
        {
            ThrowIfClosedOrDisposed();
            return RecordsAffectedCore ?? -1;
        }
    }

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

    // DbDataReader cannot specialize GetColumnSchemaAsync's return type, so expose typed counterparts.
    /// <summary>Gets the column schema (<see cref="T:Slon.SlonDbColumn" /> collection).</summary>
    /// <returns>The column schema (<see cref="T:Slon.SlonDbColumn" /> collection).</returns>
    public ReadOnlyCollection<SlonDbColumn> GetSlonColumnSchema()
    {
        ThrowIfClosedOrDisposed();
        var task = GetColumnSchemaCore<SlonDbColumn>(async: false);
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    /// <summary>Gets the column schema (<see cref="T:Slon.SlonDbColumn" /> collection).</summary>
    /// <returns>The column schema (<see cref="T:Slon.SlonDbColumn" /> collection).</returns>
    public Task<ReadOnlyCollection<SlonDbColumn>> GetSlonColumnSchemaAsync(CancellationToken cancellationToken = default)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<ReadOnlyCollection<SlonDbColumn>>(exception);

        return GetColumnSchemaCore<SlonDbColumn>(async: true, cancellationToken).AsTask();
    }

    /// <inheritdoc/>
    public override bool NextResult()
    {
        ThrowIfClosedOrDisposed();
        try { return _core.NextResult(); }
        catch (Exception ex) { AdoException.Throw(ex); return default; }
    }

    /// <inheritdoc/>
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<bool>(exception);

        return NextResultAsyncCore(cancellationToken);
    }

    async Task<bool> NextResultAsyncCore(CancellationToken cancellationToken)
    {
        try
        {
            if (_core.Current is { IsComplete: false } current)
                await current.DisposeAsync().ConfigureAwait(false);

            var next = false;
            while (_core._remainingResults > 0
                && (next = await (cancellationToken.CanBeCanceled
                    ? _core._enumerator.MoveNextAsync(cancellationToken)
                    : _core._enumerator.MoveNextAsync()).ConfigureAwait(false))
                && !_core.ProcessCurrent()) { }
            if (!next)
                await _core.DisposeEnumeratorAsync().ConfigureAwait(false);
            return next;
        }
        catch (Exception ex) { AdoException.Throw(ex); return default; }
    }

    /// <inheritdoc/>
    public override bool Read()
    {
        ThrowIfClosedOrDisposed();
        try { return _core.Read(); }
        catch (Exception ex) { AdoException.Throw(ex); return default; }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<bool>(exception);

        return ReadAsyncCore(cancellationToken);
    }

    async Task<bool> ReadAsyncCore(CancellationToken cancellationToken)
    {
        try { return await _core.ReadAsync(cancellationToken).ConfigureAwait(false); }
        catch (Exception ex) { AdoException.Throw(ex); return default; }
    }

    /// <inheritdoc/>
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    /// <inheritdoc/>
    public override string GetDataTypeName(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
    public override Type GetFieldType(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override string GetName(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override bool IsDBNull(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    /// <summary>Returns a nested data reader for the requested column.</summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <exception cref="T:System.IndexOutOfRangeException">The column index is out of range.</exception>
    /// <returns>A data reader.</returns>
    public new SlonDataReader GetData(int ordinal)
        => throw new NotImplementedException();

    /// <inheritdoc/>
    protected override DbDataReader GetDbDataReader(int ordinal)
        => GetData(ordinal);

    // Non-gvm helper to make inlining GetBoolean GetString etc possible.
    T GetFieldValueCore<T>(int ordinal)
        => GetRowOrThrow().GetValue<T>(ordinal);

    // Non-gvm helper to make inlining GetTextReaderAsync etc possible.
    ValueTask<T> GetFieldValueCoreAsync<T>(int ordinal, CancellationToken cancellationToken)
    {
        var row = GetRowOrException(out var exception);
        return exception is not null
            ? ValueTask.FromException<T>(exception)
            : row.GetValueAsync<T>(ordinal, cancellationToken);
    }

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
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override char GetChar(int ordinal)
        => GetFieldValueCore<char>(ordinal);

    /// <inheritdoc/>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
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
        _ = GetRowOrThrow();
        ArgumentNullException.ThrowIfNull(values);

        var count = Math.Min(FieldCount, values.Length);
        for (var i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    /// <inheritdoc/>
    public override void Close()
    {
        if (State is not ReaderState.Active)
            return;

        State = ReaderState.Closed;
        try { CloseCore(); }
        catch (Exception ex) { AdoException.Throw(ex); }
    }

    /// <inheritdoc/>
    public override Task CloseAsync()
    {
        if (State is not ReaderState.Active)
            return Task.CompletedTask;

        State = ReaderState.Closed;
        return CloseAsyncProjected().AsTask();
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (State is ReaderState.Disposed or ReaderState.Uninitialized)
            return;

        State = ReaderState.Disposed;
        try { DisposeCore(); }
        catch (Exception ex) { AdoException.Throw(ex); }
    }

    /// <inheritdoc/>
    public override ValueTask DisposeAsync()
    {
        if (State is ReaderState.Disposed or ReaderState.Uninitialized)
            return new();

        State = ReaderState.Disposed;
        return DisposeAsyncProjected();
    }

    async ValueTask CloseAsyncProjected()
    {
        try { await CloseAsyncCore().ConfigureAwait(false); }
        catch (Exception ex) { AdoException.Throw(ex); }
    }

    async ValueTask DisposeAsyncProjected()
    {
        try { await DisposeAsyncCore().ConfigureAwait(false); }
        catch (Exception ex) { AdoException.Throw(ex); }
    }
}
