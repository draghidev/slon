using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbCommand" />
public sealed class SlonCommand: DbCommand
{
    AdoBatchCore<AdoCommand> _batchCore;

    // Supporting state for implicit batching through SQL parsing.
    string _overallCommandText;
    CommandType _overallCommandType;
    SlonParameters? _overallParameterCollection;
    bool _isOverallStateDirty;

    internal unsafe SlonCommand(SlonConnection? connection, SlonDataSource? dataSource, string? commandText)
    {
        GC.SuppressFinalize(this);
        _isOverallStateDirty = true;
        _overallCommandText = commandText ?? string.Empty;
        _overallCommandType = CommandType.Text;
        if (connection is not null)
        {
            _batchCore = new(connection, FieldRef<AdoBatchCore<AdoCommand>>.Create(&GetBatchCore, this));
            _batchCore.Timeout = connection.DefaultCommandTimeout;
        }
        else if (dataSource is not null)
        {
            _batchCore = new(dataSource, FieldRef<AdoBatchCore<AdoCommand>>.Create(&GetBatchCore, this));
            _batchCore.Timeout = dataSource.DefaultCommandTimeout;
        }

        // ReSharper disable once AddressOfMarshalByRefObject
        static ref AdoBatchCore<AdoCommand> GetBatchCore(SlonCommand instance) => ref instance._batchCore;
    }

    public SlonCommand() : this(null, null, null) {}
    public SlonCommand(SlonConnection connection) : this(connection, null, null) {}
    public SlonCommand(string commandText) : this(null, null, commandText) {}
    public SlonCommand(SlonConnection connection, string commandText) : this(connection, null, commandText) {}
    // A data-source-bound command runs on the MULTIPLEXED path (no connection lease, no exclusive scope) -
    // the stateless fast path. Use this for one-off commands that don't need session state / transactions.
    public SlonCommand(SlonDataSource dataSource, string commandText) : this(null, dataSource, commandText) {}

    void ThrowIfDisposed() => _batchCore.ThrowIfDisposed();
    void ThrowIfDisposedOrReadOnly() => _batchCore.ThrowIfDisposedOrReadOnly();

    void SetCommandText(string? value)
    {
        _isOverallStateDirty = _isOverallStateDirty || _overallCommandText != value;
        _overallCommandText = value ?? string.Empty;
    }

    void SetCommandType(CommandType value)
    {
        if (!Enum.IsDefined(value))
            throw new ArgumentOutOfRangeException();
        _isOverallStateDirty = _isOverallStateDirty || _overallCommandType != value;
        _overallCommandType = value;
    }

    void SetupCommands()
    {
        if (_isOverallStateDirty)
            Core();

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Core()
        {
            _batchCore.Clear();
            // TODO this is where we would parse the SQL and possible create a batch of commands.
            _batchCore.Add(new AdoCommand
            {
                CommandText = _overallCommandText,
                CommandType = _overallCommandType,
                Parameters = _overallParameterCollection
            });
            _isOverallStateDirty = false;
        }
    }

    ValueTask<T> SetupCommandsWrappedExceptions<T>()
    {
        return !_isOverallStateDirty ? new() : Core();

        [MethodImpl(MethodImplOptions.NoInlining)]
        ValueTask<T> Core()
        {
            try
            {
                SetupCommands();
                return new();
            }
            catch (Exception ex)
            {
                return ValueTask.FromException<T>(ex);
            }
        }
    }

    void PrepareCore(DbParameterCollection? parameters)
    {
        SetupCommands();
        _batchCore.Prepare(parameters);
    }

    ValueTask PrepareCoreAsync(DbParameterCollection? parameters, CancellationToken cancellationToken)
    {
        if (SetupCommandsWrappedExceptions<object>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return new(task.AsTask());
        return _batchCore.PrepareAsync(parameters, cancellationToken);
    }

    public static SlonCommand Prepare(SlonConnection connection, string commandText, DbParameterCollection? parameters)
    {
        var cmd = new SlonCommand(connection, commandText);
        cmd.PrepareCore(parameters);
        return cmd;
    }

    public static async ValueTask<SlonCommand> PrepareAsync(SlonConnection connection, string commandText, DbParameterCollection? parameters, CancellationToken cancellationToken = default)
    {
        var cmd = new SlonCommand(connection, commandText);
        await cmd.PrepareCoreAsync(parameters, cancellationToken).ConfigureAwait(false);
        return cmd;
    }

    public SlonBatch ToBatch(bool withParameters = true) => ToBatch(1, withParameters);
    public SlonBatch ToBatch(int copies, bool withParameters = false)
    {
        ThrowIfDisposed();
        SetupCommands();
        return SlonBatch.CreateFromDbCommand(_batchCore, copies, withParameters);
    }

    /// <summary>
    /// Return whether the instance is ready for mutations. It can become read-only, for example, if it has been prepared.
    /// </summary>
    public bool IsReadOnly => _batchCore.IsReadOnly;

    /// <inheritdoc/>
    public override void Prepare()
        => PrepareCore(parameters: null);

    /// <summary>Creates a prepared (or compiled) version of the command on the data source.</summary>
    /// <param name="parameters">The parameter collection deciding which parameter database types to use.</param>
    public void Prepare(DbParameterCollection parameters)
        => PrepareCore(parameters);

    /// <inheritdoc/>
    public override Task PrepareAsync(CancellationToken cancellationToken = default)
        => PrepareCoreAsync(parameters: null, cancellationToken).AsTask();

    /// <summary>
    /// Asynchronously creates a prepared (or compiled) version of the command on the data source.
    /// </summary>
    /// <param name="parameters">The parameter collection deciding which parameter database types to use.</param>
    /// <param name="cancellationToken">An optional token to cancel the asynchronous operation. The default value is None.</param>
    /// <returns></returns>
    /// <exception cref="OperationCanceledException">The cancellation token was canceled. This exception is stored into the returned task.</exception>
    public ValueTask PrepareAsync(DbParameterCollection parameters, CancellationToken cancellationToken = default)
        => PrepareCoreAsync(parameters, cancellationToken);

    /// <inheritdoc/>
    [AllowNull]
    public override string CommandText
    {
        get => _overallCommandText;
        set
        {
            ThrowIfDisposedOrReadOnly();
            SetCommandText(value);
        }
    }

    /// <inheritdoc/>
    public override int CommandTimeout
    {
        get => (int)_batchCore.Timeout.TotalSeconds;
        set => _batchCore.Timeout = TimeSpan.FromSeconds(value);
    }

    /// <inheritdoc/>
    public override CommandType CommandType
    {
        get => _overallCommandType;
        set
        {
            ThrowIfDisposedOrReadOnly();
            if (value is not CommandType.Text)
                throw new NotSupportedException();
            SetCommandType(value);
        }
    }

    /// <summary>
    /// Setting this property is ignored by Slon as its values are not respected.
    /// Gets or sets how command results are applied to the DataRow when used by the
    /// DbDataAdapter.Update(DataSet) method.
    /// </summary>
    /// <value>One of the <see cref="System.Data.UpdateRowSource"/> values.</value>
    public override UpdateRowSource UpdatedRowSource
    {
        get => UpdateRowSource.None;
        set { }
    }

    /// <inheritdoc cref="System.Data.Common.DbCommand.Parameters" />
    public new SlonParameters Parameters => _overallParameterCollection ??= new();

    /// <summary>
    /// Setting this property is ignored by Slon. PostgreSQL only supports a single transaction at a given time on
    /// a given connection, and all commands implicitly run inside the current transaction started via
    /// <see cref="SlonConnection.BeginTransaction()"/>
    /// </summary>
    public new SlonTransaction? Transaction
        => !_batchCore.TryGetDataSource(out _, out var connection) ? connection.CurrentTransaction : null;

    /// <inheritdoc/>
    public override bool DesignTimeVisible { get; set; }

    /// <inheritdoc/>
    public override void Cancel()
    {
        // Callers may cancel before assigning a connection.
        if (_batchCore.TryGetDataSource(out _, out var connection))
            return;

        connection.PerformUserCancellation();
    }

    /// <summary>Executes the command against its connection object, returning the number of rows affected.</summary>
    /// <returns>The number of records affected.</returns>
    public override int ExecuteNonQuery()
    {
        SetupCommands();
        return _batchCore.ExecuteNonQuery(parameters: null);
    }

    /// <summary>Executes the command against its connection object, returning the number of rows affected.</summary>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <returns>The number of records affected.</returns>
    public int ExecuteNonQuery(DbParameterCollection parameters)
    {
        SetupCommands();
        return _batchCore.ExecuteNonQuery(parameters);
    }

    /// <summary>Executes the command against its connection object, returning the number of rows affected.</summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the number of records affected.</returns>
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        if (SetupCommandsWrappedExceptions<int>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task.AsTask();

        return _batchCore.ExecuteNonQueryAsync(parameters: null, cancellationToken).AsTask();
    }

    /// <summary>Executes the command against its connection object, returning the number of rows affected.</summary>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the number of records affected.</returns>
    public ValueTask<int> ExecuteNonQueryAsync(DbParameterCollection parameters, CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<int>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task;

        return _batchCore.ExecuteNonQueryAsync(parameters, cancellationToken);
    }

    /// <summary>Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.</summary>
    /// <returns>The first column of the first row in the first result set.</returns>
    public override object? ExecuteScalar()
    {
        SetupCommands();
        return _batchCore.ExecuteScalar(parameters: null);
    }

    /// <summary>Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.</summary>
    /// <returns>The first column of the first row in the first result set.</returns>
    public object? ExecuteScalar(DbParameterCollection parameters)
    {
        SetupCommands();
        return _batchCore.ExecuteScalar(parameters);
    }

    /// <summary>Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.</summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the first column of the first row in the first result set.</returns>
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        if (SetupCommandsWrappedExceptions<object?>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task.AsTask();

        return _batchCore.ExecuteScalarAsync(parameters: null, cancellationToken).AsTask();
    }

    /// <summary>Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.</summary>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the first column of the first row in the first result set.</returns>
    public ValueTask<object?> ExecuteScalarAsync(DbParameterCollection parameters, CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<object?>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task;

        return _batchCore.ExecuteScalarAsync(parameters, cancellationToken);
    }

    /// <summary>Executes the command against its connection, returning a <see cref="T:Slon.SlonDataReader" /> which can be used to access the results.</summary>
    /// <returns>An <see cref="T:Slon.SlonDataReader" /> object.</returns>
    public new SlonDataReader ExecuteReader()
    {
        SetupCommands();
        return _batchCore.ExecuteReader(parameters: null, CommandBehavior.Default);
    }

    /// <summary>Executes the command against its connection, returning a <see cref="T:Slon.SlonDataReader" /> which can be used to access the results.</summary>
    /// <param name="behavior">An instance of <see cref="T:System.Data.CommandBehavior" />, specifying options for command execution and data retrieval.</param>
    /// <returns>An <see cref="T:Slon.SlonDataReader" /> object.</returns>
    public new SlonDataReader ExecuteReader(CommandBehavior behavior)
    {
        SetupCommands();
        return _batchCore.ExecuteReader(parameters: null, behavior);
    }

    /// <summary>Executes the command against its connection, returning a <see cref="T:Slon.SlonDataReader" /> which can be used to access the results.</summary>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <returns>An <see cref="T:Slon.SlonDataReader" /> object.</returns>
    public SlonDataReader ExecuteReader(DbParameterCollection parameters)
    {
        SetupCommands();
        return _batchCore.ExecuteReader(parameters, CommandBehavior.Default);
    }

    /// <summary>Executes the command against its connection, returning a <see cref="T:Slon.SlonDataReader" /> which can be used to access the results.</summary>
    /// <param name="behavior">An instance of <see cref="T:System.Data.CommandBehavior" />, specifying options for command execution and data retrieval.</param>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <returns>An <see cref="T:Slon.SlonDataReader" /> object.</returns>
    public SlonDataReader ExecuteReader(DbParameterCollection parameters, CommandBehavior behavior)
    {
        SetupCommands();
        return _batchCore.ExecuteReader(parameters, behavior);
    }

    /// <inheritdoc/>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        SetupCommands();
        return _batchCore.ExecuteReader(parameters: null, behavior);
    }

    /// <inheritdoc cref="System.Data.Common.DbCommand.ExecuteReaderAsync(CancellationToken)"/>
    public new ValueTask<SlonDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<SlonDataReader>() is { IsCompletedSuccessfully: false, IsCompleted: true } task)
            return task;
        return _batchCore.ExecuteReaderAsync(parameters: null, CommandBehavior.Default, cancellationToken);
    }

    /// <inheritdoc cref="System.Data.Common.DbCommand.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>
    public new ValueTask<SlonDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<SlonDataReader>() is { IsCompletedSuccessfully: false, IsCompleted: true } task)
            return task;
        return _batchCore.ExecuteReaderAsync(parameters: null, behavior, cancellationToken);
    }

    /// <summary>Invokes <see cref="M:System.Data.Common.DbCommand.ExecuteDbDataReaderAsync(System.Data.CommandBehavior,System.Threading.CancellationToken)" />.</summary>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <exception cref="T:System.Data.Common.DbException">An error occurred while executing the command.</exception>
    /// <exception cref="T:System.ArgumentException">An invalid <see cref="T:System.Data.CommandBehavior" /> value.</exception>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask<SlonDataReader> ExecuteReaderAsync(DbParameterCollection parameters, CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<SlonDataReader>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task;
        return _batchCore.ExecuteReaderAsync(parameters, CommandBehavior.Default, cancellationToken);
    }

    /// <summary>Invokes <see cref="M:System.Data.Common.DbCommand.ExecuteDbDataReaderAsync(System.Data.CommandBehavior,System.Threading.CancellationToken)" />.</summary>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <param name="behavior">One of the enumeration values that specifies the command behavior.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <exception cref="T:System.Data.Common.DbException">An error occurred while executing the command.</exception>
    /// <exception cref="T:System.ArgumentException">An invalid <see cref="T:System.Data.CommandBehavior" /> value.</exception>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask<SlonDataReader> ExecuteReaderAsync(DbParameterCollection parameters, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<SlonDataReader>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task;
        return _batchCore.ExecuteReaderAsync(parameters, behavior, cancellationToken);
    }

    /// <inheritdoc/>
    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (SetupCommandsWrappedExceptions<DbDataReader>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task.AsTask();
        return _batchCore.ExecuteDbReaderAsync(parameters: null, behavior, cancellationToken).AsTask();
    }

    /// <summary>Creates a new instance of a <see cref="T:Slon.SlonDbParameter" /> object.</summary>
    /// <returns>A <see cref="T:Slon.SlonDbParameter" /> object.</returns>
    public new SlonDbParameter CreateParameter() => new SlonParameter();

    /// <inheritdoc/>
    protected override DbParameter CreateDbParameter() => CreateParameter();
    /// <inheritdoc/>
    protected override DbConnection? DbConnection
    {
        get => !_batchCore.TryGetDataSource(out _, out var connection) ? connection : null;
        set
        {
            ThrowIfDisposedOrReadOnly();
            if (value is not SlonConnection conn)
            {
                ThrowHelper.ThrowArgumentException(nameof(value), $"Value is not an instance of {nameof(SlonConnection)}.");
                return;
            }
            _batchCore.SetConnection(conn);
        }
    }

    /// <inheritdoc/>
    protected override DbParameterCollection DbParameterCollection => Parameters;
    /// <inheritdoc/>
    protected override DbTransaction? DbTransaction { get => Transaction; set {} }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        _batchCore.Dispose();
        base.Dispose(true);
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        await _batchCore.DisposeAsync().ConfigureAwait(false);
        base.Dispose(true);
    }

    struct AdoCommand : IAdoCommand
    {
        public void MakeReadOnly() { }
        public TrackedCommand? Tracked { get; set; }
        public string CommandText { get; set; }
        public CommandType CommandType { get; set; }
        public SlonParameters? Parameters { get; set; }
        public bool AppendErrorBarrier { get; set; }
    }
}
