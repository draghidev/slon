using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Runtime.CompilerServices;
using Slon.Pg.Protocol.Flows;

namespace Slon;

// Implementation
/// <inheritdoc cref="System.Data.Common.DbCommand" />
public sealed partial class SlonCommand
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
        var fieldRef = FieldRef<AdoBatchCore<AdoCommand>>.Create(&GetBatchCore, this);
        if (connection is not null)
        {
            _batchCore = new(connection, fieldRef);
            _batchCore.Timeout = connection.DefaultCommandTimeout;
        }
        else if (dataSource is not null)
        {
            _batchCore = new(dataSource, fieldRef);
            _batchCore.Timeout = dataSource.DefaultCommandTimeout;
        }
        else
            _batchCore = new(fieldRef);

        // ReSharper disable once AddressOfMarshalByRefObject
        static ref AdoBatchCore<AdoCommand> GetBatchCore(SlonCommand instance) => ref instance._batchCore;
    }

    void SetupCommands()
    {
        if (_isOverallStateDirty)
            Rebuild();

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Rebuild()
        {
            if (string.IsNullOrWhiteSpace(_overallCommandText))
                throw new InvalidOperationException("CommandText must be set before executing or preparing the command.");

            _batchCore.Clear();
            // TODO this is where we would parse the SQL and possibly create a batch of commands.
            _batchCore.Add(new AdoCommand
            {
                CommandText = _overallCommandText,
                CommandType = _overallCommandType,
                Parameters = _overallParameterCollection
            });
            _isOverallStateDirty = false;
        }
    }

    internal void OnFlowStarted(CommandFlow flow)
        => _batchCore.OnFlowStarted(flow);

    internal void OnFlowCompleting(CommandFlow flow, Exception? exception)
        => _batchCore.OnFlowCompleting(flow, exception);

    struct AdoCommand : IAdoCommand
    {
        public void MakeReadOnly() { }
        public TrackedCommand? Tracked { get; set; }
        public string CommandText { get; set; }
        public CommandType CommandType { get; set; }
        public SlonParameters? Parameters { get; set; }
        public bool AppendErrorBarrier { get; set; }
        public bool AllowAutoPreparation => true;
    }
}

// Public surface & ADO.NET
public sealed partial class SlonCommand : DbCommand
{
    /// Initializes an unbound command.
    public SlonCommand() : this(null, null, null) {}

    /// <summary>Initializes a command bound to the specified connection.</summary>
    /// <param name="connection">The connection on which the command executes.</param>
    public SlonCommand(SlonConnection connection)
        : this(connection ?? throw new ArgumentNullException(nameof(connection)), null, null) {}

    /// <summary>Initializes an unbound command with the specified command text.</summary>
    /// <param name="commandText">The SQL statement to execute.</param>
    public SlonCommand(string commandText) : this(null, null, commandText) {}

    /// <summary>Initializes a command bound to the specified connection.</summary>
    /// <param name="connection">The connection on which the command executes.</param>
    /// <param name="commandText">The SQL statement to execute.</param>
    public SlonCommand(SlonConnection connection, string commandText)
        : this(connection ?? throw new ArgumentNullException(nameof(connection)), null, commandText) {}
    /// <summary>Initializes a multiplexed command bound to the specified datasource.</summary>
    /// <param name="dataSource">The datasource through which the command executes.</param>
    /// <param name="commandText">The SQL statement to execute.</param>
    /// <remarks>
    /// A datasource-bound command uses the stateless multiplexed path and does not hold a connection
    /// lease or exclusive scope. Use a connection-bound command for session state and transactions.
    /// </remarks>
    public SlonCommand(SlonDataSource dataSource, string commandText)
        : this(null, dataSource ?? throw new ArgumentNullException(nameof(dataSource)), commandText) {}

    /// <summary>Creates and synchronously prepares a command on the specified connection.</summary>
    /// <param name="connection">The connection on which to prepare the command.</param>
    /// <param name="commandText">The SQL statement to prepare.</param>
    /// <returns>The prepared command.</returns>
    public static SlonCommand Prepare(SlonConnection connection, string commandText)
    {
        var cmd = new SlonCommand(connection, commandText);
        cmd.Prepare();
        return cmd;
    }

    /// <summary>Creates and asynchronously prepares a command on the specified connection.</summary>
    /// <param name="connection">The connection on which to prepare the command.</param>
    /// <param name="commandText">The SQL statement to prepare.</param>
    /// <param name="cancellationToken">A token for cancelling the preparation.</param>
    /// <returns>The prepared command.</returns>
    public static async ValueTask<SlonCommand> PrepareAsync(SlonConnection connection, string commandText,
        CancellationToken cancellationToken = default)
    {
        var cmd = new SlonCommand(connection, commandText);
        await cmd.PrepareAsync(cancellationToken).ConfigureAwait(false);
        return cmd;
    }

    /// <summary>Gets whether the command shape is read-only.</summary>
    /// <remarks>A successfully prepared command is read-only.</remarks>
    public bool IsReadOnly => _batchCore.IsReadOnly;

    /// <summary>Gets or sets whether executions of this command are eligible for automatic preparation.</summary>
    /// <remarks>
    /// Explicit <see cref="Prepare()"/> creates an owned prepared command regardless of this value.
    /// </remarks>
    public bool AllowAutoPreparation
    {
        get => _batchCore.AllowAutoPreparation;
        set => _batchCore.AllowAutoPreparation = value;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Successful preparation makes the command shape read-only. Values in <see cref="Parameters"/>
    /// remain mutable and are used by the ordinary <see cref="DbCommand"/> execution methods.
    /// </remarks>
    public override void Prepare()
    {
        SetupCommands();
        _batchCore.Prepare(parameters: null);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Successful preparation makes the command shape read-only. Values in <see cref="Parameters"/>
    /// remain mutable and are used by the ordinary <see cref="DbCommand"/> execution methods.
    /// </remarks>
    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        SetupCommands();
        return _batchCore.PrepareAsync(parameters: null, cancellationToken).AsTask();
    }

    /// <inheritdoc/>
    [AllowNull]
    public override string CommandText
    {
        get => _overallCommandText;
        set
        {
            _batchCore.ThrowIfDisposedOrReadOnly();
            value ??= string.Empty;
            _isOverallStateDirty = _isOverallStateDirty || _overallCommandText != value;
            _overallCommandText = value;
        }
    }

    /// <inheritdoc/>
    public override int CommandTimeout
    {
        get => SlonDataSourceOptions.ToAdoTimeoutSeconds(_batchCore.Timeout);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _batchCore.Timeout = TimeSpan.FromSeconds(value);
        }
    }

    /// <summary>
    /// Gets or sets how long the command may wait for the driver to begin consuming its response.
    /// For datasource commands, this includes waiting for an eligible pooled connection and, when
    /// pipelined, waiting for the responses of earlier operations after this command has been written.
    /// The default follows <see cref="CommandTimeout"/> until explicitly set. Zero means no timeout.
    /// </summary>
    public TimeSpan PendingTimeout
    {
        get => _batchCore.PendingTimeout;
        set => _batchCore.PendingTimeout = value;
    }

    /// <inheritdoc/>
    public override CommandType CommandType
    {
        get => _overallCommandType;
        set
        {
            _batchCore.ThrowIfDisposedOrReadOnly();
            if (value is not CommandType.Text)
                throw new NotSupportedException();
            _isOverallStateDirty = _isOverallStateDirty || _overallCommandType != value;
            _overallCommandType = value;
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
        => !_batchCore.TryGetDataSource(out _, out var connection) ? connection?.CurrentTransaction : null;

    /// <inheritdoc/>
    public override bool DesignTimeVisible { get; set; }

    /// <inheritdoc/>
    public override void Cancel()
        => _batchCore.Cancel();

    /// <summary>Requests cancellation and waits until delivery settles or execution ends.</summary>
    public Task CancelAsync(CancellationToken cancellationToken = default)
        => _batchCore.CancelAsync(cancellationToken);

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
        ArgumentNullException.ThrowIfNull(parameters);
        SetupCommands();
        return _batchCore.ExecuteNonQuery(parameters);
    }

    /// <summary>Executes the command against its connection object, returning the number of rows affected.</summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the number of records affected.</returns>
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        SetupCommands();
        return _batchCore.ExecuteNonQueryAsync(parameters: null, cancellationToken).AsTask();
    }

    /// <summary>Executes the command against its connection object, returning the number of rows affected.</summary>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the number of records affected.</returns>
    public ValueTask<int> ExecuteNonQueryAsync(DbParameterCollection parameters, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        SetupCommands();
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
        ArgumentNullException.ThrowIfNull(parameters);
        SetupCommands();
        return _batchCore.ExecuteScalar(parameters);
    }

    /// <summary>Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.</summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the first column of the first row in the first result set.</returns>
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        SetupCommands();
        return _batchCore.ExecuteScalarAsync(parameters: null, cancellationToken).AsTask();
    }

    /// <summary>Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.</summary>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the first column of the first row in the first result set.</returns>
    public ValueTask<object?> ExecuteScalarAsync(DbParameterCollection parameters, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        SetupCommands();
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
        ArgumentNullException.ThrowIfNull(parameters);
        SetupCommands();
        return _batchCore.ExecuteReader(parameters, CommandBehavior.Default);
    }

    /// <summary>Executes the command against its connection, returning a <see cref="T:Slon.SlonDataReader" /> which can be used to access the results.</summary>
    /// <param name="behavior">An instance of <see cref="T:System.Data.CommandBehavior" />, specifying options for command execution and data retrieval.</param>
    /// <param name="parameters">The parameter collection used for this invocation.</param>
    /// <returns>An <see cref="T:Slon.SlonDataReader" /> object.</returns>
    public SlonDataReader ExecuteReader(DbParameterCollection parameters, CommandBehavior behavior)
    {
        ArgumentNullException.ThrowIfNull(parameters);
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
        SetupCommands();
        return _batchCore.ExecuteReaderAsync(parameters: null, CommandBehavior.Default, cancellationToken);
    }

    /// <inheritdoc cref="System.Data.Common.DbCommand.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>
    public new ValueTask<SlonDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        SetupCommands();
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
        ArgumentNullException.ThrowIfNull(parameters);
        SetupCommands();
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
        ArgumentNullException.ThrowIfNull(parameters);
        SetupCommands();
        return _batchCore.ExecuteReaderAsync(parameters, behavior, cancellationToken);
    }

    /// <inheritdoc/>
    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        SetupCommands();
        return _batchCore.ExecuteDbReaderAsync(parameters: null, behavior, cancellationToken).AsTask();
    }

    /// <summary>Creates a new instance of a <see cref="T:Slon.SlonParameter" /> object.</summary>
    /// <returns>A <see cref="T:Slon.SlonParameter" /> object.</returns>
    public new SlonParameter CreateParameter() => new();

    /// <inheritdoc/>
    protected override DbParameter CreateDbParameter() => CreateParameter();
    /// <inheritdoc/>
    protected override DbConnection? DbConnection
    {
        get => !_batchCore.TryGetDataSource(out _, out var connection) ? connection : null;
        set
        {
            if (value is not null and not SlonConnection)
            {
                ThrowHelper.ThrowArgumentException(nameof(value), $"Value is not an instance of {nameof(SlonConnection)}.");
                return;
            }
            _batchCore.SetConnection((SlonConnection?)value);
        }
    }

    /// <inheritdoc/>
    protected override DbParameterCollection DbParameterCollection => Parameters;
    /// <inheritdoc/>
    protected override DbTransaction? DbTransaction { get => Transaction; set {} }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        try
        {
            if (disposing)
                _batchCore.Dispose();
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
        finally
        {
            base.Dispose(disposing);
        }
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        try
        {
            await _batchCore.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
        finally
        {
            base.Dispose(true);
        }
    }

}
