using System.Data;
using System.Data.Common;
using Slon.Pg;
using Slon.Runtime.CompilerServices;
using Slon.Pg.Protocol.Flows;

namespace Slon;

// TODO introduce execute overloads that take an array of parameter collections, requiring one for each batch command.
/// <inheritdoc cref="System.Data.Common.DbBatch" />
public sealed class SlonBatch : DbBatch
{
    AdoBatchCore<SlonBatchCommand> _batchCore;
    SlonBatchCommands? _batchCommands;

    unsafe SlonBatch(SlonConnection? conn, SlonDataSource? dataSource)
    {
        if (conn is not null)
        {
            _batchCore = new(conn, FieldRef<AdoBatchCore<SlonBatchCommand>>.Create(&GetBatchCore, this));
            _batchCore.Timeout = conn.DefaultCommandTimeout;
        }
        else if (dataSource is not null)
        {
            _batchCore = new(dataSource, FieldRef<AdoBatchCore<SlonBatchCommand>>.Create(&GetBatchCore, this));
            _batchCore.Timeout = dataSource.DefaultCommandTimeout;
        }
    }

    public SlonBatch() : this(null, null) {}
    public SlonBatch(SlonConnection conn) : this(conn, null) {}
    internal SlonBatch(SlonDataSource dataSource) : this(null, dataSource) {}

    internal static unsafe SlonBatch CreateFromDbCommand<TCommand>(AdoBatchCore<TCommand> dbCommandCore, int copies, bool withParameters) where TCommand : IAdoCommand
    {
        var dbCommands = dbCommandCore.Commands;
        var commands = new AdoCommandList<SlonBatchCommand>(dbCommands.Count);
        for (var i = 0; i < copies; i++)
        {
            foreach (var dbCommand in dbCommands)
            {
                var batchCommand = new SlonBatchCommand
                {
                    CommandText = dbCommand.CommandText,
                    CommandType = dbCommand.CommandType,
                    AppendErrorBarrier = dbCommand.AppendErrorBarrier
                };
                ((IAdoCommand)batchCommand).Tracked = dbCommand.Tracked;
                if (withParameters && batchCommand.Parameters is { Count: > 0 } parameters)
                    batchCommand.Parameters.AddRange(parameters);
                commands.Add(batchCommand);
            }
        }
        var batchCore = new AdoBatchCore<SlonBatchCommand>();
        var batch = new SlonBatch { _batchCore = batchCore };
        batch._batchCore.InitializeFrom(FieldRef<AdoBatchCore<SlonBatchCommand>>.Create(&GetBatchCore, batch), dbCommandCore, commands);
        return batch;
    }

    void SetupCommands()
    {
    }

    ValueTask<T> SetupCommandsWrappedExceptions<T>()
    {
        return new();
        // try
        // {
        //     SetupCommands();
        //     return new();
        // }
        // catch (Exception ex)
        // {
        //     return ValueTask.FromException<T>(ex);
        // }
    }

    /// <summary>
    /// Return whether the instance is ready for mutations. It can become read-only, for example, if it has been prepared.
    /// </summary>
    public bool IsReadOnly => _batchCore.IsReadOnly;

    /// <summary>Whether to place an error barrier between every command in this batch. The default value is <see langword="false" />.</summary>
    public bool EnableErrorBarriers
    {
        get => _batchCore.EnableErrorBarriers;
        set => _batchCore.EnableErrorBarriers = value;
    }

    /// <inheritdoc/>
    public override void Prepare()
        => _batchCore.Prepare(parameters: null);

    /// <inheritdoc/>
    public override Task PrepareAsync(CancellationToken cancellationToken = default)
        => _batchCore.PrepareAsync(parameters: null, cancellationToken).AsTask();

    /// <summary>Executes the command against its connection object, returning the number of rows affected.</summary>
    /// <returns>The number of records affected.</returns>
    public override int ExecuteNonQuery()
    {
        SetupCommands();
        return _batchCore.ExecuteNonQuery(parameters: null);
    }

    /// <summary>Executes the command against its connection object, returning the number of rows affected.</summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the number of records affected.</returns>
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<int>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task.AsTask();

        return _batchCore.ExecuteNonQueryAsync(parameters: null, cancellationToken).AsTask();
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
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<object?>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task.AsTask();

        return _batchCore.ExecuteScalarAsync(parameters: null, cancellationToken).AsTask();
    }

    /// <summary>Executes the command against its connection, returning a <see cref="T:Slon.SlonDataReader" /> which can be used to access the results.</summary>
    /// <param name="behavior">An instance of <see cref="T:System.Data.CommandBehavior" />, specifying options for command execution and data retrieval.</param>
    /// <returns>An <see cref="T:Slon.SlonDataReader" /> object.</returns>
    public new SlonDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default)
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

    /// <inheritdoc/>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        SetupCommands();
        return _batchCore.ExecuteReader(parameters: null, behavior);
    }

    /// <inheritdoc cref="System.Data.Common.DbBatch.ExecuteReaderAsync(CancellationToken)"/>
    public new ValueTask<SlonDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<SlonDataReader>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task;
        return _batchCore.ExecuteReaderAsync(parameters: null, CommandBehavior.Default, cancellationToken);
    }

    /// <inheritdoc cref="System.Data.Common.DbBatch.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>
    public new ValueTask<SlonDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        if (SetupCommandsWrappedExceptions<SlonDataReader>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task;
        return _batchCore.ExecuteReaderAsync(parameters: null, behavior, cancellationToken);
    }

    /// <inheritdoc/>
    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (SetupCommandsWrappedExceptions<DbDataReader>() is { IsCompleted: true, IsCompletedSuccessfully: false } task)
            return task.AsTask();
        return _batchCore.ExecuteDbReaderAsync(parameters: null, behavior, cancellationToken).AsTask();
    }

    /// <inheritdoc/>
    public override void Cancel() => _batchCore.Cancel();

    /// <summary>Requests cancellation and waits until delivery settles or execution ends.</summary>
    public Task CancelAsync(CancellationToken cancellationToken = default)
        => _batchCore.CancelAsync(cancellationToken);

    internal void OnFlowStarted(CommandFlow flow)
        => _batchCore.OnFlowStarted(flow);

    internal void OnCommandResult(CommandFlow flow, CommandResult result)
        => _batchCore.OnCommandResult(flow, result);

    internal void OnFlowCompleting(CommandFlow flow, Exception? exception)
        => _batchCore.OnFlowCompleting(flow, exception);

    /// <summary>
    /// Setting this property is ignored by Slon. PostgreSQL only supports a single transaction at a given time on
    /// a given connection, and all commands implicitly run inside the current transaction started via
    /// <see cref="Slon.SlonConnection.BeginTransaction()"/>
    /// </summary>
    public new SlonTransaction? Transaction
        => !_batchCore.TryGetDataSource(out _, out var connection) ? connection?.CurrentTransaction : null;

    /// <summary>Gets the collection of <see cref="T:Slon.SlonBatchCommand" /> objects.</summary>
    /// <returns>The commands contained within the batch.</returns>
    public new SlonBatchCommands BatchCommands => _batchCommands ??= CreateBatchCommandCollection();

    unsafe SlonBatchCommands CreateBatchCommandCollection()
        => new(FieldRef<AdoBatchCore<SlonBatchCommand>>.Create(&GetBatchCore, this));

    /// <summary>Creates a new instance of a <see cref="T:Slon.SlonBatchCommand" /> object.</summary>
    /// <param name="commandText">The command text to be used.</param>
    /// <returns>A <see cref="T:Slon.SlonBatchCommand" /> object.</returns>
    public SlonBatchCommand CreateBatchCommand(string commandText) => new() { CommandText = commandText };

    /// <summary>Creates a new instance of a <see cref="T:Slon.SlonBatchCommand" /> object.</summary>
    /// <returns>A <see cref="T:Slon.SlonBatchCommand" /> object.</returns>
    public new SlonBatchCommand CreateBatchCommand() => new();

    /// <inheritdoc/>
    protected override DbBatchCommand CreateDbBatchCommand() => CreateBatchCommand();
    /// <inheritdoc/>
    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;

    /// <inheritdoc/>
    public override int Timeout
    {
        get => (int)_batchCore.Timeout.TotalSeconds;
        set => _batchCore.Timeout = TimeSpan.FromSeconds(value);
    }

    /// <summary>
    /// Gets or sets how long the batch may wait for the driver to begin consuming its response.
    /// For datasource batches, this includes waiting for an eligible pooled connection and, when
    /// pipelined, waiting for the responses of earlier operations after this batch has been written.
    /// The default follows <see cref="Timeout"/> until explicitly set. Zero means no timeout.
    /// </summary>
    public TimeSpan PendingTimeout
    {
        get => _batchCore.PendingTimeout;
        set => _batchCore.PendingTimeout = value;
    }

    /// <summary>Gets or sets the <see cref="T:Slon.SlonConnection" /> used by this <see cref="T:Slon.SlonBatch" />.</summary>
    /// <returns>The connection to the data source.</returns>
    public new SlonConnection? Connection
    {
        get => !_batchCore.TryGetDataSource(out _, out var connection) ? connection : null;
        set
        {
            _batchCore.ThrowIfDisposedOrReadOnly();
            if (value is not { } conn)
            {
                ThrowHelper.ThrowArgumentException(nameof(value), $"Value is not an instance of {nameof(SlonConnection)}.");
                return;
            }
            _batchCore.SetConnection(conn);
        }
    }

    /// <inheritdoc/>
    protected override DbConnection? DbConnection
    {
        get => Connection;
        set => Connection = (SlonConnection?)value;
    }

    /// <inheritdoc/>
    protected override DbTransaction? DbTransaction { get => Transaction; set {} }

    static ref AdoBatchCore<SlonBatchCommand> GetBatchCore(SlonBatch instance) => ref instance._batchCore;
}
