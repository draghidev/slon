using System.Data;
using System.Data.Common;
using Slon.Runtime.CompilerServices;
using Slon.Pg.Protocol.Flows;

namespace Slon;

// Implementation
/// <inheritdoc cref="System.Data.Common.DbBatch" />
public sealed partial class SlonBatch
{
    AdoBatchCore<SlonBatchCommand> _batchCore;
    SlonBatchCommands? _batchCommands;

    unsafe SlonBatch(SlonConnection? connection, SlonDataSource? dataSource)
    {
        var fieldRef = FieldRef<AdoBatchCore<SlonBatchCommand>>.Create(&GetBatchCore, this);
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
    }

    unsafe SlonBatchCommands CreateBatchCommandCollection()
        => new(FieldRef<AdoBatchCore<SlonBatchCommand>>.Create(&GetBatchCore, this));

    internal void OnFlowStarted(CommandFlow flow)
        => _batchCore.OnFlowStarted(flow);

    internal void OnFlowCompleting(CommandFlow flow, Exception? exception)
        => _batchCore.OnFlowCompleting(flow, exception);

    static ref AdoBatchCore<SlonBatchCommand> GetBatchCore(SlonBatch instance) => ref instance._batchCore;
}

// Public surface & ADO.NET
public sealed partial class SlonBatch : DbBatch
{
    /// Initializes an unbound batch.
    public SlonBatch() : this(null, null) {}

    /// <summary>Initializes a batch bound to the specified connection.</summary>
    /// <param name="conn">The connection on which the batch executes.</param>
    public SlonBatch(SlonConnection conn)
        : this(conn ?? throw new ArgumentNullException(nameof(conn)), null) {}
    internal SlonBatch(SlonDataSource dataSource) : this(null, dataSource) {}

    /// <summary>Gets whether the batch shape is read-only.</summary>
    /// <remarks>A successfully prepared batch is read-only.</remarks>
    public bool IsReadOnly => _batchCore.IsReadOnly;

    /// <summary>Gets or sets whether to place an error barrier between every command in this batch.</summary>
    public bool EnableErrorBarriers
    {
        get => _batchCore.EnableErrorBarriers;
        set => _batchCore.EnableErrorBarriers = value;
    }

    /// <summary>Gets or sets whether commands in this batch are eligible for automatic preparation.</summary>
    /// <remarks>
    /// Individual commands can further restrict eligibility through
    /// <see cref="SlonBatchCommand.AllowAutoPreparation"/>. Explicit preparation creates owned
    /// prepared commands independently of both settings.
    /// </remarks>
    public bool AllowAutoPreparation
    {
        get => _batchCore.AllowAutoPreparation;
        set => _batchCore.AllowAutoPreparation = value;
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
        => _batchCore.ExecuteNonQuery(parameters: null);

    /// <summary>Executes the command against its connection object, returning the number of rows affected.</summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the number of records affected.</returns>
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
        => _batchCore.ExecuteNonQueryAsync(parameters: null, cancellationToken).AsTask();

    /// <summary>Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.</summary>
    /// <returns>The first column of the first row in the first result set.</returns>
    public override object? ExecuteScalar()
        => _batchCore.ExecuteScalar(parameters: null);

    /// <summary>Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.</summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task returning the first column of the first row in the first result set.</returns>
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
        => _batchCore.ExecuteScalarAsync(parameters: null, cancellationToken).AsTask();

    /// <summary>Executes the command against its connection, returning a <see cref="T:Slon.SlonDataReader" /> which can be used to access the results.</summary>
    /// <param name="behavior">An instance of <see cref="T:System.Data.CommandBehavior" />, specifying options for command execution and data retrieval.</param>
    /// <returns>An <see cref="T:Slon.SlonDataReader" /> object.</returns>
    public new SlonDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default)
        => _batchCore.ExecuteReader(parameters: null, behavior);

    /// <inheritdoc/>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => _batchCore.ExecuteReader(parameters: null, behavior);

    /// <inheritdoc cref="System.Data.Common.DbBatch.ExecuteReaderAsync(CancellationToken)"/>
    public new ValueTask<SlonDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
        => _batchCore.ExecuteReaderAsync(parameters: null, CommandBehavior.Default, cancellationToken);

    /// <inheritdoc cref="System.Data.Common.DbBatch.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>
    public new ValueTask<SlonDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
        => _batchCore.ExecuteReaderAsync(parameters: null, behavior, cancellationToken);

    /// <inheritdoc/>
    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        => _batchCore.ExecuteDbReaderAsync(parameters: null, behavior, cancellationToken).AsTask();

    /// <inheritdoc/>
    public override void Cancel() => _batchCore.Cancel();

    /// <summary>Requests cancellation and waits until delivery settles or execution ends.</summary>
    public Task CancelAsync(CancellationToken cancellationToken = default)
        => _batchCore.CancelAsync(cancellationToken);

    /// <summary>Gets the transaction active on this batch's connection, if any.</summary>
    /// <remarks>
    /// PostgreSQL has one current transaction per connection, so batches implicitly participate in
    /// the transaction started through <see cref="SlonConnection.BeginTransaction()"/>.
    /// </remarks>
    public new SlonTransaction? Transaction
        => !_batchCore.TryGetDataSource(out _, out var connection) ? connection?.CurrentTransaction : null;

    /// <summary>Gets the collection of <see cref="T:Slon.SlonBatchCommand" /> objects.</summary>
    /// <returns>The commands contained within the batch.</returns>
    public new SlonBatchCommands BatchCommands => _batchCommands ??= CreateBatchCommandCollection();

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
        get => SlonDataSourceOptions.ToAdoTimeoutSeconds(_batchCore.Timeout);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _batchCore.Timeout = TimeSpan.FromSeconds(value);
        }
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
        set => _batchCore.SetConnection(value);
    }

    /// <inheritdoc/>
    protected override DbConnection? DbConnection
    {
        get => Connection;
        set
        {
            if (value is not null and not SlonConnection)
                throw new ArgumentException($"Value is not an instance of {nameof(SlonConnection)}.", nameof(value));
            Connection = (SlonConnection?)value;
        }
    }

    /// <inheritdoc/>
    protected override DbTransaction? DbTransaction { get => Transaction; set {} }

    /// <inheritdoc/>
    public override void Dispose()
    {
        try
        {
            _batchCore.Dispose();
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
        finally
        {
            base.Dispose();
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
            base.Dispose();
        }
    }
}
