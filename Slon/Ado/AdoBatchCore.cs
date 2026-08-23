using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Serialization;
using Slon.Runtime.CompilerServices;

namespace Slon;

// Shared between DbBatch and DbCommand
partial struct AdoBatchCore<TCommand> where TCommand : IAdoCommand
{
    readonly FieldRef<AdoBatchCore<TCommand>> _fieldRef;
    object _dataSourceOrConnection;
    bool _disposed;
    bool _explicitlyPrepared;
    TimeSpan _timeout;
    TimeSpan? _pendingTimeout;
    bool _enableErrorBarriers;
    CommandFlow? _activeFlow;
    AdoCommandList<TCommand> _commands;

    public AdoBatchCore(FieldRef<AdoBatchCore<TCommand>> fieldRef)
    {
        _dataSourceOrConnection = null!;
        _fieldRef = fieldRef;
    }

    public AdoBatchCore(SlonConnection connection, FieldRef<AdoBatchCore<TCommand>> fieldRef)
    {
        _dataSourceOrConnection = connection;
        _fieldRef = fieldRef;
    }

    public AdoBatchCore(SlonDataSource dataSource, FieldRef<AdoBatchCore<TCommand>> fieldRef)
    {
        _dataSourceOrConnection = dataSource;
        _fieldRef = fieldRef;
    }

    [UnscopedRef]
    public ref AdoCommandList<TCommand> Commands => ref _commands;

    public TimeSpan Timeout
    {
        get => _timeout;
        set
        {
            // No need to throw if read-only, we allow any state unrelated to preparation to be changed.
            ThrowIfDisposed();
            _timeout = value;
        }
    }

    public TimeSpan PendingTimeout
    {
        get => _pendingTimeout ?? _timeout;
        set
        {
            ThrowIfDisposed();
            if (value < TimeSpan.Zero && value != System.Threading.Timeout.InfiniteTimeSpan)
                throw new ArgumentOutOfRangeException(nameof(value),
                    "Value must be non-negative or Timeout.InfiniteTimeSpan.");
            _pendingTimeout = value;
        }
    }

    /// <summary>Whether to place an error barrier between every command in this batch. The default value is <see langword="false" />.</summary>
    public bool EnableErrorBarriers
    {
        get => _enableErrorBarriers;
        set
        {
            ThrowIfDisposed();
            _enableErrorBarriers = value;
        }
    }

    /// Return whether the instance is ready for mutations. It can become read-only, for example, if it has been prepared.
    public bool IsReadOnly => _explicitlyPrepared;

    bool HasCloseConnection(CommandBehavior behavior) => (behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection;
    void ThrowIfHasCloseConnection(CommandBehavior behavior)
    {
        // Only for DbConnection commands, throws for DbDataSource commands (alternatively we can choose to ignore it).
        if (HasCloseConnection(behavior))
            ThrowHelper.ThrowArgumentException(nameof(behavior), $"Cannot pass {nameof(CommandBehavior.CloseConnection)} to a DbDataSource command, this is only valid when a command has a connection.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ThrowIfDisposedOrReadOnly()
    {
        ThrowIfDisposed();
        if (Volatile.Read(ref _activeFlow) is not null)
            ThrowHelper.ThrowInvalidOperation("The command cannot be changed while an execution is active.");
        ThrowIfReadOnly();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ThrowIfDisposed()
    {
        if (_disposed)
            Throw(_fieldRef.Instance);

        static void Throw(object instance) => throw new ObjectDisposedException(instance.GetType().Name);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ThrowIfReadOnly()
    {
        if (_explicitlyPrepared)
            ThrowHelper.ThrowInvalidOperation("Command is prepared and cannot be changed until it is unprepared.");
    }

    public void SetConnection(SlonConnection? connection)
    {
        ThrowIfDisposedOrReadOnly();
        if (TryGetDataSource(out _, out _))
            ThrowHelper.ThrowInvalidOperation("This is a DbDataSource command and cannot be assigned to connections.");

        // Explicitly prepared commands are read-only, so rebinding only needs to discard
        // connection-local tracking inherited from an earlier unprepared execution.
        ClearTrackedRefs();

        _dataSourceOrConnection = connection!;
    }

    void ClearTrackedRefs()
    {
        // Only the explicit (Command) kind is stale on connection move. Auto refs point at
        // workload-shared TCs that stay valid across SlonConnections in the same datasource.
        foreach (ref var command in _commands.AsSpan())
        {
            if (command.Tracked is { Kind: TrackedCommandKind.Command })
                command.Tracked = null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetDataSource([NotNullWhen(true)]out SlonDataSource? dataSource,
        out SlonConnection? connection)
    {
        if (_dataSourceOrConnection is SlonDataSource source)
        {
            dataSource = source;
            connection = null;
            return true;
        }

        dataSource = null;
        connection = _dataSourceOrConnection as SlonConnection;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal AdoCommandFlowOptions CreateAdoCommandFlowOptions(
        ReadOnlySpan<DbParameterCollection?> parametersSpan, CommandBehavior behavior,
        SlonDataSource.PgDbDependencies dependencies, SlonConnection? connection = null,
        PgConnection? pgConnection = null, TimeSpan? pendingTimeout = null, bool preparing = false)
    {
        var factory = new AdoCommandFlowFactory<TCommand>(
            _fieldRef.Instance, _commands.AsSpan(), dependencies);
        return factory.Create(
            parametersSpan, behavior, _explicitlyPrepared, _enableErrorBarriers, Timeout,
            connection, pgConnection, pendingTimeout, preparing);
    }

    SlonDataSource.PgDbDependencies GetDependencies()
    {
        TryGetDataSource(out var dataSource, out var connection);
        connection ??= dataSource is null ? ThrowConnectionNotInitialized() : null;
        return (dataSource ?? connection!.DbDataSource).GetDbDependencies();
    }

    ValueTask<SlonDataSource.PgDbDependencies> GetDependenciesAsync(
        CancellationToken cancellationToken)
    {
        TryGetDataSource(out var dataSource, out var connection);
        connection ??= dataSource is null ? ThrowConnectionNotInitialized() : null;
        return (dataSource ?? connection!.DbDataSource).GetDbDependenciesAsync(cancellationToken);
    }

    CommandFlow Enqueue(DbParameterCollection? parameters, CommandBehavior behavior,
        SlonDataSource.PgDbDependencies dependencies, bool preparing = false)
    {
        if (TryGetDataSource(out var dataSource, out var connection))
        {
            ThrowIfHasCloseConnection(behavior);
            var pendingTimeout = PendingTimeout;
            return dataSource.EnqueueCommands(
                new AdoCommandFlow<TCommand>(
                    async: false, _fieldRef, parameters, behavior, dependencies,
                    connection: null, pendingTimeout, preparing, _commands.Count,
                    _explicitlyPrepared && _fieldRef.Instance is SlonCommand ? null : _fieldRef.Instance),
                pendingTimeout);
        }

        connection ??= ThrowConnectionNotInitialized();
        return connection.Enqueue(new AdoCommandFlow<TCommand>(
            async: false, _fieldRef, parameters, behavior, dependencies,
            connection, PendingTimeout, preparing, _commands.Count, _fieldRef.Instance));
    }

    ValueTask<CommandFlow> EnqueueAsync(DbParameterCollection? parameters,
        CommandBehavior behavior, SlonDataSource.PgDbDependencies dependencies,
        CancellationToken cancellationToken, bool preparing = false)
    {
        if (TryGetDataSource(out var dataSource, out var connection))
        {
            ThrowIfHasCloseConnection(behavior);
            var pendingTimeout = PendingTimeout;
            return dataSource.EnqueueCommandsAsync(
                new AdoCommandFlow<TCommand>(
                    async: true, _fieldRef, parameters, behavior, dependencies,
                    connection: null, pendingTimeout, preparing, _commands.Count,
                    _explicitlyPrepared && _fieldRef.Instance is SlonCommand ? null : _fieldRef.Instance),
                pendingTimeout, cancellationToken);
        }

        connection ??= ThrowConnectionNotInitialized();
        return connection.EnqueueAsync<CommandFlow>(new AdoCommandFlow<TCommand>(
            async: true, _fieldRef, parameters, behavior, dependencies,
            connection, PendingTimeout, preparing, _commands.Count, _fieldRef.Instance), cancellationToken);
    }

    [DoesNotReturn]
    static SlonConnection ThrowConnectionNotInitialized()
        => throw new InvalidOperationException("The command has no connection or data source.");

    public int ExecuteNonQuery(DbParameterCollection? parameters)
    {
        ThrowIfDisposed();
        using var activity = StartActivity();
        try
        {
            return ExecuteNonQueryCore(parameters);
        }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default;
        }
    }

    int ExecuteNonQueryCore(DbParameterCollection? parameters)
    {
        var recordsAffected = -1L;
        var dependencies = GetDependencies();
        foreach (var result in Enqueue(parameters, CommandBehavior.Default, dependencies))
        {
            // Drive the result to its CommandComplete so RecordsAffected is populated (we discard any
            // rows - this is ExecuteNonQuery). Only data-modifying statements contribute a non-zero count.
            // RecordsAffected throws a PgErrorException on a failed command, so the error surfaces here
            // instead of silently reporting 0 affected.
            foreach (var _ in result) { }
            var resultRecordsAffected = result.GetCommandComplete().BatchRecordsAffected;
            if (resultRecordsAffected >= 0)
                recordsAffected = recordsAffected < 0
                    ? resultRecordsAffected
                    : checked(recordsAffected + resultRecordsAffected);
        }
        return checked((int)recordsAffected);
    }

    static async ValueTask<int> ExecuteNonQueryAsyncCore(FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters, CancellationToken cancellationToken)
    {
        ref var thisRef = ref fieldRef.Invoke();
        using var activity = thisRef.StartActivity();
        try
        {
            thisRef.ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            var dependencies = await thisRef.GetDependenciesAsync(cancellationToken)
                .ConfigureAwait(false);
            var flow = await fieldRef.Invoke().EnqueueAsync(parameters, CommandBehavior.Default,
                dependencies, cancellationToken).ConfigureAwait(false);
            return checked((int)await flow.ConsumeNonQueryAsync(cancellationToken).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default;
        }
    }

    public ValueTask<int> ExecuteNonQueryAsync(DbParameterCollection? parameters, CancellationToken cancellationToken = default)
    {
        var fieldRef = _fieldRef;
        return ExecuteNonQueryAsyncCore(fieldRef, parameters, cancellationToken);
    }

    public object? ExecuteScalar(DbParameterCollection? parameters)
    {
        ThrowIfDisposed();
        using var activity = StartActivity();
        try
        {
            return ExecuteScalarCore(parameters);
        }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default;
        }
    }

    object? ExecuteScalarCore(DbParameterCollection? parameters)
    {
        var dependencies = GetDependencies();
        var fieldReader = new PgSerializerFieldReader(dependencies.SerializerOptions);
        foreach (var result in Enqueue(parameters, CommandBehavior.Default, dependencies))
        {
            using var rowEnumerator = result.GetAsyncEnumerator();
            if (rowEnumerator.MoveNext())
            {
                fieldReader.Initialize(result);
                return result.FieldCount is not 0
                    ? fieldReader.ReadObject(rowEnumerator.Current, 0)
                    : null;
            }
            // No row from this result: surface a failed command (stored ErrorResponse) instead of
            // silently returning null.
            result.GetCommandComplete();
        }
        return null;
    }

    static async ValueTask<object?> ExecuteScalarAsyncCore(FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters, CancellationToken cancellationToken)
    {
        ref var thisRef = ref fieldRef.Invoke();
        using var activity = thisRef.StartActivity();
        CommandFlow.Enumerator enumerator = default;
        try
        {
            thisRef.ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            var dependencies = await thisRef.GetDependenciesAsync(cancellationToken)
                .ConfigureAwait(false);
            var fieldReader = new PgSerializerFieldReader(dependencies.SerializerOptions);
            enumerator = (await fieldRef.Invoke().EnqueueAsync(parameters, CommandBehavior.Default,
                dependencies, cancellationToken).ConfigureAwait(false))
                .GetAsyncEnumerator(cancellationToken);
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                var rowEnumerator = enumerator.Current.GetAsyncEnumerator(cancellationToken);
                try
                {
                    if (await rowEnumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    {
                        fieldReader.Initialize(enumerator.Current);
                        return enumerator.Current.FieldCount is not 0
                            ? await fieldReader.ReadObjectAsync(rowEnumerator.Current, 0,
                                cancellationToken)
                                .ConfigureAwait(false)
                            : null;
                    }
                }
                finally
                {
                    await rowEnumerator.DisposeAsync().ConfigureAwait(false);
                }
                // No row from this result: surface a failed command (stored ErrorResponse) instead of
                // silently returning null.
                enumerator.Current.GetCommandComplete();
            }
            return null;
        }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default;
        }
        finally
        {
            try
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SlonTracing.RecordException(activity, ex);
                AdoException.Throw(ex);
            }
        }
    }

    public ValueTask<object?> ExecuteScalarAsync(DbParameterCollection? parameters, CancellationToken cancellationToken = default)
    {
        var fieldRef = _fieldRef;
        return ExecuteScalarAsyncCore(fieldRef, parameters, cancellationToken);
    }

    public SlonDataReader ExecuteReader(DbParameterCollection? parameters, CommandBehavior behavior)
    {
        ThrowIfDisposed();
        using var activity = StartActivity();
        try
        {
            return ExecuteReaderCore(parameters, behavior);
        }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default;
        }
    }

    SlonDataReader ExecuteReaderCore(DbParameterCollection? parameters, CommandBehavior behavior)
    {
        var dependencies = GetDependencies();
        return SlonDataReader.Create(behavior, Enqueue(parameters, behavior, dependencies),
            dependencies.SerializerOptions, GetConnectionToClose(behavior));
    }

    SlonConnection? GetConnectionToClose(CommandBehavior behavior)
    {
        if (!HasCloseConnection(behavior) || TryGetDataSource(out _, out var connection))
            return null;
        return connection;
    }

    public ValueTask<DbDataReader> ExecuteDbReaderAsync(DbParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return ValueTask.FromException<DbDataReader>(new ObjectDisposedException(_fieldRef.Instance.GetType().Name));
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<DbDataReader>(cancellationToken);

        return ExecuteReaderAsyncCore<DbDataReader>(
            _fieldRef, parameters, behavior, cancellationToken);
    }

    public ValueTask<SlonDataReader> ExecuteReaderAsync(DbParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return ValueTask.FromException<SlonDataReader>(new ObjectDisposedException(_fieldRef.Instance.GetType().Name));
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<SlonDataReader>(cancellationToken);

        return ExecuteReaderAsyncCore<SlonDataReader>(
            _fieldRef, parameters, behavior, cancellationToken);
    }

    static ValueTask<TReader> ExecuteReaderAsyncCore<TReader>(
        FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters,
        CommandBehavior behavior, CancellationToken cancellationToken)
        where TReader : DbDataReader
    {
        ref var core = ref fieldRef.Invoke();
        var activity = core.StartActivity();
        try
        {
            var connectionToClose = core.GetConnectionToClose(behavior);
            var dependenciesTask = core.GetDependenciesAsync(cancellationToken);
            return dependenciesTask.IsCompletedSuccessfully
                ? BeginReaderCreation<TReader>(fieldRef, parameters, behavior, cancellationToken,
                    connectionToClose, dependenciesTask.Result, activity)
                : AwaitDependenciesAndCreateReaderAsync<TReader>(fieldRef, parameters, behavior,
                    cancellationToken, connectionToClose, dependenciesTask, activity);
        }
        catch (Exception ex)
        {
            return FailReaderCreation<TReader>(activity, ex);
        }
    }

    static ValueTask<TReader> BeginReaderCreation<TReader>(
        FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters,
        CommandBehavior behavior, CancellationToken cancellationToken,
        SlonConnection? connectionToClose, SlonDataSource.PgDbDependencies dependencies,
        Activity? activity)
        where TReader : DbDataReader
    {
        try
        {
            return SlonDataReader.CreateAsync<TReader>(behavior,
                fieldRef.Invoke().EnqueueAsync(parameters, behavior, dependencies, cancellationToken),
                dependencies.SerializerOptions, cancellationToken, connectionToClose, activity);
        }
        catch (Exception ex)
        {
            return FailReaderCreation<TReader>(activity, ex);
        }
    }

    static async ValueTask<TReader> AwaitDependenciesAndCreateReaderAsync<TReader>(
        FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters,
        CommandBehavior behavior, CancellationToken cancellationToken,
        SlonConnection? connectionToClose,
        ValueTask<SlonDataSource.PgDbDependencies> dependenciesTask, Activity? activity)
        where TReader : DbDataReader
    {
        SlonDataSource.PgDbDependencies dependencies;
        try
        {
            dependencies = await dependenciesTask.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            activity?.Dispose();
            AdoException.Throw(ex);
            return default!;
        }

        return await BeginReaderCreation<TReader>(fieldRef, parameters, behavior, cancellationToken,
            connectionToClose, dependencies, activity).ConfigureAwait(false);
    }

    static ValueTask<TReader> FailReaderCreation<TReader>(Activity? activity, Exception exception)
        where TReader : DbDataReader
    {
        SlonTracing.RecordException(activity, exception);
        activity?.Dispose();
        return ValueTask.FromException<TReader>(AdoException.Project(exception));
    }

    Activity? StartActivity()
    {
        TryGetDataSource(out var dataSource, out var connection);
        return dataSource is null && connection is null
            ? null
            : SlonTracing.Start(dataSource ?? connection!.DbDataSource, _commands.Count);
    }

    public void Add(TCommand command)
    {
        _commands.Add(command);
    }

    public void Clear()
    {
        _commands.Clear();
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        if (!_explicitlyPrepared)
            return;
        if (TryGetDataSource(out var dataSource, out var connection))
        {
            _ = dataSource.ReleaseOwnedPreparedCommand(_fieldRef.Instance, awaitable: false);
            return;
        }

        if (connection is not null)
            connection.UnprepareOwned(async: false, _fieldRef.Instance).GetAwaiter().GetResult();
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return new();

        _disposed = true;
        if (!_explicitlyPrepared)
            return new();
        if (TryGetDataSource(out var dataSource, out var connection))
            return dataSource.ReleaseOwnedPreparedCommand(_fieldRef.Instance, awaitable: true);

        return connection?.UnprepareOwned(async: true, _fieldRef.Instance) ?? default;
    }

    public void Cancel()
    {
        if (_explicitlyPrepared && TryGetDataSource(out _, out _))
            ThrowPreparedCancellationNotSupported();
        Volatile.Read(ref _activeFlow)?.CancelAsync().GetAwaiter().GetResult();
    }

    public Task CancelAsync(CancellationToken cancellationToken = default)
    {
        if (_explicitlyPrepared && TryGetDataSource(out _, out _))
            ThrowPreparedCancellationNotSupported();
        var flow = Volatile.Read(ref _activeFlow);
        if (flow is null)
            return Task.CompletedTask;
        var task = flow.CancelAsync();
        return cancellationToken.CanBeCanceled ? task.WaitAsync(cancellationToken) : task;
    }

    [DoesNotReturn]
    static void ThrowPreparedCancellationNotSupported()
        => throw new NotSupportedException(
            "A datasource-prepared command can have multiple executions; cancel an execution with its token.");

    internal void OnFlowStarted(CommandFlow flow) => Volatile.Write(ref _activeFlow, flow);

    internal void OnFlowCompleting(CommandFlow flow, Exception? exception)
    {
        // A flow-level fault while holding an ADO connection lease breaks that lease. SQL errors don't
        // reach here: they surface on CommandResult and the flow completes cleanly. OnCompleting runs
        // before terminal publication, so an awaiter observing the fault also observes Broken.
        if (exception is not null && !TryGetDataSource(out _, out var connection) && connection is not null)
            connection.Break(exception);

        Interlocked.CompareExchange(ref _activeFlow, null, flow);
    }

}
