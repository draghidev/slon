using System.Buffers;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;

namespace Slon;

readonly struct EnqueueArgs<TCommand> where TCommand : IAdoCommand
{
    public readonly FieldRef<AdoBatchCore<TCommand>> FieldRef;
    public readonly DbParameterCollection? Parameters;
    public readonly CommandBehavior Behavior;
    public readonly SlonConnection Connection;

    public EnqueueArgs(FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters, CommandBehavior behavior, SlonConnection connection)
    {
        FieldRef = fieldRef;
        Parameters = parameters;
        Behavior = behavior;
        Connection = connection;
    }
}

// Shared between DbBatch and DbCommand
struct AdoBatchCore<TCommand> where TCommand : IAdoCommand
{
    FieldRef<AdoBatchCore<TCommand>> _fieldRef;
    object _dataSourceOrConnection;
    bool _disposed;
    bool _explicitlyPrepared;
    TimeSpan _timeout;
    bool _enableErrorBarriers;
    AdoCommandList<TCommand> _commands;

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

    internal void InitializeFrom<TOther>(FieldRef<AdoBatchCore<TCommand>> fieldRef, AdoBatchCore<TOther> other, AdoCommandList<TCommand> mappedCommands) where TOther : IAdoCommand
    {
        _dataSourceOrConnection = other._dataSourceOrConnection;
        _fieldRef = fieldRef;
        _disposed = other._disposed;
        _explicitlyPrepared = other._explicitlyPrepared;
        _timeout = other._timeout;
        _enableErrorBarriers = other._enableErrorBarriers;
        if (IsReadOnly)
        {
            foreach (var commands in mappedCommands)
                commands.MakeReadOnly();
        }
        _commands = mappedCommands;
    }

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
        if (IsReadOnly)
            Throw();

        static void Throw() => throw new InvalidOperationException("Command is prepared, no changes can be made until it's unprepared.");
    }

    public void SetConnection(SlonConnection connection)
    {
        ThrowIfDisposedOrReadOnly();
        // TODO We probably can...
        if (TryGetDataSource(out _, out var oldConnection))
            ThrowHelper.ThrowInvalidOperation("This is a DbDataSource command and cannot be assigned to connections.");

        // Conceptually a Dispose against the old connection (release owned state, clear stale
        // tracking refs) followed by a re-bind to the new one.
        ReleaseOwned(oldConnection);
        ClearTrackedRefs();

        _dataSourceOrConnection = connection;
    }

    void ReleaseOwned(SlonConnection connection)
    {
        if (_explicitlyPrepared)
            connection.CloseOwned(_fieldRef.Instance);
    }

    ValueTask ReleaseOwnedAsync(SlonConnection connection)
        => _explicitlyPrepared ? connection.CloseOwnedAsync(_fieldRef.Instance) : new();

    void ClearTrackedRefs()
    {
        // Only the explicit (Command) kind is stale on connection move. Auto refs point at
        // workload-shared TCs that stay valid across SlonConnections in the same datasource.
        foreach (var command in _commands.AsSpan())
        {
            var ado = (IAdoCommand)command;
            if (ado.Tracked is { Kind: TrackedCommandKind.Command })
                ado.Tracked = null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetDataSource([NotNullWhen(true)]out SlonDataSource? dataSource, [NotNullWhen(false)]out SlonConnection? connection)
    {
        Debug.Assert(_dataSourceOrConnection is not null);
        if (_dataSourceOrConnection.GetType() == typeof(SlonDataSource))
        {
            dataSource = (SlonDataSource)_dataSourceOrConnection;
            connection = null;
            return true;
        }

        dataSource = null;
        connection = (SlonConnection)_dataSourceOrConnection;
        return false;
    }

    CommandFlowOptions CreateCommandFlowOptions(ReadOnlySpan<DbParameterCollection?> parametersSpan, CommandBehavior behavior, SlonConnection? connection = null, CommandTracker? tracker = null, PgConnection? pgConnection = null)
    {
        if (_commands.Count is 0)
            ThrowHelper.ThrowInvalidOperation("No commands were added to the batch.");

        // We also allow the 1 case for passing all parameters in one collection, mostly for SlonCommand implicit batching.
        var indexParameters = parametersSpan.Length == _commands.Count;
        if (!indexParameters && parametersSpan.Length is not (0 or 1))
            ThrowHelper.ThrowArgumentException(nameof(parametersSpan), "The number of parameter collections must match the number of commands.");

        var commands = _commands.AsSpan();
        var pendingPrefix = connection?.TakePendingTransactionStatement();
        var commandOffset = pendingPrefix is null ? 0 : 1;
        var commandCount = commands.Length + commandOffset;
        var commandArray = commandCount > 1 ? ArrayPool<Command>.Shared.Rent(commandCount) : null;
        if (pendingPrefix is not null)
            commandArray![0] = Command.Create(pendingPrefix);
        (Command Command, TrackerResult TrackerResult) result = default;
        Action<CommandResult, object?>? onResultAction = null;
        object? onResultActionState = null;
        (Action<CommandResult, object?>, object?)?[]? completionArray = null;
        // Tracks TrackedCommands that this batch's earlier commands have already issued Parse for
        // as the Winner. Subsequent same-TC commands in the same batch get the Present shape
        // (rely on the earlier in-batch Parse in the same Sync window).
        HashSet<TrackedCommand>? intraBatchWinners = null;
        try
        {
            for (var i = 0; i < commands.Length; i++)
            {
                ref var adoCommand = ref commands[i];
                TrackerContext trackerContext = default;
                if (connection is not null)
                {
                    trackerContext = _explicitlyPrepared && adoCommand.Tracked is null
                        ? TrackerContext.Create(connection, _fieldRef.Instance)
                        : TrackerContext.Create(connection, adoCommand.Tracked);
                }
                else if (tracker is not null)
                {
                    trackerContext = TrackerContext.Create(tracker, adoCommand.Tracked);
                }

                var parameters = indexParameters ? parametersSpan[i] : !parametersSpan.IsEmpty ? parametersSpan[0] : null;
                result = adoCommand.CreateCommand(_enableErrorBarriers, behavior, trackerContext, parameters, Timeout);
                // Refresh the cache when the tracker resolved to a different TC than the one we
                // passed in (catches workload-tracker recreation via DbDepsRevision++ and similar).
                if (result.TrackerResult.Tracked is not null && !ReferenceEquals(adoCommand.Tracked, result.TrackerResult.Tracked))
                    adoCommand.Tracked = result.TrackerResult.Tracked;

                // Per-command completion (presence-aware on the connection-bound path with a protocol
                // in scope, falls through to the legacy TrackerResult-driven completion otherwise).
                Action<CommandResult, object?>? thisCompletion;
                object? thisCompletionState;

                if (pgConnection is not null && result.TrackerResult.Tracked is { } tracked)
                {
                    // Same-SQL earlier in this batch already Winner'd → safe to ride its in-batch Parse.
                    var isIntraBatchWinner = intraBatchWinners?.Contains(tracked) ?? false;
                    var status = isIntraBatchWinner ? TrackedStatus.Tracked : pgConnection.GetTrackedStatus(tracked);

                    if (status is TrackedStatus.Tracked)
                    {
                        // A later same-SQL command in this batch rides the winner's earlier Parse. Its
                        // tracker cannot expose a prepared descriptor until that Parse is observed, so
                        // install the Bind/Execute shape here without claiming durable presence yet.
                        if (isIntraBatchWinner && !result.Command.Descriptor.IsPrepared)
                        {
                            var descriptor = result.Command.Descriptor;
                            result = (result.Command with
                            {
                                Descriptor = CommandDescriptor.CreatePrepared(
                                    tracked.CommandName, descriptor.ParameterTypes, rowDescription: null)
                            }, result.TrackerResult);
                        }
                        thisCompletion = null;
                        thisCompletionState = null;
                    }
                    else if (status is TrackedStatus.Preparing)
                    {
                        // Anonymous tag-along: another flow is preparing this name on this session.
                        // run our own anonymous Parse so we don't depend on its success.
                        var commandText = adoCommand.CommandText;
                        var paramTypes = result.Command.Descriptor.ParameterTypes;
                        result = (result.Command with { Descriptor = CommandDescriptor.Create(commandText, paramTypes, default) }, result.TrackerResult);
                        thisCompletion = null;
                        thisCompletionState = null;
                    }
                    else // None
                    {
                        if (pgConnection.TryBeginPreparing(tracked))
                        {
                            // Winner: descriptor becomes unprepared-with-name so WriteAuto emits
                            // Parse+Bind+Execute. Completion updates tracked status on the protocol.
                            var commandText = adoCommand.CommandText;
                            var paramTypes = result.Command.Descriptor.ParameterTypes;
                            result = (result.Command with { Descriptor = CommandDescriptor.Create(commandText, paramTypes, tracked.CommandName) }, result.TrackerResult);
                            thisCompletion = static (cmdResult, state) =>
                            {
                                var (p, t) = ((PgConnection, TrackedCommand))state!;
                                var metadata = cmdResult.GetMetadata();
                                // Here we would make RowDescription portable (once we need it).
                                if (metadata.IsPrepared)
                                {
                                    p.CompletePreparing(t, metadata.ToPreparedDescriptor());
                                }
                                else
                                {
                                    // Parse failed (or was skipped because a sibling command in this
                                    // Sync window errored earlier). Leave tracked at Initialized so
                                    // a future caller can re-attempt. Remove the Preparing marker so
                                    // they aren't blocked.
                                    p.RemoveTracked(t);
                                }
                            };
                            thisCompletionState = (pgConnection, tracked);
                            (intraBatchWinners ??= new()).Add(tracked);
                        }
                        else
                        {
                            // Lost the None → Preparing race: anonymous tag-along.
                            var commandText = adoCommand.CommandText;
                            var paramTypes = result.Command.Descriptor.ParameterTypes;
                            result = (result.Command with { Descriptor = CommandDescriptor.Create(commandText, paramTypes, default) }, result.TrackerResult);
                            thisCompletion = null;
                            thisCompletionState = null;
                        }
                    }
                }
                else
                {
                    // Data-source path placeholder, or no admission: no per-command completion.
                    // (The data-source orchestrator, when implemented, will use the same delegate-
                    // baked path as the connection-bound case here.)
                    thisCompletion = null;
                    thisCompletionState = null;
                }

                if (commandArray is null)
                {
                    if (thisCompletion is not null)
                    {
                        onResultAction = thisCompletion;
                        onResultActionState = thisCompletionState;
                    }
                }
                else
                {
                    if (thisCompletion is not null)
                    {
                        if (completionArray is null)
                        {
                            completionArray = new (Action<CommandResult, object?>, object?)?[commandCount];
                            if (onResultAction is not null)
                                completionArray[commandOffset] = (onResultAction, onResultActionState);

                            onResultAction = static (result, state) =>
                            {
                                var completions = ((Action<CommandResult, object?> Action, object? State)?[])state!;
                                if (completions[result.GetMetadata().CommandIndex] is { } completion)
                                    completion.Action(result, completion.State);
                            };
                            onResultActionState = completionArray;
                        }
                        completionArray[i + commandOffset] = (thisCompletion, thisCompletionState);
                    }

                    commandArray[i + commandOffset] = result.Command;
                }
            }

            return new()
            {
                OnCommandResultAction = onResultAction,
                OnCommandResultActionState = onResultActionState,
                OnCommandErrorAction = pgConnection is null
                    ? null
                    : static (descriptor, error, state) =>
                        ((PgConnection)state!).ReconcilePreparedError(descriptor, error.SqlState),
                OnCommandErrorActionState = pgConnection,
                Commands = commandArray is null ? new(result.Command) : new(commandArray, commandCount, isPooled: true),
                LeadingResultCount = commandOffset
            };
        }
        catch
        {
            if (commandArray is not null)
                ArrayPool<Command>.Shared.Return(commandArray, clearArray: true);
            if (pendingPrefix is not null)
                connection!.RestorePendingTransactionStatement(pendingPrefix);
            throw;
        }
    }

    CommandFlow Enqueue(DbParameterCollection? parameters, CommandBehavior behavior)
    {
        if (TryGetDataSource(out var dataSource, out var connection))
        {
            ThrowIfHasCloseConnection(behavior);
            _ = dataSource.GetCommandTracker(); // Ensures the data source and pool are initialized.
            // Pool selection happens after flow construction. Until preparation can be resolved
            // transactionally inside the selected-wire callback, this path must stay unprepared.
            var options = CreateCommandFlowOptions([parameters], behavior);
            return dataSource.EnqueueCommands(options);
        }

        return connection.Enqueue(
            static (pgConnection, args) =>
            {
                ref var core = ref args.FieldRef.Invoke();
                var options = core.CreateCommandFlowOptions([args.Parameters], args.Behavior, args.Connection, tracker: null, pgConnection);
                return new CommandFlow(async: false, options);
            },
            new EnqueueArgs<TCommand>(_fieldRef, parameters, behavior, connection),
            closeConnection: HasCloseConnection(behavior));
    }

    ValueTask<CommandFlow> EnqueueAsync(DbParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (TryGetDataSource(out var dataSource, out var connection))
            return DataSourceCore(_fieldRef, dataSource, parameters, behavior, cancellationToken);

        return connection.EnqueueAsync(
            static (pgConnection, args) =>
            {
                // pgConnection is the snapshot of the current PgConnection. Presence consultation
                // and three-shape baking happens here so the wire-shape decisions are atomic against
                // the connection the flow will run on.
                ref var core = ref args.FieldRef.Invoke();
                var options = core.CreateCommandFlowOptions([args.Parameters], args.Behavior, args.Connection, tracker: null, pgConnection);
                return new CommandFlow(async: true, options);
            },
            new EnqueueArgs<TCommand>(_fieldRef, parameters, behavior, connection),
            closeConnection: HasCloseConnection(behavior),
            cancellationToken);

        static async ValueTask<CommandFlow> DataSourceCore(FieldRef<AdoBatchCore<TCommand>> fieldRef, SlonDataSource dataSource, DbParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken)
        {
            fieldRef.Invoke().ThrowIfHasCloseConnection(behavior);
            await dataSource.GetCommandTrackerAsync(cancellationToken).ConfigureAwait(false);
            var options = fieldRef.Invoke().CreateCommandFlowOptions([parameters], behavior);
            return await dataSource.EnqueueCommandsAsync(options, cancellationToken).ConfigureAwait(false);
        }
    }

    public void Prepare(DbParameterCollection? parameters)
    {
        ThrowIfDisposedOrReadOnly();
        // TODO begin tracking the protocol on flow Bind, once we have it we can also queue close flows correctly for datasource commands on subsequent command errors.
        // For now we only support explicit preparation on connections as we don't yet track the protocol from a flow, so we can't recover from errors.
        if (TryGetDataSource(out _, out var connection))
            ThrowHelper.ThrowInvalidOperation("Explicit preparation is not supported for DbDataSource commands.");

        _explicitlyPrepared = true;
        CommandFlow.Enumerator enumerator = default;
        List<(int, Exception)>? exceptions = null;
        try
        {
            var flow = Enqueue(parameters, CommandBehavior.SchemaOnly);
            enumerator = flow.GetEnumerator();
            for (var i = 0; i < flow.LeadingResultCount; i++)
            {
                if (!enumerator.MoveNext())
                    ThrowHelper.ThrowUnexpected("The flow returned fewer infrastructure results than expected.");
                foreach (var _ in enumerator.Current) { }
                enumerator.Current.GetCommandComplete();
            }
            var span = _commands.AsSpan();
            for (var i = 0; i < span.Length; i++)
            {
                if (!enumerator.MoveNext())
                    ThrowHelper.ThrowUnexpected("Not enough results returned.");
                var result = enumerator.Current;
                try
                {
                    if (result.HasRows)
                        ThrowHelper.ThrowUnexpected("Rows were returned?");
                }
                catch (Exception ex)
                {
                    exceptions ??= new();
                    exceptions.Add((i, ex));
                }
            }

            if (exceptions is not null)
                throw new AggregateException(SelectException(exceptions));
        }
        catch (Exception)
        {
            _explicitlyPrepared = false;
            // TODO execute a Close flow for all indices that are not in the exceptions list.
            throw;
        }
        finally
        {
            enumerator.Dispose();
        }

        IEnumerable<Exception> SelectException(List<(int, Exception)> exceptions)
        {
            foreach (var (index, ex) in exceptions)
                yield return ex;
        }
    }

    // Async instance methods on structs make a copy of 'this', we don't want that so we define a static method passing in our field ref.
    static async ValueTask PrepareAsyncCore(FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters, CancellationToken cancellationToken)
    {
        ref var thisRef = ref fieldRef.Invoke();
        thisRef.ThrowIfDisposedOrReadOnly();

        // TODO begin tracking the protocol on flow Bind, once we have it we can also queue close flows correctly for datasource commands on subsequent command errors.
        // For now we only support explicit preparation on connections as we don't yet track the protocol from a flow, so we can't recover from errors.
        if (thisRef.TryGetDataSource(out _, out var connection))
            ThrowHelper.ThrowInvalidOperation("Explicit preparation is not supported for DbDataSource commands.");

        thisRef._explicitlyPrepared = true;
        CommandFlow.Enumerator enumerator = default;
        List<(int, Exception)>? exceptions = null;
        try
        {
            var flow = await thisRef.EnqueueAsync(parameters, CommandBehavior.SchemaOnly, cancellationToken).ConfigureAwait(false);
            enumerator = flow.GetAsyncEnumerator(cancellationToken);
            for (var i = 0; i < flow.LeadingResultCount; i++)
            {
                if (!await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    ThrowHelper.ThrowUnexpected("The flow returned fewer infrastructure results than expected.");
                var rows = enumerator.Current.GetAsyncEnumerator(cancellationToken);
                try
                {
                    while (await rows.MoveNextAsync().ConfigureAwait(false)) { }
                }
                finally
                {
                    await rows.DisposeAsync().ConfigureAwait(false);
                }
                enumerator.Current.GetCommandComplete();
            }
            for (var i = 0; i < fieldRef.Invoke()._commands.AsSpan().Length; i++)
            {
                try
                {
                    // TODO we should check whether a prepared parse gets rolled back/closed if a subsequent bind or describe fails, if so we don't need to close manually.
                    if (!await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                        ThrowHelper.ThrowUnexpected("Not enough results returned.");
                    var result = enumerator.Current;
                    if (result.HasRows)
                        ThrowHelper.ThrowUnexpected("Rows were returned?");
                }
                catch (Exception ex)
                {
                    exceptions ??= new();
                    exceptions.Add((i, ex));
                }
            }

            if (exceptions is not null)
                throw new AggregateException(SelectException(exceptions));
        }
        catch (Exception)
        {
            fieldRef.Invoke()._explicitlyPrepared = false;
            // TODO execute a Close flow as well.
            throw;
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }

        IEnumerable<Exception> SelectException(List<(int, Exception)> exceptions)
        {
            foreach (var (index, ex) in exceptions)
                yield return ex;
        }
    }

    public ValueTask PrepareAsync(DbParameterCollection? parameters, CancellationToken cancellationToken = default)
        => PrepareAsyncCore(_fieldRef, parameters, cancellationToken);

    public int ExecuteNonQuery(DbParameterCollection? parameters)
    {
        ThrowIfDisposed();
        var recordsAffected = 0L;
        foreach (var result in Enqueue(parameters, CommandBehavior.Default))
        {
            // Drive the result to its CommandComplete so RecordsAffected is populated (we discard any
            // rows - this is ExecuteNonQuery). Only data-modifying statements contribute a non-zero count.
            // RecordsAffected throws a PostgresException on a failed command, so the error surfaces here
            // instead of silently reporting 0 affected.
            foreach (var _ in result) { }
            recordsAffected = checked(recordsAffected + result.RecordsAffected);
        }
        return checked((int)recordsAffected);
    }

    static async ValueTask<int> ExecuteNonQueryAsyncCore(FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters, CancellationToken cancellationToken)
    {
        ref var thisRef = ref fieldRef.Invoke();
        thisRef.ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        CommandFlow.Enumerator enumerator = default;
        try
        {
            enumerator = (await thisRef.EnqueueAsync(parameters, CommandBehavior.Default, cancellationToken).ConfigureAwait(false)).GetAsyncEnumerator(cancellationToken);
            var recordsAffected = await enumerator.ConsumeNonQueryAsync().ConfigureAwait(false);
            return checked((int)recordsAffected);
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    public ValueTask<int> ExecuteNonQueryAsync(DbParameterCollection? parameters, CancellationToken cancellationToken = default)
        => ExecuteNonQueryAsyncCore(_fieldRef, parameters, cancellationToken);

    public object? ExecuteScalar(DbParameterCollection? parameters)
    {
        ThrowIfDisposed();
        foreach (var result in Enqueue(parameters, CommandBehavior.Default))
        {
            using var rowEnumerator = result.GetAsyncEnumerator();
            if (rowEnumerator.MoveNext())
                return result.FieldCount is not 0 ? rowEnumerator.Current.GetValue<object>(0) : null;
            // No row from this result: surface a failed command (stored ErrorResponse) instead of
            // silently returning null.
            result.GetCommandComplete();
        }
        return null;
    }

    static async ValueTask<object?> ExecuteScalarAsyncCore(FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters, CancellationToken cancellationToken)
    {
        ref var thisRef = ref fieldRef.Invoke();
        thisRef.ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        CommandFlow.Enumerator enumerator = default;
        try
        {
            enumerator = (await thisRef.EnqueueAsync(parameters, CommandBehavior.Default, cancellationToken).ConfigureAwait(false)).GetAsyncEnumerator(cancellationToken);
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                var rowEnumerator = enumerator.Current.GetAsyncEnumerator(cancellationToken);
                try
                {
                    if (await rowEnumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                        return enumerator.Current.FieldCount is not 0
                            ? await rowEnumerator.Current.GetValueAsync<object>(0, cancellationToken).ConfigureAwait(false)
                            : null;
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
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    public ValueTask<object?> ExecuteScalarAsync(DbParameterCollection? parameters, CancellationToken cancellationToken = default)
        => ExecuteScalarAsyncCore(_fieldRef, parameters, cancellationToken);

    public SlonDataReader ExecuteReader(DbParameterCollection? parameters, CommandBehavior behavior)
    {
        ThrowIfDisposed();
        return SlonDataReader.Create(behavior, Enqueue(parameters, behavior));
    }

    public ValueTask<DbDataReader> ExecuteDbReaderAsync(DbParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return ValueTask.FromException<DbDataReader>(new ObjectDisposedException(_fieldRef.Instance.GetType().Name));
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<DbDataReader>(cancellationToken);

        return SlonDataReader.CreateAsync<DbDataReader>(behavior, EnqueueAsync(parameters, behavior, cancellationToken), cancellationToken);
    }

    public ValueTask<SlonDataReader> ExecuteReaderAsync(DbParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return ValueTask.FromException<SlonDataReader>(new ObjectDisposedException(_fieldRef.Instance.GetType().Name));
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<SlonDataReader>(cancellationToken);

        return SlonDataReader.CreateAsync<SlonDataReader>(behavior, EnqueueAsync(parameters, behavior, cancellationToken), cancellationToken);
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
        if (_disposed || !_explicitlyPrepared)
            return;

        _disposed = true;
        if (TryGetDataSource(out _, out var connection))
        {
            // TODO once we support explicit prepare for datasource commands, unprepare here.
            return;
        }

        ReleaseOwned(connection);
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed || !_explicitlyPrepared)
            return new();

        _disposed = true;
        if (TryGetDataSource(out _, out var connection))
        {
            // TODO once we support explicit prepare for datasource commands, unprepare here.
            return new();
        }

        return ReleaseOwnedAsync(connection);
    }

    // TODO what to do with concurrent callers, datasource commands etc.?
    public void Cancel()
    {
        // We can't throw in connectionless scenarios as dapper etc expect this method to work.
        if (TryGetDataSource(out _, out var connection))
            return;

        connection.PerformUserCancellation();
    }
}
