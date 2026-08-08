using System.Buffers;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Serialization;
using Slon.Runtime.CompilerServices;

namespace Slon;

sealed class SlonCommandFlowObserver : CommandFlowObserver
{
    internal static readonly SlonCommandFlowObserver Instance = new();

    internal override void OnStarted(CommandFlow flow, object? state)
        => ((SlonCommand)state!).OnFlowStarted(flow);

    internal override void OnCommandResult(CommandFlow flow, CommandResult result, object? state)
        => ((SlonCommand)state!).OnCommandResult(flow, result);

    internal override void OnCompleting(PgClientFlow flow, Exception? exception, object? state)
        => ((SlonCommand)state!).OnFlowCompleting((CommandFlow)flow, exception);
}

sealed class SlonBatchFlowObserver : CommandFlowObserver
{
    internal static readonly SlonBatchFlowObserver Instance = new();

    internal override void OnStarted(CommandFlow flow, object? state)
        => ((SlonBatch)state!).OnFlowStarted(flow);

    internal override void OnCommandResult(CommandFlow flow, CommandResult result, object? state)
        => ((SlonBatch)state!).OnCommandResult(flow, result);

    internal override void OnCompleting(PgClientFlow flow, Exception? exception, object? state)
        => ((SlonBatch)state!).OnFlowCompleting((CommandFlow)flow, exception);
}

readonly struct EnqueueArgs<TCommand> where TCommand : IAdoCommand
{
    public readonly FieldRef<AdoBatchCore<TCommand>> FieldRef;
    public readonly DbParameterCollection? Parameters;
    public readonly CommandBehavior Behavior;
    public readonly SlonConnection Connection;
    public readonly PgSerializerOptions SerializerOptions;
    public readonly ParameterWriterStrategy ParameterWriterStrategy;

    public EnqueueArgs(FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters,
        CommandBehavior behavior, SlonConnection connection, PgSerializerOptions serializerOptions,
        ParameterWriterStrategy parameterWriterStrategy)
    {
        FieldRef = fieldRef;
        Parameters = parameters;
        Behavior = behavior;
        Connection = connection;
        SerializerOptions = serializerOptions;
        ParameterWriterStrategy = parameterWriterStrategy;
    }
}

readonly struct MultiplexedEnqueueArgs<TCommand> where TCommand : IAdoCommand
{
    public readonly FieldRef<AdoBatchCore<TCommand>> FieldRef;
    public readonly DbParameterCollection? Parameters;
    public readonly CommandBehavior Behavior;
    public readonly CommandTracker Tracker;
    public readonly PgSerializerOptions SerializerOptions;
    public readonly ParameterWriterStrategy ParameterWriterStrategy;

    public MultiplexedEnqueueArgs(FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters,
        CommandBehavior behavior, CommandTracker tracker, PgSerializerOptions serializerOptions,
        ParameterWriterStrategy parameterWriterStrategy)
    {
        FieldRef = fieldRef;
        Parameters = parameters;
        Behavior = behavior;
        Tracker = tracker;
        SerializerOptions = serializerOptions;
        ParameterWriterStrategy = parameterWriterStrategy;
    }
}

sealed class PreparationClaims(PgConnection connection) : HashSet<TrackedCommand>
{
    public void Rollback()
    {
        foreach (var tracked in this)
            connection.RemoveTracked(tracked);
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
    CommandFlow? _activeFlow;
    Action<CommandResult, object?>? _onCommandResultAction;
    object? _onCommandResultActionState;
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
        if (oldConnection is not null)
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

    CommandFlowOptions CreateCommandFlowOptions(ReadOnlySpan<DbParameterCollection?> parametersSpan,
        CommandBehavior behavior, SlonConnection? connection = null, CommandTracker? tracker = null,
        PgConnection? pgConnection = null, PgSerializerOptions? serializerOptions = null,
        ParameterWriterStrategy? parameterWriterStrategy = null)
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
            commandArray![0] = Command.Create(pendingPrefix) with { SuppressEnumeration = true };
        (Command Command, TrackerResult TrackerResult) result = default;
        Action<CommandResult, object?>? onResultAction = null;
        object? onResultActionState = null;
        (Action<CommandResult, object?>, object?)?[]? resultActions = null;
        // Tracks TrackedCommands that this batch's earlier commands have already issued Parse for
        // as the Winner. Subsequent same-TC commands in the same batch get the Present shape
        // (rely on the earlier in-batch Parse in the same Sync window).
        PreparationClaims? preparationClaims = null;
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
                var conversionContext = pgConnection is null ? null : new PgConversionContext
                {
                    TextEncoding = pgConnection.Protocol.FlowControl.ClientEncoding
                };
                result = adoCommand.CreateCommand(_enableErrorBarriers, behavior, trackerContext, parameters,
                    Timeout, serializerOptions, conversionContext);
                // Refresh the cache when the tracker resolved to a different TC than the one we
                // passed in (catches workload-tracker recreation via DbDepsRevision++ and similar).
                if (result.TrackerResult.Tracked is not null && !ReferenceEquals(adoCommand.Tracked, result.TrackerResult.Tracked))
                    adoCommand.Tracked = result.TrackerResult.Tracked;

                // Per-command result observation (presence-aware on the connection-bound path with a protocol
                // in scope, falls through to the legacy TrackerResult-driven completion otherwise).
                Action<CommandResult, object?>? thisResultAction;
                object? thisResultActionState;

                if (pgConnection is not null && result.TrackerResult.Tracked is { } tracked)
                {
                    // Same-SQL earlier in this batch already Winner'd → safe to ride its in-batch Parse.
                    var isIntraBatchWinner = preparationClaims?.Contains(tracked) ?? false;
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
                        thisResultAction = static (result, state) => AttachPreparedTerminalObserver(result, (PgConnection)state!);
                        thisResultActionState = pgConnection;
                    }
                    else if (status is TrackedStatus.Preparing)
                    {
                        // Anonymous tag-along: another flow is preparing this name on this session.
                        // run our own anonymous Parse so we don't depend on its success.
                        var commandText = adoCommand.CommandText;
                        var paramTypes = result.Command.Descriptor.ParameterTypes;
                        result = (result.Command with { Descriptor = CommandDescriptor.Create(commandText, paramTypes, default) }, result.TrackerResult);
                        thisResultAction = null;
                        thisResultActionState = null;
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
                            thisResultAction = static (cmdResult, state) =>
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

                                if (metadata.IsPrepared)
                                    AttachPreparedTerminalObserver(cmdResult, p);
                            };
                            thisResultActionState = (pgConnection, tracked);
                            (preparationClaims ??= new(pgConnection)).Add(tracked);
                        }
                        else
                        {
                            // Lost the None → Preparing race: anonymous tag-along.
                            var commandText = adoCommand.CommandText;
                            var paramTypes = result.Command.Descriptor.ParameterTypes;
                            result = (result.Command with { Descriptor = CommandDescriptor.Create(commandText, paramTypes, default) }, result.TrackerResult);
                            thisResultAction = null;
                            thisResultActionState = null;
                        }
                    }
                }
                else
                {
                    // Data-source path placeholder, or no admission: no per-command completion.
                    // (The data-source orchestrator, when implemented, will use the same delegate-
                    // baked path as the connection-bound case here.)
                    thisResultAction = null;
                    thisResultActionState = null;
                }

                if (commandArray is null)
                {
                    if (thisResultAction is not null)
                    {
                        onResultAction = thisResultAction;
                        onResultActionState = thisResultActionState;
                    }
                }
                else
                {
                    if (thisResultAction is not null)
                    {
                        if (resultActions is null)
                        {
                            resultActions = new (Action<CommandResult, object?>, object?)?[commandCount];
                            if (onResultAction is not null)
                                resultActions[commandOffset] = (onResultAction, onResultActionState);

                            onResultAction = static (result, state) =>
                            {
                                var actions = ((Action<CommandResult, object?> Action, object? State)?[])state!;
                                if (actions[result.GetMetadata().CommandIndex] is { } action)
                                    action.Action(result, action.State);
                            };
                            onResultActionState = resultActions;
                        }
                        resultActions[i + commandOffset] = (thisResultAction, thisResultActionState);
                    }

                    commandArray[i + commandOffset] = result.Command;
                }
            }

            _onCommandResultAction = onResultAction;
            _onCommandResultActionState = onResultActionState;
            var observerState = _fieldRef.Instance;
            var observer = observerState switch
            {
                SlonCommand => (CommandFlowObserver)SlonCommandFlowObserver.Instance,
                SlonBatch => SlonBatchFlowObserver.Instance,
                _ => throw new NotSupportedException($"Unsupported ADO command owner {observerState.GetType()}.")
            };
            return new()
            {
                Observer = observer,
                ObserverState = observerState,
                Commands = commandArray is null ? new(result.Command) : new(commandArray, commandCount, isPooled: true),
                SerializerOptions = serializerOptions ?? connection?.DbDataSource.GetDbDependencies().SerializerOptions,
                ParameterWriterStrategy = parameterWriterStrategy ?? SerializerParameterWriterStrategy.Instance
            };
        }
        catch
        {
            preparationClaims?.Rollback();
            if (commandArray is not null)
                ArrayPool<Command>.Shared.Return(commandArray, clearArray: true);
            if (pendingPrefix is not null)
                connection!.RestorePendingTransactionStatement(pendingPrefix);
            throw;
        }
    }

    static void AttachPreparedTerminalObserver(CommandResult result, PgConnection connection)
        => result.OnCompleted(static (completed, state) =>
        {
            var connection = (PgConnection)state!;
            try
            {
                if (completed.Error is not { } error)
                    return;

                var metadata = completed.GetMetadata();
                if (metadata.IsPrepared)
                    connection.ReconcilePreparedError(metadata.ToPreparedDescriptor(), error.SqlState);
            }
            catch (Exception ex)
            {
                connection.ReportUnobservedCallback(ex, "a command-result observer");
            }
        }, connection);

    CommandFlow Enqueue(DbParameterCollection? parameters, CommandBehavior behavior)
    {
        if (TryGetDataSource(out var dataSource, out var connection))
        {
            ThrowIfHasCloseConnection(behavior);
            var dependencies = dataSource.GetDbDependencies();
            return dataSource.EnqueueCommands(
                static (pgConnection, args) => args.FieldRef.Invoke().CreateCommandFlowOptions(
                    [args.Parameters], args.Behavior, tracker: args.Tracker, pgConnection: pgConnection,
                    serializerOptions: args.SerializerOptions,
                    parameterWriterStrategy: args.ParameterWriterStrategy),
                new MultiplexedEnqueueArgs<TCommand>(_fieldRef, parameters, behavior,
                    dependencies.CommandsTracker, dependencies.SerializerOptions,
                    dependencies.ParameterWriterStrategy));
        }

        connection ??= ThrowConnectionNotInitialized();
        var connectionDependencies = connection.DbDataSource.GetDbDependencies();
        return connection.Enqueue(
            static (pgConnection, args) =>
            {
                ref var core = ref args.FieldRef.Invoke();
                var options = core.CreateCommandFlowOptions([args.Parameters], args.Behavior,
                    args.Connection, tracker: null, pgConnection, args.SerializerOptions,
                    args.ParameterWriterStrategy);
                return new CommandFlow(async: false, options);
            },
            new EnqueueArgs<TCommand>(_fieldRef, parameters, behavior, connection,
                connectionDependencies.SerializerOptions, connectionDependencies.ParameterWriterStrategy),
            closeConnection: HasCloseConnection(behavior));
    }

    ValueTask<CommandFlow> EnqueueAsync(DbParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (TryGetDataSource(out var dataSource, out var connection))
            return DataSourceCore(_fieldRef, dataSource, parameters, behavior, cancellationToken);

        connection ??= ThrowConnectionNotInitialized();
        var connectionDependencies = connection.DbDataSource.GetDbDependencies();
        return connection.EnqueueAsync(
            static (pgConnection, args) =>
            {
                // pgConnection is the snapshot of the current PgConnection. Presence consultation
                // and three-shape baking happens here so the wire-shape decisions are atomic against
                // the connection the flow will run on.
                ref var core = ref args.FieldRef.Invoke();
                var options = core.CreateCommandFlowOptions([args.Parameters], args.Behavior,
                    args.Connection, tracker: null, pgConnection, args.SerializerOptions,
                    args.ParameterWriterStrategy);
                return new CommandFlow(async: true, options);
            },
            new EnqueueArgs<TCommand>(_fieldRef, parameters, behavior, connection,
                connectionDependencies.SerializerOptions, connectionDependencies.ParameterWriterStrategy),
            closeConnection: HasCloseConnection(behavior),
            cancellationToken);

        static async ValueTask<CommandFlow> DataSourceCore(FieldRef<AdoBatchCore<TCommand>> fieldRef, SlonDataSource dataSource, DbParameterCollection? parameters, CommandBehavior behavior, CancellationToken cancellationToken)
        {
            fieldRef.Invoke().ThrowIfHasCloseConnection(behavior);
            var dependencies = await dataSource.GetDbDependenciesAsync(cancellationToken).ConfigureAwait(false);
            return await dataSource.EnqueueCommandsAsync(
                static (pgConnection, args) => args.FieldRef.Invoke().CreateCommandFlowOptions(
                    [args.Parameters], args.Behavior, tracker: args.Tracker, pgConnection: pgConnection,
                    serializerOptions: args.SerializerOptions,
                    parameterWriterStrategy: args.ParameterWriterStrategy),
                new MultiplexedEnqueueArgs<TCommand>(fieldRef, parameters, behavior,
                    dependencies.CommandsTracker, dependencies.SerializerOptions,
                    dependencies.ParameterWriterStrategy),
                cancellationToken).ConfigureAwait(false);
        }
    }

    [DoesNotReturn]
    static SlonConnection ThrowConnectionNotInitialized()
        => throw new InvalidOperationException("The command has no connection or data source.");

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
            if (!_explicitlyPrepared)
                connection!.CloseOwned(_fieldRef.Instance);
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
            if (!fieldRef.Invoke()._explicitlyPrepared)
                await connection!.CloseOwnedAsync(fieldRef.Instance).ConfigureAwait(false);
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
        using var activity = StartActivity();
        try { return ExecuteNonQueryCore(parameters); }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default;
        }
    }

    int ExecuteNonQueryCore(DbParameterCollection? parameters)
    {
        var recordsAffected = 0L;
        foreach (var result in Enqueue(parameters, CommandBehavior.Default))
        {
            // Drive the result to its CommandComplete so RecordsAffected is populated (we discard any
            // rows - this is ExecuteNonQuery). Only data-modifying statements contribute a non-zero count.
            // RecordsAffected throws a PgErrorException on a failed command, so the error surfaces here
            // instead of silently reporting 0 affected.
            foreach (var _ in result) { }
            recordsAffected = checked(recordsAffected + result.RecordsAffected);
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
            var flow = await thisRef.EnqueueAsync(parameters, CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
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
        try { return ExecuteScalarCore(parameters); }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default;
        }
    }

    object? ExecuteScalarCore(DbParameterCollection? parameters)
    {
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
        using var activity = thisRef.StartActivity();
        CommandFlow.Enumerator enumerator = default;
        try
        {
            thisRef.ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
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
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default;
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
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
        try { return ExecuteReaderCore(parameters, behavior); }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default;
        }
    }

    SlonDataReader ExecuteReaderCore(DbParameterCollection? parameters, CommandBehavior behavior)
        => SlonDataReader.Create(behavior, Enqueue(parameters, behavior));

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

    static async ValueTask<TReader> ExecuteReaderAsyncCore<TReader>(
        FieldRef<AdoBatchCore<TCommand>> fieldRef, DbParameterCollection? parameters,
        CommandBehavior behavior, CancellationToken cancellationToken)
        where TReader : DbDataReader
    {
        ref var core = ref fieldRef.Invoke();
        using var activity = core.StartActivity();
        try
        {
            return await SlonDataReader.CreateAsync<TReader>(behavior,
                core.EnqueueAsync(parameters, behavior, cancellationToken), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
            return default!;
        }
    }

    Activity? StartActivity()
    {
        TryGetDataSource(out var dataSource, out var connection);
        return SlonTracing.Start(dataSource ?? connection!.DbDataSource, _commands.Count);
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

        if (connection is not null)
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

        return connection is null ? default : ReleaseOwnedAsync(connection);
    }

    public void Cancel()
    {
        Volatile.Read(ref _activeFlow)?.CancelAsync().GetAwaiter().GetResult();
    }

    public Task CancelAsync(CancellationToken cancellationToken = default)
    {
        var flow = Volatile.Read(ref _activeFlow);
        if (flow is null)
            return Task.CompletedTask;
        var task = flow.CancelAsync();
        return cancellationToken.CanBeCanceled ? task.WaitAsync(cancellationToken) : task;
    }

    internal void OnFlowStarted(CommandFlow flow) => Volatile.Write(ref _activeFlow, flow);

    internal void OnCommandResult(CommandFlow flow, CommandResult result)
    {
        if (ReferenceEquals(Volatile.Read(ref _activeFlow), flow))
            _onCommandResultAction?.Invoke(result, _onCommandResultActionState);
    }

    internal void OnFlowCompleting(CommandFlow flow, Exception? exception)
    {
        // A flow-level fault while holding an ADO connection lease breaks that lease. SQL errors don't
        // reach here: they surface on CommandResult and the flow completes cleanly. OnCompleting runs
        // before terminal publication, so an awaiter observing the fault also observes Broken.
        if (exception is not null && !TryGetDataSource(out _, out var connection) && connection is not null)
            ((IAdoConnection)connection).Break(exception);

        if (!ReferenceEquals(Interlocked.CompareExchange(ref _activeFlow, null, flow), flow))
            return;

        _onCommandResultAction = null;
        _onCommandResultActionState = null;
    }
}
