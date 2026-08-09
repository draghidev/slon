using System.Runtime.CompilerServices;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon;

interface IAdoConnection
{
    void Break(Exception exception);
}

// Proxy allows us to decouple the actual connection used for database work from the ado connection itself.
// For example allows us to enable seamless reconnects or offer apis for executing a set of commands across a guaranteed set of distinct connections.
sealed class AdoConnectionProxy : IDisposable, IAsyncDisposable
{
    readonly SlonDataSource? _dataSource;
    readonly CommandTracker _tracker;
    readonly PgConnection _pgConnection;
    readonly IAdoConnection _connection;

    CommandFlow? _cachedFlow;
    // A SlonConnection holds an exclusive scope for its whole lease (acquired at Open), so its commands run
    // serially on one wire instead of multiplexed - the safe default, since Slon can't parse SQL to know
    // which commands carry session state (SET / LISTEN / temp tables / BEGIN...). Null on the data-source
    // path (transient per-command proxy, which never Opens, so it stays multiplexed). Routing keys on this
    // being non-null, so the connection/data-source split needs no separate flag.
    ExclusiveScopeLease? _exclusiveFlow;

    internal AdoConnectionProxy(SlonDataSource dataSource, PgConnection pgConnection, IAdoConnection connection)
    {
        _dataSource = dataSource;
        _connection = connection;
        _pgConnection = pgConnection;
        // Auto-prepare uses the workload-scope tracker directly. Explicit-prepare bookkeeping
        // lives on SlonConnection (per Policy A, survives Close-Open). PgConnection ↔ tracker
        // registration happens at PgConnection construction (in the factory), not here. Proxy
        // is per-lease, PgConnection-tracker binding is per-session.
        _tracker = dataSource.GetCommandTracker(initializedOnly: true);
    }

    internal AdoConnectionProxy(PgConnection pgConnection, IAdoConnection connection, bool autoPrepare, CommandTracker? tracker = null)
    {
        const int MaxAuto = 100;
        const int AutoMinimumUses = 5;

        _connection = connection;
        _tracker = new(autoPrepare ? MaxAuto : 0, AutoMinimumUses, parent: tracker);
        _pgConnection = pgConnection;
    }

    internal PgClientFlow? CurrentReadingFlow { get; set; }
    internal PgClientFlow? CurrentWritingFlow { get; set; }

    internal CommandTracker Tracker => _tracker;
    internal PgConnection PgConnection => _pgConnection;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TrackerResult TrackCommand(in CommandDescriptor descriptor, TrackedCommand? tracked = null, object? owningInstance = null)
        => _tracker.Track(descriptor, tracked, owningInstance);

    public CommandFlow RentCommandFlow(bool async, in CommandFlowOptions options)
    {
        return new CommandFlow(async, options);
        // Re-enabling this (and ReturnCommandFlow) pools CommandFlow, which arms the activation timeout.
        // PgClientFlow.Reset throws on that combination until generation-checked completion lands (the
        // wrong-tenure heartbeat hazard).
        // return Interlocked.Exchange(ref _cachedFlow, null) ?? new();
    }

    public void ReturnCommandFlow(CommandFlow flow)
    {
        flow.Reset();
        // We don't care about the race here.
        _ = Interlocked.CompareExchange(ref _cachedFlow, flow, null);
    }

    public void Enqueue(PgClientFlow flow)
    {
        if (!TryQueue(flow))
        {
            flow.DiscardUnqueued();
            ThrowHelper.ThrowInvalidOperation("Could not enqueue flow.");
        }
    }

    // Sync delegate-based enqueue, sibling to the async variant: snapshots the current PgConnection
    // so the flowFactory callback's decisions (presence consultation, descriptor baking) and the queue
    // operation share a single atomic reference.
    public TFlow Enqueue<TArg, TFlow>(Func<PgConnection, TArg, TFlow> flowFactory, TArg arg)
        where TFlow : PgClientFlow
        where TArg : allows ref struct
    {
        var connection = _pgConnection;
        var flow = flowFactory(connection, arg);
        if (!TryQueueOn(connection, flow))
        {
            flow.DiscardUnqueued();
            ThrowHelper.ThrowInvalidOperation("Could not enqueue flow.");
        }
        return flow;
    }

    // Returns the given flow to allow an async caller to directly return this task.
    public ValueTask<TFlow> EnqueueAsync<TFlow>(TFlow flow, CancellationToken cancellationToken) where TFlow : PgClientFlow
    {
        if (!TryQueue(flow))
        {
            flow.DiscardUnqueued();
            ThrowHelper.ThrowInvalidOperation("Could not enqueue flow.");
        }

        return new(flow);
    }

    // Delegate-based enqueue: snapshots the current PgConnection into a local for the duration of
    // the flowFactory callback so the flow's wire-shape decisions (presence consultation, TryBeginPreparing,
    // descriptor baking) happen atomically against the connection the flow will run on. Caller passes
    // state through `arg` to avoid closure allocation.
    public ValueTask<TFlow> EnqueueAsync<TArg, TFlow>(
        Func<PgConnection, TArg, TFlow> flowFactory,
        TArg arg,
        CancellationToken cancellationToken)
        where TFlow : PgClientFlow
        where TArg : allows ref struct
    {
        var connection = _pgConnection;
        var flow = flowFactory(connection, arg);
        if (!TryQueueOn(connection, flow))
        {
            flow.DiscardUnqueued();
            ThrowHelper.ThrowInvalidOperation("Could not enqueue flow.");
        }
        return new(flow);
    }

    bool TryQueue(PgClientFlow flow) => TryQueueOn(_pgConnection, flow);

    bool TryQueueOn(PgConnection connection, PgClientFlow flow)
    {
        // A SlonConnection holds an exclusive scope for its lease: route the command as a subflow into the
        // held scope's inner pipeline (serial on this one wire) instead of onto the multiplexed protocol
        // pipeline. The data-source path never acquires a scope, so it falls through to the direct enqueue.
        if (_exclusiveFlow is { } scope)
        {
            scope.Queue(flow);
            return true;
        }
        if (!connection.TryQueue(flow))
            return false;
        return true;
    }

    public void PerformUserCancellation(TimeSpan? timeout = null)
    {
        // TODO spin up a connection and write out cancel
    }

    public bool InExclusiveScope => _exclusiveFlow is not null;

    // Acquire the connection's exclusive scope at Open and hold it for the whole lease. The scope flow's
    // mode matches the caller: a sync acquire (async: false) is driven to activation by THIS thread via the
    // source handoff (WaitForExecutor), so the caller drives the scope + its subflows end-to-end on one
    // thread; an async acquire is executor-driven. From here every command on this proxy routes as a subflow.
    public void AcquireExclusiveScope(bool longRunning = false)
    {
        _exclusiveFlow = _pgConnection.Protocol.BeginExclusiveScope(longRunning);
    }

    public async ValueTask AcquireExclusiveScopeAsync(CancellationToken cancellationToken = default,
        bool longRunning = false)
    {
        _exclusiveFlow = await _pgConnection.Protocol.BeginExclusiveScopeAsync(
            cancellationToken, longRunning).ConfigureAwait(false);
    }

    internal bool TryQueueExclusiveScope(bool async, bool longRunning, bool mustPipeline)
    {
        var options = (longRunning ? FlowEnqueueOptions.BlockAdmission : FlowEnqueueOptions.None) |
            (mustPipeline ? FlowEnqueueOptions.RequireExistingPipeline : FlowEnqueueOptions.None);
        if (!_pgConnection.Protocol.TryQueueExclusiveScope(
                async, options, out var flow))
            return false;
        _exclusiveFlow = flow;
        return true;
    }

    internal void WaitForExclusiveScope()
        => _exclusiveFlow!.WaitForHandoffAsync(CancellationToken.None).GetAwaiter().GetResult();

    internal ValueTask WaitForExclusiveScopeAsync(CancellationToken cancellationToken)
        => new(_exclusiveFlow!.WaitForHandoffAsync(cancellationToken));

    public void EndExclusiveScope()
    {
        if (_exclusiveFlow is { } flow)
        {
            _exclusiveFlow = null;
            flow.CompleteScopeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    public async ValueTask EndExclusiveScopeAsync()
    {
        if (_exclusiveFlow is { } flow)
        {
            _exclusiveFlow = null;
            await flow.CompleteScopeAsync().ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        // PgConnection survives the lease, it's the pool unit. Tracker deregister happens on
        // PgConnection.CompleteAsync (true session end).
    }

    public ValueTask DisposeAsync()
    {
        return new();
    }
}
