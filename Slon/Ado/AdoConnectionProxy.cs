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
    PgConnection _pgConnection = null!;
    readonly IAdoConnection _connection;

    CommandFlow? _cachedFlow;
    // A SlonConnection holds an exclusive scope for its whole lease (acquired at Open), so its commands run
    // serially on one wire instead of multiplexed - the safe default, since Slon can't parse SQL to know
    // which commands carry session state (SET / LISTEN / temp tables / BEGIN...). Null on the data-source
    // path (transient per-command proxy, which never Opens, so it stays multiplexed). Routing keys on this
    // being non-null, so the connection/data-source split needs no separate flag.
    ExclusiveScopeLease? _exclusiveScope;

    internal AdoConnectionProxy(SlonDataSource dataSource, IAdoConnection connection)
    {
        _dataSource = dataSource;
        _connection = connection;
        // Auto-prepare uses the workload-scope tracker directly. Explicit-prepare bookkeeping
        // lives on SlonConnection (per Policy A, survives Close-Open). PgConnection ↔ tracker
        // registration happens at PgConnection construction (in the factory), not here. Proxy
        // is per-lease, PgConnection-tracker binding is per-session.
        _tracker = dataSource.GetCommandTracker(initializedOnly: true);
    }

    internal AdoConnectionProxy(SlonDataSource dataSource, PgConnection pgConnection, IAdoConnection connection)
        : this(dataSource, connection)
        => _pgConnection = pgConnection;

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
        // stale heartbeat callback hazard).
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

    bool TryQueue(PgClientFlow flow) => TryQueueOn(_pgConnection, flow);

    bool TryQueueOn(PgConnection connection, PgClientFlow flow)
    {
        // A SlonConnection holds an exclusive scope for its lease: route the command as a subflow into the
        // held scope's inner pipeline (serial on this one wire) instead of onto the multiplexed protocol
        // pipeline. The data-source path never acquires a scope, so it falls through to the direct enqueue.
        if (_exclusiveScope is { } scope)
        {
            scope.Queue(flow);
            return true;
        }
        if (!connection.TryQueue(flow))
            return false;
        return true;
    }

    public bool InExclusiveScope => _exclusiveScope is not null;

    internal bool TryStartExclusiveScope(PgConnection connection, bool async, FlowEnqueueOptions options)
    {
        if (_pgConnection is not null)
            throw new InvalidOperationException("The proxy is already bound to a pooled connection.");
        if (!connection.Protocol.TryQueueExclusiveScope(
                async, options, out var scope))
            return false;
        _pgConnection = connection;
        _exclusiveScope = scope;
        return true;
    }

    internal void AcquireExclusiveScope()
        => _exclusiveScope!.WaitForHandoffSynchronously();

    internal ValueTask AcquireExclusiveScopeAsync(CancellationToken cancellationToken)
        => new(_exclusiveScope!.WaitForHandoffAsync(cancellationToken));

    public void ReleaseExclusiveScope()
    {
        if (_exclusiveScope is { } scope)
        {
            _exclusiveScope = null;
            scope.CompleteScopeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    public async ValueTask ReleaseExclusiveScopeAsync()
    {
        if (_exclusiveScope is { } scope)
        {
            _exclusiveScope = null;
            await scope.CompleteScopeAsync().ConfigureAwait(false);
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
