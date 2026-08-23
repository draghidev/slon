using System.Data;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon;

// Routes ADO work through either a connection's exclusive scope or a directly selected physical connection.
sealed class AdoConnectionProxy
{
    readonly CommandTracker _tracker;
    PgConnection _pgConnection = null!;
    // A SlonConnection holds an exclusive scope for its whole lease (acquired at Open), so its commands run
    // serially on one wire instead of multiplexed - the safe default, since Slon can't parse SQL to know
    // which commands carry session state (SET / LISTEN / temp tables / BEGIN...). Null on the data-source
    // path (transient per-command proxy, which never Opens, so it stays multiplexed). Routing keys on this
    // being non-null, so the connection/data-source split needs no separate flag.
    ExclusiveScopeLease? _exclusiveScope;

    internal AdoConnectionProxy(SlonDataSource dataSource)
    {
        // Auto-prepare uses the workload-scope tracker directly. Explicit-prepare bookkeeping
        // lives on SlonConnection (per Policy A, survives Close-Open). PgConnection ↔ tracker
        // registration happens at PgConnection construction (in the factory), not here. Proxy
        // is per-lease, PgConnection-tracker binding is per-session.
        _tracker = dataSource.GetCommandTracker(initializedOnly: true);
    }

    internal AdoConnectionProxy(SlonDataSource dataSource, PgConnection pgConnection)
        : this(dataSource)
        => _pgConnection = pgConnection;

    internal AdoConnectionProxy(PgConnection pgConnection, bool autoPrepare, CommandTracker? tracker = null)
    {
        const int MaxAuto = 100;
        const int AutoMinimumUses = 5;

        _tracker = new(autoPrepare ? MaxAuto : 0, AutoMinimumUses, parent: tracker);
        _pgConnection = pgConnection;
    }

    internal ConnectionState State
    {
        get
        {
            var scope = _exclusiveScope;
            if (scope is null)
                return ConnectionState.Open;

            var activatedFlow = scope.ActivatedFlow;
            if (activatedFlow is CommandFlow { IsResultReady: true })
                return ConnectionState.Fetching;

            return activatedFlow is not null || scope.ExecutingFlow is not null
                ? ConnectionState.Executing
                : ConnectionState.Open;
        }
    }

    internal CommandTracker Tracker => _tracker;
    internal PgConnection PgConnection => _pgConnection;

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

    bool TryQueue(PgClientFlow flow)
    {
        // A SlonConnection holds an exclusive scope for its lease: route the command as a subflow into the
        // held scope's inner pipeline (serial on this one wire) instead of onto the multiplexed protocol
        // pipeline. The data-source path never acquires a scope, so it falls through to the direct enqueue.
        if (_exclusiveScope is { } scope)
        {
            scope.Queue(flow);
            return true;
        }
        return _pgConnection.TryQueue(flow);
    }

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

}
