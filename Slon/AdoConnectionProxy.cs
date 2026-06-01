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
    readonly PgClientProtocol _client;
    readonly IAdoConnection _connection;

    CommandFlow? _cachedFlow;
    int _pipelineDepth;
    bool _inExclusiveScope;

    internal AdoConnectionProxy(SlonDataSource dataSource, PgClientProtocol client, IAdoConnection connection)
    {
        _dataSource = dataSource;
        _tracker = _dataSource.GetCommandTracker(initializedOnly: true);
        _client = client;
        _connection = connection;
    }

    internal AdoConnectionProxy(PgClientProtocol client, IAdoConnection connection, bool autoPrepare, CommandTracker? tracker = null)
    {
        const int MaxAuto = 100;
        const int AutoMinimumUses = 5;

        _client = client;
        _connection = connection;
        _tracker = new(autoPrepare ? MaxAuto : 0, AutoMinimumUses, parent: tracker);
    }

    public string ConnectionString => ""; // TODO pull from client or pass it in somehow.

    public int PipeplineDepth => _pipelineDepth;

    internal PgClientFlow? CurrentReadingFlow { get; set; }
    internal PgClientFlow? CurrentWritingFlow { get; set; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TrackerResult TrackCommand(in CommandDescriptor descriptor, TrackedCommand? tracked = null, object? owningInstance = null)
        => _tracker.Track(descriptor, tracked, owningInstance);

    public CommandFlow RentCommandFlow(bool async, in CommandFlowOptions options)
    {
        return new CommandFlow(async, options);
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
            ThrowHelper.ThrowInvalidOperation("Could not enqueue flow.");
    }

    // Returns the given flow to allow an async caller to directly return this task.
    public ValueTask<TFlow> EnqueueAsync<TFlow>(TFlow flow, CancellationToken cancellationToken) where TFlow : PgClientFlow
    {
        if (!TryQueue(flow))
            ThrowHelper.ThrowInvalidOperation("Could not enqueue flow.");

        return new(flow);
    }

    bool TryQueue(PgClientFlow flow)
    {
        Interlocked.Increment(ref _pipelineDepth);
        flow.SetCompletionAction(static (flow, exception, state) =>
        {
            var instance = (AdoConnectionProxy)state!;
            Interlocked.Decrement(ref instance._pipelineDepth);
            // If we're in an exclusive scope we must report a broken state to the connection.
            if (exception is not null && instance._inExclusiveScope)
                instance._connection.Break(exception);
        }, this);
        if (!_client.TryQueue(flow))
        {
            Interlocked.Decrement(ref _pipelineDepth);
            return false;
        }
        return true;
    }

    public void PerformUserCancellation(TimeSpan? timeout = null)
    {
        // TODO spin up a connection and write out cancel
    }

    public bool InExclusiveScope => _inExclusiveScope;

    public void BeginExclusiveScope()
    {
        _inExclusiveScope = true;
    }

    public ValueTask BeginExclusiveScopeAsync(CancellationToken cancellationToken = default)
    {
        _inExclusiveScope = true;
        return new();
    }

    public void EndExclusiveScope()
    {

    }

    public ValueTask EndExclusiveScopeAsync()
    {
        return new();
    }

    public void Dispose()
    {

    }

    public ValueTask DisposeAsync()
    {
        // TODO release managed resources here
        return new();
    }
}
