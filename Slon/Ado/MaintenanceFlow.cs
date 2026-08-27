using System.Runtime.CompilerServices;

using Slon.Pg.Protocol;
using Slon.Runtime.CompilerServices;
using Slon.Text;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon;

/// <summary>Closes prepared statements as queued session maintenance or as a one-shot operation.</summary>
/// <remarks>
/// The reusable queued mode drains a <see cref="PgConnection"/> maintenance range, reports errors
/// as best-effort diagnostics, commits completed work and re-arms when producers appended another
/// range. Its write and read phases are pipelineable, so draining responses does not hold up the
/// next flow's writes.
///
/// The one-shot mode is enqueued directly on an exclusive connection pipeline. It closes the names
/// supplied by that scope, supports synchronous execution and propagates server errors to its caller.
/// </remarks>
sealed class MaintenanceFlow : PgClientFlow
{
    PgConnection? _connection;
    readonly EncodedCString[]? _oneShotNames;
    readonly ValueTaskSourcePromise<bool>? _readPromise;

    internal MaintenanceFlow() : base(supportsDeferredFlush: true)
    {
        _readPromise = new();
        IsAsync = true;
    }

    internal MaintenanceFlow(EncodedCString[] names, bool async) : base(supportsDeferredFlush: true)
    {
        _oneShotNames = names;
        IsAsync = async;
    }

    // Owned by MaintenanceFlow so PgConnection.TryArmAndSchedule doesn't need to know how the
    // lifecycle hooks together. The stateless observer receives the bound connection as state,
    // returns the flow to the cache, disarms, and re-arms if work arrived during the drain.
    sealed class ObserverImpl : PgClientFlowObserver
    {
        internal static readonly ObserverImpl Instance = new();

        protected internal override void OnCompleted(PgClientFlow flow, Exception? exception, object? state)
        {
            var conn = (PgConnection)state!;
            conn.OnMaintenanceFlowCompleted((MaintenanceFlow)flow);
        }
    }

    internal void Bind(PgConnection connection)
    {
        _connection = connection;
        SetObserver(ObserverImpl.Instance, connection);
    }

    internal new void Reset()
    {
        base.Reset();
        _connection = null;
    }

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
        => _oneShotNames is { } names ? ExecuteOneShot(context, names) : ExecuteQueued(context);

    ValueTask<FlowTasks> ExecuteQueued(Context context)
    {
        var connection = _connection
            ?? throw new InvalidOperationException("MaintenanceFlow has not been bound to a PgConnection.");
        // Process a stable prefix. Later appends remain for the next pass, while failure leaves this
        // idempotent Close range available for retry.
        var (head, tail) = connection.SnapshotMaintenanceRange();
        if (head is null)
            return new(ValueTask.CompletedTask);

        // Write the prefix as one window; the trailing flush lets its read phase pipeline.
        var encoder = context.GetEncoder();
        var node = head;
        while (true)
        {
            switch (node)
            {
                case EvictDeallocate evict:
                    encoder.WriteClose(evict.Name);
                    break;
                case CloseStatement close:
                    encoder.WriteClose(close.Name);
                    break;
            }
            if (ReferenceEquals(node, tail))
                break;
            node = node.Next!;
        }
        encoder.WriteSync();
        var flushTask = encoder.FlushAsync();

        // Back the captured read phase with the flow's reusable promise.
        using (PromiseAsyncValueTaskMethodBuilder.BeginCallScope(_readPromise!))
        {
            return new(new FlowTasks(trailingExecutionTask: flushTask, pipelineTask: ReadPhase()));
        }

        [RuntimeAsyncMethodGeneration(false)]
        [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder))]
        async ValueTask ReadPhase()
        {
            // Maintenance errors are diagnostic; finish the window before committing the prefix.
            var decoder = await context.GetDecoderAsync().ConfigureAwait(false);
            while (true)
            {
                var message = await decoder.GetNextAsync().ConfigureAwait(false);
                if (message.Header.Type is BackendType.ErrorResponse)
                {
                    var error = ErrorOrNoticeMessage.Create(message, []);
                    connection.ReportMaintenanceError(error.SqlState, error.MessageText);
                }
                if (message.Header.Type is BackendType.ReadyForQuery)
                    break;
            }

            // Consume node state before commit clears the links.
            var n = head;
            while (true)
            {
                if (n is EvictDeallocate evict)
                    connection.RemoveTracked(evict.Tracked);
                n.Completion?.TrySetResult();
                if (ReferenceEquals(n, tail))
                    break;
                n = n.Next!;
            }

            connection.CommitMaintenanceRange(tail);
        }
    }

    static async ValueTask<FlowTasks> ExecuteOneShot(Context context, EncodedCString[] names)
    {
        var encoder = context.GetEncoder();
        foreach (var name in names)
            encoder.WriteClose(name);
        encoder.WriteSync();
        await encoder.FlushAuto().ConfigureAwait(false);

        var decoder = await context.GetDecoderAuto().ConfigureAwait(false);
        while (true)
        {
            var message = await decoder.GetNextAuto().ConfigureAwait(false);
            if (message.Header.Type is BackendType.ErrorResponse)
                PgErrorException.Throw(new(ErrorOrNoticeMessage.Create(message, [])));
            if (message.Header.Type is BackendType.ReadyForQuery)
                return ValueTask.CompletedTask;
        }
    }
}
