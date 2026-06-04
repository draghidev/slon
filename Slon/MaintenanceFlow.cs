using Slon.Pg.Protocol;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon;

// ADO-layer flow that drains a PgConnection's maintenance queue. Groups work items by kind into
// a single wire batch where possible (currently: EvictDeallocate items aggregated into one
// Close+Sync window). Lives in the Slon namespace because it knows about TrackedCommand and
// PgConnection; the protocol layer just sees a PgClientFlow.
sealed class MaintenanceFlow : PgClientFlow
{
    PgConnection? _connection;

    public MaintenanceFlow() : base(supportsPipelining: false)
    {
        IsAsync = true;
    }

    // Owned by MaintenanceFlow (rather than wired externally) so PgConnection.TryArmAndSchedule
    // doesn't need to know how the flow's lifecycle hooks together. Bind sets the completion
    // action too. The static delegate captures the connection from state, returns the flow to
    // the cache, disarms, and re-arms if work arrived during the drain.
    static readonly Action<PgClientFlow, Exception?, object?> OnCompletedAction =
        static (flow, _, state) =>
        {
            var conn = (PgConnection)state!;
            conn.OnMaintenanceFlowCompleted((MaintenanceFlow)flow);
        };

    internal void Bind(PgConnection connection)
    {
        _connection = connection;
        SetCompletionAction(OnCompletedAction, connection);
    }

    public new void Reset()
    {
        base.Reset();
        _connection = null;
    }

    protected override async ValueTask<FlowTasks> Execute(Context context)
    {
        var connection = _connection ?? throw new InvalidOperationException("MaintenanceFlow has not been bound to a PgConnection.");
        // Snapshot the range to process. (Head, Tail) define an immutable window in the intrusive
        // linked list, producers can append beyond `tail` during execution. We stop at `tail`.
        // Items remain linked until CommitMaintenanceRange runs. An exception before commit leaves
        // the range visible for the next flow (at-least-once default, Close is idempotent).
        var (head, tail) = connection.SnapshotMaintenanceRange();
        if (head is null)
            return ValueTask.CompletedTask;

        // One Sync window for the whole batch. Per protocol spec, Close against a nonexistent
        // statement is NOT an error — it returns CloseComplete — so racy cleanup and leak salvage
        // don't trip the after-error-skip semantics. ErrorResponse on Close is reserved for
        // genuine server-side difficulty (OOM etc.); accepted as best-effort for the batch.
        var encoder = context.GetEncoder();
        var node = head;
        while (true)
        {
            switch (node)
            {
                case EvictDeallocate evict:
                    encoder.WriteClose(evict.Tracked.CommandName);
                    break;
                case CloseStatement close:
                    encoder.WriteClose(close.Name);
                    break;
                case CloseStatements many:
                    foreach (var name in many.Names)
                        encoder.WriteClose(name);
                    break;
            }
            if (ReferenceEquals(node, tail))
                break;
            node = node.Next!;
        }
        encoder.WriteSync();
        await encoder.FlushAuto().ConfigureAwait(false);

        // Drain to RFQ — tolerant of any mid-window outcome so a single odd ErrorResponse doesn't
        // poison the decoder state. The protocol resyncs at RFQ either way. If this throws (e.g.
        // unrecoverable wire error), the policy will eject the PgConnection and the unconsumed
        // items go with it; no requeue needed because no future flow on this session will run.
        var decoder = await context.GetDecoderAuto().ConfigureAwait(false);
        while (true)
        {
            var message = await decoder.GetNextAsync().ConfigureAwait(false);
            if (message.Header.Type is BackendType.ReadyForQuery)
                break;
        }

        // Cleanup walk: RemoveTracked for EvictDeallocate entries, fire completion TCSs. Must
        // happen BEFORE commit since commit clears Next pointers so the GC can reclaim nodes.
        node = head;
        while (true)
        {
            if (node is EvictDeallocate evict)
                connection.RemoveTracked(evict.Tracked);
            node.Completion?.TrySetResult();
            if (ReferenceEquals(node, tail))
                break;
            node = node.Next!;
        }

        // Commit: unlink the processed range from the list. Nodes are now unreferenced from the
        // connection's queue and become GC-eligible. Producers that appended beyond `tail` during
        // our run stay linked — the completion action's HasMaintenance check picks them up.
        connection.CommitMaintenanceRange(tail);

        return ValueTask.CompletedTask;
    }
}
