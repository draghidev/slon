using System.Runtime.CompilerServices;
using Slon.Pg.Protocol;
using Slon.Runtime.CompilerServices;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon;

// ADO-layer flow that drains a PgConnection's maintenance queue. Groups work items by kind into
// a single wire batch where possible (currently: Close per item all under one Sync window).
// Lives in the Slon namespace because it knows about TrackedCommand and PgConnection. The
// protocol layer just sees a PgClientFlow.
//
// Pipelineable: the write phase (Execute) emits Close+Sync and hands off to the framework via
// ExecutePipelinedWithPromise. The read phase (ExecutePipelined) drains to RFQ and runs the
// cleanup walk. While we're in the read phase, the executor can start the next flow's write
// phase, maintenance doesn't stall the pipeline behind it.
sealed class MaintenanceFlow : PgClientFlow
{
    PgConnection? _connection;
    readonly ValueTaskSourcePromise<bool> _readPromise = new();

    public MaintenanceFlow() : base(supportsDeferredFlush: true)
    {
        IsAsync = true;
    }

    // Owned by MaintenanceFlow so PgConnection.TryArmAndSchedule doesn't need to know how the
    // lifecycle hooks together. The stateless observer receives the bound connection as state,
    // returns the flow to the cache, disarms, and re-arms if work arrived during the drain.
    sealed class ObserverImpl : PgClientFlowObserver
    {
        internal static readonly ObserverImpl Instance = new();

        internal override void OnCompleted(PgClientFlow flow, Exception? exception, object? state)
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

    public new void Reset()
    {
        base.Reset();
        _connection = null;
    }

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        var connection = _connection ?? throw new InvalidOperationException("MaintenanceFlow has not been bound to a PgConnection.");
        // Snapshot the range to process. (Head, Tail) define an immutable window in the intrusive
        // linked list, producers can append beyond `tail` during execution. We stop at `tail`.
        // Items remain linked until CommitMaintenanceRange runs. An exception before commit leaves
        // the range visible for the next flow (at-least-once default, Close is idempotent).
        var (head, tail) = connection.SnapshotMaintenanceRange();
        if (head is null)
            return new(ValueTask.CompletedTask);

        // Write phase: one Sync window for the whole batch. Per protocol spec, Close against a
        // nonexistent statement is NOT an error (it returns CloseComplete) so racy cleanup and
        // leak salvage don't trip the after-error-skip semantics. ErrorResponse on Close is
        // reserved for genuine server-side difficulty (OOM etc.), accepted as best-effort.
        // All writes are sync buffer-fills (WriteClose, WriteSync are void). The only point that
        // can yield is the flush, which we hand to FlowTasks as trailing so the read phase can
        // start concurrently with the wire I/O draining.
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

        // Hand off to the pipelined read phase as a local async function. BeginCallScope sets the
        // promise TLS so the local function's builder picks it up at Create time. The using clears
        // it once we've returned. head/tail/connection/context are closed over from outer scope.
        // The framework's await on the returned ValueTask handles activation deferral naturally
        // via GetDecoderAsync's own await suspension, no special framework deferral needed.
        using (PromiseAsyncValueTaskMethodBuilder.BeginCallScope(_readPromise))
        {
            return new(new FlowTasks(trailingExecutionTask: flushTask, pipelineTask: ReadPhase()));
        }

        [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder))]
        async ValueTask ReadPhase()
        {
            // Drain to RFQ, tolerant of any mid-window outcome so a single odd ErrorResponse
            // doesn't poison the decoder state. The protocol resyncs at RFQ either way. If this
            // throws (e.g. unrecoverable wire error), the policy will eject the PgConnection and
            // the unconsumed items go with it. No requeue needed.
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

            // Cleanup walk: RemoveTracked for EvictDeallocate entries, fire completion TCSs. Must
            // happen BEFORE commit since commit clears Next pointers so the GC can reclaim nodes.
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

            // Commit: unlink the processed range. Nodes become GC-eligible. Producers that
            // appended beyond `tail` during our run stay linked.
            connection.CommitMaintenanceRange(tail);
        }
    }
}
