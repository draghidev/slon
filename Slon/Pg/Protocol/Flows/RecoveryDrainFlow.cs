using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using Slon.Runtime.CompilerServices;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol.Flows;

// Substitute item returned by PgClientProtocol.Control.TryRecoverItemFailure to clean the wire
// when a flow's decode phase faults (or its execute phase faults with the writer still intact).
// Inherits the failed flow's outstanding RFQ obligation, optionally pushes pending buffered
// bytes to the wire, optionally injects a terminating Sync, then drains the resulting RFQs.
// The pipeline framework treats this as any other flow: standard activate/execute/complete
// lifecycle, recovery is just an item that happens to do wire-cleanup work.
sealed class RecoveryDrainFlow : PgClientFlow
{
    int _drainCount;
    bool _flushPendingBytes;
    bool _injectSync;
    readonly ValueTaskSourcePromise<bool> _readPromise = new();

    /// The flow this recovery supplanted, carried so the policy can complete it when the
    /// recovery completes. The failed item's lifetime deliberately extends as far as the
    /// recovery does: completion is the reuse gate (completion actions Reset and re-enqueue
    /// instances), and reuse must be causally after the protocol stops referencing the failed
    /// tenure's machinery (parked dispatch state, inherited RFQ bookkeeping, registrations).
    /// Ordering of anything enqueued after the failure is already handled by the pipeline.
    public PgClientFlow? FailedFlow { get; private set; }
    public Exception? FailureException { get; private set; }

    public void BindFailedFlow(PgClientFlow failedFlow, Exception exception)
    {
        FailedFlow = failedFlow;
        FailureException = exception;
    }

    // drainCount: total RFQs the drain phase expects (inheritedRfqCount + 1 if injectSync).
    //
    // injectSync: true when the failed flow's last buffered message wasn't Sync/Query and the
    // wire is sitting on an unfinished command sequence the server is waiting to terminate.
    // The drain writes Sync, adding +1 to the expected RFQ count.
    //
    // flushPendingBytes: true when UnflushedBytes > 0 at recovery time. The recovery flow
    // flushes the buffered work (the failed flow's deferred writes plus any pipelined siblings'
    // bytes) so the server actually processes what _rfqCount expects RFQs for.
    //
    // The inherited RFQ count (the failed flow's _rfqCount, source of the obligation the drain
    // is consuming) is NOT set from this constructor — TryRecoverItemFailure does the transfer
    // at its call site so the inheritance setup is visible at the recovery seam rather than
    // hidden inside the flow's construction path.
    public RecoveryDrainFlow(bool async, int drainCount, bool injectSync, bool flushPendingBytes)
        : base(supportsPipelining: true)
    {
        _drainCount = drainCount;
        _injectSync = injectSync;
        _flushPendingBytes = flushPendingBytes;
        IsAsync = async;
    }

    public new void Reset()
    {
        base.Reset();
        _drainCount = 0;
        _flushPendingBytes = false;
        _injectSync = false;
        FailedFlow = null;
        FailureException = null;
    }

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        // Wire-already-clean fast path: no obligation, nothing buffered, nothing to inject. The
        // recovery contract still requires returning a flow (the pipeline expects an item to
        // substitute the failed one), but its work is trivially complete.
        if (_drainCount == 0 && !_flushPendingBytes)
            return new(ValueTask.CompletedTask);

        ValueTask flushTask = default;
        if (_injectSync || _flushPendingBytes)
        {
            var encoder = context.GetEncoder();
            if (_injectSync)
                encoder.WriteSync();
            flushTask = encoder.FlushAsync();
        }

        // Same shape as MaintenanceFlow: flush goes as trailing so the read phase can start
        // concurrently with the wire I/O. BeginCallScope sets the read promise in TLS so the
        // async local function picks it up at state-machine creation.
        using (PromiseAsyncValueTaskMethodBuilder.BeginCallScope(_readPromise))
        {
            return new(new FlowTasks(trailingExecutionTask: flushTask, pipelineTask: DrainPhase()));
        }

        [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder))]
        async ValueTask DrainPhase()
        {
            var decoder = await context.GetDecoderAsync().ConfigureAwait(false);
            int remaining = _drainCount;
            while (remaining > 0)
            {
                var message = await decoder.GetNextAsync().ConfigureAwait(false);
                // HandleMessageAutoCore decrements this flow's _rfqCount internally; the local
                // counter just drives the loop exit and stays robust against any future changes
                // to the auto-handler's count semantics.
                if (message.Header.Type is BackendType.ReadyForQuery)
                    remaining--;
            }
        }
    }
}
