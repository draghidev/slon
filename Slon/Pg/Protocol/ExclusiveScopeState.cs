using Draghi.Pipelining;
using Slon.Pipelines;
using Slon.Pg.Protocol.Flows;

namespace Slon.Pg.Protocol;

sealed partial class PgClientProtocol
{
    // The reusable per-connection exclusive-scope flyweight, collapsed into one object behind a single
    // protocol ref. Owns the scope's RESOURCES: the inner Control, the inner pipeline, the scope
    // CloseSignal (a linked child of the protocol's _close), and the per-scope decoder/writer shells over
    // the protocol's shared Read/WriteChannel. Plus the pooled ExclusiveAccessFlow that hosts the scope.
    //
    // It deliberately holds no scope IDENTITY (who is the activated exclusive flow / is a scope live) -
    // that is the pipeline's ActivatedItem/ExecutingItem, read through the inner Control. This object is
    // resources only.
    //
    // One per connection: the outer pipeline stalls for the whole duration of exclusive access (the
    // activated ExclusiveAccessFlow holds the outer slot until the scope ends), so a second concurrent
    // scope-state would have nothing to run. The inner pipeline uses the SAME PgClientFlowSource + Policy
    // as the protocol level (no divergence), so sync handoff and pipelining compose recursively to the
    // root. Nested in PgClientProtocol so it reaches Control/Policy without widening their accessibility.
    internal sealed class ExclusiveScopeState
    {
        readonly PgClientProtocol _protocol;
        readonly Control _innerControl;
        readonly CloseSignal _scopeClose;
        // Built lazily at the FIRST AcquireForTurn (null until then) and re-initialized on every later
        // turn. Deferring construction to the turn means the inner executor starts at the won-turn point,
        // never at begin - and a done-before-executed flow that skips the acquire starts nothing. Only the
        // single activated flow ever touches it (the outer pipeline serializes turns), so the N waiters
        // share it safely - handed serially, never concurrently.
        Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>? _innerPipeline;
        // The cached flow = the zero-alloc fast path for the common sequential case (one scope at a time).
        // When it is still in a live scope, RentFlow allocates an overflow flow over the SAME state; the
        // outer pipeline serializes them so only one holds the shared inner pipeline at a time. (Pooling
        // overflow flows is a later optimization; today they are allocated fresh.)
        ExclusiveAccessFlow _cachedFlow = null!;
        // 1 while the cached flow is leased to a live scope: claimed atomically in RentFlow, released in the
        // cached flow's OnComplete. RentFlow runs OUTSIDE _syncRoot and concurrent begins race it, so the
        // claim cannot lean on IsPending/IsCompleted - those cannot tell "available" from "rented but not
        // yet executed", and two begins reading IsPending=true would both take the one flyweight and stomp
        // its per-scope state (PrepareScope) mid-flight.
        int _cachedLeased;

        ExclusiveScopeState(PgClientProtocol protocol, Control innerControl, CloseSignal scopeClose)
        {
            _protocol = protocol;
            _innerControl = innerControl;
            _scopeClose = scopeClose;
        }

        // Build the flyweight's stable resources: the inner Control, the scope CloseSignal (linked to the
        // protocol's _close), the per-scope decoder/writer shells over the protocol's shared channels, and
        // the cached hosting flow. Does NOT build the inner pipeline - that is deferred to a flow's first
        // turn (AcquireForTurn), so the inner executor never starts at begin.
        public static ExclusiveScopeState Create(PgClientProtocol protocol)
        {
            var innerControl = new Control(protocol, poolFacing: false);

            // Scope signal: a child linked to the protocol's _close, so a protocol stop/abort cascades into
            // the scope's tokens through the link. Reused across scopes (it stays untripped on the normal
            // completion path, so its linked CTSes stay pristine - the per-execute zero-alloc invariant).
            var scopeClose = CloseSignal.CreateLinked(protocol._close, protocol._options.TimeProvider);
            innerControl.BindScopeClose(scopeClose);

            // Per-scope decoder/writer shells over the protocol's shared Read/WriteChannel, carrying the
            // scope's abort token. A scope-only abort trips scopeClose, breaking a subflow parked on a wire
            // read/write through these shells, while the pooled protocol's own token (and base shells) stay
            // untripped. Created once with the flyweight, reused across scopes.
            var scopeDecoder = PgDecoder.CreateScopeShell(protocol._pgDecoder, scopeClose.AbortToken, protocol._options.ReadTimeout);
            var scopeWriter = PgProtocolDataWriter.CreateScopeShell(protocol._protocolDataWriter, scopeClose.AbortToken, innerControl);
            innerControl.BindShells(scopeDecoder, scopeWriter);

            var state = new ExclusiveScopeState(protocol, innerControl, scopeClose);
            state._cachedFlow = state.NewFlow();
            return state;
        }

        // A flow holds a back-ref to this state so it can acquire (build/re-init the inner pipeline) at its
        // TURN. CompleteInner reads _innerPipeline through the state so it follows the (re-)init; it is only
        // reachable after AcquireForTurn has run (the flow owns the wire by then). All flows over this state
        // share the one inner pipeline, handed serially by the outer pipeline's ordering.
        ExclusiveAccessFlow NewFlow()
            => new(_protocol, _innerControl, this, reason => _innerPipeline!.CompleteAsync(reason));

        // Rent a flow for a new scope. The cached flow when it is free (not in a live scope) - the zero-alloc
        // common path; otherwise an overflow flow allocated fresh over the same state (a concurrent begin
        // while a prior scope is still live). The returned flow is Reset and ready to enqueue. No
        // begin-time guard / throw: a concurrent begin gets its own waiter instead of failing.
        public ExclusiveAccessFlow RentFlow()
        {
            // Atomically claim the cached flow: the FIRST concurrent begin wins it, later begins each get a
            // fresh overflow waiter over the same state (the outer pipeline serializes their turns). The
            // claim is held for the whole tenure and released in the cached flow's OnComplete - so a second
            // begin can never re-rent a still-live flyweight and stomp it.
            var flow = Interlocked.CompareExchange(ref _cachedLeased, 1, 0) == 0 ? _cachedFlow : NewFlow();
            // Consumption gate: the release (completion-action, cascade-final) can precede the prior
            // scope's WaitForComplete continuation actually running - its GetResult token is still
            // live, and the Reset below would bump the version under it (a stale-token throw on the
            // close path). Rentability is release AND consumption: while the signal is unconsumed,
            // undo the claim and take an overflow flow; the consuming GetResult clears the flag
            // (consume-then-clear, release/acquire), after which the next rent may Reset safely.
            // Not a Dekker: registration happens-before retirement (the CompleteScopeAsync hoist),
            // which happens-before the release Exchange, which the claim CAS above acquires - so this
            // read cannot miss a registered waiter. A flow with no waiter leaves the flag clear.
            if (ReferenceEquals(flow, _cachedFlow) && flow.CompletionWaiterPending)
            {
                Interlocked.Exchange(ref _cachedLeased, 0);
                flow = NewFlow();
            }
            if (flow.IsCompleted)
                flow.Reset();
            return flow;
        }

        // Release the cached-flow claim at the end of its tenure (from OnComplete), freeing the next
        // concurrent begin to claim and Reset it. A no-op for overflow flows - only the one cached flyweight
        // is claim-tracked.
        public void ReleaseFlow(ExclusiveAccessFlow flow)
        {
            if (ReferenceEquals(flow, _cachedFlow))
                Interlocked.Exchange(ref _cachedLeased, 0);
        }

        // Acquire the scope at the flow's TURN: create the fresh per-scope source, build the inner pipeline
        // on the first turn (binding the Control to its slots), or re-initialize the existing one on later
        // turns. The inner executor starts here, at the won-turn point - not at begin. Returns the source
        // so the flow can Queue subflows onto it. A done-before-executed flow never calls this, so it
        // creates no source and starts no executor.
        public PgClientFlowSource AcquireForTurn(PgClientProtocol protocol)
        {
            var innerSource = PgClientFlowSource.Create(protocol, _innerControl, protocol._options.ExecutionScheduler);
            var first = _innerPipeline is null;
            _innerPipeline = Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(
                new Policy(protocol, _innerControl), innerSource, _innerPipeline);
            if (first)
                _innerControl.BindPipeline(new PipelineFlowSlots<Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(_innerPipeline));
            return innerSource;
        }

        // Trip the scope's CloseSignal only, breaking any subflow parked on a wire read/write via the scope
        // shells' tokens, without touching the protocol's own token (the pooled protocol survives).
        public void AbortScope() => _scopeClose.Abort();

        // The scope signal (a linked child) holds a registration on the protocol's _close; disposing it
        // releases that registration. Must run BEFORE the protocol disposes _close so the link registration
        // is gone first.
        public void Dispose() => _scopeClose.Dispose();
    }
}
