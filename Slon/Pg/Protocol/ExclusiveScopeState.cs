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
        readonly Control _innerControl;
        readonly CloseSignal _scopeClose;
        // Built lazily at the FIRST AcquireForTurn (null until then) and re-initialized on every later
        // turn. Deferring construction to the turn means the inner executor starts at the won-turn point,
        // never at begin - and a done-before-executed flow that skips the acquire starts nothing.
        Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>? _innerPipeline;
        // Set once in Create after the flow is built (the flow holds a back-ref to this state, so the two
        // are wired in two phases).
        ExclusiveAccessFlow _flow = null!;

        ExclusiveScopeState(Control innerControl, CloseSignal scopeClose)
        {
            _innerControl = innerControl;
            _scopeClose = scopeClose;
        }

        public ExclusiveAccessFlow Flow => _flow;

        // Build the flyweight's stable resources: the inner Control, the scope CloseSignal (linked to the
        // protocol's _close), the per-scope decoder/writer shells over the protocol's shared channels, and
        // the hosting flow. Does NOT build the inner pipeline - that is deferred to the flow's first turn
        // (AcquireForTurn), so the inner executor never starts at begin.
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

            var state = new ExclusiveScopeState(innerControl, scopeClose);
            // The flow holds a back-ref to the state so it can acquire (build/re-init the inner pipeline) at
            // its TURN. CompleteInner reads _innerPipeline through the state so it follows the (re-)init; it
            // is only reachable after AcquireForTurn has run (the flow owns the wire by then).
            state._flow = new ExclusiveAccessFlow(protocol, innerControl, state, reason => state._innerPipeline!.CompleteAsync(reason));
            return state;
        }

        // Begin-time reuse guard: refuse a new scope while the prior one is still completing. The cascade
        // (OnStopping/OnAbort) is a second driver of the inner pipeline, so a prior scope that hasn't fully
        // completed is reachable here; re-init over a live prior scope is a lifecycle bug, not recoverable.
        public void CheckReusable()
        {
            if (_flow is { IsCompleted: false })
                ThrowHelper.ThrowInvalidOperation("Cannot begin an exclusive scope while the prior scope is still completing.");
        }

        // Reset the pooled flow's framework + gate state for a fresh scope (begin-time, cheap).
        public void ResetFlow() => _flow.Reset();

        // Acquire the scope at the flow's TURN: create the fresh per-scope source, build the inner pipeline
        // on the first turn (binding the Control to its slots), or re-initialize the existing one on later
        // turns. The inner executor starts here, at the won-turn point - not at begin. Returns the source
        // so the flow can Queue subflows onto it. A done-before-executed flow never calls this, so it
        // creates no source and starts no executor.
        public PgClientFlowSource AcquireForTurn(PgClientProtocol protocol)
        {
            var innerSource = PgClientFlowSource.Create(protocol, protocol._options.ExecutionScheduler);
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
