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
    sealed class ExclusiveScopeState
    {
        readonly Control _innerControl;
        readonly CloseSignal _scopeClose;
        Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator> _innerPipeline;
        readonly ExclusiveAccessFlow _flow;

        ExclusiveScopeState(
            Control innerControl,
            CloseSignal scopeClose,
            Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator> innerPipeline,
            ExclusiveAccessFlow flow)
        {
            _innerControl = innerControl;
            _scopeClose = scopeClose;
            _innerPipeline = innerPipeline;
            _flow = flow;
        }

        public ExclusiveAccessFlow Flow => _flow;

        // First-time construction of the whole flyweight. Builds the inner Control, links the scope signal
        // to the protocol's _close, binds the per-scope shells over the protocol's shared channels, stands
        // up the inner pipeline, and creates the hosting flow.
        public static ExclusiveScopeState Create(PgClientProtocol protocol, PgClientFlowSource innerSource)
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

            var innerPipeline = Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(
                new Policy(protocol, innerControl), innerSource);
            innerControl.BindPipeline(new PipelineFlowSlots<Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(innerPipeline));

            var flow = new ExclusiveAccessFlow(innerControl, reason => innerPipeline.CompleteAsync(reason));
            return new ExclusiveScopeState(innerControl, scopeClose, innerPipeline, flow);
        }

        // Re-arm for a fresh scope on reuse: re-Initialize the inner pipeline over the existing Control with
        // a fresh source, then Reset the pooled flow. The cascade (OnStopping/OnAbort) is a second driver of
        // the inner pipeline, so a prior scope that hasn't fully completed is reachable here; refuse to
        // re-init over a live prior scope - a lifecycle bug, not a recoverable state.
        public void ReArm(PgClientProtocol protocol, PgClientFlowSource innerSource)
        {
            if (_flow is { IsCompleted: false })
                ThrowHelper.ThrowInvalidOperation("Cannot begin an exclusive scope while the prior scope is still completing.");
            Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(
                new Policy(protocol, _innerControl), innerSource, _innerPipeline);
            _flow.Reset();
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
