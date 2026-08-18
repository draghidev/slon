using Draghi.Pipelining;
using Slon.Pg.Protocol.Flows;

namespace Slon.Pg.Protocol;

sealed partial class PgClientProtocol
{
    // Reusable resources for exclusive scopes on this protocol. The outer pipeline serializes access to
    // the shared inner pipeline; concurrent scope requests need distinct hosting flows, not distinct state.
    internal sealed class ExclusiveScopeState
    {
        readonly PgClientProtocol _protocol;
        readonly Control _innerControl;
        readonly CloseSignal _scopeClose;
        // Created only after the hosting flow wins its turn, then reinitialized for later scopes.
        Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>? _innerPipeline;
        // The cached flow covers sequential use; concurrent requests allocate hosting flows over this state.
        ExclusiveAccessFlow _cachedFlow = null!;
        // RentFlow runs outside _syncRoot, so cached-flow ownership needs an atomic claim.
        int _cachedLeased;

        ExclusiveScopeState(PgClientProtocol protocol, Control innerControl, CloseSignal scopeClose)
        {
            _protocol = protocol;
            _innerControl = innerControl;
            _scopeClose = scopeClose;
        }

        // Builds stable scope resources. The inner pipeline remains deferred until the first won turn.
        public static ExclusiveScopeState Create(PgClientProtocol protocol)
        {
            var innerControl = new Control(protocol, poolFacing: false);

            // Protocol closure cascades through this reusable child signal.
            var scopeClose = CloseSignal.CreateLinked(protocol._close);
            innerControl.BindScopeClose(scopeClose);

            // Scope shells share the physical pipes but carry the scope's abort token.
            var scopeDecoder = PgDecoder.CreateScopeShell(protocol._pgDecoder, scopeClose.AbortToken, protocol._options.ReadTimeout);
            var scopeWriter = ProtocolDataWriter.CreateScopeShell(protocol._protocolDataWriter, scopeClose.AbortToken, innerControl);
            innerControl.BindShells(scopeDecoder, scopeWriter);

            var state = new ExclusiveScopeState(protocol, innerControl, scopeClose);
            state._cachedFlow = state.NewFlow();
            return state;
        }

        // Resolve the inner pipeline through this state because it is installed only when the flow runs.
        ExclusiveAccessFlow NewFlow()
            => new(_protocol, _innerControl, this, reason => _innerPipeline!.CompleteAsync(reason));

        // Returns the cached flow when safely reusable, otherwise a fresh flow over the same state.
        public ExclusiveAccessFlow RentFlow()
        {
            var flow = Interlocked.CompareExchange(ref _cachedLeased, 1, 0) == 0 ? _cachedFlow : NewFlow();
            // Completion may release the claim before its waiter consumes the promise token. Resetting
            // then would invalidate that token, so an unconsumed waiter forces an overflow flow.
            if (ReferenceEquals(flow, _cachedFlow) && flow.CompletionWaiterPending)
            {
                Interlocked.Exchange(ref _cachedLeased, 0);
                flow = NewFlow();
            }
            if (flow.IsCompleted)
                flow.Reset();
            return flow;
        }

        // Release the cached claim at the end of flow tenure; overflow flows are not tracked.
        public void ReleaseFlow(ExclusiveAccessFlow flow)
        {
            if (ReferenceEquals(flow, _cachedFlow))
                Interlocked.Exchange(ref _cachedLeased, 0);
        }

        // Starts or reinitializes the inner pipeline only after the hosting flow wins its outer turn.
        public PgClientFlowSource AcquireForTurn(PgClientProtocol protocol)
        {
            var innerSource = PgClientFlowSource.Create(protocol, _innerControl, protocol._executionScheduler);
            var first = _innerPipeline is null;
            _innerPipeline = Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(
                new Policy(protocol, _innerControl, this), innerSource, _innerPipeline);
            _innerControl.BindSource(innerSource);
            if (first)
                _innerControl.BindPipeline(_innerPipeline);
            return innerSource;
        }

        public bool IsPipelineEmpty(PgClientFlowSource source)
            => source.Backlog == 0 && _innerPipeline!.Depth == 0;

        public Task Completion => _innerPipeline!.Completion;

        // Inner completion may run inside its source-driver callback. Queue the outer release so that
        // callback can unwind before caller code can synchronously drive more work on this protocol.
        public void SignalScopeEnded(TaskCompletionSource scopeEnded)
            => _protocol._executionScheduler.SubmitDetached(
                static state => ((TaskCompletionSource)state!).TrySetResult(), scopeEnded);

        internal void Terminate(Exception exception)
            => _ = _innerPipeline!.CompleteAsync(exception);

        // Release the child registration before the protocol disposes its close signal.
        public void Dispose() => _scopeClose.Dispose();
    }
}
