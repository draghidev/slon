using System.Buffers.Binary;
using Slon.Transport;

namespace Slon.Pg.Protocol;

sealed partial class PgClientProtocol
{
    const int CancelRequestMessageLength = sizeof(int) * 4;
    const int CancelRequestCode = (1234 << 16) | 5678;

    CancellationCoordinator<PgClientFlow>? _cancellation;

    internal bool HasPendingCancellation
        => Volatile.Read(ref _cancellation)?.HasPendingCancellation is true;
    internal bool HasCancellationIntents
        => Volatile.Read(ref _cancellation)?.HasCancellationIntents is true;
    internal string DescribeCancellationState()
        => Volatile.Read(ref _cancellation)?.DescribeState()
           ?? "dispatching=False, intents=[], exposures=[]";

    CancellationCoordinator<PgClientFlow> GetOrCreateCancellationCoordinatorLocked()
    {
        var coordinator = _cancellation;
        if (coordinator is not null)
            return coordinator;
        Func<CancellationToken, ValueTask<CancelRequestState>>? request = null;
        if (_options.CancelSender is not null && _backendProcessId != 0)
            request = token => _options.CancelSender(_backendProcessId, _backendSecretKey, token);
        coordinator = new(_options.TimeProvider, _options.CancellationTimeout,
            _options.CancellationRetryInterval, _options.CancelRequestDelay, AbortToken,
            request, FailProtocol,
            _ => FlowControl.CancellationActivation,
            (owner, window) => FlowControl.IsAtCancellationReadFrontier(owner, window),
            static owner => owner.BackendCancellationGracePeriod,
            (exception, state) => SlonLogMessages.CancellationRequestFailed(_logger, exception, state));
        Volatile.Write(ref _cancellation, coordinator);
        return coordinator;
    }

    ValueTask WaitForCancellationAttempt()
        => Volatile.Read(ref _cancellation)?.WaitForCancellationAttempt() ?? default;

    internal void RequestServerCancellation(PgClientFlow instigator,
        int window, BackendCancellationTiming timing, TaskCompletionSource? delivery,
        object episodeKey, int scope, BackendCancellationTiming subsequentTiming)
    {
        CancellationCoordinator<PgClientFlow> coordinator;
        lock (_syncRoot)
        {
            if (_status is not ProtocolStatus.Ready || instigator.IsCompleted)
            {
                delivery?.TrySetResult();
                return;
            }
            coordinator = GetOrCreateCancellationCoordinatorLocked();
        }
        coordinator.RequestCancellation(instigator, window, timing, delivery,
            episodeKey, scope == (int)Flows.CommandFlow.CancellationScope.RemainingFlow,
            subsequentTiming);
        // Flow release publishes its coordinator down-edge before IsCompleted. Recheck after
        // publishing the episode so either that down-edge or this observation retires it.
        if (instigator.IsCompleted)
            coordinator.OnOwnerReleased(instigator, FlowControl.CancellationActivation.Owner is null);
    }

    internal void OnCancellationReadFrontier()
        => Volatile.Read(ref _cancellation)?.OnReadFrontier();

    void LeaveCancellationReadFrontier(PgClientFlow flow)
        => FlowControl.ClearCancellationReadFrontier(flow);

    internal bool HasPriorCancellationExposure(PgClientFlow flow, int window)
        => Volatile.Read(ref _cancellation)?.HasPriorExposure(flow, window) is true;

    void OnCancellationHeartbeat(TimeSpan elapsed)
        => Volatile.Read(ref _cancellation)?.OnCancellationHeartbeat(elapsed);

    void OnFlowActivated(PgClientFlow flow)
    {
        var coordinator = Volatile.Read(ref _cancellation);
        if (coordinator is null)
            return;
        if (flow.GetExecutionControl(FlowControl).RfqCount > 0)
            coordinator.AssignBoundary(flow, flow.CancellationWindow);
        coordinator.OnOwnerActivated();
    }

    internal void AssignCancellationBoundary(PgClientFlow flow, int window)
        => Volatile.Read(ref _cancellation)?.AssignBoundary(flow, window);

    internal void ResumeCancellationOwner(PgClientFlow flow)
        => FlowControl.PublishCancellationActivation(flow);

    void OnCancellationWindowCompleted(PgClientFlow flow,
        int completedWindow, int remainingWindowCount)
        => Volatile.Read(ref _cancellation)?.OnWindowCompleted(
            flow, completedWindow, remainingWindowCount > 0);

    internal bool OnBackendCancellationObserved(PgClientFlow flow, int window)
        => Volatile.Read(ref _cancellation)?.OnCancellationObserved(flow, window) is true;

    void OnFlowReleased(PgClientFlow flow, bool wireIsIdle)
        => Volatile.Read(ref _cancellation)?.OnOwnerReleased(flow, wireIsIdle);

    void OnFlowSubstituted(PgClientFlow from, PgClientFlow to)
        => Volatile.Read(ref _cancellation)?.OnOwnerSubstituted(from, to, to.CancellationWindow);

    // Called by forceful shutdown while holding _syncRoot.
    void TerminateCancellationLocked()
        => _cancellation?.Terminate();

    /// Sends PostgreSQL's 16-byte side-channel cancellation message and waits for the server's FIN.
    internal static async ValueTask SendCancelRequestAsync(TransportConnection transport,
        int processId, int secretKey, CancellationToken cancellationToken = default)
    {
        // Unlike ordinary frontend messages, CancelRequest has no leading type byte. A fresh
        // connection supplies the framing context.
        var writer = transport.Writer;
        var span = writer.GetSpan(CancelRequestMessageLength);
        BinaryPrimitives.WriteInt32BigEndian(span, CancelRequestMessageLength);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(sizeof(int)), CancelRequestCode);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(sizeof(int) * 2), processId);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(sizeof(int) * 3), secretKey);
        writer.Advance(CancelRequestMessageLength);

        var flushResult = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        if (flushResult.IsCanceled)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new OperationCanceledException();
        }

        // PostgreSQL sends no in-band acknowledgement. Closing the side connection is the only
        // indication that it received and acted on the request.
        var reader = transport.Reader;
        while (true)
        {
            var readResult = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (readResult.IsCanceled)
            {
                cancellationToken.ThrowIfCancellationRequested();
                throw new OperationCanceledException();
            }
            reader.AdvanceTo(readResult.Buffer.End);
            if (readResult.IsCompleted)
                return;
        }
    }
}
