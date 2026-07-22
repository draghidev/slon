namespace Slon.Pg.Protocol;

enum CancelRequestState
{
    // The attempt ended after PostgreSQL accepted the request connection.
    Sent,
    // No request bytes could have reached PostgreSQL; retry is safe.
    NotSent,
    // The attempt ended, but request delivery cannot be excluded.
    Unknown
}

sealed partial class PgClientProtocol
{
    CancellationIntent? _cancellationHead;
    CancellationIntent? _cancellationTail;
    CancellationExposure? _exposureHead;
    CancellationExposure? _exposureTail;
    bool _cancellationDispatching;
    bool _hasCancellationIntents;
    bool _hasCancellationExposures;
    bool _hasUnassignedCancellationBoundary;

    internal bool HasPendingCancellation
        => Volatile.Read(ref _hasCancellationIntents) || Volatile.Read(ref _hasCancellationExposures);
    internal bool HasUnassignedCancellationBoundary => Volatile.Read(ref _hasUnassignedCancellationBoundary);

    // Flow-owned request state. It ends when delivery becomes possible; any remaining connection-wide
    // reach is represented by a CancellationExposure instead.
    sealed class CancellationIntent(PgClientFlow instigator, Control control)
    {
        internal readonly PgClientFlow Instigator = instigator;
        internal readonly Control Control = control;
        internal CancellationIntent? Next;
        internal bool Dispatching;
        internal bool InstigatorCompleted;
        internal byte Attempts;
    }

    // Connection-owned residue from a Sent/Unknown request. It may outlive its instigating flow and is
    // retained until idle or an RFQ boundary proves the request can no longer strike later work.
    sealed class CancellationExposure(PgClientFlow instigator, Control control)
    {
        internal readonly PgClientFlow Instigator = instigator;
        internal readonly Control Control = control;
        internal CancellationExposure? Next;
        internal PgClientFlow? BoundaryFlow;
    }

    internal void RequestServerCancellation(PgClientFlow instigator, Control control)
    {
        if (_options.CancelSender is null || _backendProcessId is 0)
            return;

        CancellationIntent intent;
        bool dispatch;
        lock (_syncRoot)
        {
            if (_status is not ProtocolStatus.Ready)
                return;
            for (var existing = _cancellationHead; existing is not null; existing = existing.Next)
            {
                if (ReferenceEquals(existing.Instigator, instigator) && ReferenceEquals(existing.Control, control))
                    return;
            }

            intent = new(instigator, control);
            if (_cancellationTail is null)
                _cancellationHead = intent;
            else
                _cancellationTail.Next = intent;
            _cancellationTail = intent;
            Volatile.Write(ref _hasCancellationIntents, true);
            dispatch = TryBeginCancellationDispatchLocked(intent);
        }
        if (dispatch)
            _ = DispatchCancellationAsync(intent);
    }

    bool TryBeginCancellationDispatchLocked(CancellationIntent intent)
    {
        if (_status is not ProtocolStatus.Ready || _cancellationDispatching || intent.Dispatching
            || intent.InstigatorCompleted || intent.Attempts >= 2
            || !ReferenceEquals(intent.Control.ActivatedFlow, intent.Instigator))
            return false;
        _cancellationDispatching = true;
        intent.Dispatching = true;
        intent.Attempts++;
        return true;
    }

    void OnFlowActivated(Control control, PgClientFlow flow)
    {
        if (!Volatile.Read(ref _hasCancellationIntents))
            return;
        CancellationIntent? dispatchIntent = null;
        lock (_syncRoot)
        {
            if (!_cancellationDispatching)
            {
                for (var intent = _cancellationHead; intent is not null; intent = intent.Next)
                {
                    if (ReferenceEquals(intent.Control, control) && ReferenceEquals(intent.Instigator, flow)
                        && TryBeginCancellationDispatchLocked(intent))
                    {
                        dispatchIntent = intent;
                        break;
                    }
                }
            }
        }
        if (dispatchIntent is not null)
            _ = DispatchCancellationAsync(dispatchIntent);
    }

    async Task DispatchCancellationAsync(CancellationIntent intent)
    {
        CancelRequestState state;
        try
        {
            state = await _options.CancelSender!(_backendProcessId, _backendSecretKey, AbortToken).ConfigureAwait(false);
        }
        catch
        {
            state = CancelRequestState.Unknown;
        }

        lock (_syncRoot)
        {
            intent.Dispatching = false;
            _cancellationDispatching = false;
            if (state is CancelRequestState.NotSent)
            {
                if (intent.InstigatorCompleted)
                    RemoveCancellationIntentLocked(intent);
            }
            else
            {
                if (!intent.Control.IsIdle)
                    AddCancellationExposureLocked(new(intent.Instigator, intent.Control));
                RemoveCancellationIntentLocked(intent);
            }
        }
        TryDispatchNextCancellation();
    }

    void AddCancellationExposureLocked(CancellationExposure exposure)
    {
        if (_exposureTail is null)
            _exposureHead = exposure;
        else
            _exposureTail.Next = exposure;
        _exposureTail = exposure;
        Volatile.Write(ref _hasCancellationExposures, true);
        Volatile.Write(ref _hasUnassignedCancellationBoundary, true);
    }

    void TryDispatchNextCancellation()
    {
        CancellationIntent? dispatchIntent = null;
        lock (_syncRoot)
        {
            if (!_cancellationDispatching)
            {
                for (var intent = _cancellationHead; intent is not null; intent = intent.Next)
                {
                    if (TryBeginCancellationDispatchLocked(intent))
                    {
                        dispatchIntent = intent;
                        break;
                    }
                }
            }
        }
        if (dispatchIntent is not null)
            _ = DispatchCancellationAsync(dispatchIntent);
    }

    internal void AssignCancellationBoundary(Control control, PgClientFlow flow)
    {
        if (!Volatile.Read(ref _hasUnassignedCancellationBoundary))
            return;
        lock (_syncRoot)
        {
            var anyUnassigned = false;
            for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
            {
                if (exposure.BoundaryFlow is not null)
                    continue;
                if (ReferenceEquals(exposure.Control, control))
                    exposure.BoundaryFlow = flow;
                else
                    anyUnassigned = true;
            }
            Volatile.Write(ref _hasUnassignedCancellationBoundary, anyUnassigned);
        }
    }

    void OnFlowRfq(Control control, PgClientFlow flow)
    {
        if (!Volatile.Read(ref _hasCancellationExposures))
            return;
        lock (_syncRoot)
        {
            var exposure = _exposureHead;
            while (exposure is not null)
            {
                var next = exposure.Next;
                if (ReferenceEquals(exposure.Control, control) && ReferenceEquals(exposure.BoundaryFlow, flow))
                    RemoveCancellationExposureLocked(exposure);
                exposure = next;
            }
        }
    }

    void OnFlowCompleted(Control control, PgClientFlow flow, int remainingDepth)
    {
        if (!HasPendingCancellation)
            return;
        lock (_syncRoot)
        {
            var intent = _cancellationHead;
            while (intent is not null)
            {
                var next = intent.Next;
                if (ReferenceEquals(intent.Control, control) && ReferenceEquals(intent.Instigator, flow))
                {
                    intent.InstigatorCompleted = true;
                    if (!intent.Dispatching)
                        RemoveCancellationIntentLocked(intent);
                }
                intent = next;
            }

            var exposure = _exposureHead;
            while (exposure is not null)
            {
                var next = exposure.Next;
                if (ReferenceEquals(exposure.Control, control))
                {
                    if (remainingDepth is 0)
                        RemoveCancellationExposureLocked(exposure);
                    else if (ReferenceEquals(exposure.BoundaryFlow, flow))
                    {
                        exposure.BoundaryFlow = null;
                        Volatile.Write(ref _hasUnassignedCancellationBoundary, true);
                    }
                }
                exposure = next;
            }
        }
    }

    void OnFlowSubstituted(Control control, PgClientFlow from, PgClientFlow to)
    {
        if (!HasPendingCancellation)
            return;
        lock (_syncRoot)
        {
            var intent = _cancellationHead;
            while (intent is not null)
            {
                var next = intent.Next;
                if (ReferenceEquals(intent.Control, control) && ReferenceEquals(intent.Instigator, from))
                {
                    intent.InstigatorCompleted = true;
                    if (!intent.Dispatching)
                        RemoveCancellationIntentLocked(intent);
                }
                intent = next;
            }

            for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
            {
                if (ReferenceEquals(exposure.Control, control) && ReferenceEquals(exposure.BoundaryFlow, from))
                    exposure.BoundaryFlow = to;
            }
        }
    }

    void RemoveCancellationIntentLocked(CancellationIntent removed)
    {
        CancellationIntent? previous = null;
        for (var current = _cancellationHead; current is not null; current = current.Next)
        {
            if (!ReferenceEquals(current, removed))
            {
                previous = current;
                continue;
            }
            if (previous is null)
                _cancellationHead = current.Next;
            else
                previous.Next = current.Next;
            if (ReferenceEquals(_cancellationTail, current))
                _cancellationTail = previous;
            current.Next = null;
            if (_cancellationHead is null)
                Volatile.Write(ref _hasCancellationIntents, false);
            return;
        }
    }

    void RemoveCancellationExposureLocked(CancellationExposure removed)
    {
        CancellationExposure? previous = null;
        for (var current = _exposureHead; current is not null; current = current.Next)
        {
            if (!ReferenceEquals(current, removed))
            {
                previous = current;
                continue;
            }
            if (previous is null)
                _exposureHead = current.Next;
            else
                previous.Next = current.Next;
            if (ReferenceEquals(_exposureTail, current))
                _exposureTail = previous;
            current.Next = null;

            var anyUnassigned = false;
            for (var remaining = _exposureHead; remaining is not null; remaining = remaining.Next)
                anyUnassigned |= remaining.BoundaryFlow is null;
            Volatile.Write(ref _hasUnassignedCancellationBoundary, anyUnassigned);
            if (_exposureHead is null)
                Volatile.Write(ref _hasCancellationExposures, false);
            return;
        }
    }
}
