namespace Slon.Pg.Protocol;

enum CancelRequestDelivery
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
    bool _cancellationDispatching;
    bool _hasCancellationIntents;
    bool _hasUnassignedCancellationBoundary;

    internal bool HasPendingCancellation => Volatile.Read(ref _hasCancellationIntents);
    internal bool HasUnassignedCancellationBoundary => Volatile.Read(ref _hasUnassignedCancellationBoundary);

    sealed class CancellationIntent(PgClientFlow instigator, Control control)
    {
        internal readonly PgClientFlow Instigator = instigator;
        internal readonly Control Control = control;
        internal CancellationIntent? Next;
        internal PgClientFlow? BoundaryFlow;
        internal bool Dispatching;
        internal bool MayHaveBeenDelivered;
        internal bool InstigatorCompleted;
        internal byte Attempts;
    }

    internal readonly struct CancellationRequester(Control control, PgClientFlow instigator)
    {
        public bool IsInitialized => control is not null;
        public void Request() => control.RequestServerCancellation(instigator);
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
            || intent.MayHaveBeenDelivered || intent.InstigatorCompleted || intent.Attempts >= 2
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
        CancelRequestDelivery delivery;
        try
        {
            delivery = await _options.CancelSender!(_backendProcessId, _backendSecretKey, AbortToken).ConfigureAwait(false);
        }
        catch
        {
            delivery = CancelRequestDelivery.Unknown;
        }

        lock (_syncRoot)
        {
            intent.Dispatching = false;
            _cancellationDispatching = false;
            if (delivery is CancelRequestDelivery.NotSent)
            {
                if (intent.InstigatorCompleted)
                    RemoveCancellationIntentLocked(intent);
            }
            else
            {
                intent.MayHaveBeenDelivered = true;
                if (intent.Control.IsIdle)
                    RemoveCancellationIntentLocked(intent);
                else
                    Volatile.Write(ref _hasUnassignedCancellationBoundary, true);
            }
        }
        TryDispatchNextCancellation();
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
        // Publication follows the completed delivery attempt; this flow's final RFQ bounds its reach.
        if (!Volatile.Read(ref _hasUnassignedCancellationBoundary))
            return;
        lock (_syncRoot)
        {
            var anyUnassigned = false;
            for (var intent = _cancellationHead; intent is not null; intent = intent.Next)
            {
                if (!intent.MayHaveBeenDelivered || intent.BoundaryFlow is not null)
                    continue;
                if (ReferenceEquals(intent.Control, control))
                    intent.BoundaryFlow = flow;
                else
                    anyUnassigned = true;
            }
            Volatile.Write(ref _hasUnassignedCancellationBoundary, anyUnassigned);
        }
    }

    void OnFlowRfq(Control control, PgClientFlow flow)
    {
        if (!Volatile.Read(ref _hasCancellationIntents))
            return;
        lock (_syncRoot)
        {
            var intent = _cancellationHead;
            while (intent is not null)
            {
                var next = intent.Next;
                if (intent.MayHaveBeenDelivered && ReferenceEquals(intent.Control, control)
                    && ReferenceEquals(intent.BoundaryFlow, flow))
                    RemoveCancellationIntentLocked(intent);
                intent = next;
            }
        }
    }

    void OnFlowCompleted(Control control, PgClientFlow flow, int remainingDepth)
    {
        if (!Volatile.Read(ref _hasCancellationIntents))
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
                    if (!intent.MayHaveBeenDelivered && !intent.Dispatching)
                    {
                        RemoveCancellationIntentLocked(intent);
                        intent = next;
                        continue;
                    }
                }
                if (intent.MayHaveBeenDelivered && ReferenceEquals(intent.Control, control))
                {
                    if (remainingDepth is 0)
                        RemoveCancellationIntentLocked(intent);
                    else if (ReferenceEquals(intent.BoundaryFlow, flow))
                    {
                        intent.BoundaryFlow = null;
                        Volatile.Write(ref _hasUnassignedCancellationBoundary, true);
                    }
                }
                intent = next;
            }
        }
    }

    void OnFlowSubstituted(Control control, PgClientFlow from, PgClientFlow to)
    {
        if (!Volatile.Read(ref _hasCancellationIntents))
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
                    if (!intent.MayHaveBeenDelivered && !intent.Dispatching)
                    {
                        RemoveCancellationIntentLocked(intent);
                        intent = next;
                        continue;
                    }
                }
                if (intent.MayHaveBeenDelivered && ReferenceEquals(intent.Control, control)
                    && ReferenceEquals(intent.BoundaryFlow, from))
                    intent.BoundaryFlow = to;
                intent = next;
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

            var anyUnassigned = false;
            for (var remaining = _cancellationHead; remaining is not null; remaining = remaining.Next)
                anyUnassigned |= remaining.MayHaveBeenDelivered && remaining.BoundaryFlow is null;
            Volatile.Write(ref _hasUnassignedCancellationBoundary, anyUnassigned);
            if (_cancellationHead is null)
                Volatile.Write(ref _hasCancellationIntents, false);
            return;
        }
    }
}
