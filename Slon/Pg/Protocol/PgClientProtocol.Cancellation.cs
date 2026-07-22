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
    // Wire-wide admission latch. It confines growth of the exposed prefix while delivery is
    // unresolved; bytes already written remain part of the cancellation exposure.
    TaskCompletionSource? _cancellationAttempt;

    internal bool HasPendingCancellation
        => Volatile.Read(ref _hasCancellationIntents) || Volatile.Read(ref _hasCancellationExposures);
    internal bool HasCancellationIntents => Volatile.Read(ref _hasCancellationIntents);
    internal bool HasUnassignedCancellationBoundary => Volatile.Read(ref _hasUnassignedCancellationBoundary);

    ValueTask WaitForCancellationAttempt()
    {
        lock (_syncRoot)
            return _cancellationAttempt is { } attempt ? new(attempt.Task) : default;
    }

    void ArmCancellationAttemptLocked()
        => _cancellationAttempt ??= new(TaskCreationOptions.RunContinuationsAsynchronously);

    void ReleaseCancellationAttemptLocked()
    {
        var attempt = _cancellationAttempt;
        _cancellationAttempt = null;
        attempt?.SetResult();
    }

    // Flow-owned request state. It ends when delivery becomes possible; any remaining connection-wide
    // reach is represented by a CancellationExposure instead.
    sealed class CancellationIntent(PgClientFlow instigator, Control control, PgClientFlow.BackendCancellationExtent extent, int window)
    {
        internal readonly PgClientFlow Instigator = instigator;
        internal readonly Control Control = control;
        internal CancellationIntent? Next;
        internal PgClientFlow.BackendCancellationExtent Extent = extent;
        internal int Window = window;
        internal bool Dispatching;
        internal bool AwaitingBoundary;
        // Caller cancellation waits until buffered input has been consumed. Timeouts bypass this
        // condition because their read cancellation has already established the failure boundary.
        internal bool RequiresCancellationReadFrontier;
        internal bool InstigatorCompleted;
        internal byte Attempts;
        internal long RemainingDelayTicks;
    }

    // Connection-owned residue from a Sent/Unknown request. It may outlive its instigating flow and is
    // retained until idle or an RFQ boundary proves the request can no longer strike later work.
    sealed class CancellationExposure(PgClientFlow instigator, Control control)
    {
        internal readonly PgClientFlow Instigator = instigator;
        internal readonly Control Control = control;
        internal CancellationExposure? Next;
        internal PgClientFlow? BoundaryFlow;
        internal int BoundaryWindow;
    }

    internal void RequestServerCancellation(PgClientFlow instigator, Control control,
        PgClientFlow.BackendCancellationExtent extent, int window, PgClientFlow.BackendCancellationTiming timing)
    {
        if (_options.CancelSender is null || _backendProcessId is 0)
            return;

        CancellationIntent? intent = null;
        var dispatch = false;
        lock (_syncRoot)
        {
            if (_status is not ProtocolStatus.Ready)
                return;
            for (var existing = _cancellationHead; existing is not null; existing = existing.Next)
            {
                if (ReferenceEquals(existing.Instigator, instigator) && ReferenceEquals(existing.Control, control))
                {
                    if (extent > existing.Extent)
                        existing.Extent = extent;
                    if (timing is PgClientFlow.BackendCancellationTiming.Immediate)
                    {
                        existing.RemainingDelayTicks = 0;
                        existing.RequiresCancellationReadFrontier = false;
                    }
                    dispatch = TryBeginCancellationDispatchLocked(existing);
                    intent = existing;
                    break;
                }
            }

            if (intent is null)
            {
                intent = new(instigator, control, extent, window);
                intent.RemainingDelayTicks = timing is PgClientFlow.BackendCancellationTiming.Immediate
                    ? 0 : GetCancelRequestDelayTicks();
                intent.RequiresCancellationReadFrontier = timing is PgClientFlow.BackendCancellationTiming.AfterGrace;
                if (_cancellationTail is null)
                    _cancellationHead = intent;
                else
                    _cancellationTail.Next = intent;
                _cancellationTail = intent;
                // Full-fence publication before TryBegin probes the read frontier. The reader
                // atomically publishes its frontier before probing this level.
                Interlocked.Exchange(ref _hasCancellationIntents, true);
                dispatch = TryBeginCancellationDispatchLocked(intent);
            }
        }
        if (dispatch)
            _ = DispatchCancellationAsync(intent!);
    }

    bool TryBeginCancellationDispatchLocked(CancellationIntent intent)
    {
        if (_status is not ProtocolStatus.Ready || _cancellationDispatching || intent.Dispatching
            || intent.AwaitingBoundary || intent.InstigatorCompleted || intent.Attempts >= 2
            || intent.RemainingDelayTicks > 0
            || intent.RequiresCancellationReadFrontier && !IsAtCancellationReadFrontierLocked(intent)
            || !ReferenceEquals(intent.Control.ActivatedFlow, intent.Instigator))
            return false;
        _cancellationDispatching = true;
        intent.Dispatching = true;
        intent.Attempts++;
        ArmCancellationAttemptLocked();
        return true;
    }

    bool IsAtCancellationReadFrontierLocked(CancellationIntent intent)
        => intent.Control.IsAtCancellationReadFrontier(intent.Instigator, intent.Window);

    internal void OnCancellationReadFrontier(Control control, PgClientFlow flow, int window)
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

    internal bool HasPriorCancellationExposure(Control control, PgClientFlow flow, int window)
    {
        if (!Volatile.Read(ref _hasCancellationExposures))
            return false;
        lock (_syncRoot)
        {
            for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
            {
                if (ReferenceEquals(exposure.Control, control)
                    && ReferenceEquals(exposure.BoundaryFlow, flow)
                    && exposure.BoundaryWindow == window
                    && !ReferenceEquals(exposure.Instigator, flow))
                    return true;
            }
        }
        return false;
    }

    long GetCancelRequestDelayTicks()
        => _options.CancelRequestDelay == Timeout.InfiniteTimeSpan
            ? long.MaxValue
            : Math.Max(0, _options.CancelRequestDelay.Ticks);

    void OnCancellationHeartbeat(TimeSpan elapsed)
    {
        if (!Volatile.Read(ref _hasCancellationIntents))
            return;
        CancellationIntent? dispatchIntent = null;
        lock (_syncRoot)
        {
            if (_cancellationDispatching)
                return;
            for (var intent = _cancellationHead; intent is not null; intent = intent.Next)
            {
                if (intent.RemainingDelayTicks is > 0 and < long.MaxValue)
                    intent.RemainingDelayTicks = Math.Max(0, intent.RemainingDelayTicks - elapsed.Ticks);
                if (TryBeginCancellationDispatchLocked(intent))
                {
                    dispatchIntent = intent;
                    break;
                }
            }
        }
        if (dispatchIntent is not null)
            _ = DispatchCancellationAsync(dispatchIntent);
    }

    void OnFlowActivated(Control control, PgClientFlow flow)
    {
        if (!HasPendingCancellation)
            return;
        CancellationIntent? dispatchIntent = null;
        lock (_syncRoot)
        {
            AssignCancellationBoundaryLocked(control, flow);
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
            // Release admission before processing the result. The request attempt is over; all
            // remaining state describes its possible reach and must not hold later writes.
            ReleaseCancellationAttemptLocked();
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
                if (intent.InstigatorCompleted || intent.Extent is PgClientFlow.BackendCancellationExtent.CurrentWindow)
                    RemoveCancellationIntentLocked(intent);
                else
                {
                    // The attempt has ended. An RFQ for this or a later window is therefore an
                    // ACK-relative frontier: the request cannot reach beyond it.
                    intent.Window = intent.Instigator.CancellationWindow;
                    intent.AwaitingBoundary = true;
                }
            }
        }
        TryDispatchNextCancellation();
    }

    void AddCancellationExposureLocked(CancellationExposure exposure)
    {
        if (exposure.Control.ActivatedFlow is { IsCompleted: false } boundary)
        {
            exposure.BoundaryFlow = boundary;
            exposure.BoundaryWindow = boundary.CancellationWindow;
        }
        if (_exposureTail is null)
            _exposureHead = exposure;
        else
            _exposureTail.Next = exposure;
        _exposureTail = exposure;
        Volatile.Write(ref _hasCancellationExposures, true);
        if (exposure.BoundaryFlow is null)
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
            AssignCancellationBoundaryLocked(control, flow);
        }
    }

    void AssignCancellationBoundaryLocked(Control control, PgClientFlow flow)
    {
        var anyUnassigned = false;
        for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
        {
            if (exposure.BoundaryFlow is not null)
                continue;
            if (ReferenceEquals(exposure.Control, control))
            {
                exposure.BoundaryFlow = flow;
                exposure.BoundaryWindow = flow.CancellationWindow;
            }
            else
                anyUnassigned = true;
        }
        Volatile.Write(ref _hasUnassignedCancellationBoundary, anyUnassigned);
    }

    void OnCancellationWindowCompleted(Control control, PgClientFlow flow, int completedWindow, int remainingRfqCount)
    {
        if (!HasPendingCancellation)
            return;
        CancellationIntent? dispatchIntent = null;
        lock (_syncRoot)
        {
            var exposure = _exposureHead;
            while (exposure is not null)
            {
                var next = exposure.Next;
                if (ReferenceEquals(exposure.Control, control)
                    && ReferenceEquals(exposure.BoundaryFlow, flow)
                    && completedWindow >= exposure.BoundaryWindow)
                    RemoveCancellationExposureLocked(exposure);
                exposure = next;
            }

            for (var intent = _cancellationHead; intent is not null; intent = intent.Next)
            {
                if (!ReferenceEquals(intent.Control, control) || !ReferenceEquals(intent.Instigator, flow)
                    || completedWindow < intent.Window)
                    continue;
                if (!intent.AwaitingBoundary)
                {
                    if (intent.Dispatching)
                        break;
                    if (remainingRfqCount is 0 || intent.Extent is PgClientFlow.BackendCancellationExtent.CurrentWindow)
                        RemoveCancellationIntentLocked(intent);
                    else
                        intent.Window = completedWindow + 1;
                    break;
                }
                if (remainingRfqCount is 0)
                    RemoveCancellationIntentLocked(intent);
                else
                {
                    intent.AwaitingBoundary = false;
                    intent.Window = completedWindow + 1;
                    intent.Attempts = 0;
                    intent.RemainingDelayTicks = GetCancelRequestDelayTicks();
                    intent.RequiresCancellationReadFrontier = true;
                    if (TryBeginCancellationDispatchLocked(intent))
                        dispatchIntent = intent;
                }
                break;
            }
        }
        if (dispatchIntent is not null)
            _ = DispatchCancellationAsync(dispatchIntent);
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
                        exposure.BoundaryWindow = 0;
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
                {
                    exposure.BoundaryFlow = to;
                    exposure.BoundaryWindow = to.CancellationWindow;
                }
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
