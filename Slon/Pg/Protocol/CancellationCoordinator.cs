namespace Slon.Pg.Protocol;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public enum CancelRequestState
{
    /// The attempt ended after PostgreSQL accepted the request connection.
    Sent,
    /// No request bytes could have reached PostgreSQL. Retry is safe.
    NotSent,
    /// The attempt ended, but request delivery cannot be excluded.
    Unknown
}

enum BackendCancellationTiming : byte
{
    /// Dispatch after the configured or flow-specific grace period.
    AfterGrace,
    /// Dispatch once the flow reaches its cancellation read frontier.
    AtReadFrontier,
    /// Dispatch immediately, or escalate an already-pending attempt.
    Immediate
}

/// Protocol-independent cancellation state machine. The host supplies opaque identities, progress
/// events, and one physical request operation. Once <c>RequestCancellation</c> is accepted, the
/// coordinator's timer guarantees convergence without further host cooperation.
//
// This type is deliberately substantial because one user cancellation creates several related but
// independently ending activities. The logical episode follows the user's requested scope across the
// affected pipelined commands, advancing from one command window to the next until that scope drains.
// A physical CancelRequest sender has its own tenure because its socket operation may still be pending
// after the logical request has progressed or completed. Until that operation settles, no newer sender
// may take its gate and its late continuation must not mutate newer state. A request also leaves an
// attribution record: PostgreSQL may report its cancellation against the requested command or a
// successor, and one request can even deliver two SIGINTs due to historical reasons.
// That record therefore follows only the bounded set of command windows the request could have reached,
// then retires so it cannot claim an unrelated future error.
// The coordinator also owns retry pacing and one absolute deadline across those activities.
// Keeping the transitions under one lock makes late sender completion, collateral
// strikes, recovery substitution, ReadyForQuery progress, and termination auditable as one machine.
// The coordinator instance is the wire boundary. TOwner remains opaque so the mechanism can be tested
// without a protocol or flow runtime. Live activation/frontier queries resolve from the owner itself;
// internal pipeline/control boundaries never enter the cancellation state model.
// Host activation, frontier, and grace queries run under the private coordinator lock. They must be
// fast, non-blocking, non-reentrant, and non-throwing.
sealed class CancellationCoordinator<TOwner> : IDisposable
    where TOwner : class
{
    readonly Lock _lock = new();
    readonly TimeProvider _timeProvider;
    readonly TimeSpan _cancellationTimeout;
    readonly TimeSpan _cancellationRetryInterval;
    readonly TimeSpan _cancelRequestDelay;
    readonly CancellationToken _abortToken;
    readonly Func<CancellationToken, ValueTask<CancelRequestState>>? _cancelRequest;
    readonly Action<Exception> _fail;
    readonly Action<Exception, CancelRequestState>? _requestFailed;
    readonly Func<TOwner, (TOwner? Owner, int Window)> _getActivation;
    readonly Func<TOwner, int, bool> _isAtReadFrontier;
    readonly Func<TOwner, TimeSpan?> _getGracePeriod;
    CancellationEpisode? _episodeHead;
    CancellationIntent? _intentHead;
    CancellationIntent? _intentTail;
    CancellationExposure? _exposureHead;
    CancellationExposure? _exposureTail;
    DispatchLease? _activeDispatch;
    bool _hasCancellationEpisodes;
    bool _hasCancellationIntents;
    bool _hasCancellationExposures;
    bool _hasUnassignedCancellationBoundary;
    bool _terminated;
    ITimer? _deadlineTimer;
    CancellationEpisode? _armedDeadlineEpisode;

    internal CancellationCoordinator(TimeProvider timeProvider,
        TimeSpan cancellationTimeout, TimeSpan cancellationRetryInterval,
        TimeSpan cancelRequestDelay, CancellationToken abortToken,
        Func<CancellationToken, ValueTask<CancelRequestState>>? cancelRequest,
        Action<Exception> fail, Func<TOwner, (TOwner? Owner, int Window)> getActivation,
        Func<TOwner, int, bool> isAtReadFrontier,
        Func<TOwner, TimeSpan?> getGracePeriod,
        Action<Exception, CancelRequestState>? requestFailed = null)
    {
        _timeProvider = timeProvider;
        _cancellationTimeout = cancellationTimeout;
        _cancellationRetryInterval = cancellationRetryInterval;
        _cancelRequestDelay = cancelRequestDelay;
        _abortToken = abortToken;
        _cancelRequest = cancelRequest;
        _fail = fail;
        _getActivation = getActivation;
        _isAtReadFrontier = isAtReadFrontier;
        _getGracePeriod = getGracePeriod;
        _requestFailed = requestFailed;
    }

    internal bool HasPendingCancellation
        => Volatile.Read(ref _hasCancellationEpisodes)
            || Volatile.Read(ref _hasCancellationIntents);
    internal bool HasRetainedAttribution
        => Volatile.Read(ref _hasCancellationExposures);
    bool HasTrackedCancellation
        => HasPendingCancellation || HasRetainedAttribution;
    internal bool HasCancellationIntents => Volatile.Read(ref _hasCancellationIntents);

    internal string DescribeState()
    {
        lock (_lock)
        {
            var intents = new List<string>();
            for (var intent = _intentHead; intent is not null; intent = intent.Next)
                intents.Add(intent.Describe(ReferenceEquals(_activeDispatch?.Intent, intent)));

            var exposures = new List<string>();
            for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
                exposures.Add(exposure.Describe(ReferenceEquals(_activeDispatch?.Exposure, exposure)));

            return $"dispatching={_activeDispatch is not null}, " +
                   $"intents=[{string.Join("; ", intents)}], exposures=[{string.Join("; ", exposures)}]";
        }
    }

    internal ValueTask WaitForCancellationAttempt()
    {
        lock (_lock)
            return _activeDispatch is { } lease
                ? lease.WaitForCompletion(_abortToken)
                : default;
    }

    internal void RequestCancellation(TOwner origin,
        int window, BackendCancellationTiming timing, TaskCompletionSource? delivery,
        object key, bool continuesAcrossWindows, BackendCancellationTiming subsequentTiming)
    {
        CancellationAdmission admission;
        lock (_lock)
            admission = AdmitCancellationLocked(origin, window, timing, delivery,
                key, continuesAcrossWindows, subsequentTiming);

        admission.Delivery?.TrySetResult();
        admission.StopLease?.Stop();
        if (admission.AbortReason is not null)
            _fail(admission.AbortReason);
        else if (admission.DispatchLease is not null)
            StartDispatch(admission.DispatchLease);
    }

    CancellationAdmission AdmitCancellationLocked(TOwner origin,
        int window, BackendCancellationTiming timing, TaskCompletionSource? delivery,
        object key, bool continuesAcrossWindows, BackendCancellationTiming subsequentTiming)
    {
        if (_terminated)
            return new(Delivery: delivery);

        TaskCompletionSource? completedDelivery = null;
        var episode = FindCancellationEpisodeLocked(key);
        if (episode is null)
        {
            episode = new(key, origin, window, continuesAcrossWindows,
                _timeProvider.GetTimestamp(), delivery);
            episode.SubsequentTiming = subsequentTiming;
            episode.Next = _episodeHead;
            _episodeHead = episode;
            Volatile.Write(ref _hasCancellationEpisodes, true);
            EnsureCancellationDeadlineTimerLocked();
        }
        else
        {
            completedDelivery = episode.AttachDelivery(delivery);
            episode.ContinuesAcrossWindows |= continuesAcrossWindows;
            if (subsequentTiming > episode.SubsequentTiming)
                episode.SubsequentTiming = subsequentTiming;
            if (window > episode.CurrentWindow)
                AdvanceEpisodeWindowLocked(episode, window);
        }

        if (_cancelRequest is null)
        {
            RearmCancellationDeadlineLocked();
            return new(Delivery: completedDelivery ?? episode.DetachDelivery());
        }

        // Publish the obligation before probing live host state or stopping the wire-wide sender.
        // A different episode can own that sender, and this episode must remain dispatchable after
        // the old tenure settles.
        var requiresReadFrontier = episode.TransportAttempts > 0
            || timing is not BackendCancellationTiming.Immediate;
        var intent = FindCancellationIntentLocked(episode, window);
        var gracePeriod = timing is BackendCancellationTiming.AfterGrace
            ? _getGracePeriod(episode.ProgressOwner)
            : null;
        var delay = GetCancellationDelayTicks(timing, gracePeriod);
        if (intent is null && !episode.ScopeComplete && !episode.WindowAcknowledged)
        {
            intent = new(episode, window, delay, requiresReadFrontier);
            AppendIntentLocked(intent);
        }
        else
        {
            intent?.TightenDispatchTiming(delay, requiresReadFrontier);
        }

        var escalation = timing is BackendCancellationTiming.Immediate
            ? TryEscalateLocked(episode)
            : default;
        var dispatchLease = escalation.AbortReason is null && escalation.StopLease is null
            && intent is not null
                ? TryBeginCancellationDispatchLocked(intent)
                : null;
        RearmCancellationDeadlineLocked();
        return new(dispatchLease, escalation.StopLease, escalation.AbortReason, completedDelivery);
    }

    CancellationEscalation TryEscalateLocked(CancellationEpisode episode)
    {
        if (episode.WindowAcknowledged)
            return default;
        if (_activeDispatch is { } active)
            return active.TryRequestStop()
                ? new(StopLease: active)
                : new(AbortReason: CreateCancellationConvergenceException(episode,
                    "the wire-wide cancellation sender remained pending after cancellation was requested"));
        return CountWindowReservationsLocked(episode, episode.CurrentWindow) >= 2
            ? new(AbortReason: CreateCancellationConvergenceException(episode,
                "two cancellation requests did not reach a safe protocol boundary"))
            : default;
    }

    CancellationEpisode? FindCancellationEpisodeLocked(object key)
    {
        for (var episode = _episodeHead; episode is not null; episode = episode.Next)
        {
            if (ReferenceEquals(episode.Key, key))
                return episode;
        }
        return null;
    }

    CancellationIntent? FindCancellationIntentLocked(CancellationEpisode episode, int window)
    {
        for (var intent = _intentHead; intent is not null; intent = intent.Next)
        {
            if (intent.Matches(episode, window))
                return intent;
        }
        return null;
    }

    DispatchLease? TryBeginCancellationDispatchLocked(CancellationIntent intent)
    {
        var episode = intent.Episode;
        if (_activeDispatch is not null
            || episode.ScopeComplete || episode.DeadlineClaimed
            || episode.WindowAcknowledged
            || CountWindowReservationsLocked(episode, episode.CurrentWindow) >= 2
            || !intent.IsDispatchReady(_isAtReadFrontier(
                episode.ProgressOwner, episode.CurrentWindow))
            || !ReferenceEquals(_getActivation(episode.ProgressOwner).Owner, episode.ProgressOwner)
            || _cancelRequest is null)
            return null;

        episode.TransportAttempts++;
        var exposure = intent.CreateExposure();
        var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_abortToken);
        var lease = new DispatchLease(intent, exposure, cancellation);
        _activeDispatch = lease;
        AppendExposureLocked(exposure);
        return lease;
    }

    internal void OnReadFrontier()
    {
        TryDispatchNextCancellation();
    }

    internal bool HasPriorExposure(TOwner owner, int window)
    {
        if (!Volatile.Read(ref _hasCancellationExposures))
            return false;
        lock (_lock)
        {
            for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
            {
                if (exposure.CanReach(owner, window, IsPendingDispatchLocked(exposure))
                    && !ReferenceEquals(exposure.Episode.Origin, owner))
                    return true;
            }
        }
        return false;
    }

    static long NormalizeDelayTicks(TimeSpan delay)
        => delay == Timeout.InfiniteTimeSpan
            ? long.MaxValue
            : Math.Max(0, delay.Ticks);

    long GetCancellationDelayTicks(BackendCancellationTiming timing, TimeSpan? gracePeriod)
    {
        if (timing is not BackendCancellationTiming.AfterGrace)
            return 0;
        return NormalizeDelayTicks(gracePeriod ?? _cancelRequestDelay);
    }

    internal void OnCancellationHeartbeat(TimeSpan elapsed)
    {
        if (!Volatile.Read(ref _hasCancellationIntents)
            && !Volatile.Read(ref _hasCancellationExposures))
            return;
        DispatchLease? dispatchLease = null;
        lock (_lock)
        {
            var exposure = _exposureHead;
            while (exposure is not null)
            {
                var next = exposure.Next;
                exposure.AdvanceIdleRetention(elapsed, IsPendingDispatchLocked(exposure));
                TryRetireExposureLocked(exposure);
                exposure = next;
            }
            for (var intent = _intentHead; intent is not null; intent = intent.Next)
            {
                intent.AdvanceDelay(elapsed);
                dispatchLease ??= TryBeginCancellationDispatchLocked(intent);
            }
        }
        if (dispatchLease is not null)
            StartDispatch(dispatchLease);
    }

    internal void OnOwnerActivated()
        => TryDispatchNextCancellation();

    void StartDispatch(DispatchLease lease)
    {
        // Sender delegates may execute arbitrary synchronous work before returning their ValueTask.
        // The queued callback claims physical start immediately before invocation, so neither a
        // dormant work item nor blocked sender can occupy the coordinator lock or deadline thread.
        if (!ThreadPool.UnsafeQueueUserWorkItem(static state => state.Coordinator.InvokeDispatch(state.Lease),
                (Coordinator: this, Lease: lease), preferLocal: false))
            _ = ObserveDispatchAsync(lease, new(CancelRequestState.NotSent));
    }

    void InvokeDispatch(DispatchLease lease)
    {
        var start = false;
        lock (_lock)
            start = !_terminated && ReferenceEquals(_activeDispatch, lease) && lease.TryStart();
        if (!start)
        {
            _ = ObserveDispatchAsync(lease, new(CancelRequestState.NotSent));
            return;
        }

        ValueTask<CancelRequestState> request;
        try
        {
            request = _cancelRequest!(lease.SenderToken);
        }
        catch (Exception ex)
        {
            request = ValueTask.FromException<CancelRequestState>(ex);
        }
        _ = ObserveDispatchAsync(lease, request);
    }

    async Task ObserveDispatchAsync(DispatchLease lease, ValueTask<CancelRequestState> request)
    {
        CancelRequestState state;
        try
        {
            state = await request.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            state = CancelRequestState.Unknown;
            try
            {
                if (!lease.SenderToken.IsCancellationRequested)
                    _requestFailed?.Invoke(ex, state);
            }
            catch
            {
                // Observability must not retain the physical sender tenure.
            }
        }

        DispatchSettlement settlement;
        try
        {
            lock (_lock)
                settlement = SettleDispatchLocked(lease, state);
            settlement.Delivery?.TrySetResult();
        }
        finally
        {
            await lease.DisposeAsync().ConfigureAwait(false);
        }

        if (settlement.DriveNext)
            TryDispatchNextCancellation();
    }

    DispatchSettlement SettleDispatchLocked(DispatchLease lease, CancelRequestState state)
    {
        var exposure = lease.Exposure;
        if (!ReferenceEquals(_activeDispatch, lease))
        {
            if (state is CancelRequestState.NotSent)
                RemoveExposureLocked(exposure);
            return default;
        }

        var intent = lease.Intent;
        var episode = intent.Episode;
        var currentWindow = intent.IsForWindow(episode.CurrentWindow);
        _activeDispatch = null;
        lease.Complete();
        TaskCompletionSource? delivery = null;

        if (state is CancelRequestState.NotSent)
        {
            RemoveExposureLocked(exposure);
            if (!currentWindow || episode.ScopeComplete)
            {
                RemoveIntentLocked(intent);
                delivery = episode.DetachDelivery();
            }
            else
            {
                intent.ScheduleRetryAtFrontier(_cancellationRetryInterval);
                if (episode.TransportAttempts >= 2)
                    delivery = episode.DetachDelivery();
            }
        }
        else
        {
            var activation = _getActivation(episode.ProgressOwner);
            exposure.Seal(activation.Owner, activation.Window);
            delivery = episode.DetachDelivery();
            TryRetireExposureLocked(exposure);

            if (!currentWindow || episode.WindowAcknowledged
                || CountWindowReservationsLocked(episode, episode.CurrentWindow) >= 2
                || episode.ScopeComplete)
                RemoveIntentLocked(intent);
            else
                intent.ScheduleRetryAtFrontier(_cancellationRetryInterval);
        }

        if (episode.ScopeComplete)
            TryRemoveCompletedEpisodeLocked(episode);
        RearmCancellationDeadlineLocked();
        return new(DriveNext: true, Delivery: delivery);
    }

    void AppendIntentLocked(CancellationIntent intent)
    {
        if (_intentTail is null)
            _intentHead = intent;
        else
            _intentTail.Next = intent;
        _intentTail = intent;
        // Full-fence half of the intent/read-frontier missed-edge closure: publish the intent,
        // then probe the frontier; the decoder publishes the frontier, then probes this level.
        Interlocked.Exchange(ref _hasCancellationIntents, true);
    }

    void AppendExposureLocked(CancellationExposure exposure)
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
        DispatchLease? dispatchLease = null;
        lock (_lock)
        {
            if (_activeDispatch is null)
            {
                for (var intent = _intentHead; intent is not null; intent = intent.Next)
                {
                    if (TryBeginCancellationDispatchLocked(intent) is { } lease)
                    {
                        dispatchLease = lease;
                        break;
                    }
                }
            }
        }
        if (dispatchLease is not null)
            StartDispatch(dispatchLease);
    }

    internal void AssignBoundary(TOwner owner, int window)
    {
        if (!Volatile.Read(ref _hasUnassignedCancellationBoundary))
            return;
        lock (_lock)
        {
            AssignCancellationBoundaryLocked(owner, window);
        }
    }

    void AssignCancellationBoundaryLocked(TOwner owner, int window)
    {
        var anyUnassigned = false;
        for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
        {
            if (IsPendingDispatchLocked(exposure) || !exposure.HasBoundary)
                exposure.AssignBoundary(owner, window);
            anyUnassigned |= !exposure.HasBoundary;
        }
        Volatile.Write(ref _hasUnassignedCancellationBoundary, anyUnassigned);
    }

    internal void OnWindowCompleted(TOwner owner,
        int completedWindow, bool hasRemainingWindows)
    {
        if (!HasTrackedCancellation)
            return;
        TaskCompletionSource? delivery = null;
        DispatchLease? stopLease = null;
        lock (_lock)
        {
            var exposure = _exposureHead;
            while (exposure is not null)
            {
                var next = exposure.Next;
                exposure.ObserveWindowCompleted(owner, completedWindow);
                TryRetireExposureLocked(exposure);
                exposure = next;
            }

            for (var episode = _episodeHead; episode is not null; episode = episode.Next)
            {
                if (!ReferenceEquals(episode.ProgressOwner, owner)
                    || completedWindow < episode.CurrentWindow)
                    continue;

                if (_activeDispatch is { } active
                    && ReferenceEquals(active.Intent.Episode, episode)
                    && active.TryRequestStop())
                    stopLease = active;

                if (episode.ContinuesAcrossWindows && hasRemainingWindows)
                {
                    var nextWindow = completedWindow + 1;
                    AdvanceEpisodeWindowLocked(episode, nextWindow);
                    if (_cancelRequest is not null)
                    {
                        var gracePeriod = episode.SubsequentTiming is BackendCancellationTiming.AfterGrace
                            ? _getGracePeriod(episode.ProgressOwner)
                            : null;
                        var intent = new CancellationIntent(episode, episode.CurrentWindow,
                            GetCancellationDelayTicks(episode.SubsequentTiming, gracePeriod),
                            episode.SubsequentTiming is not BackendCancellationTiming.Immediate);
                        AppendIntentLocked(intent);
                    }
                }
                else
                {
                    RemoveEpisodeIntentsLocked(episode);
                    episode.ScopeComplete = true;
                    delivery = episode.DetachDelivery();
                    TryRemoveCompletedEpisodeLocked(episode);
                }
                break;
            }
            RearmCancellationDeadlineLocked();
        }
        delivery?.TrySetResult();
        stopLease?.Stop();
        TryDispatchNextCancellation();
    }

    int CountWindowReservationsLocked(CancellationEpisode episode, int window)
    {
        var count = 0;
        for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
        {
            if (ReferenceEquals(exposure.Episode, episode)
                && exposure.ReservesRequestFor(window, IsPendingDispatchLocked(exposure)))
                count++;
        }
        return count;
    }

    internal bool OnCancellationObserved(TOwner owner, int window)
    {
        if (!Volatile.Read(ref _hasCancellationExposures))
            return false;
        TaskCompletionSource? delivery = null;
        DispatchLease? stopLease = null;
        var acknowledged = false;
        lock (_lock)
        {
            CancellationExposure? match = null;
            for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
            {
                if (exposure.CanReach(owner, window, IsPendingDispatchLocked(exposure)))
                {
                    match = exposure;
                    break;
                }
            }
            if (match is not null)
            {
                delivery = AcknowledgeCancellationWindowLocked(owner, window,
                    out var acknowledgedEpisode);
                acknowledged = acknowledgedEpisode is not null;
                if (_activeDispatch is { } active
                    && (ReferenceEquals(active.Exposure, match)
                        || ReferenceEquals(active.Intent.Episode, acknowledgedEpisode))
                    && active.TryRequestStop())
                    stopLease = active;
                TryRetireExposureLocked(match);
                RearmCancellationDeadlineLocked();
            }
        }
        delivery?.TrySetResult();
        stopLease?.Stop();
        return acknowledged;
    }

    TaskCompletionSource? AcknowledgeCancellationWindowLocked(TOwner owner, int window,
        out CancellationEpisode? acknowledgedEpisode)
    {
        for (var episode = _episodeHead; episode is not null; episode = episode.Next)
        {
            if (!ReferenceEquals(episode.ProgressOwner, owner)
                || episode.CurrentWindow != window)
                continue;
            if (episode.WindowAcknowledged)
            {
                acknowledgedEpisode = episode;
                return null;
            }
            episode.WindowAcknowledged = true;
            RemoveEpisodeIntentsLocked(episode);
            if (ReferenceEquals(_armedDeadlineEpisode, episode))
                _armedDeadlineEpisode = null;
            acknowledgedEpisode = episode;
            return episode.DetachDelivery();
        }
        acknowledgedEpisode = null;
        return null;
    }

    internal void OnOwnerReleased(TOwner owner, bool wireIsIdle)
    {
        if (!HasTrackedCancellation)
            return;
        List<TaskCompletionSource>? deliveries = null;
        DispatchLease? stopLease = null;
        lock (_lock)
        {
            var episode = _episodeHead;
            while (episode is not null)
            {
                var next = episode.Next;
                if (!ReferenceEquals(episode.ProgressOwner, owner))
                {
                    episode = next;
                    continue;
                }
                if (_activeDispatch is { } active
                    && ReferenceEquals(active.Intent.Episode, episode)
                    && active.TryRequestStop())
                    stopLease = active;
                RemoveEpisodeIntentsLocked(episode);
                episode.ScopeComplete = true;
                if (episode.DetachDelivery() is { } delivery)
                    (deliveries ??= []).Add(delivery);
                TryRemoveCompletedEpisodeLocked(episode);
                episode = next;
            }

            if (wireIsIdle && _activeDispatch is { } idleActive
                && idleActive.TryRequestStop())
                stopLease = idleActive;

            var exposure = _exposureHead;
            while (exposure is not null)
            {
                var next = exposure.Next;
                if (wireIsIdle)
                {
                    // Structural idle closes the known command reach, but PostgreSQL may still deliver
                    // a CancelRequest's second SIGINT. Retain attribution for one pacing interval.
                    exposure.ObserveIdle(_cancellationRetryInterval);
                    if (exposure.ClearBoundary(owner))
                        Volatile.Write(ref _hasUnassignedCancellationBoundary, true);
                }
                else if (exposure.ClearBoundary(owner))
                {
                    Volatile.Write(ref _hasUnassignedCancellationBoundary, true);
                }
                exposure = next;
            }
            RearmCancellationDeadlineLocked();
        }
        if (deliveries is not null)
        {
            foreach (var delivery in deliveries)
                delivery.TrySetResult();
        }
        stopLease?.Stop();
    }

    internal void OnOwnerSubstituted(TOwner from, TOwner to, int window)
    {
        if (!HasTrackedCancellation)
            return;
        lock (_lock)
        {
            for (var episode = _episodeHead; episode is not null; episode = episode.Next)
            {
                if (ReferenceEquals(episode.ProgressOwner, from))
                {
                    episode.ProgressOwner = to;
                    episode.CurrentWindow = window;
                    for (var intent = _intentHead; intent is not null; intent = intent.Next)
                    {
                        if (ReferenceEquals(intent.Episode, episode) && !IsDispatchingLocked(intent))
                            intent.Retarget(episode.CurrentWindow);
                    }
                }
            }
            for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
            {
                exposure.RetargetBoundary(from, to, window);
            }
        }
        TryDispatchNextCancellation();
    }

    void TryRemoveCompletedEpisodeLocked(CancellationEpisode episode)
    {
        if (!episode.ScopeComplete
            || _activeDispatch is { } active
                && ReferenceEquals(active.Intent.Episode, episode))
            return;
        RemoveEpisodeLocked(episode);
    }

    bool IsDispatchingLocked(CancellationIntent intent)
        => ReferenceEquals(_activeDispatch?.Intent, intent);

    bool IsPendingDispatchLocked(CancellationExposure exposure)
        => ReferenceEquals(_activeDispatch?.Exposure, exposure);

    void AdvanceEpisodeWindowLocked(CancellationEpisode episode, int window)
    {
        RemoveEpisodeIntentsLocked(episode);
        episode.BeginWindow(window, _timeProvider.GetTimestamp());
    }

    void TryRetireExposureLocked(CancellationExposure exposure)
    {
        if (IsPendingDispatchLocked(exposure)
            || !exposure.ReachIsDead)
            return;
        RemoveExposureLocked(exposure);
    }

    void RemoveEpisodeIntentsLocked(CancellationEpisode episode)
    {
        var intent = _intentHead;
        while (intent is not null)
        {
            var next = intent.Next;
            if (ReferenceEquals(intent.Episode, episode) && !IsDispatchingLocked(intent))
                RemoveIntentLocked(intent);
            intent = next;
        }
    }

    void RemoveEpisodeLocked(CancellationEpisode removed)
    {
        CancellationEpisode? previous = null;
        for (var current = _episodeHead; current is not null; current = current.Next)
        {
            if (!ReferenceEquals(current, removed))
            {
                previous = current;
                continue;
            }
            if (previous is null)
                _episodeHead = current.Next;
            else
                previous.Next = current.Next;
            current.Next = null;
            if (ReferenceEquals(_armedDeadlineEpisode, current))
                _armedDeadlineEpisode = null;
            if (_episodeHead is null)
                Volatile.Write(ref _hasCancellationEpisodes, false);
            return;
        }
    }

    void RemoveIntentLocked(CancellationIntent removed)
    {
        CancellationIntent? previous = null;
        for (var current = _intentHead; current is not null; current = current.Next)
        {
            if (!ReferenceEquals(current, removed))
            {
                previous = current;
                continue;
            }
            if (previous is null)
                _intentHead = current.Next;
            else
                previous.Next = current.Next;
            if (ReferenceEquals(_intentTail, current))
                _intentTail = previous;
            current.Next = null;
            if (_intentHead is null)
                Volatile.Write(ref _hasCancellationIntents, false);
            return;
        }
    }

    void RemoveExposureLocked(CancellationExposure removed)
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
                anyUnassigned |= !remaining.HasBoundary;
            Volatile.Write(ref _hasUnassignedCancellationBoundary, anyUnassigned);
            if (_exposureHead is null)
                Volatile.Write(ref _hasCancellationExposures, false);
            return;
        }
    }

    void EnsureCancellationDeadlineTimerLocked()
    {
        _deadlineTimer ??= _timeProvider.CreateTimer(
            static state => ((CancellationCoordinator<TOwner>)state!).OnCancellationDeadline(),
            this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    void RearmCancellationDeadlineLocked()
    {
        if (_deadlineTimer is null)
            return;
        // Replacing this relative timer from a fresh elapsed-time sample can move the same absolute
        // deadline later if time advances (or the thread is preempted) between the sample and Change.
        // The current earliest episode owns the arm until it fires or leaves the episode set.
        if (_armedDeadlineEpisode is { } armed && IsDeadlineActiveLocked(armed))
            return;
        TimeSpan? earliest = null;
        CancellationEpisode? earliestEpisode = null;
        for (var episode = _episodeHead; episode is not null; episode = episode.Next)
        {
            if (!IsDeadlineActiveLocked(episode))
                continue;
            var elapsed = _timeProvider.GetElapsedTime(episode.WindowStartedTimestamp);
            var remaining = _cancellationTimeout - elapsed;
            if (remaining < TimeSpan.Zero)
                remaining = TimeSpan.Zero;
            if (earliest is null || remaining < earliest.Value)
            {
                earliest = remaining;
                earliestEpisode = episode;
            }
        }
        _armedDeadlineEpisode = earliestEpisode;
        _deadlineTimer.Change(
            earliest ?? Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    void OnCancellationDeadline()
    {
        Exception? reason = null;
        lock (_lock)
        {
            _armedDeadlineEpisode = null;
            for (var episode = _episodeHead; episode is not null; episode = episode.Next)
            {
                if (!IsDeadlineActiveLocked(episode)
                    || _timeProvider.GetElapsedTime(episode.WindowStartedTimestamp)
                        < _cancellationTimeout)
                    continue;
                episode.DeadlineClaimed = true;
                reason = CreateCancellationConvergenceException(episode,
                    "the cancellation convergence deadline elapsed");
                break;
            }
            RearmCancellationDeadlineLocked();
        }
        if (reason is not null)
            _fail(reason);
    }

    bool IsDeadlineActiveLocked(CancellationEpisode episode)
        => !episode.DeadlineClaimed
           && (!episode.WindowAcknowledged
               || _activeDispatch is { } active
                   && ReferenceEquals(active.Intent.Episode, episode));

    Exception CreateCancellationConvergenceException(CancellationEpisode episode, string reason)
    {
        var ambiguous = 0;
        for (var exposure = _exposureHead; exposure is not null; exposure = exposure.Next)
        {
            if (ReferenceEquals(exposure.Episode, episode)
                && exposure.Reserves(episode.CurrentWindow, IsPendingDispatchLocked(exposure)))
                ambiguous++;
        }
        return new TimeoutException($"PostgreSQL cancellation did not converge: {reason}; "
            + $"window={episode.CurrentWindow}, attempts={episode.TransportAttempts}, "
            + $"ambiguous={ambiguous}, acknowledged={episode.WindowAcknowledged}.");
    }

    // Forceful shutdown sets the terminal bit under the coordinator lock. A request that already
    // passed the protocol status check is rejected here before it can publish new state.
    internal void Terminate()
    {
        DispatchLease? stopLease = null;
        CancellationEpisode? episodes;
        lock (_lock)
        {
            _terminated = true;
            if (_activeDispatch is { } active && active.TryRequestStop())
                stopLease = active;
            episodes = _episodeHead;
            _activeDispatch?.Complete();
            _activeDispatch = null;
            _episodeHead = null;
            _intentHead = _intentTail = null;
            _exposureHead = _exposureTail = null;
            Volatile.Write(ref _hasCancellationEpisodes, false);
            Volatile.Write(ref _hasCancellationIntents, false);
            Volatile.Write(ref _hasCancellationExposures, false);
            Volatile.Write(ref _hasUnassignedCancellationBoundary, false);
            _armedDeadlineEpisode = null;
            _deadlineTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }
        for (var episode = episodes; episode is not null; episode = episode.Next)
            episode.DetachDelivery()?.TrySetResult();
        stopLease?.Stop();
    }

    public void Dispose()
    {
        Terminate();
        ITimer? timer;
        lock (_lock)
        {
            timer = _deadlineTimer;
            _deadlineTimer = null;
            _armedDeadlineEpisode = null;
        }
        timer?.Dispose();
    }

    readonly record struct CancellationAdmission(
        DispatchLease? DispatchLease = null,
        DispatchLease? StopLease = null,
        Exception? AbortReason = null,
        TaskCompletionSource? Delivery = null);

    readonly record struct DispatchSettlement(
        bool DriveNext = false,
        TaskCompletionSource? Delivery = null);

    readonly record struct CancellationEscalation(
        DispatchLease? StopLease = null,
        Exception? AbortReason = null);

    sealed class CancellationEpisode(object key, TOwner origin, int window,
        bool continuesAcrossWindows, long startedTimestamp, TaskCompletionSource? delivery)
    {
        TaskCompletionSource? _delivery = delivery;
        bool _deliveryCompleted;

        public object Key { get; } = key;
        public TOwner Origin { get; } = origin;
        public long WindowStartedTimestamp { get; private set; } = startedTimestamp;
        public TOwner ProgressOwner { get; set; } = origin;
        public CancellationEpisode? Next { get; set; }
        public bool ContinuesAcrossWindows { get; set; } = continuesAcrossWindows;
        public int CurrentWindow { get; set; } = window;
        public int TransportAttempts { get; set; }
        public bool WindowAcknowledged { get; set; }
        public bool ScopeComplete { get; set; }
        public bool DeadlineClaimed { get; set; }
        public BackendCancellationTiming SubsequentTiming { get; set; }
            = BackendCancellationTiming.AfterGrace;

        public void BeginWindow(int window, long startedTimestamp)
        {
            CurrentWindow = window;
            WindowStartedTimestamp = startedTimestamp;
            TransportAttempts = 0;
            WindowAcknowledged = false;
            DeadlineClaimed = false;
        }

        public TaskCompletionSource? AttachDelivery(TaskCompletionSource? delivery)
        {
            if (delivery is null || ReferenceEquals(_delivery, delivery))
                return null;
            if (_deliveryCompleted)
                return delivery;
            if (_delivery is not null)
                throw new InvalidOperationException("A cancellation episode has one delivery waiter.");
            _delivery = delivery;
            return null;
        }

        public TaskCompletionSource? DetachDelivery()
        {
            _deliveryCompleted = true;
            var delivery = _delivery;
            _delivery = null;
            return delivery;
        }
    }

    /// One owner-window dispatch obligation. Delivery and convergence deliberately have different
    /// lifetimes: the public delivery waiter can complete while the episode still owns a deadline.
    sealed class CancellationIntent(CancellationEpisode episode, int window,
        long dispatchDelayTicks, bool requiresCancellationReadFrontier)
    {
        int _window = window;
        long _dispatchDelayTicks = dispatchDelayTicks;
        bool _requiresCancellationReadFrontier = requiresCancellationReadFrontier;

        public CancellationEpisode Episode { get; } = episode;
        public CancellationIntent? Next { get; set; }

        public bool Matches(CancellationEpisode episode, int window)
            => ReferenceEquals(Episode, episode) && _window == window;

        public bool IsForWindow(int window)
            => _window == window;

        public CancellationExposure CreateExposure()
            => new(Episode, _window);

        public bool IsDispatchReady(bool atReadFrontier)
            => _dispatchDelayTicks <= 0
               && (!_requiresCancellationReadFrontier || atReadFrontier);

        public void TightenDispatchTiming(long delayTicks, bool requiresReadFrontier)
        {
            var improvesDelay = delayTicks < _dispatchDelayTicks;
            var relaxesFrontier = delayTicks == _dispatchDelayTicks
                && _requiresCancellationReadFrontier && !requiresReadFrontier;
            if (!improvesDelay && !relaxesFrontier)
                return;
            _dispatchDelayTicks = delayTicks;
            _requiresCancellationReadFrontier = requiresReadFrontier;
        }

        public void ScheduleRetryAtFrontier(TimeSpan delay)
        {
            _dispatchDelayTicks = delay.Ticks;
            _requiresCancellationReadFrontier = true;
        }

        public void Retarget(int window)
            => _window = window;

        public void AdvanceDelay(TimeSpan elapsed)
        {
            if (_dispatchDelayTicks is > 0 and < long.MaxValue)
                _dispatchDelayTicks = Math.Max(0, _dispatchDelayTicks - elapsed.Ticks);
        }

        public string Describe(bool dispatching)
            => $"window={_window},dispatching={dispatching},attempts={Episode.TransportAttempts}," +
               $"frontierRequired={_requiresCancellationReadFrontier},scopeComplete={Episode.ScopeComplete}," +
               $"delay={_dispatchDelayTicks}";
    }

    /// One physical CancelRequest's possible reach. It is registered before sender invocation and
    /// stays unsealed while the sender is pending. Three command-window positions cover its target
    /// plus both SIGINT deliveries when another request happens to satisfy the target first.
    sealed class CancellationExposure(CancellationEpisode episode, int requestedWindow)
    {
        TOwner? _boundaryOwner;
        int _boundaryWindow = requestedWindow;
        int _reachThroughWindow = checked(requestedWindow + 2);
        int _highestCompletedWindow = requestedWindow - 1;
        long _idleRetentionTicks = -1;

        public CancellationEpisode Episode { get; } = episode;
        public int RequestedWindow { get; } = requestedWindow;
        public CancellationExposure? Next { get; set; }
        public bool HasBoundary => _boundaryOwner is not null;
        public bool ReachIsDead
            => _idleRetentionTicks is 0 || _highestCompletedWindow >= _reachThroughWindow;

        public bool CanReach(TOwner owner, int window, bool pendingDispatch)
        {
            int firstReachableWindow;
            if (ReferenceEquals(_boundaryOwner, owner))
                firstReachableWindow = _boundaryWindow;
            else if (ReferenceEquals(Episode.Origin, owner)
                || ReferenceEquals(Episode.ProgressOwner, owner))
                firstReachableWindow = RequestedWindow;
            else
                return false;
            return window >= firstReachableWindow
                && (pendingDispatch || window <= _reachThroughWindow);
        }

        public bool Reserves(int window, bool pendingDispatch)
            => window >= RequestedWindow
               && (pendingDispatch || !ReachIsDead && window <= _reachThroughWindow);

        public bool ReservesRequestFor(int window, bool pendingDispatch)
            => RequestedWindow == window && Reserves(window, pendingDispatch);

        public void AssignBoundary(TOwner owner, int window)
        {
            var remainingReach = Math.Max(0, _reachThroughWindow - _highestCompletedWindow);
            _boundaryOwner = owner;
            _boundaryWindow = window;
            _reachThroughWindow = checked(window + remainingReach - 1);
            _highestCompletedWindow = window - 1;
            // New work turns a timed idle tombstone back into normal reach. Preserve its unused
            // command-window positions while rebasing owner-local window numbers.
            _idleRetentionTicks = -1;
        }

        public void ObserveWindowCompleted(TOwner owner, int window)
        {
            if (ReferenceEquals(_boundaryOwner, owner))
                _highestCompletedWindow = Math.Max(_highestCompletedWindow, window);
        }

        public void ObserveIdle(TimeSpan retention)
        {
            if (_idleRetentionTicks < 0)
                _idleRetentionTicks = retention.Ticks;
        }

        public void AdvanceIdleRetention(TimeSpan elapsed, bool pendingDispatch)
        {
            if (!pendingDispatch && _idleRetentionTicks > 0)
                _idleRetentionTicks = Math.Max(0, _idleRetentionTicks - elapsed.Ticks);
        }

        public bool ClearBoundary(TOwner owner)
        {
            if (!ReferenceEquals(_boundaryOwner, owner))
                return false;
            _boundaryOwner = null;
            return true;
        }

        public void RetargetBoundary(TOwner from, TOwner to, int window)
        {
            if (!ReferenceEquals(_boundaryOwner, from))
                return;
            AssignBoundary(to, window);
        }

        public void Seal(TOwner? activatedOwner, int activatedWindow)
        {
            if (activatedOwner is { } boundary)
            {
                _boundaryOwner = boundary;
                _boundaryWindow = activatedWindow;
            }
            else if (_boundaryOwner is null)
            {
                _boundaryOwner = Episode.ProgressOwner;
                _boundaryWindow = Episode.CurrentWindow;
            }
            // A pending sender has unsealed reach. Settlement anchors a fresh bounded span at the
            // activation it could most recently have affected.
            _reachThroughWindow = checked(_boundaryWindow + 2);
            _highestCompletedWindow = _boundaryWindow - 1;
        }

        public string Describe(bool pendingDispatch)
            => $"requested={RequestedWindow},boundary={_boundaryWindow},assigned={_boundaryOwner is not null}," +
               $"pending={pendingDispatch}";
    }

    /// Physical sender tenure. Only the current lease may release the wire-wide write gate. A stale
    /// continuation may remove its own NotSent exposure, but cannot mutate newer coordinator state.
    sealed class DispatchLease(CancellationIntent intent, CancellationExposure exposure,
        CancellationTokenSource cancellation) : IAsyncDisposable
    {
        TaskCompletionSource? _stopRequestCompletion;
        bool _started;
        readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CancellationIntent Intent { get; } = intent;
        public CancellationExposure Exposure { get; } = exposure;
        public CancellationToken SenderToken => cancellation.Token;

        public ValueTask WaitForCompletion(CancellationToken cancellationToken)
            => new(_completion.Task.WaitAsync(cancellationToken));

        public void Complete()
            => _completion.TrySetResult();

        public bool TryRequestStop()
        {
            // Stopping ends observation of this sender; it cannot recall CancelRequest bytes that
            // are already on the wire. The exposure therefore remains ambiguous until settlement.
            if (_stopRequestCompletion is not null)
                return false;
            _stopRequestCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            return true;
        }

        public bool TryStart()
        {
            if (_started || _stopRequestCompletion is not null)
                return false;
            _started = true;
            return true;
        }

        public void Stop()
        {
            try
            {
                cancellation.Cancel();
            }
            finally
            {
                _stopRequestCompletion!.TrySetResult();
            }
        }

        public ValueTask DisposeAsync()
        {
            if (_stopRequestCompletion is not { } completion)
            {
                cancellation.Dispose();
                return default;
            }
            return DisposeAfterStopAsync(completion.Task);

            async ValueTask DisposeAfterStopAsync(Task stopCompletion)
            {
                await stopCompletion.ConfigureAwait(false);
                cancellation.Dispose();
            }
        }
    }
}
