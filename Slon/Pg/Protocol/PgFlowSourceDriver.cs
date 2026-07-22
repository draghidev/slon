using Draghi.Pipelining.Internal;

namespace Slon.Pg.Protocol;

/// <summary>
/// Serializes source-wait continuations and transfers the executor to a synchronous caller.
/// Notifications received while a continuation is active become one trailing drive.
/// </summary>
sealed class PgFlowSourceDriver
{
    readonly PgClientFlowSource.State _source;
    readonly SourceWakeEvent _wakeEvent;
    readonly Action _signalHeldSyncFlow;
    readonly Action _scheduleRun;

    bool _active;
    bool _redrive;
    bool _inlineOneShot;
    SourceWakeEvent.WaitClaim _claim;

    public PgFlowSourceDriver(PgClientFlowSource.State source, SourceWakeEvent wakeEvent)
    {
        _source = source;
        _wakeEvent = wakeEvent;
        _signalHeldSyncFlow = source.SignalHeldSyncFlow;
        _scheduleRun = ScheduleRun;
        wakeEvent.OnWaitReady = OnWaitReady;
    }

    public bool IsInlineOneShot => _inlineOneShot;

    public void Drive(bool runContinuationsAsynchronously)
    {
        var becameDriver = false;
        using (var claimScope = _wakeEvent.BeginClaim())
        {
            if (!CanDrive)
                return;

            if (_active)
            {
                _redrive = true;
            }
            else if (claimScope.TryClaim(out _claim))
            {
                _active = true;
                _inlineOneShot = !runContinuationsAsynchronously;
                becameDriver = true;
            }
        }

        if (!becameDriver)
            return;

        if (runContinuationsAsynchronously)
            _wakeEvent.Scheduler.SubmitDetached(static driver => driver.Run(), this);
        else
            Run();
    }

    public HandoffStatus TryClaimHandoff(
        PgClientFlow flow,
        ManualResetEventSlim handoffEvent,
        out HandoffClaim claim)
    {
        using var claimScope = _wakeEvent.BeginClaim();

        // The event is only an edge. Reset it inside the claim boundary, then consult and
        // transition the authoritative held-head state before the caller is allowed to park.
        handoffEvent.Reset();
        if (ReferenceEquals(_source.HeldSyncFlow, flow)
            && !_active
            && claimScope.TryClaim(out _claim))
        {
            _source.TakeoverPending = true;
            _source.SyncHeadReserved = false;
            _active = true;
            _inlineOneShot = true;
            claim = new(this);
            return HandoffStatus.Claimed;
        }

        claim = default;
        return Volatile.Read(ref _source.IsCompleted)
            ? HandoffStatus.Completed
            : HandoffStatus.Pending;
    }

    bool CanDrive => !_source.SyncHeadReserved || Volatile.Read(ref _source.IsCompleted);

    void OnWaitReady(SourceWakeEvent.WaitReadyContext context)
    {
        var held = _source.HeldSyncFlow;
        _source.SyncHeadReserved = held is not null;
        if (held is not null)
        {
            if (!_active)
                context.RunAfterWaitLock(_signalHeldSyncFlow);
            return;
        }

        if (!_source.HasItem())
            return;

        if (_active)
        {
            _redrive = true;
        }
        else if (context.TryClaim(out _claim))
        {
            _active = true;
            context.RunAfterWaitLock(_scheduleRun);
        }
    }

    void ScheduleRun()
        => _wakeEvent.Scheduler.SubmitDetached(static driver => driver.Run(), this);

    void Run()
    {
        while (true)
        {
            _claim.Dispatch(runContinuationsAsynchronously: false);

            var transfer = false;
            using (var claimScope = _wakeEvent.BeginClaim())
            {
                if (_inlineOneShot)
                {
                    _inlineOneShot = false;
                    transfer = _redrive && CanDrive && claimScope.TryClaim(out _claim);
                    if (transfer)
                        _redrive = false;
                    else
                        Relinquish();
                }
                else if (_redrive && CanDrive && claimScope.TryClaim(out _claim))
                {
                    _redrive = false;
                    continue;
                }
                else
                {
                    Relinquish();
                    if (!Volatile.Read(ref _source.IsCompleted))
                        _source.SignalHeldSyncFlow();
                    return;
                }
            }

            if (transfer)
                _wakeEvent.Scheduler.SubmitDetached(static driver => driver.Run(), this);
            return;
        }
    }

    void Relinquish()
    {
        _active = false;
        _redrive = false;
    }

    public enum HandoffStatus : byte
    {
        Pending,
        Claimed,
        Completed
    }

    public readonly struct HandoffClaim
    {
        readonly PgFlowSourceDriver? _driver;

        internal HandoffClaim(PgFlowSourceDriver driver) => _driver = driver;

        public void DispatchInline()
            => (_driver ?? throw new InvalidOperationException("No handoff was claimed.")).Run();
    }
}
