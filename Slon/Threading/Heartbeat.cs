namespace Slon.Threading;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

sealed class Heartbeat : IDisposable
{
    internal static readonly TimeSpan DefaultInterval = TimeSpan.FromMilliseconds(250);

    // Threshold for flagging a tick as drifted. 1.5x the requested interval matches the
    // ecosystem norm (PeriodicTimer / Tokio's Delay MissedTickBehavior) - small drift below
    // this is normal scheduler jitter, above it indicates real backpressure on the timer
    // driver (TP saturation, GC pause, suspended process). The callback receives the
    // requested interval as the assumed-elapsed budget; consumers don't need precise elapsed
    // because their use case (activation timeout / read timeout countdown) tolerates the
    // small under-charge that drift implies, and the driver-level drift counter gives
    // operator visibility into when the heartbeat stops being a faithful clock.
    const double DriftThresholdMultiplier = 1.5;

    readonly Lock _lock = new();
    readonly PeriodicTimer _timer;
    readonly TimeProvider _timeProvider;
    readonly ILogger _logger;
    ActionNode? _actionHead;
    ActionNode? _actionTail;
    Action<TimeSpan, TimeSpan>? _onDrift;
    bool _disposed;

    /// Invoked when a tick arrives later than <see cref="DriftThresholdMultiplier"/> times the
    /// requested interval. Callback receives (requestedInterval, actualElapsed). Wire to a
    /// logger, metrics sink, or operator alert as the embedder prefers; null = no observation.
    /// Read via <see cref="Volatile.Read{T}(ref T)"/> per tick so a late-attached handler picks
    /// up on the next drifted tick without restarting the heartbeat.
    public Action<TimeSpan, TimeSpan>? OnDrift
    {
        get => Volatile.Read(ref _onDrift);
        set => Volatile.Write(ref _onDrift, value);
    }

    public Heartbeat(TimeSpan interval) : this(interval, TimeProvider.System, NullLogger.Instance) { }
    public Heartbeat(TimeSpan interval, TimeProvider timeProvider, ILogger? logger = null)
    {
        _timer = new(interval, timeProvider);
        _timeProvider = timeProvider;
        _logger = logger ?? NullLogger.Instance;
        // Task completes when timer is disposed.
        _ = DoHeartbeat();
        async Task DoHeartbeat()
        {
            // Dispose the timer when all the code consuming callbacks has completed
            using (_timer)
            {
                var tasks = new List<Task>();
                var actions = new List<Func<TimeSpan, ValueTask>>();
                // 0 = no previous tick observed yet; the first tick has no prior reference
                // to measure drift against, so we just stamp it and skip the check.
                long previousTickTimestamp = 0;
                // The TimerAwaitable will return true until Stop is called
                while (await _timer.WaitForNextTickAsync().ConfigureAwait(false))
                {
                    var period = _timer.Period;
                    var nowTimestamp = _timeProvider.GetTimestamp();
                    var previousTimestamp = previousTickTimestamp;
                    previousTickTimestamp = nowTimestamp;

                    try
                    {
                        // Snapshot registrations so callbacks execute without holding the lifetime
                        // lock. Unregistration can then physically unlink its node and release the
                        // captured connection immediately; a callback already in this tick's
                        // snapshot may finish once, as with any concurrent disposal boundary.
                        lock (_lock)
                        {
                            var next = _actionHead;
                            while (next is not null)
                            {
                                actions.Add(next.Action!);
                                next = next.Next;
                            }
                        }

                        foreach (var action in actions)
                        {
                            // TODO this smells, for flexibility the signature should be ValueTask, but there is no ValueTask.WhenAll.
                            // TODO even though single await (and continuation) IValueTaskSources would be perfectly compatible with WhenAny/All etc.
                            tasks.Add(action(period).AsTask());
                        }
                        await Task.WhenAll(tasks).ConfigureAwait(false);

                        // Drift reporting after the tick's actions: OnDrift is a best-effort observer
                        // and shares this catch, so a throwing handler can't starve the actions above
                        // or stop the clock. 0 = no prior tick to measure against.
                        if (previousTimestamp != 0)
                        {
                            var elapsed = _timeProvider.GetElapsedTime(previousTimestamp, nowTimestamp);
                            if (elapsed.TotalMilliseconds > period.TotalMilliseconds * DriftThresholdMultiplier
                                && OnDrift is { } handler)
                            {
                                handler(period, elapsed);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        SlonLogMessages.UnobservedCallbackException(
                            _logger, ex, "heartbeat tick");
                    }

                    actions.Clear();
                    tasks.Clear();
                }
            }
        }
    }

    public void Change(TimeSpan interval = default)
    {
        _timer.Period = interval;
    }

    public IDisposable Register(Func<TimeSpan, ValueTask> action)
    {
        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(Heartbeat));

            var node = new ActionNode(this, action) { Previous = _actionTail };
            if (_actionTail is not null)
                _actionTail.Next = node;
            else
                _actionHead = node;
            _actionTail = node;
            return node;
        }
    }

    void Unregister(ActionNode node)
    {
        lock (_lock)
        {
            if (!ReferenceEquals(node.Owner, this))
                return;

            if (node.Previous is { } previous)
                previous.Next = node.Next;
            else
                _actionHead = node.Next;

            if (node.Next is { } next)
                next.Previous = node.Previous;
            else
                _actionTail = node.Previous;

            node.Detach();
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed)
                return;

            _disposed = true;
            var next = _actionHead;
            while (next is not null)
            {
                var current = next;
                next = next.Next;
                current.Detach();
            }
            _actionHead = _actionTail = null;
        }

        _timer.Dispose();
    }

    sealed class ActionNode(Heartbeat owner, Func<TimeSpan, ValueTask> action) : IDisposable
    {
        public Heartbeat? Owner { get; private set; } = owner;
        public Func<TimeSpan, ValueTask>? Action { get; private set; } = action;
        public ActionNode? Previous { get; set; }
        public ActionNode? Next { get; set; }

        public void Dispose() => Owner?.Unregister(this);

        public void Detach()
        {
            Owner = null;
            Action = null;
            Previous = null;
            Next = null;
        }
    }

}
