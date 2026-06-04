namespace Slon.Pg.Protocol;

// TODO need to track start and endtime every beat and pass elapsed instead of interval.
// TODO if elapsed > interval log a warning heartbeat can't keep up.
public sealed class Heartbeat : IDisposable
{
    readonly Lock _lock = new();
    readonly PeriodicTimer _timer;
    ActionNode? _actionHead;
    ActionNode? _actionTail;

    public Heartbeat(TimeSpan interval) : this(interval, TimeProvider.System) { }
    public Heartbeat(TimeSpan interval, TimeProvider timeProvider)
    {
        _timer = new(interval, timeProvider);
        // Task completes when timer is disposed.
        _ = DoHeartbeat();
        async Task DoHeartbeat()
        {
            // Dispose the timer when all the code consuming callbacks has completed
            using (_timer)
            {
                var tasks = new List<Task>();
                // The TimerAwaitable will return true until Stop is called
                while (await _timer.WaitForNextTickAsync().ConfigureAwait(false))
                {
                    var period = _timer.Period;
                    try
                    {
                        var next = _actionHead;
                        while (next is not null)
                        {
                            // TODO this smells, for flexibility the signature should be ValueTask, but there is no ValueTask.WhenAll.
                            // TODO even though single await (and continuation) IValueTaskSources would be perfectly compatible with WhenAny/All etc.
                            var task = next.Action(period).AsTask();
                            tasks.Add(task);
                            next = next.Next;
                        }
                        await Task.WhenAll(tasks).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        //TODO log
                    }

                    tasks.TrimExcess();
                    tasks.Clear();
                }
            }
        }
    }

    public void Change(TimeSpan interval = default)
    {
        _timer.Period = interval;
    }

    public void Register(Func<TimeSpan, ValueTask> action)
    {
        lock (_lock)
        {
            var node = new ActionNode(action);
            if (_actionTail is not null)
                _actionTail.Next = node;
            if (_actionHead is null)
                _actionHead = node;
            _actionTail = node;
        }
    }

    public void Dispose() => _timer.Dispose();

    sealed class ActionNode(Func<TimeSpan, ValueTask> action)
    {
        public Func<TimeSpan, ValueTask> Action { get; } = action;
        public ActionNode? Next { get; set; }
    }
}
