namespace Slon;

readonly struct Deadline
{
    readonly TimeSpan _timespan;
    readonly long _startTicksMs;

    public Deadline(TimeSpan value)
    {
        if (value < Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(value), "Value must be -1 or non-negative.");

        _timespan = value == default ? Timeout.InfiniteTimeSpan : value;
        _startTicksMs = _timespan == Timeout.InfiniteTimeSpan ? 0 : Environment.TickCount64;
    }

    public TimeSpan TotalDuration => _timespan;

    public bool IsElapsed
        => _timespan != Timeout.InfiniteTimeSpan && Environment.TickCount64 - _startTicksMs >= _timespan.TotalMilliseconds;

    public TimeSpan GetRemaining()
    {
        if (!TryGetRemaining(out var remaining))
            throw new TimeoutException("The operation has timed out.");
        return remaining;
    }

    public bool TryGetRemaining(out TimeSpan remaining)
    {
        if (_timespan != Timeout.InfiniteTimeSpan)
        {
            var elapsed = Environment.TickCount64 - _startTicksMs;
            var totalMilliseconds = _timespan.TotalMilliseconds;
            if (elapsed >= totalMilliseconds)
            {
                remaining = TimeSpan.Zero;
                return false;
            }
            remaining = TimeSpan.FromMilliseconds(totalMilliseconds - elapsed);
        }
        else
        {
            remaining = Timeout.InfiniteTimeSpan;
        }
        return true;
    }

    public static Deadline None => new(Timeout.InfiniteTimeSpan);
}
