namespace Slon.Runtime;

readonly struct Deadline
{
    readonly TimeSpan _timespan;
    readonly TimeProvider _timeProvider;
    readonly long _startTimestamp;

    // Default(TimeSpan) is the API sentinel for no deadline; an explicit negative value other
    // than Timeout.InfiniteTimeSpan remains invalid.
    public Deadline(TimeSpan value, TimeProvider? timeProvider = null)
    {
        if (value < Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(value), "Value must be -1 or non-negative.");

        _timespan = value == default ? Timeout.InfiniteTimeSpan : value;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _startTimestamp = _timespan == Timeout.InfiniteTimeSpan ? 0 : _timeProvider.GetTimestamp();
    }

    public TimeSpan TotalDuration => _timespan;

    public bool IsElapsed
        => _timespan != Timeout.InfiniteTimeSpan && _timeProvider.GetElapsedTime(_startTimestamp) >= _timespan;

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
            var elapsed = _timeProvider.GetElapsedTime(_startTimestamp);
            if (elapsed >= _timespan)
            {
                remaining = TimeSpan.Zero;
                return false;
            }
            remaining = _timespan - elapsed;
        }
        else
        {
            remaining = Timeout.InfiniteTimeSpan;
        }
        return true;
    }

    public static Deadline None => new(Timeout.InfiniteTimeSpan);
}
