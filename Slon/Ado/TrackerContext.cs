using Slon.Pg;
using Slon.Text;

namespace Slon;

readonly struct TrackerContext
{
    readonly object _trackerOrConnection = null!;
    readonly object? _owningInstance;
    readonly TrackedCommand? _tracked;

    TrackerContext(TrackedCommand? tracked) => _tracked = tracked;

    TrackerContext(CommandTracker tracker, TrackedCommand? tracked)
        : this(tracked) => _trackerOrConnection = tracker;

    TrackerContext(CommandTracker tracker, object owningInstance)
    {
        _trackerOrConnection = tracker;
        _owningInstance = owningInstance;
    }

    TrackerContext(SlonConnection connection, TrackedCommand? tracked)
        : this(tracked)
        => _trackerOrConnection = connection;

    TrackerContext(SlonConnection connection, object owningInstance)
    {
        _trackerOrConnection = connection;
        _owningInstance = owningInstance;
    }

    public EncodedString CommandName => _tracked?.CommandName ?? default;

    public static TrackerContext Create(CommandTracker tracker, TrackedCommand? tracked)
        => new(tracker, tracked);

    public static TrackerContext Create(CommandTracker tracker, object owningInstance)
        => new(tracker, owningInstance);

    public static TrackerContext Create(SlonConnection connection, TrackedCommand? tracked)
        => new(connection, tracked);

    public static TrackerContext Create(SlonConnection connection, object owningInstance)
        => new(connection, owningInstance);

    public TrackerResult TrackCommand(string commandText, ParameterTypeList parameterTypes)
    {
        switch (_trackerOrConnection)
        {
            case SlonConnection connection:
                return connection.TrackCommand(
                    descriptor: CommandDescriptor.Create(commandText, parameterTypes, CommandName),
                    tracked: _tracked,
                    owningInstance: _owningInstance);
            case CommandTracker tracker:
                return tracker.Track(
                    descriptor: CommandDescriptor.Create(commandText, parameterTypes, CommandName),
                    tracked: _tracked,
                    owningInstance: _owningInstance);
        }
        return default;
    }
}
