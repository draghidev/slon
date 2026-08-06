using System.Text;
namespace Slon;

/// <summary>
/// Configures which connection state is reset when an exclusive scope is released.
/// </summary>
public sealed class ScopeResetOptions
{
    bool _closeCursors = true;
    bool _resetSessionAuthorization = true;
    bool _resetParameters = true;
    bool _clearListeners = true;
    bool _releaseAdvisoryLocks = true;
    bool _dropTemporaryObjects = true;
    string? _command;
    bool _commandResolved;

    /// <summary>Closes cursors left open by the scope.</summary>
    public bool CloseCursors
    {
        get => _closeCursors;
        set => Set(ref _closeCursors, value);
    }

    /// <summary>Restores the authenticated session identity and clears an active role.</summary>
    public bool ResetSessionAuthorization
    {
        get => _resetSessionAuthorization;
        set => Set(ref _resetSessionAuthorization, value);
    }

    /// <summary>Restores run-time parameters to their defaults.</summary>
    public bool ResetParameters
    {
        get => _resetParameters;
        set => Set(ref _resetParameters, value);
    }

    /// <summary>Removes asynchronous notification registrations.</summary>
    public bool ClearListeners
    {
        get => _clearListeners;
        set => Set(ref _clearListeners, value);
    }

    /// <summary>Releases session-level advisory locks.</summary>
    public bool ReleaseAdvisoryLocks
    {
        get => _releaseAdvisoryLocks;
        set => Set(ref _releaseAdvisoryLocks, value);
    }

    /// <summary>Drops temporary objects owned by the session.</summary>
    public bool DropTemporaryObjects
    {
        get => _dropTemporaryObjects;
        set => Set(ref _dropTemporaryObjects, value);
    }

    internal ScopeResetOptions Snapshot() => new()
    {
        CloseCursors = CloseCursors,
        ResetSessionAuthorization = ResetSessionAuthorization,
        ResetParameters = ResetParameters,
        ClearListeners = ClearListeners,
        ReleaseAdvisoryLocks = ReleaseAdvisoryLocks,
        DropTemporaryObjects = DropTemporaryObjects,
    };

    internal string? ResolveCommand()
    {
        if (_commandResolved)
            return _command;

        var command = new StringBuilder();
        Append(command, CloseCursors, "CLOSE ALL");
        Append(command, ResetSessionAuthorization, "SET SESSION AUTHORIZATION DEFAULT");
        Append(command, ResetParameters, "RESET ALL");
        Append(command, ClearListeners, "UNLISTEN *");
        Append(command, ReleaseAdvisoryLocks, "SELECT pg_advisory_unlock_all()");
        Append(command, DropTemporaryObjects, "DISCARD TEMP");

        _command = command.Length is 0 ? null : command.ToString();
        _commandResolved = true;
        return _command;
    }

    void Set(ref bool field, bool value)
    {
        if (field == value)
            return;
        field = value;
        _command = null;
        _commandResolved = false;
    }

    static void Append(StringBuilder command, bool enabled, string statement)
    {
        if (!enabled)
            return;
        if (command.Length is not 0)
            command.Append("; ");
        command.Append(statement);
    }
}
