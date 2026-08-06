using System.Text;
using Slon.Pg;

namespace Slon;

/// <summary>
/// Configures which connection state is reset when an exclusive scope is released.
/// </summary>
public sealed class ScopeResetOptions
{
    /// <summary>Closes cursors left open by the scope.</summary>
    public bool CloseCursors { get; set; } = true;

    /// <summary>Restores the authenticated session identity and clears an active role.</summary>
    public bool ResetSessionAuthorization { get; set; } = true;

    /// <summary>Restores run-time parameters to their defaults.</summary>
    public bool ResetParameters { get; set; } = true;

    /// <summary>Removes asynchronous notification registrations.</summary>
    public bool ClearListeners { get; set; } = true;

    /// <summary>Releases session-level advisory locks.</summary>
    public bool ReleaseAdvisoryLocks { get; set; } = true;

    /// <summary>Drops temporary objects owned by the session.</summary>
    public bool DropTemporaryObjects { get; set; } = true;

    internal ScopeResetOptions Snapshot() => new()
    {
        CloseCursors = CloseCursors,
        ResetSessionAuthorization = ResetSessionAuthorization,
        ResetParameters = ResetParameters,
        ClearListeners = ClearListeners,
        ReleaseAdvisoryLocks = ReleaseAdvisoryLocks,
        DropTemporaryObjects = DropTemporaryObjects,
    };

    internal string? ResolveCommand(PgBackendCapabilities capabilities)
    {
        // A configured reset action whose capability is absent is deliberately omitted: dialect
        // compatibility may therefore provide weaker pool-reuse hygiene than PostgreSQL itself.
        var command = new StringBuilder();
        Append(command, CloseCursors && capabilities.SupportsCloseAll, "CLOSE ALL");
        Append(command, ResetSessionAuthorization && capabilities.SupportsSessionAuthorization,
            "SET SESSION AUTHORIZATION DEFAULT");
        Append(command, ResetParameters && capabilities.SupportsResetAll, "RESET ALL");
        Append(command, ClearListeners && capabilities.SupportsUnlisten, "UNLISTEN *");
        Append(command, ReleaseAdvisoryLocks && capabilities.SupportsAdvisoryLocks,
            "SELECT pg_advisory_unlock_all()");
        Append(command, DropTemporaryObjects && capabilities.SupportsDiscardTemp, "DISCARD TEMP");

        return command.Length is 0 ? null : command.ToString();
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
