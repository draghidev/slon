using Slon.Runtime;

namespace Slon.Pg.Protocol;

// A one-shot transfer of an undispatched flow. The retired protocol retains only the terminal
// authority needed if replacement placement fails; it cannot otherwise control the flow again.
sealed class FlowMigration
{
    readonly Deadline _deadline;
    readonly PgClientFlow _flow;
    readonly PgClientFlow.ExecutionControl _priorControl;
    int _completed;

    internal FlowMigration(PgClientFlow flow, PgClientFlow.ExecutionControl priorControl,
        TimeProvider timeProvider)
    {
        _flow = flow;
        _priorControl = priorControl;
        _deadline = new(priorControl.RemainingActivationTimeout, timeProvider);
    }

    internal CancellationToken CancellationToken => _flow.MigrationCancellationToken;
    internal TimeSpan GetRemainingTimeout() => _deadline.GetRemaining();

    internal PgClientFlow PreparePlacement()
    {
        _flow.UpdatePendingTimeout(GetRemainingTimeout());
        return _flow;
    }

    internal bool CompletePlacement(bool placed)
    {
        if (!placed)
            return false;
        if (Interlocked.Exchange(ref _completed, 1) != 0)
            ThrowHelper.ThrowInvalidOperation("The inert flow migration was already completed.");
        return true;
    }

    internal void Fail(Exception exception)
    {
        if (Interlocked.Exchange(ref _completed, 1) == 0)
            _priorControl.FailUnstarted(exception);
    }
}
