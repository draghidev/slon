namespace Slon.Pg.Protocol;

/// Raised when an operation observes that the <see cref="PgClientProtocol"/> has been closed.
/// Inherits from <see cref="InvalidOperationException"/> rather than
/// <see cref="OperationCanceledException"/>: closure is a permanent state of the resource, not a
/// cancellation event of a single operation. The cancellation token mechanism the framework uses
/// internally to propagate closure is just plumbing - flow bodies catch this type to identify the
/// cause without comparing tokens.
sealed class PgClientClosedException : InvalidOperationException
{
    public PgClientClosedException(Exception? closeReason = null)
        : base("The PgClient was closed.", closeReason) { }
}
