using Microsoft.Extensions.Logging;
using Slon.Pg.Protocol;

namespace Slon;

static partial class SlonLogMessages
{
    [LoggerMessage(1, LogLevel.Error,
        "An exception thrown by {callback} could not be propagated to its caller.")]
    public static partial void UnobservedCallbackException(
        ILogger logger, Exception exception, string callback);

    [LoggerMessage(2, LogLevel.Error,
        "A background protocol operation failed after its caller had continued.")]
    public static partial void BackgroundProtocolOperationFailed(
        ILogger logger, Exception exception);

    [LoggerMessage(3, LogLevel.Trace,
        "Ignoring untracked PostgreSQL parameter status {parameterName}.")]
    public static partial void IgnoredParameterStatus(ILogger logger, string parameterName);

    [LoggerMessage(4, LogLevel.Error,
        "A completed flow violated a protocol invariant; the connection will be discarded.")]
    public static partial void ProtocolInvariantViolation(ILogger logger, Exception exception);

    [LoggerMessage(5, LogLevel.Error,
        "A connection failed while the pool was {operation} it.")]
    public static partial void PoolConnectionTeardownFailed(
        ILogger logger, Exception exception, string operation);

    [LoggerMessage(6, LogLevel.Debug,
        "A PostgreSQL side-channel cancellation request failed; delivery is {deliveryState}.")]
    public static partial void CancellationRequestFailed(
        ILogger logger, Exception exception, CancelRequestState deliveryState);

    [LoggerMessage(7, LogLevel.Warning,
        "OAuth token refresh failed; continuing with the previously cached token.")]
    public static partial void OAuthRefreshFailedUsingFallback(
        ILogger logger, Exception exception);

    [LoggerMessage(8, LogLevel.Warning,
        "PostgreSQL rejected best-effort connection maintenance: {messageText} (SQLSTATE {sqlState}).")]
    public static partial void MaintenanceCommandFailed(
        ILogger logger, string sqlState, string messageText);

    [LoggerMessage(9, LogLevel.Debug,
        "Best-effort transaction rollback during disposal failed.")]
    public static partial void TransactionDisposeRollbackFailed(
        ILogger logger, Exception exception);

    [LoggerMessage(10, LogLevel.Trace,
        "Sending PostgreSQL Terminate during graceful connection shutdown failed; closing the transport directly.")]
    public static partial void TerminateWriteFailed(ILogger logger, Exception exception);
}
