using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using Slon.Pg.Protocol;

namespace Slon;

/// <summary>Base exception for failures reported through Slon's ADO.NET surface.</summary>
public class SlonException : DbException
{
    internal SlonException(string message, Exception projectedException, Exception? innerException = null)
        : base(message, innerException)
    {
        ProjectedException = projectedException;
    }

    internal Exception ProjectedException { get; }

    /// <summary>The error reported by PostgreSQL, when this failure contains one.</summary>
    public PostgreSqlException? PostgreSqlError
        => this as PostgreSqlException ?? InnerException as PostgreSqlException;

    /// <summary>
    /// Whether this operation failed because another operation or PostgreSQL itself affected the shared session.
    /// </summary>
    public bool IsCollateral => ProjectedException is PgCollateralException;

    /// <inheritdoc />
    public override bool IsTransient => IsTransientFailure(ProjectedException);

    static bool IsTransientFailure(Exception exception) => exception switch
    {
        PgCollateralException { CollateralSource: PgCollateralSource.Cancellation } => true,
        PgErrorException error => error.IsTransient,
        PgProtocolException or PgClientClosedException => false,
        PgClientException { InnerException: { } cause } => IsTransientFailure(cause),
        IOException or SocketException or TimeoutException => true,
        _ => false
    };
}

/// <summary>Represents an error reported by PostgreSQL.</summary>
public sealed class PostgreSqlException : SlonException
{
    internal PostgreSqlException(PgErrorException error)
        : base(error.Message, error)
    {
        Severity = error.Severity;
        SqlState = error.SqlState;
        MessageText = error.MessageText;
        Detail = error.Detail;
        Hint = error.Hint;
        Position = error.Position;
        Where = error.Where;
        SchemaName = error.SchemaName;
        TableName = error.TableName;
        ColumnName = error.ColumnName;
        ConstraintName = error.ConstraintName;
        IsCollateralCancellation = error.IsCollateralCancellation;
        _isTransient = error.IsTransient;
    }

    readonly bool _isTransient;

    /// The localized or nonlocalized severity reported by PostgreSQL.
    public string Severity { get; }
    /// <inheritdoc />
    public override string SqlState { get; }
    /// The primary human-readable PostgreSQL error message.
    public string MessageText { get; }
    /// Optional detail about the error.
    public string? Detail { get; }
    /// Optional advice for resolving the error.
    public string? Hint { get; }
    /// The one-based character position of the error, or zero when PostgreSQL did not report one.
    public int Position { get; }
    /// Context describing where the error occurred.
    public string? Where { get; }
    /// The related schema name, when reported.
    public string? SchemaName { get; }
    /// The related table name, when reported.
    public string? TableName { get; }
    /// The related column name, when reported.
    public string? ColumnName { get; }
    /// The related constraint name, when reported.
    public string? ConstraintName { get; }
    /// Whether this cancellation error affected the operation collaterally.
    public bool IsCollateralCancellation { get; }
    /// <inheritdoc />
    public override bool IsTransient => _isTransient;
}

static class AdoException
{
    public static Exception Project(Exception exception)
    {
        if (exception is PostgreSqlException or SlonException
            or OperationCanceledException or ObjectDisposedException
            or ArgumentException or NotSupportedException)
            return exception;

        if (exception is PgErrorException error)
            return new PostgreSqlException(error);

        // The low-level drain preserves every PostgreSQL error. ADO follows the conventional
        // single-error surface and reports the first server failure.
        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions)
            {
                if (inner is PgErrorException pgError)
                    return new PostgreSqlException(pgError);
            }
        }

        return exception switch
        {
            PgCollateralException collateral => new SlonException(
                collateral.Message, collateral,
                collateral.InnerException is { } cause ? Project(cause) : null),
            PgClientException client => new SlonException(
                client.Message, client, client.InnerException),
            PgProtocolException protocol => new SlonException(
                protocol.Message, protocol, protocol),
            PgClientClosedException closed => new SlonException(
                closed.Message, closed, closed.InnerException),
            IOException io => new SlonException(io.Message, io, io.InnerException),
            _ => exception
        };
    }

    [DoesNotReturn]
    public static void Throw(Exception original)
    {
        var projected = Project(original);
        if (ReferenceEquals(projected, original))
            ExceptionDispatchInfo.Capture(original).Throw();
        throw projected;
    }
}
