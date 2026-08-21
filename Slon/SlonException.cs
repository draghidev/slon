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

    public string Severity { get; }
    public override string SqlState { get; }
    public string MessageText { get; }
    public string? Detail { get; }
    public string? Hint { get; }
    public int Position { get; }
    public string? Where { get; }
    public string? SchemaName { get; }
    public string? TableName { get; }
    public string? ColumnName { get; }
    public string? ConstraintName { get; }
    public bool IsCollateralCancellation { get; }
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
