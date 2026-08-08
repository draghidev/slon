using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
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

    /// <summary>Identifies the broad source of the ADO.NET failure.</summary>
    public SlonExceptionKind Kind => ProjectedException switch
    {
        PgErrorException => SlonExceptionKind.PostgreSqlError,
        PgCollateralException => SlonExceptionKind.Collateral,
        PgProtocolException => SlonExceptionKind.ProtocolFailure,
        PgClientClosedException => SlonExceptionKind.Closed,
        PgClientException => SlonExceptionKind.ClientFailure,
        _ => SlonExceptionKind.ClientFailure
    };

    public override bool IsTransient
        => ProjectedException is PgCollateralException { Kind: PgCollateralKind.Cancellation };
}

public enum SlonExceptionKind : byte
{
    ClientFailure,
    ProtocolFailure,
    Closed,
    Collateral,
    PostgreSqlError
}

/// <summary>Represents an error reported by PostgreSQL.</summary>
public sealed class PostgresException : SlonException
{
    internal PostgresException(PgErrorException error)
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
        if (exception is PostgresException or SlonException
            or OperationCanceledException or ObjectDisposedException
            or ArgumentException or NotSupportedException)
            return exception;

        if (exception is PgErrorException error)
            return new PostgresException(error);

        // The low-level drain preserves every PostgreSQL error. ADO follows the conventional
        // single-error surface and reports the first server failure.
        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions)
            {
                if (inner is PgErrorException pgError)
                    return new PostgresException(pgError);
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
                protocol.Message, protocol, protocol.InnerException),
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
