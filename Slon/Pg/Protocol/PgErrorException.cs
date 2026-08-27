using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Slon.Pg.Protocol;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed class PgErrorException : Exception
{
    // Throwing is the escape boundary: the exception can propagate anywhere and be inspected long
    // after the message buffer is recycled, so it captures its message eagerly and Preserve()s the
    // underlying field bytes (one copy) up front, while the buffer is still valid. The field
    // accessors then decode lazily from that owned copy - safe to read from anywhere.
    internal PgErrorException(PgError error) : base(BuildMessage(error))
        => Error = error.Preserve();

    public PgError Error { get; }

    public string Severity => Error.Severity;
    public string SqlState => Error.SqlState;
    public string MessageText => Error.MessageText;
    public string? Detail => Error.Detail;
    public string? Hint => Error.Hint;
    public int Position => Error.Position;
    public string? Where => Error.Where;
    public string? SchemaName => Error.SchemaName;
    public string? TableName => Error.TableName;
    public string? ColumnName => Error.ColumnName;
    public string? ConstraintName => Error.ConstraintName;
    public bool IsTransient => Error.IsTransientError;
    /// True when this operation was cancelled by a CancelRequest issued for an earlier
    /// pipelined operation on the same PostgreSQL connection.
    public bool IsCollateralCancellation => Error.IsCollateralCancellation;

    // Eager: built at throw time while the buffer is valid, so Message renders anywhere the exception
    // travels. Replaces the base "Exception of type ... was thrown".
    // Shape: "FATAL: sorry, too many clients already (SQLSTATE 53300)".
    static string BuildMessage(PgError error)
    {
        var severity = error.Severity is { Length: > 0 } s ? s : "ERROR";
        var text = error.MessageText is { Length: > 0 } m
            ? m
            : "An error response was received from the PostgreSQL backend.";
        var message = error.SqlState is { Length: > 0 } code
            ? $"{severity}: {text} (SQLSTATE {code})"
            : $"{severity}: {text}";
        return message;
    }

    [DoesNotReturn]
    [StackTraceHidden]
    internal static void Throw(PgError error) => throw Create(error);

    internal static Exception Create(PgError error)
    {
        var exception = new PgErrorException(error);
        if (error.IsCollateralCancellation)
            return new PgCollateralException(PgCollateralSource.Cancellation, exception);
        if (error.IsBackendTermination)
            return new PgCollateralException(PgCollateralSource.BackendTermination, exception);
        return exception;
    }
}
