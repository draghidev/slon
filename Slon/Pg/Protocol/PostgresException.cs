using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Slon.Pg.Protocol;

sealed class PostgresException : Exception
{
    // Throwing is the escape boundary: the exception can propagate anywhere and be inspected long
    // after the message buffer is recycled, so it captures its message eagerly and Preserve()s the
    // underlying field bytes (one copy) up front, while the buffer is still valid. The field
    // accessors then decode lazily from that owned copy - safe to read from anywhere.
    internal PostgresException(PgError error) : base(BuildMessage(error))
        => OriginalPgError = error.Preserve();

    internal PgError OriginalPgError { get; }

    public string Severity => OriginalPgError.Severity;
    public string SqlState => OriginalPgError.SqlState;
    public string MessageText => OriginalPgError.MessageText;
    public string? Detail => OriginalPgError.Detail;
    public string? Hint => OriginalPgError.Hint;
    public int Position => OriginalPgError.Position;
    public string? Where => OriginalPgError.Where;
    public string? SchemaName => OriginalPgError.SchemaName;
    public string? TableName => OriginalPgError.TableName;
    public string? ColumnName => OriginalPgError.ColumnName;
    public string? ConstraintName => OriginalPgError.ConstraintName;
    public bool IsTransient => OriginalPgError.IsTransientError;

    // Eager: built at throw time while the buffer is valid, so Message renders anywhere the exception
    // travels. Replaces the base "Exception of type ... was thrown".
    // Shape: "FATAL: sorry, too many clients already (SQLSTATE 53300)".
    static string BuildMessage(PgError error)
    {
        var severity = error.Severity is { Length: > 0 } s ? s : "ERROR";
        var text = error.MessageText is { Length: > 0 } m
            ? m
            : "An error response was received from the PostgreSQL backend.";
        return error.SqlState is { Length: > 0 } code
            ? $"{severity}: {text} (SQLSTATE {code})"
            : $"{severity}: {text}";
    }

    [DoesNotReturn]
    [StackTraceHidden]
    public static void Throw(PgError error) => throw new PostgresException(error);
}
