namespace Slon.Pg.Protocol;

/// <summary>Raised when a PostgreSQL exchange violates protocol invariants.</summary>
public sealed class PgProtocolException : IOException
{
    internal PgProtocolException(string message)
        : base(message) { }

    internal PgProtocolException(string message, Exception innerException)
        : base(message, innerException) { }

    internal static PgProtocolException NotEnoughData(string? description = null)
        => new(description is null
            ? "The PostgreSQL message does not contain enough data."
            : $"The PostgreSQL message does not contain enough {description} data.");
}
