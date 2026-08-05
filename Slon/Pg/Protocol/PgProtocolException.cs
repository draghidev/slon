namespace Slon.Pg.Protocol;

/// <summary>Raised when a PostgreSQL exchange violates protocol invariants.</summary>
public class PgProtocolException : IOException
{
    // Construction is protocol-owned. Recovery depends on the more precise internal subtype below,
    // not on every exchange-level protocol violation carried by this public type.
    internal PgProtocolException(string message)
        : base(message) { }

    internal PgProtocolException(string message, Exception innerException)
        : base(message, innerException) { }

    internal static PgProtocolException NotEnoughData(string? description = null)
        => new(description is null
            ? "The PostgreSQL message does not contain enough data."
            : $"The PostgreSQL message does not contain enough {description} data.");

    internal static PgProtocolException UnexpectedEof()
        => new PgFramingException("PostgreSQL closed the connection while a backend message was expected.",
            new EndOfStreamException());

    internal static PgProtocolException UnexpectedEof(EndOfStreamException innerException)
        => new PgFramingException("PostgreSQL closed the connection within a backend message.", innerException);
}

// The decoder can no longer trust its message boundary. Unlike an expectation mismatch in a flow,
// this cannot be recovered by reading toward RFQ because doing so may interpret message-body bytes.
sealed class PgFramingException : PgProtocolException
{
    internal PgFramingException(string message)
        : base(message) { }

    internal PgFramingException(string message, Exception innerException)
        : base(message, innerException) { }
}
