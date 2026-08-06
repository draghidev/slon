namespace Slon.Pg.Protocol;

/// <summary>Base exception for failures produced by the PostgreSQL client.</summary>
public class PgClientException : IOException
{
    internal const string Summary = "The PostgreSQL client could not complete the operation.";

    internal PgClientException(Exception innerException)
        : base(Summary, innerException) { }

    internal PgClientException(string message, Exception innerException)
        : base(message, innerException) { }
}
