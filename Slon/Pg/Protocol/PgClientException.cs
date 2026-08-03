namespace Slon.Pg.Protocol;

/// <summary>Base exception for failures produced by the PostgreSQL client.</summary>
public class PgClientException : IOException
{
    internal PgClientException(Exception innerException)
        : base($"The PostgreSQL client could not complete the operation. {innerException.Message}", innerException) { }
}
