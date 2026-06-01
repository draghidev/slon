using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Slon.Pg.Protocol;

sealed class PostgresException(PgError error) : Exception
{
    internal PgError OriginalPgError { get; } = error;

    [DoesNotReturn]
    [StackTraceHidden]
    public static void Throw(PgError message) => throw new PostgresException(message);
}