namespace Slon.Pg;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public interface IFieldDecoder<T, TState>
{
    static abstract T Read(PgFieldReader reader, TState state);
    static abstract ValueTask<T> ReadAsync(PgFieldReader reader, TState state,
        CancellationToken cancellationToken = default);
}

[Flags]
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public enum FieldReadMode : byte
{
    None = 0,
    BufferedView = 1,
    SkipCleanupWhenBuffered = 2,
    ResultIsLease = 4
}
