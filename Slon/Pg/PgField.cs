using System.Diagnostics.CodeAnalysis;

namespace Slon.Pg;

interface IColumnLease
{
    void Revoke();
}

// A tenure-bound field handle. Consumers provide a decoder; this layer owns the reusable cursor
// and any view or column lease retained from it.
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct PgField(Row row, int ordinal)
{
    public ref readonly RowDescriptionField Metadata => ref row.GetFieldMetadata(ordinal);
    public bool IsPast => row.IsColumnPast(ordinal);

    public T Read<T, TDecoder, TState>(FieldReadMode mode, TState state)
        where TDecoder : IFieldDecoder<T, TState>
        => row.ReadField<T, TDecoder, TState>(ordinal, mode, state);

    public ValueTask<T> ReadAsync<T, TDecoder, TState>(FieldReadMode mode, TState state,
        CancellationToken cancellationToken = default)
        where TDecoder : IFieldDecoder<T, TState>
        => row.ReadFieldAsync<T, TDecoder, TState>(ordinal, mode, state, cancellationToken);

    public bool TryGetLease<T>([MaybeNullWhen(false)] out T lease)
    {
        if (row.GetColumnLease(ordinal) is T typed)
        {
            lease = typed;
            return true;
        }
        lease = default;
        return false;
    }
}
