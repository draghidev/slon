using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Slon.Pg.Serialization;

namespace Slon.Pg;

interface IColumnLease
{
    int Revoke();
    ValueTask<int> RevokeAsync();
}

// A tenure-bound field handle. Strategies choose buffered access today; incremental cursor access
// can be added here without exposing the protocol reader or changing Row's generic dispatch seam.
readonly struct PgField(Row row, int ordinal)
{
    public ref readonly RowDescriptionField Metadata => ref row.GetFieldMetadata(ordinal);
    public bool IsPast => row.IsColumnPast(ordinal);

    public ReadOnlySequence<byte> GetBuffered() => row.GetBufferedField(ordinal);

    public ValueTask<ReadOnlySequence<byte>> GetBufferedAsync(CancellationToken cancellationToken = default)
        => row.GetBufferedFieldAsync(ordinal, cancellationToken);

    public PgReader OpenReader(PgConversionContext conversionContext)
        => row.OpenFieldReader(ordinal, conversionContext);

    public ValueTask<PgReader> OpenReaderAsync(PgConversionContext conversionContext,
        CancellationToken cancellationToken = default)
        => row.OpenFieldReaderAsync(ordinal, conversionContext, cancellationToken);

    public void CompleteReader(PgReader reader) => row.CompleteFieldReader(ordinal, reader);

    public ValueTask CompleteReaderAsync(PgReader reader)
        => row.CompleteFieldReaderAsync(ordinal, reader);

    public bool TryGetLease<T>([NotNullWhen(true)] out T? lease) where T : class, IColumnLease
        => row.TryGetColumnLease(ordinal, out lease);

    public void Lease(IColumnLease lease) => row.LeaseColumn(ordinal, lease);
}
