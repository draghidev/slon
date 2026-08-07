namespace Slon.Pg.Serialization;

interface IFieldReader<T>
{
    T Read(PgField field);
    ValueTask<T> ReadAsync(PgField field, CancellationToken cancellationToken = default);
}

readonly struct PgSerializerFieldReader<T>(PgSerializerOptions options,
    PgConversionContext conversionContext) : IFieldReader<T>
{
    public T Read(PgField field)
    {
        ref readonly var metadata = ref field.Metadata;
        var info = options.GetTypeInfo(typeof(T), metadata.TypeOid);
        var format = metadata.Format is PgFormat.Binary ? DataFormat.Binary : DataFormat.Text;
        var binding = info.BindField(conversionContext, format);
        var reader = field.OpenReader(conversionContext);
        var leased = false;
        try
        {
            var result = binding.Converter.Read<T>(reader);
            if (reader.HasActiveView)
            {
                field.Lease(reader.ActiveViewLease);
                leased = true;
            }
            else
            {
                field.CompleteReader(reader);
            }
            return result;
        }
        finally
        {
            if (!leased)
                reader.Dispose();
        }
    }

    public async ValueTask<T> ReadAsync(PgField field, CancellationToken cancellationToken = default)
    {
        ref readonly var metadata = ref field.Metadata;
        var info = options.GetTypeInfo(typeof(T), metadata.TypeOid);
        var format = metadata.Format is PgFormat.Binary ? DataFormat.Binary : DataFormat.Text;
        var binding = info.BindField(conversionContext, format);
        var reader = await field.OpenReaderAsync(conversionContext, cancellationToken).ConfigureAwait(false);
        var leased = false;
        try
        {
            var result = await binding.Converter.ReadAsync<T>(reader, cancellationToken)
                .ConfigureAwait(false);
            if (reader.HasActiveView)
            {
                field.Lease(reader.ActiveViewLease);
                leased = true;
            }
            else
            {
                await field.CompleteReaderAsync(reader).ConfigureAwait(false);
            }
            return result;
        }
        finally
        {
            if (!leased)
                await reader.DisposeAsync().ConfigureAwait(false);
        }
    }
}
