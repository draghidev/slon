using System.Runtime.CompilerServices;

namespace Slon.Pg.Serialization;

struct PgSerializerFieldReader
{
    const int MaxRetainedBindingCapacity = 256;

    PgSerializerOptions? _options;
    PgConversionContext _conversionContext = PgConversionContext.Empty;
    RowDescription? _rowDescription;
    ReadFieldBinding[] _bindings = [];
    int _bindingCount;
    internal PgSerializerFieldReader(PgSerializerOptions options)
    {
        _options = options;
        _conversionContext = options.ConversionContext;
    }

    internal void Initialize(CommandResult result)
    {
        _rowDescription = result.GetMetadata().RowDescription;
        if (_bindings.Length > MaxRetainedBindingCapacity)
            _bindings = [];
        else
            _bindings.AsSpan(0, _bindingCount).Clear();
        _bindingCount = 0;
    }

    internal T Read<T>(Row row, int ordinal, bool sequential = false)
    {
        ref readonly var binding = ref GetBinding<T>(ordinal);
        return Read<T>(new PgField(row, ordinal), in binding, _conversionContext, sequential);
    }

    internal ValueTask<T> ReadAsync<T>(Row row, int ordinal,
        CancellationToken cancellationToken = default)
        => ReadAsync<T>(new PgField(row, ordinal), GetBinding<T>(ordinal), _conversionContext,
            cancellationToken);

    internal object ReadObject(Row row, int ordinal)
    {
        _ = GetField(ordinal);
        return row.IsDBNull(ordinal) ? DBNull.Value : Read<object>(row, ordinal);
    }

    internal ValueTask<object> ReadObjectAsync(Row row, int ordinal,
        CancellationToken cancellationToken = default)
    {
        _ = GetField(ordinal);
        ref readonly var binding = ref GetBinding<object>(ordinal);
        return ReadObjectAsync(row.IsDBNullAsync(ordinal, cancellationToken),
            new(row, ordinal), binding, _conversionContext, cancellationToken);

        static async ValueTask<object> ReadObjectAsync(ValueTask<bool> isDbNull,
            PgField field, PgFieldBinding binding, PgConversionContext conversionContext,
            CancellationToken cancellationToken)
            => await isDbNull.ConfigureAwait(false)
                ? DBNull.Value
                : await ReadAsync<object>(field, binding, conversionContext, cancellationToken)
                    .ConfigureAwait(false);
    }

    internal string GetDataTypeName(int ordinal)
        => GetOptions().GetDataTypeName(GetField(ordinal).TypeOid).DisplayName;

    internal Type GetFieldType(int ordinal)
        => GetOptions().GetTypeInfo(type: null, GetField(ordinal).TypeOid).Type;

    internal SlonDbType GetSlonDbType(int ordinal)
        => new(GetOptions().GetDataTypeName(GetField(ordinal).TypeOid));

    internal RowDescription RowDescription
        => _rowDescription
            ?? throw new InvalidOperationException("The current result has no row description.");

    ref readonly PgFieldBinding GetBinding<T>(int ordinal)
    {
        var requestedType = typeof(T);
        if ((uint)ordinal < (uint)_bindingCount)
        {
            ref var cached = ref _bindings[ordinal];
            if (ReferenceEquals(cached.RequestedType, requestedType))
                return ref cached.Binding;
        }
        return ref ResolveBinding(ordinal, requestedType);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    ref readonly PgFieldBinding ResolveBinding(int ordinal, Type requestedType)
    {
        ref readonly var field = ref GetField(ordinal);
        if (_bindings.Length <= ordinal)
            Array.Resize(ref _bindings, _rowDescription!.FieldCount);
        _bindingCount = Math.Max(_bindingCount, ordinal + 1);

        ref var cached = ref _bindings[ordinal];
        var format = field.Format is PgFormat.Binary ? DataFormat.Binary : DataFormat.Text;
        var info = GetOptions().GetTypeInfo(requestedType, field.TypeOid, format);
        cached = new(requestedType, info.BindField(_conversionContext, format));
        return ref cached.Binding;
    }

    ref readonly RowDescriptionField GetField(int ordinal)
    {
        var description = _rowDescription
            ?? throw new InvalidOperationException("The current result has no row description.");
        if (description.FieldCount is 0)
            throw new InvalidOperationException("The current result has no columns.");
        return ref description[ordinal];
    }

    PgSerializerOptions GetOptions()
        => _options
            ?? throw new InvalidOperationException("No serializer was attached to this result.");

    static T Read<T>(PgField field, in PgFieldBinding binding,
        PgConversionContext conversionContext, bool sequential)
    {
        var mode = binding.Converter.IsReadViewBased
            ? FieldReadMode.BufferedView
            : binding.RequiresReaderCleanup ? FieldReadMode.None : FieldReadMode.SkipCleanupWhenBuffered;
        if (binding.ResultIsColumnLease)
        {
            if (field.TryGetLease<T>(out var existing))
                return existing;
            if (sequential && field.IsPast)
                throw new InvalidOperationException(
                    "Attempted to read a column preceding the sequential row cursor.");
            mode |= FieldReadMode.ResultIsLease;
        }
        return field.Read<T, SerializerDecoder<T>, SerializerReadState>(mode,
            new(binding.Converter, conversionContext, sequential));
    }

    static async ValueTask<T> ReadAsync<T>(PgField field, PgFieldBinding binding,
        PgConversionContext conversionContext, CancellationToken cancellationToken = default)
    {
        var mode = binding.Converter.IsReadViewBased
            ? FieldReadMode.BufferedView
            : binding.RequiresReaderCleanup ? FieldReadMode.None : FieldReadMode.SkipCleanupWhenBuffered;
        return await field.ReadAsync<T, SerializerDecoder<T>, SerializerReadState>(
            mode, new(binding.Converter, conversionContext), cancellationToken)
            .ConfigureAwait(false);
    }

    readonly record struct SerializerReadState(PgConverter Converter,
        PgConversionContext ConversionContext, bool Sequential = false);

    readonly struct SerializerDecoder<T> : IFieldDecoder<T, SerializerReadState>
    {
        public static T Read(PgFieldReader reader, SerializerReadState state)
            => state.Converter.Read<T>(new PgReader(reader, state.ConversionContext,
                state.Sequential));

        public static ValueTask<T> ReadAsync(PgFieldReader reader, SerializerReadState state,
            CancellationToken cancellationToken = default)
            => state.Converter.ReadAsync<T>(new PgReader(reader, state.ConversionContext,
                state.Sequential), cancellationToken);
    }

    struct ReadFieldBinding(Type requestedType, PgFieldBinding binding)
    {
        public Type RequestedType = requestedType;
        public PgFieldBinding Binding = binding;
    }
}
