using Slon.Pg.Types;

namespace Slon.Pg.Serialization;

/// <summary>
/// A resolved CLR/PostgreSQL type pairing. Resolution is datasource-scoped; execution captures
/// the containing serializer options so a catalog reload cannot move underneath a command.
/// </summary>
sealed class PgTypeInfo
{
    readonly BufferRequirements _binaryRequirements;
    readonly bool _descriptorIsInvariant;
    readonly bool _converterIsDbNullable;
    readonly DataFormat? _readFormat;

    internal PgTypeInfo(PgSerializerOptions options, PgConverter converter, PgTypeId pgTypeId,
        Type? requestedType = null, DataFormat? readFormat = DataFormat.Binary)
    {
        Options = options;
        Converter = converter;
        PgTypeId = options.GetCanonicalTypeId(pgTypeId);
        Type = ResolveType(converter.TypeToConvert, requestedType);

        var descriptor = converter.GetDescriptor(new() { ConversionContext = PgConversionContext.Empty });
        _descriptorIsInvariant = descriptor.IsInvariant;
        _binaryRequirements = descriptor.BufferRequirements;
        _converterIsDbNullable = converter.IsDbNullable;
        _readFormat = readFormat;
    }

    public PgSerializerOptions Options { get; }
    public PgConverter Converter { get; }
    public PgTypeId PgTypeId { get; }
    public Type Type { get; }
    public DataFormat PreferredFormat => DataFormat.Binary;

    public bool CanReadTo(Type type) => Type == type;

    public PgFieldBinding BindField(PgConversionContext conversionContext, DataFormat format)
    {
        if (_readFormat is { } supportedFormat && format != supportedFormat)
            throw new NotSupportedException($"Converter for type {Type} does not support {format} format.");

        var requirements = ResolveRequirements(conversionContext);
        return new(format, requirements.Read, Converter, _descriptorIsInvariant,
            Converter.RequiresReaderCleanup, Converter.ResultIsColumnLease);
    }

    public int BindParameterValue<T>(PgConversionContext conversionContext, T? value,
        out object? writeState)
    {
        var requirements = ResolveRequirements(conversionContext);
        writeState = null;
        try
        {
            if (_converterIsDbNullable && Converter.IsDbNull(value, writeState))
                return -1;

            var context = BindContext.CreateUnchecked(DataFormat.Binary, requirements.Write,
                requirements.IsBindOptional, conversionContext);
            var size = Converter.Bind(context, value, ref writeState);
            return size.Value;
        }
        catch
        {
            (var disposable, writeState) = (writeState, null);
            if (disposable is not null && disposable is IDisposable disposableState)
                disposableState.Dispose();
            throw;
        }
    }

    BufferRequirements ResolveRequirements(PgConversionContext conversionContext)
        => _descriptorIsInvariant
            ? _binaryRequirements
            : Converter.GetDescriptor(new() { ConversionContext = conversionContext }).BufferRequirements;

    static Type ResolveType(Type converterType, Type? requestedType)
    {
        if (requestedType is null || requestedType == converterType)
            return converterType;
        if (requestedType.IsEnum && requestedType.GetEnumUnderlyingType() == converterType)
            return requestedType;
        throw new ArgumentException(
            $"The requested type {requestedType} is incompatible with converter type {converterType}.",
            nameof(requestedType));
    }
}

readonly struct PgFieldBinding(DataFormat dataFormat, Size bufferRequirement, PgConverter converter,
    bool isBindingInvariant, bool requiresReaderCleanup, bool resultIsColumnLease)
{
    public DataFormat DataFormat { get; } = dataFormat;
    public Size BufferRequirement { get; } = bufferRequirement;
    public PgConverter Converter { get; } = converter;
    public bool IsBindingInvariant { get; } = isBindingInvariant;
    public bool RequiresReaderCleanup { get; } = requiresReaderCleanup;
    public bool ResultIsColumnLease { get; } = resultIsColumnLease;
}
