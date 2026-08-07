using System.Diagnostics.CodeAnalysis;
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

    internal PgTypeInfo(PgSerializerOptions options, PgConverter converter, PgTypeId pgTypeId,
        Type? requestedType = null)
    {
        Options = options;
        Converter = converter;
        PgTypeId = options.GetCanonicalTypeId(pgTypeId);
        Type = ResolveType(converter.TypeToConvert, requestedType);

        var descriptor = converter.GetDescriptor(new() { ConversionContext = PgConversionContext.Empty });
        _descriptorIsInvariant = descriptor.IsInvariant;
        _binaryRequirements = descriptor.BufferRequirements;
    }

    public PgSerializerOptions Options { get; }
    public PgConverter Converter { get; }
    public PgTypeId PgTypeId { get; }
    public Type Type { get; }
    public DataFormat PreferredFormat => DataFormat.Binary;

    public bool CanReadTo(Type type) => Type == type;

    public PgFieldBinding BindField(PgConversionContext conversionContext, DataFormat format)
    {
        if (format is not DataFormat.Binary)
            throw new NotSupportedException($"Converter for type {Type} does not support {format} format.");

        var requirements = ResolveRequirements(conversionContext);
        return new(format, requirements.Read, Converter, _descriptorIsInvariant);
    }

    public PgValueBinding BindParameterValue<T>(PgConversionContext conversionContext, T? value,
        object? writeState = null)
    {
        var requirements = ResolveRequirements(conversionContext);
        try
        {
            if (Converter.IsDbNull(value, writeState))
                return new(DataFormat.Binary, Size.Zero, null, writeState, Converter,
                    isBindingInvariant: true);

            var context = BindContext.CreateUnchecked(DataFormat.Binary, requirements.Write,
                requirements.IsBindOptional, conversionContext);
            var size = Converter.Bind(context, value, ref writeState);
            return new(DataFormat.Binary, requirements.Write, size, writeState, Converter,
                _descriptorIsInvariant && requirements.IsBindOptional);
        }
        catch
        {
            (var disposable, writeState) = (writeState, null);
            (disposable as IDisposable)?.Dispose();
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
    bool isBindingInvariant)
{
    public DataFormat DataFormat { get; } = dataFormat;
    public Size BufferRequirement { get; } = bufferRequirement;
    public PgConverter Converter { get; } = converter;
    public bool IsBindingInvariant { get; } = isBindingInvariant;
}

readonly struct PgValueBinding(DataFormat dataFormat, Size bufferRequirement, Size? size,
    object? writeState, PgConverter converter, bool isBindingInvariant)
{
    public DataFormat DataFormat { get; } = dataFormat;
    public Size BufferRequirement { get; } = bufferRequirement;
    public Size? Size { get; } = size;
    public object? WriteState { get; } = writeState;
    public PgConverter Converter { get; } = converter;
    public bool IsBindingInvariant { get; } = isBindingInvariant;

    [MemberNotNullWhen(false, nameof(Size))]
    public bool IsDbNullBinding => Size is null;
}
