using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon;

static class AdoParameterTypeResolver
{
    internal static ParameterTypeResolution Resolve(object? value, PgSerializerOptions options,
        PgTypeId? preparedTypeId = null, bool allowUnspecified = false)
    {
        if (value is SlonParameter parameter)
        {
            var (dbType, valueType) = parameter.GetResolutionInput();
            if (allowUnspecified && dbType.IsInfer
                && (valueType is null || valueType == typeof(DBNull)))
                return default;

            PgTypeId? parameterTypeId = null;
            if (!dbType.IsInfer)
            {
                var dataTypeName = DataTypeName.CreateFullyQualifiedName(dbType.DataTypeName);
                if (dbType.ResolveMultirangeType)
                    dataTypeName = dataTypeName.ToDefaultMultirangeName();
                if (dbType.ResolveArrayType)
                    dataTypeName = dataTypeName.ToArrayName();
                parameterTypeId = dataTypeName;
            }

            var typeInfo = options.GetTypeInfo(valueType, parameterTypeId ?? preparedTypeId);
            if (parameterTypeId is not null && preparedTypeId is { } expectedTypeId
                && options.GetCanonicalTypeId(typeInfo.PgTypeId)
                    != options.GetCanonicalTypeId(expectedTypeId))
            {
                throw new InvalidOperationException(
                    $"Parameter type '{typeInfo.PgTypeId}' does not match prepared type '{expectedTypeId}'.");
            }
            return new(typeInfo);
        }

        if (allowUnspecified && value is null or DBNull)
            return default;

        return new(options.GetTypeInfo(value?.GetType(), preparedTypeId));
    }
}

readonly struct ParameterTypeResolution(PgTypeInfo? typeInfo)
{
    internal bool IsResolved => typeInfo is not null;
    internal PgTypeId PgTypeId => typeInfo?.PgTypeId ?? default;

    internal PgTypeInfo GetTypeInfo(int parameterIndex)
    {
        if (typeInfo is null)
            ThrowHelper.ThrowInvalidOperation(
                $"Parameter ${parameterIndex + 1} cannot be bound before PostgreSQL resolves its type.");
        return typeInfo;
    }
}
