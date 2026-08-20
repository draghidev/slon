using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon;

// ADO adapter over the serializer-backed parameter writer. Only SlonParameters access and
// SlonParameter<T>'s typed dispatch remain here; binding tenure and converter execution are shared.
sealed class SerializerParameterWriter : PgSerializerParameterWriter<SlonParameters>
{
    public static SerializerParameterWriter Instance { get; } = new();

    SerializerParameterWriter() { }

    public override PgTypeId GetParameterType(SlonParameters source, int index)
        => source.GetResolvedParameterType(index);

    public override int GetParameterCount(SlonParameters source) => source.Count;

    protected override void ApplyParameter(SlonParameters source, int parameterIndex,
        PgParameterValueOperation operation)
    {
        source.GetResolvedParameter(parameterIndex, out var value, out var typeInfo);
        if (value is SlonParameter parameter)
        {
            parameter.Apply(typeInfo, operation);
            return;
        }

        operation.Apply(typeInfo, value);
    }
}
