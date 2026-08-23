using System.Buffers;
using System.Collections.Immutable;
using Slon.Pg.Types;

namespace Slon.Pg.Protocol;

// https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARAMETERDESCRIPTION
readonly struct ParameterDescriptionMessage
{
    public ParameterTypeList ParameterTypes { get; }

    ParameterDescriptionMessage(ParameterTypeList parameterTypes)
    {
        ParameterTypes = parameterTypes;
    }

    public static ParameterDescriptionMessage Create(in BackendMessage message)
    {
        message.EnsureExpected(PgTypes.BackendType.ParameterDescription);
        message.EnsureBuffered();

        var reader = message.BodyReader;
        reader.TryReadBigEndian(out short parameterCount);

        var builder = ImmutableArray.CreateBuilder<PgTypeId>(parameterCount);
        for (var i = 0; i < parameterCount; i++)
        {
            reader.TryReadBigEndian(out int oid);
            builder.Add((Oid)unchecked((uint)oid));
        }

        return new(new ParameterTypeList(builder.MoveToImmutable()));
    }
}
