using System.Buffers;
using System.Collections.Immutable;
using Slon.Pg.Protocol;
using Slon.Pg.Types;

namespace Slon.Pg;

static class ParameterDescription
{
    public static ParameterTypeList Parse(SequenceReader<byte> reader)
    {
        if (!reader.TryReadBigEndian(out short parameterCount) || parameterCount < 0)
            throw PgProtocolException.NotEnoughData(nameof(ParameterDescription));

        var builder = ImmutableArray.CreateBuilder<PgTypeId>(parameterCount);
        for (var i = 0; i < parameterCount; i++)
        {
            if (!reader.TryReadBigEndian(out int oid))
                throw PgProtocolException.NotEnoughData(nameof(ParameterDescription));
            builder.Add((Oid)unchecked((uint)oid));
        }

        if (reader.Remaining != 0)
            throw new PgProtocolException("ParameterDescription contains trailing data.");

        return new(builder.MoveToImmutable());
    }
}
