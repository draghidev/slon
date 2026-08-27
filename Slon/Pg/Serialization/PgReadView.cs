using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Slon.Pg.Serialization;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly ref struct PgReadView
{
    readonly ReadOnlySpan<byte> _bytes;

    internal PgReadView(ReadOnlySpan<byte> bytes) => _bytes = bytes;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadInt32() => BinaryPrimitives.ReadInt32BigEndian(_bytes);
}
