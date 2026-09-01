using System.Buffers;

namespace Slon.Pg.Protocol;

readonly struct CurrentMessageBuffer(
    ReadOnlySequence<byte> buffer, bool isComplete)
{
    public ReadOnlySequence<byte> Buffer { get; } = buffer;
    public bool IsComplete { get; } = isComplete;
}
