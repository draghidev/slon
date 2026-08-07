using System.Buffers;

namespace Slon.Buffers;

// Incremental sequence input with explicit ownership release. Consumers retain Buffer only until
// AdvanceTo followed by the next read publishes a replacement window.
interface IInputReader
{
    ReadOnlySequence<byte> Buffer { get; }
    bool IsComplete { get; }
    void AdvanceTo(SequencePosition consumed, long consumedLength);
    void Read();
    ValueTask ReadAsync(CancellationToken cancellationToken = default);
}
