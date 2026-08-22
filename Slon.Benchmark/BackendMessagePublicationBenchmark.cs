using System.Buffers;
using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Slon.Pg.Protocol;

namespace Slon.Benchmark;

[MemoryDiagnoser]
public class BackendMessagePublicationBenchmark
{
    const int MessageCount = 256;
    readonly BackendMessageContext _context = new();
    readonly byte[] _messages = CreateMessages();

    [Benchmark]
    public int PublishBufferedMessages()
    {
        _context.RetireCurrentBatch();
        _context.SetBatch(new BackendMessageBatch(new ReadOnlySequence<byte>(_messages)));
        var count = 0;
        while (_context.TryMoveNext())
            count++;
        return count;
    }

    static byte[] CreateMessages()
    {
        var messages = new byte[MessageCount * BackendHeader.ByteCount];
        for (var offset = 0; offset < messages.Length; offset += BackendHeader.ByteCount)
        {
            messages[offset] = (byte)PgTypes.BackendType.CommandComplete;
            BinaryPrimitives.WriteInt32BigEndian(messages.AsSpan(offset + 1), sizeof(int));
        }
        return messages;
    }
}
