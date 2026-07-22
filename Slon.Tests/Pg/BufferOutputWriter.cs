using System.Buffers;
using Slon.Buffers;

namespace Slon.Tests.Pg;

sealed class BufferOutputWriter : IOutputWriter
{
    readonly ArrayBufferWriter<byte> _writer = new();
    int _flushed;

    public long UnflushedBytes => _writer.WrittenCount - _flushed;

    public void Advance(int count) => _writer.Advance(count);
    public Memory<byte> GetMemory(int sizeHint = 0) => _writer.GetMemory(sizeHint);
    public Span<byte> GetSpan(int sizeHint = 0) => _writer.GetSpan(sizeHint);

    public void Flush(TimeSpan timeout = default) => _flushed = _writer.WrittenCount;

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        Flush();
        return default;
    }

    public byte[] ToArray() => _writer.WrittenSpan.ToArray();
}
