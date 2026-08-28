using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Slon.Buffers;

namespace Slon.Pipelines;

// IBufferWriter implementations like Pipes need a wrapper to support IOutputWriter.
// TODO can make AsStream return a sync write supporting stream, no need yet.
sealed class PipeOutputWriter(PipeWriter pipeWriter) : IOutputWriter
{
    public void Advance(int count) => pipeWriter.Advance(count);
    public Memory<byte> GetMemory(int sizeHint = 0) => pipeWriter.GetMemory(sizeHint);
    public Span<byte> GetSpan(int sizeHint = 0) => pipeWriter.GetSpan(sizeHint);
    public long UnflushedBytes => pipeWriter.UnflushedBytes;

    public FlushResult Flush(TimeSpan timeout = default)
    {
        if (pipeWriter is not StreamPipeWriter writer)
            throw new NotSupportedException("The underlying writer does not support sync operations.");

        return writer.Flush(timeout);
    }

    ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        => pipeWriter.FlushAsync(cancellationToken);

    void IOutputWriter.Flush(TimeSpan timeout)
    {
        var result = Flush(timeout);
        if (result.IsCompleted)
            throw new InvalidOperationException("Other pipe end was already completed.");
        if (result.IsCanceled)
            throw new OperationCanceledException();
    }

    ValueTask IOutputWriter.FlushAsync(CancellationToken cancellationToken)
    {
        var flushTask = FlushAsync(cancellationToken);
        if (!flushTask.IsCompletedSuccessfully)
            return Core(flushTask);

        EnsureFlushed(flushTask.Result);
        return new();

#if !NET11_0_OR_GREATER
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
#endif
        static async ValueTask Core(ValueTask<FlushResult> flushTask)
            => EnsureFlushed(await flushTask.ConfigureAwait(false));

        static void EnsureFlushed(FlushResult result)
        {
            if (result.IsCompleted)
                throw new InvalidOperationException("Other pipe end was already completed.");
            if (result.IsCanceled)
                throw new OperationCanceledException();
        }
    }
}
