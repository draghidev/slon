using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Slon.Pipelines;
using Slon.Buffers;

namespace Slon.Pipelines;

// IBufferWriter implementations like Pipes need a wrapper to support IOutputWriter.
// TODO can make AsStream return a sync write supporting stream, no need yet.
sealed class PipeStreamingWriter(PipeWriter pipeWriter) : PipeWriter, IOutputWriter<byte>
{
    public override void Advance(int count) => pipeWriter.Advance(count);
    public override Memory<byte> GetMemory(int sizeHint = 0) => pipeWriter.GetMemory(sizeHint);
    public override Span<byte> GetSpan(int sizeHint = 0) => pipeWriter.GetSpan(sizeHint);

    public override void Complete(Exception? exception = null) => pipeWriter.Complete(exception);
    public override ValueTask CompleteAsync(Exception? exception = null) => pipeWriter.CompleteAsync(exception);

    public override bool CanGetUnflushedBytes => pipeWriter.CanGetUnflushedBytes;
    public override long UnflushedBytes => pipeWriter.UnflushedBytes;

    public override void CancelPendingFlush() => pipeWriter.CancelPendingFlush();

    public FlushResult Flush(TimeSpan timeout = default)
    {
        if (pipeWriter is not StreamPipeWriter writer)
            throw new NotSupportedException("The underlying writer does not support sync operations.");

        return writer.Flush(timeout);
    }

    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        => pipeWriter.FlushAsync(cancellationToken);

    void IOutputWriter<byte>.Flush(TimeSpan timeout)
    {
        var result = Flush(timeout);
        if (result.IsCompleted)
            throw new InvalidOperationException("Other pipe end was already completed.");
        if (result.IsCanceled)
            throw new OperationCanceledException();
    }

    ValueTask IOutputWriter<byte>.FlushAsync(CancellationToken cancellationToken)
    {
        var flushTask = FlushAsync(cancellationToken);
        if (!flushTask.IsCompletedSuccessfully)
            return Core(flushTask);

        _ = flushTask.Result;
        return new();

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        static async ValueTask Core(ValueTask<FlushResult> flushTask)
        {
            var result = await flushTask.ConfigureAwait(false);
            if (result.IsCompleted)
                throw new InvalidOperationException("Other pipe end was already completed.");
            if (result.IsCanceled)
                throw new OperationCanceledException();
        }
    }
}
