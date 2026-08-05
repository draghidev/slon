using System.Buffers;
using System.IO.Pipelines;
using Slon.Pipelines;

namespace Slon.Tests.Pipelines;

[TestClass]
public class StreamPipeReaderTests
{
    static async Task<DefaultStreamPipeReader> CreateBufferedReader(byte[] bytes)
    {
        var reader = new DefaultStreamPipeReader(
            new MemoryStream(bytes, writable: false),
            new StreamPipeReaderOptions(bufferSize: 1024, useZeroByteReads: false),
            supportCancelPending: false);
        var read = await reader.ReadAsync();
        Assert.IsTrue(read.Buffer.Length > 0);
        reader.AdvanceTo(read.Buffer.Start);
        return reader;
    }

    [TestMethod]
    public async Task CopyToAsync_Stream_ConsumesSuccessfullyCopiedBufferedData()
    {
        var bytes = Enumerable.Range(0, 64).Select(static i => (byte)i).ToArray();
        var reader = await CreateBufferedReader(bytes);
        await using var destination = new MemoryStream();

        await reader.CopyToAsync(destination);

        CollectionAssert.AreEqual(bytes, destination.ToArray());
        Assert.IsFalse(reader.TryRead(out _), "successfully copied buffered data must not be published again");
        await reader.CompleteAsync();
    }

    [TestMethod]
    public async Task CopyToAsync_PipeWriter_ConsumesSuccessfullyCopiedBufferedData()
    {
        var bytes = Enumerable.Range(0, 64).Select(static i => (byte)i).ToArray();
        var reader = await CreateBufferedReader(bytes);
        var destination = new Pipe();

        await reader.CopyToAsync(destination.Writer);
        await destination.Writer.CompleteAsync();

        var copied = await destination.Reader.ReadAsync();
        CollectionAssert.AreEqual(bytes, copied.Buffer.ToArray());
        destination.Reader.AdvanceTo(copied.Buffer.End);
        await destination.Reader.CompleteAsync();
        Assert.IsFalse(reader.TryRead(out _), "successfully copied buffered data must not be published again");
        await reader.CompleteAsync();
    }

    [TestMethod]
    public async Task CopyToAsync_CompletedDestination_RetainsUnwrittenSuccessorSegment()
    {
        var bytes = Enumerable.Range(0, 2048).Select(static i => (byte)i).ToArray();
        var reader = new DefaultStreamPipeReader(
            new MemoryStream(bytes, writable: false),
            new StreamPipeReaderOptions(bufferSize: 1024, useZeroByteReads: false),
            supportCancelPending: false);
        var first = await reader.ReadAsync();
        var firstLength = (int)first.Buffer.Length;
        reader.AdvanceTo(first.Buffer.Start, first.Buffer.End);
        var buffered = await reader.ReadAsync();
        Assert.IsGreaterThan(firstLength, buffered.Buffer.Length);
        reader.AdvanceTo(buffered.Buffer.Start);

        await reader.CopyToAsync(new CompletingPipeWriter());

        Assert.IsTrue(reader.TryRead(out var remaining));
        CollectionAssert.AreEqual(bytes.AsSpan(firstLength).ToArray(), remaining.Buffer.ToArray());
        reader.AdvanceTo(remaining.Buffer.End);
        await reader.CompleteAsync();
    }

    [TestMethod]
    public async Task CompleteAsync_WithOutstandingRead_ThrowsBeforeReturningBuffer()
    {
        var stream = new HeldReadStream();
        var pool = new TrackingMemoryPool();
        var reader = new DefaultStreamPipeReader(
            stream,
            new StreamPipeReaderOptions(pool: pool, bufferSize: 1024, useZeroByteReads: false),
            supportCancelPending: false);

        var read = reader.ReadAsync().AsTask();
        await stream.ReadStarted.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.ThrowsExactly<InvalidOperationException>(() => { _ = reader.CompleteAsync(); });
        Assert.IsFalse(pool.OwnerDisposed.IsCompleted,
            "the receive destination was returned while its stream read was still outstanding");

        stream.CompleteRead();
        var result = await read.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(result.IsCompleted);
        await reader.CompleteAsync();
        await stream.Disposed.WaitAsync(TimeSpan.FromSeconds(5));
        await pool.OwnerDisposed.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [TestMethod]
    public async Task CompleteAsync_WithOutstandingCopy_ThrowsBeforeReturningBuffer()
    {
        var pool = new TrackingMemoryPool();
        var reader = new DefaultStreamPipeReader(
            new MemoryStream([1, 2, 3], writable: false),
            new StreamPipeReaderOptions(pool: pool, bufferSize: 1024, useZeroByteReads: false),
            supportCancelPending: false);
        var read = await reader.ReadAsync();
        reader.AdvanceTo(read.Buffer.Start);
        var destination = new HeldWriteStream();

        var copy = reader.CopyToAsync(destination);
        await destination.WriteStarted.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.ThrowsExactly<InvalidOperationException>(() => { _ = reader.CompleteAsync(); });
        Assert.IsFalse(pool.OwnerDisposed.IsCompleted,
            "the copied buffer was returned while the destination still owned its write");

        destination.CompleteWrite();
        await copy.WaitAsync(TimeSpan.FromSeconds(5));
        await reader.CompleteAsync();
        await pool.OwnerDisposed.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [TestMethod]
    public async Task CompletionDuringZeroByteRead_DoesNotStartDataRead()
    {
        var stream = new HeldZeroByteReadStream();
        var reader = new DefaultStreamPipeReader(
            stream,
            new StreamPipeReaderOptions(bufferSize: 1024, useZeroByteReads: true),
            supportCancelPending: false);

        var read = reader.ReadAsync().AsTask();
        await stream.ZeroByteReadStarted.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.ThrowsExactly<InvalidOperationException>(() => { _ = reader.CompleteAsync(); });

        stream.CompleteZeroByteRead();
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => read);
        Assert.AreEqual(0, stream.DataReads);
        await reader.CompleteAsync();
    }

    [TestMethod]
    public void SyncRead_TimeoutRestoreFault_ReleasesReadTenure()
    {
        var stream = new TimeoutRestoreFaultStream();
        var reader = new DefaultStreamPipeReader(
            stream,
            new StreamPipeReaderOptions(bufferSize: 1024, useZeroByteReads: false),
            supportCancelPending: false);

        Assert.ThrowsExactly<ObjectDisposedException>(
            () => reader.Read(TimeSpan.FromSeconds(1)));

        // The timeout restoration failure may replace the read failure, but it must not skip the
        // reader's ownership release and leave completion permanently reporting an active read.
        reader.Complete();
    }

    sealed class HeldReadStream : Stream
    {
        readonly TaskCompletionSource<int> _read = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _readStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task ReadStarted => _readStarted.Task;
        public Task Disposed => _disposed.Task;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public void CompleteRead() => _read.SetResult(0);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _readStarted.TrySetResult();
            return new(_read.Task);
        }

        protected override void Dispose(bool disposing)
        {
            _disposed.TrySetResult();
            base.Dispose(disposing);
        }

        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    sealed class CompletingPipeWriter : PipeWriter
    {
        readonly byte[] _buffer = new byte[4096];

        public override void Advance(int bytes) { }
        public override void CancelPendingFlush() { }
        public override void Complete(Exception? exception = null) { }
        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            => new(new FlushResult(isCanceled: false, isCompleted: true));
        public override Memory<byte> GetMemory(int sizeHint = 0) => _buffer;
        public override Span<byte> GetSpan(int sizeHint = 0) => _buffer;
    }

    sealed class HeldWriteStream : MemoryStream
    {
        readonly TaskCompletionSource _write = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _writeStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task WriteStarted => _writeStarted.Task;
        public void CompleteWrite() => _write.SetResult();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            _writeStarted.TrySetResult();
            await _write.Task.WaitAsync(cancellationToken);
            await base.WriteAsync(buffer, cancellationToken);
        }
    }

    sealed class TrackingMemoryPool : MemoryPool<byte>
    {
        readonly TaskCompletionSource _ownerDisposed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task OwnerDisposed => _ownerDisposed.Task;
        public override int MaxBufferSize => int.MaxValue;
        public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
            => new Owner(new byte[Math.Max(1, minBufferSize)], _ownerDisposed);
        protected override void Dispose(bool disposing) { }

        sealed class Owner(byte[] buffer, TaskCompletionSource disposed) : IMemoryOwner<byte>
        {
            public Memory<byte> Memory { get; private set; } = buffer;

            public void Dispose()
            {
                Memory = default;
                disposed.TrySetResult();
            }
        }
    }

    sealed class HeldZeroByteReadStream : Stream
    {
        readonly TaskCompletionSource<int> _zeroByteRead = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _zeroByteReadStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task ZeroByteReadStarted => _zeroByteReadStarted.Task;
        public int DataReads { get; private set; }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public void CompleteZeroByteRead() => _zeroByteRead.SetResult(0);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (buffer.IsEmpty)
            {
                _zeroByteReadStarted.TrySetResult();
                return new(_zeroByteRead.Task);
            }
            DataReads++;
            return new(0);
        }

        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    sealed class TimeoutRestoreFaultStream : Stream
    {
        int _timeoutSetCount;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanTimeout => true;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int ReadTimeout
        {
            get => 5_000;
            set
            {
                if (++_timeoutSetCount > 1)
                    throw new ObjectDisposedException(nameof(TimeoutRestoreFaultStream));
            }
        }

        public override int Read(Span<byte> buffer) => throw new IOException("synthetic read failure");
        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
