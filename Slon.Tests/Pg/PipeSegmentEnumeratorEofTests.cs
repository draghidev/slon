using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using Slon.Pipelines;
using Slon.Pg.Protocol;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Tests.Pg;

// Regression: a PipeSegmentEnumerator re-driven past completion (as the recovery drain loop does after
// a mid-exchange peer close) must not re-apply its deferred consume position. The deferred advance
// stores a SequencePosition into the segment it just produced. After that segment is fully consumed and
// pool-recycled, a re-applied advance drove SegmentChainBuilder's buffer accounting negative, so the
// next ReadResult build hit ArgumentOutOfRangeException('length') from GetReadOnlySequence.
[TestClass]
public class PipeSegmentEnumeratorEofTests
{
    // Treats the first 4 big-endian bytes as the total segment length. Fully buffered => Done (so the
    // enumerator takes the deferred-consume branch, exactly the state the defect needs).
    struct FixedSegmenter : IPipeSegmenter<int>
    {
        public int MinimumSize => 4;

        public OperationStatus CreateSegment(in ReadOnlySequence<byte> buffer, out long segmentLength, out int segment)
        {
            segment = 0;
            var reader = new SequenceReader<byte>(buffer);
            if (!reader.TryReadBigEndian(out int len))
            {
                segmentLength = 0;
                return OperationStatus.NeedMoreData;
            }
            segmentLength = len;
            if (buffer.Length < len)
                return OperationStatus.NeedMoreData;
            segment = len;
            return OperationStatus.Done;
        }
    }

    struct StreamingSegmenter : IPipeSegmenter<ReadOnlySequence<byte>>
    {
        public int MinimumSize => 4;

        public OperationStatus CreateSegment(in ReadOnlySequence<byte> buffer, out long segmentLength,
            out ReadOnlySequence<byte> segment)
        {
            var reader = new SequenceReader<byte>(buffer);
            if (!reader.TryReadBigEndian(out int len))
            {
                segmentLength = 0;
                segment = default;
                return OperationStatus.NeedMoreData;
            }

            segmentLength = len;
            segment = buffer.Slice(0, Math.Min(buffer.Length, len));
            return buffer.Length < len ? OperationStatus.NeedMoreData : OperationStatus.Done;
        }
    }

    static byte[] LenPrefixed(int total)
    {
        var bytes = new byte[total];
        bytes[0] = (byte)(total >> 24);
        bytes[1] = (byte)(total >> 16);
        bytes[2] = (byte)(total >> 8);
        bytes[3] = (byte)total;
        return bytes;
    }

    static byte[] BackendMessageBytes(BackendType type, int totalLength)
    {
        var bytes = new byte[totalLength];
        bytes[0] = (byte)type;
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(1), totalLength - 1);
        return bytes;
    }

    static PipeSegmentEnumerator<FixedSegmenter, int> BuildEnumerator(byte[] wire)
    {
        // A MemoryStream returns the wire bytes then 0 (EOF) on every subsequent read, so re-drives
        // after completion re-hit the same terminal state the recovery drain does against a closed peer.
        var reader = new DefaultStreamPipeReader(
            new MemoryStream(wire, writable: false),
            new StreamPipeReaderOptions(bufferSize: 8192, useZeroByteReads: false),
            supportCancelPending: false);
        return new(reader, new FixedSegmenter(), ownsReader: true);
    }

    [TestMethod]
    public void BackendMessage_BodyAccessThrowsAfterStreamingWindowAdvances()
    {
        var context = new BackendMessageContext();
        var message = new BackendMessage(
            new BackendHeader(BackendType.DataRow, 32),
            new ReadOnlySequence<byte>(BackendMessageBytes(BackendType.DataRow, 8)),
            context,
            token: 0);

        Assert.AreEqual(3, message.GetSequence().Length);
        context.MarkBodyWindowAdvanced(0);

        Assert.ThrowsExactly<InvalidOperationException>(() => message.GetSequence());
    }

    [TestMethod]
    public async Task ReDriveAfterEof_Async_ReturnsFalseWithoutCorruption()
    {
        var e = BuildEnumerator(LenPrefixed(24));

        Assert.IsTrue(await e.MoveNextAsync(), "first segment should be produced");
        Assert.AreEqual(24, e.Current);
        Assert.IsFalse(await e.MoveNextAsync(), "second call consumes the segment and reaches EOF");

        // The recovery drain keeps pulling after completion; before the fix each of these re-applied the
        // stale deferred advance and threw ArgumentOutOfRangeException('length') from the ReadResult build.
        for (var i = 0; i < 6; i++)
            Assert.IsFalse(await e.MoveNextAsync(), $"re-drive #{i} past completion must stay false");

        await e.DisposeAsync();
    }

    [TestMethod]
    public void ReDriveAfterEof_Sync_ReturnsFalseWithoutCorruption()
    {
        var e = BuildEnumerator(LenPrefixed(24));

        Assert.IsTrue(e.MoveNext(), "first segment should be produced");
        Assert.AreEqual(24, e.Current);
        Assert.IsFalse(e.MoveNext(), "second call consumes the segment and reaches EOF");

        for (var i = 0; i < 6; i++)
            Assert.IsFalse(e.MoveNext(), $"re-drive #{i} past completion must stay false");

        e.Dispose();
    }

    [TestMethod]
    public async Task TryMoveNext_PollsFragmentedSegmentWithoutSuspending()
    {
        var pipe = new Pipe();
        var e = new PipeSegmentEnumerator<FixedSegmenter, int>(pipe.Reader, new FixedSegmenter());

        Assert.IsFalse(e.TryMoveNext(out var completed));
        Assert.IsFalse(completed);

        var wire = LenPrefixed(24);
        await pipe.Writer.WriteAsync(wire.AsMemory(0, 2));
        Assert.IsFalse(e.TryMoveNext(out completed), "a partial header must request another read");
        Assert.IsFalse(completed);

        await pipe.Writer.WriteAsync(wire.AsMemory(2));
        Assert.IsTrue(e.TryMoveNext(out completed));
        Assert.IsFalse(completed);
        Assert.AreEqual(24, e.Current);

        await pipe.Writer.CompleteAsync();
        Assert.IsFalse(e.TryMoveNext(out completed));
        Assert.IsTrue(completed);
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task DirectRead_PreservesFramingAndTerminalState()
    {
        var e = BuildEnumerator(LenPrefixed(24));

        Assert.IsTrue(e.TryBeginDirectRead(default, out var read));
        var length = await read;
        Assert.IsTrue(e.CompleteDirectRead(length, default, out _, out var readFinished, out var completed));
        Assert.IsTrue(readFinished);
        Assert.IsFalse(completed);
        Assert.AreEqual(24, e.Current);

        Assert.IsFalse(e.TryMoveNext(out completed));
        Assert.IsFalse(completed);
        Assert.IsTrue(e.TryBeginDirectRead(default, out read));
        length = await read;
        Assert.IsFalse(e.CompleteDirectRead(length, default, out _, out readFinished, out completed));
        Assert.IsTrue(readFinished);
        Assert.IsTrue(completed);

        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task ContinueCurrentSegment_StreamsWithoutCrossingNextSegment()
    {
        var pipe = new Pipe();
        var e = new PipeSegmentEnumerator<StreamingSegmenter, ReadOnlySequence<byte>>(
            pipe.Reader, new StreamingSegmenter());
        var first = LenPrefixed(12);
        var second = LenPrefixed(8);

        await pipe.Writer.WriteAsync(first.AsMemory(0, 6));
        Assert.IsTrue(e.TryMoveNext(out _));
        Assert.AreEqual(6, e.Current.Length);

        Assert.IsFalse(e.TryContinueCurrentSegment(e.Current.End, e.Current.Length, out _));

        var tail = new byte[first.Length - 6 + second.Length];
        first.AsSpan(6).CopyTo(tail);
        second.CopyTo(tail.AsSpan(first.Length - 6));
        await pipe.Writer.WriteAsync(tail);

        Assert.IsTrue(e.TryContinueCurrentSegment(e.Current.End, e.Current.Length, out var continuation));
        Assert.IsTrue(continuation.IsComplete);
        Assert.AreEqual(6, continuation.Buffer.Length);

        Assert.IsTrue(e.TryMoveNext(out _));
        Assert.AreEqual(8, e.Current.Length);

        await pipe.Writer.CompleteAsync();
        Assert.IsFalse(await e.MoveNextAsync());
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task ContinueCurrentSegmentAsync_PreservesPartialProgress()
    {
        var pipe = new Pipe();
        var e = new PipeSegmentEnumerator<StreamingSegmenter, ReadOnlySequence<byte>>(
            pipe.Reader, new StreamingSegmenter());
        var wire = LenPrefixed(12);

        await pipe.Writer.WriteAsync(wire.AsMemory(0, 6));
        Assert.IsTrue(e.TryMoveNext(out _));

        var pending = e.ContinueCurrentSegmentAsync(e.Current.End, e.Current.Length);
        Assert.IsFalse(pending.IsCompleted);
        await pipe.Writer.WriteAsync(wire.AsMemory(6, 3));
        var middle = await pending;
        Assert.IsFalse(middle.IsComplete);
        Assert.AreEqual(3, middle.Buffer.Length);

        pending = e.ContinueCurrentSegmentAsync(middle.Buffer.End, middle.Buffer.Length);
        await pipe.Writer.WriteAsync(wire.AsMemory(9));
        var final = await pending;
        Assert.IsTrue(final.IsComplete);
        Assert.AreEqual(3, final.Buffer.Length);

        await pipe.Writer.CompleteAsync();
        Assert.IsFalse(await e.MoveNextAsync());
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task ExtendCurrentSegmentAsync_RetainsUntilTheCompleteSegment()
    {
        var wire = LenPrefixed(128 * 1024);
        var reader = new DefaultStreamPipeReader(
            new MemoryStream(wire, writable: false),
            new StreamPipeReaderOptions(bufferSize: 8192, useZeroByteReads: false),
            supportCancelPending: false);
        var e = new PipeSegmentEnumerator<StreamingSegmenter, ReadOnlySequence<byte>>(
            reader, new StreamingSegmenter(), ownsReader: true);

        Assert.IsTrue(await e.MoveNextAsync());
        CurrentSegmentBuffer current;
        do current = await e.ExtendCurrentSegmentAsync();
        while (!current.IsComplete);

        Assert.AreEqual(wire.Length, current.Buffer.Length);
        Assert.IsFalse(await e.MoveNextAsync());
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task BackendBodyReader_ExtendsPrefixThenSlides()
    {
        var pipe = new Pipe();
        var wire = BackendMessageBytes(BackendType.DataRow, 32);
        for (var i = BackendHeader.ByteCount; i < wire.Length; i++)
            wire[i] = (byte)i;

        var segments = new PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch>(
            pipe.Reader, new BackendMessageBatch.Segmenter(8));
        var decoder = new PgDecoder(segments, CancellationToken.None, Timeout.InfiniteTimeSpan);
        decoder.Pipe.BindDecoder(decoder);

        await pipe.Writer.WriteAsync(wire.AsMemory(0, 8));
        Assert.IsTrue(decoder.Pipe.TryMoveNextBatch(out _));
        Assert.IsTrue(decoder.Pipe.TryMoveNext());
        var body = decoder.Pipe.Current.OpenBodyReader();
        Assert.AreEqual(3, body.Buffer.Length);

        await pipe.Writer.WriteAsync(wire.AsMemory(8, 5));
        Assert.IsTrue(body.TryExtend());
        Assert.AreEqual(8, body.Buffer.Length);
        CollectionAssert.AreEqual(wire.AsSpan(BackendHeader.ByteCount, 8).ToArray(), body.Buffer.ToArray());

        var consumed = body.Buffer.GetPosition(4);
        body.AdvanceTo(consumed, 4);
        await pipe.Writer.WriteAsync(wire.AsMemory(13));
        Assert.IsTrue(body.TryRead());
        Assert.IsTrue(body.IsComplete);
        CollectionAssert.AreEqual(wire.AsSpan(BackendHeader.ByteCount + 4).ToArray(), body.Buffer.ToArray());

        await pipe.Writer.CompleteAsync();
        await ((IAsyncDisposable)decoder).DisposeAsync();
    }

    [TestMethod]
    public async Task BackendSegmenter_ExtendedRowAdvancesToTrailingMessage()
    {
        var bind = BackendMessageBytes(BackendType.BindComplete, BackendHeader.ByteCount);
        var row = BackendMessageBytes(BackendType.DataRow, 128 * 1024);
        var complete = BackendMessageBytes(BackendType.CommandComplete, BackendHeader.ByteCount);
        var wire = new byte[bind.Length + row.Length + complete.Length];
        bind.CopyTo(wire, 0);
        row.CopyTo(wire, bind.Length);
        complete.CopyTo(wire, bind.Length + row.Length);
        var reader = new DefaultStreamPipeReader(
            new MemoryStream(wire, writable: false),
            new StreamPipeReaderOptions(bufferSize: 64 * 1024, useZeroByteReads: false),
            supportCancelPending: false);
        var e = new PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch>(
            reader, new BackendMessageBatch.Segmenter(), ownsReader: true);

        Assert.IsTrue(await e.MoveNextAsync());
        CurrentSegmentBuffer current;
        do current = await e.ExtendCurrentSegmentAsync();
        while (!current.IsComplete);

        Assert.IsTrue(await e.MoveNextAsync());
        Assert.IsTrue(e.Current.TryReadNextInPlace(out var header, out _, out _));
        Assert.AreEqual(BackendType.CommandComplete, header.Type);
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task ContinueCurrentSegment_SlidesPastOnlyTheConsumedPrefix()
    {
        var pipe = new Pipe();
        var e = new PipeSegmentEnumerator<StreamingSegmenter, ReadOnlySequence<byte>>(
            pipe.Reader, new StreamingSegmenter());
        var wire = LenPrefixed(12);
        for (var i = 4; i < wire.Length; i++)
            wire[i] = (byte)i;

        await pipe.Writer.WriteAsync(wire.AsMemory(0, 8));
        Assert.IsTrue(e.TryMoveNext(out _));
        var consumed = e.Current.GetPosition(6);
        Assert.IsFalse(e.TryContinueCurrentSegment(consumed, 6, out _));

        await pipe.Writer.WriteAsync(wire.AsMemory(8, 2));
        Assert.IsTrue(e.TryContinueCurrentSegment(consumed, 6, out var middle));
        Assert.IsFalse(middle.IsComplete);
        CollectionAssert.AreEqual(wire.AsSpan(6, 4).ToArray(), middle.Buffer.ToArray());

        await pipe.Writer.WriteAsync(wire.AsMemory(10));
        var final = await e.ContinueCurrentSegmentAsync(middle.Buffer.End, middle.Buffer.Length);
        Assert.IsTrue(final.IsComplete);
        CollectionAssert.AreEqual(wire.AsSpan(10).ToArray(), final.Buffer.ToArray());

        await pipe.Writer.CompleteAsync();
        Assert.IsFalse(await e.MoveNextAsync());
        await e.DisposeAsync();
    }

    [TestMethod]
    public async Task MoveNext_CanDrainAfterAContinuationPollParks()
    {
        var pipe = new Pipe();
        var e = new PipeSegmentEnumerator<StreamingSegmenter, ReadOnlySequence<byte>>(
            pipe.Reader, new StreamingSegmenter());
        var first = LenPrefixed(12);
        var second = LenPrefixed(8);

        await pipe.Writer.WriteAsync(first.AsMemory(0, 6));
        Assert.IsTrue(e.TryMoveNext(out _));
        Assert.IsFalse(e.TryContinueCurrentSegment(e.Current.End, e.Current.Length, out _));

        await pipe.Writer.WriteAsync(first.AsMemory(6));
        await pipe.Writer.WriteAsync(second);
        Assert.IsTrue(await e.MoveNextAsync(), "normal iteration must take over the pending continuation read");
        Assert.AreEqual(second.Length, e.Current.Length);

        await pipe.Writer.CompleteAsync();
        Assert.IsFalse(await e.MoveNextAsync());
        await e.DisposeAsync();
    }

    [TestMethod]
    public void BackendMessage_BufferedRequiresTagAndDeclaredLength()
    {
        var header = new BackendHeader(BackendType.DataRow, 11);
        var context = new BackendMessageContext();

        Assert.IsFalse(new BackendMessage(header, new ReadOnlySequence<byte>(new byte[11]), context, 0).Buffered);
        Assert.IsTrue(new BackendMessage(header, new ReadOnlySequence<byte>(new byte[12]), context, 0).Buffered);
    }

    [TestMethod]
    public void BackendSegmenter_WaitsForUsefulPartialDataRowPrefix()
    {
        var rowLength = 128 * 1024;
        var wire = BackendMessageBytes(BackendType.DataRow, rowLength);
        var segmenter = new BackendMessageBatch.Segmenter();

        var smallPrefix = new ReadOnlySequence<byte>(wire.AsMemory(0, 32));
        Assert.AreEqual(OperationStatus.NeedMoreData,
            segmenter.CreateSegment(smallPrefix, out var length, out _));
        Assert.AreEqual(0, length);
        Assert.AreEqual(BackendMessageBatch.Segmenter.DefaultDataRowStreamingThreshold, segmenter.MinimumSize);

        var usefulPrefix = new ReadOnlySequence<byte>(
            wire.AsMemory(0, BackendMessageBatch.Segmenter.DefaultDataRowStreamingThreshold));
        Assert.AreEqual(OperationStatus.Done,
            segmenter.CreateSegment(usefulPrefix, out length, out var batch));
        Assert.AreEqual(rowLength, length);
        Assert.IsTrue(batch.TryReadNextInPlace(out var rowHeader, out var partialRow, out _));
        Assert.AreEqual(BackendType.DataRow, rowHeader.Type);
        Assert.AreEqual(BackendMessageBatch.Segmenter.DefaultDataRowStreamingThreshold, partialRow.Length);
        Assert.IsFalse(new BackendMessage(rowHeader, partialRow, new BackendMessageContext(), 0).Buffered);
    }
}
