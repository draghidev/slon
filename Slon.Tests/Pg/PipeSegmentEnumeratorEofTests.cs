using System.Buffers;
using System.IO.Pipelines;
using Slon.Pipelines;

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

    static byte[] LenPrefixed(int total)
    {
        var bytes = new byte[total];
        bytes[0] = (byte)(total >> 24);
        bytes[1] = (byte)(total >> 16);
        bytes[2] = (byte)(total >> 8);
        bytes[3] = (byte)total;
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
}
