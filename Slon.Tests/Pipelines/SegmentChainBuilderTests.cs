using System.Buffers;
using Slon.Pipelines;

namespace Slon.Tests.Pipelines;

[TestClass]
public class SegmentChainBuilderTests
{
    [TestMethod]
    public void AdvanceToEmpty_RetainsSegmentAndOwnedMemory_WhenEnabled()
    {
        using var builder = new SegmentChainBuilder(
            MemoryPool<byte>.Shared, minimumBufferSize: 4096, retainBufferOnEmpty: true);

        var buffer = builder.Reserve(1, enforceHint: false);
        buffer.Span[0] = 42;
        builder.Grow(1);

        var sequence = builder.GetReadOnlySequence();
        var segment = builder.HeadInfo.Head;
        var memoryOwner = segment!.MemoryOwner;

        builder.AdvanceTo(sequence.End);

        Assert.AreEqual(0, builder.BufferedBytes);
        Assert.AreSame(segment, builder.HeadInfo.Head);
        Assert.AreSame(memoryOwner, segment.MemoryOwner);

        var reused = builder.Reserve(1, enforceHint: false);
        Assert.AreSame(segment, builder.HeadInfo.Head);
        Assert.AreSame(memoryOwner, segment.MemoryOwner);
        Assert.AreEqual(4096, reused.Length);
    }

    [TestMethod]
    public void AdvanceToEmpty_ReleasesOversizedSegment_WhenRetentionEnabled()
    {
        using var builder = new SegmentChainBuilder(
            MemoryPool<byte>.Shared, minimumBufferSize: 4096, retainBufferOnEmpty: true);

        var buffer = builder.Reserve(8192, enforceHint: true);
        buffer.Span[0] = 42;
        builder.Grow(1);

        builder.AdvanceTo(builder.GetReadOnlySequence().End);

        Assert.AreEqual(0, builder.BufferedBytes);
        Assert.IsNull(builder.HeadInfo.Head);

        var baseline = builder.Reserve(1, enforceHint: false);
        Assert.AreEqual(4096, baseline.Length);
    }

    [TestMethod]
    public void DisablingRetention_ReleasesRetainedEmptySegment()
    {
        using var builder = new SegmentChainBuilder(
            MemoryPool<byte>.Shared, minimumBufferSize: 4096, retainBufferOnEmpty: true);

        builder.Reserve(1, enforceHint: false).Span[0] = 42;
        builder.Grow(1);
        builder.AdvanceTo(builder.GetReadOnlySequence().End);

        Assert.IsNotNull(builder.HeadInfo.Head);

        builder.RetainBufferOnEmpty = false;

        Assert.IsNull(builder.HeadInfo.Head);
        Assert.AreEqual(0, builder.BufferedBytes);
    }

    [TestMethod]
    public void AdvanceTo_AcceptsCurrentHeadArrayAfterChainGrowth()
    {
        using var builder = new SegmentChainBuilder(MemoryPool<byte>.Shared, minimumBufferSize: 16);

        builder.Reserve(16, enforceHint: true).Span.Fill(1);
        builder.Grow(16);
        var singleSegment = builder.GetReadOnlySequence();
        var consumed = singleSegment.GetPosition(4);

        builder.Reserve(16, enforceHint: true).Span[0] = 2;
        builder.Grow(1);
        var chained = builder.GetReadOnlySequence();

        var examined = builder.AdvanceTo(consumed, chained.End);

        Assert.AreEqual(17, examined);
        Assert.AreEqual(13, builder.BufferedBytes);
        Assert.AreEqual(13, builder.GetReadOnlySequence().Length);
    }

    [TestMethod]
    public void AdvanceTo_RejectsArrayPositionAfterItsHeadWasConsumed()
    {
        using var builder = new SegmentChainBuilder(MemoryPool<byte>.Shared, minimumBufferSize: 16);

        builder.Reserve(16, enforceHint: true).Span.Fill(1);
        builder.Grow(16);
        var original = builder.GetReadOnlySequence();

        builder.Reserve(16, enforceHint: true).Span[0] = 2;
        builder.Grow(1);
        var chained = builder.GetReadOnlySequence();
        builder.AdvanceTo(chained.GetPosition(16));

        Assert.ThrowsExactly<InvalidCastException>(() => builder.AdvanceTo(original.Start));
    }

    [TestMethod]
    public void AdvanceTo_TranslatesArrayPositionAfterHeadConsolidation()
    {
        using var builder = new SegmentChainBuilder(MemoryPool<byte>.Shared, minimumBufferSize: 16);

        builder.Reserve(16, enforceHint: true).Span.Fill(1);
        builder.Grow(16);
        var initial = builder.GetReadOnlySequence();
        builder.AdvanceTo(initial.GetPosition(4));

        var beforeConsolidation = builder.GetReadOnlySequence();
        var consumed = beforeConsolidation.GetPosition(4);
        builder.Reserve(16, enforceHint: true).Span[0] = 2;
        builder.Grow(1);
        var consolidated = builder.GetReadOnlySequence();

        var examined = builder.AdvanceTo(consumed, consolidated.End);

        Assert.AreEqual(13, examined);
        Assert.AreEqual(9, builder.BufferedBytes);
        Assert.AreEqual(9, builder.GetReadOnlySequence().Length);
    }
}
