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
}
