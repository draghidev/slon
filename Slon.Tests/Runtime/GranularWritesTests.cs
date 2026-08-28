using System.Buffers;
using Slon.Runtime.InteropServices;

namespace Slon.Tests.Runtime;

// GranularWrites mirrors the BCL memory structs' field layout; these pin that layout against the
// public API so a runtime change fails here rather than in the decoder.
[TestClass]
public class GranularWritesTests
{
    [TestMethod]
    public void SequenceLayout_MatchesPublicApi()
    {
        var array = new byte[16];
        var sequence = new ReadOnlySequence<byte>(array, 2, 10);
        var (start, end) = GranularWrites.SequenceObjects(in sequence);
        Assert.AreSame(array, start);
        Assert.AreSame(array, end);

        var first = new Segment(new byte[4]);
        var last = first.Append(new byte[4]);
        var multi = new ReadOnlySequence<byte>(first, 1, last, 3);
        (start, end) = GranularWrites.SequenceObjects(in multi);
        Assert.AreSame(first, start);
        Assert.AreSame(last, end);
    }

    [TestMethod]
    public void MemoryLayout_MatchesPublicApi()
    {
        var array = new byte[16];
        var memory = new ReadOnlyMemory<byte>(array, 3, 5);
        var (obj, index, length) = GranularWrites.MemoryFields(in memory);
        Assert.AreSame(array, obj);
        Assert.AreEqual(3, index);
        Assert.AreEqual(5, length);
    }

    [TestMethod]
    public void Write_ProducesEqualValues()
    {
        var array = new byte[16];
        ReadOnlySequence<byte> sequence = default;
        var value = new ReadOnlySequence<byte>(array, 4, 8);
        GranularWrites.Write(ref sequence, in value);
        Assert.IsTrue(sequence.First.Span.SequenceEqual(value.First.Span));
        Assert.AreEqual(value.Length, sequence.Length);
        var shifted = new ReadOnlySequence<byte>(array, 1, 2);
        GranularWrites.Write(ref sequence, in shifted);
        Assert.AreEqual(2, sequence.Length);
        Assert.AreEqual(1, sequence.GetOffset(sequence.Start));

        ReadOnlyMemory<byte> memory = default;
        var memoryValue = new ReadOnlyMemory<byte>(array, 2, 6);
        GranularWrites.Write(ref memory, in memoryValue);
        Assert.IsTrue(memory.Equals(memoryValue));
        var other = new ReadOnlyMemory<byte>(new byte[4], 1, 2);
        GranularWrites.Write(ref memory, in other);
        Assert.IsTrue(memory.Equals(other));
    }

    sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
