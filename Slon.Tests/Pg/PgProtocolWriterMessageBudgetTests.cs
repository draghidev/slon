using System;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Buffers;
using Slon.Pg.Protocol;

namespace Slon.Tests.Pg;

// The writer's current-message tracking (StartMessage / AdvanceMessageBytesFlushed) enforces one
// invariant: no more than a message's declared length ever reaches the wire, regardless of why the
// declared length disagrees with what got written (overflow, compositional sizing bug, a broken
// converter). The cap is the CVE-2024-32655 defense. These tests pin it at the writer, the single
// chokepoint through which bytes leave for the wire. PgEncoder.StartMessage just forwards here.
//
// MemoryBufferWriter only commits bytes on Advance, which BufferingWriter defers to Flush, so a
// throwing Flush leaves zero committed bytes - that is the "nothing over-length on the wire" assert.
[TestClass]
public class PgProtocolWriterMessageBudgetTests
{
    static (PgProtocolDataWriter Writer, MemoryBufferWriter Sink) NewWriter()
    {
        var sink = new MemoryBufferWriter();
        // control is only reached on the abort-cancelled async/sync flush path, none of which these
        // sync budget tests exercise (the throw precedes the flush; success cases leave abort unset).
        var writer = new PgProtocolDataWriter(sink, Encoding.UTF8, static () => { }, default, null!);
        return (writer, sink);
    }

    [TestMethod]
    public void OverWrite_FaultsAtFlush_BeforeAnyBytesReachWire()
    {
        var (writer, sink) = NewWriter();
        writer.StartMessage(totalLength: 5);
        writer.WriteRaw(new byte[8]);

        Assert.ThrowsExactly<InvalidOperationException>(() => writer.Flush());
        Assert.AreEqual(0, sink.ToArray().Length, "over-length bytes must not reach the wire");
    }

    [TestMethod]
    public void OverWrite_FaultsAtNextStartMessage()
    {
        var (writer, _) = NewWriter();
        writer.StartMessage(totalLength: 5);
        writer.WriteRaw(new byte[8]);

        Assert.ThrowsExactly<InvalidOperationException>(() => writer.StartMessage(totalLength: 5));
    }

    [TestMethod]
    public void UnderWrite_FaultsAtNextStartMessage()
    {
        var (writer, _) = NewWriter();
        writer.StartMessage(totalLength: 5);
        writer.WriteRaw(new byte[3]);

        Assert.ThrowsExactly<InvalidOperationException>(() => writer.StartMessage(totalLength: 5));
    }

    [TestMethod]
    public void ExactWrite_Flushes_AllBytesReachWire()
    {
        var (writer, sink) = NewWriter();
        writer.StartMessage(totalLength: 5);
        writer.WriteRaw(new byte[5]);

        writer.Flush();
        Assert.AreEqual(5, sink.ToArray().Length);
    }

    [TestMethod]
    public void StackedMessages_BothExact_FlushSucceeds()
    {
        var (writer, sink) = NewWriter();
        writer.StartMessage(totalLength: 5);
        writer.WriteRaw(new byte[5]);
        writer.StartMessage(totalLength: 3);
        writer.WriteRaw(new byte[3]);

        writer.Flush();
        Assert.AreEqual(8, sink.ToArray().Length);
    }

    [TestMethod]
    public void StackedMessages_OverWriteOnLater_FaultsAtFlush()
    {
        // The -unflushed anchoring isolates each stacked message's budget: the second message's
        // over-write trips even though the first (valid, unflushed) message shares the buffer.
        var (writer, sink) = NewWriter();
        writer.StartMessage(totalLength: 5);
        writer.WriteRaw(new byte[5]);
        writer.StartMessage(totalLength: 3);
        writer.WriteRaw(new byte[6]);

        Assert.ThrowsExactly<InvalidOperationException>(() => writer.Flush());
        Assert.AreEqual(0, sink.ToArray().Length);
    }

    [TestMethod]
    public void Padding_CompletesTornMessage_ToDeclaredLength()
    {
        var (writer, sink) = NewWriter();
        writer.StartMessage(totalLength: 8);
        writer.WriteRaw(new byte[3]);

        var padded = writer.CompleteCurrentMessageWithPadding();
        Assert.AreEqual(5, padded);

        writer.Flush();
        Assert.AreEqual(8, sink.ToArray().Length);
    }

    [TestMethod]
    public void Padding_NoMessageInFlight_ReturnsZero()
    {
        var (writer, _) = NewWriter();
        Assert.AreEqual(0, writer.CompleteCurrentMessageWithPadding());
    }
}
