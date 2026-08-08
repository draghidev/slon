using System;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Serialization;

namespace Slon.Tests.Pg;

// The writer's current-message tracking (StartMessage / AdvanceMessageBytesFlushed) enforces one
// invariant: no more than a message's declared length ever reaches the wire, regardless of why the
// declared length disagrees with what got written (overflow, compositional sizing bug, a broken
// converter). The cap is the CVE-2024-32655 defense. These tests pin it at the writer, the single
// chokepoint through which bytes leave for the wire. PgEncoder.StartMessage just forwards here.
//
// BufferOutputWriter only commits bytes on Advance, which BufferingWriter defers to Flush, so a
// throwing Flush leaves zero committed bytes - that is the "nothing over-length on the wire" assert.
[TestClass]
public class ProtocolDataWriterMessageBudgetTests
{
    static (ProtocolDataWriter Writer, BufferOutputWriter Sink) NewWriter()
    {
        var sink = new BufferOutputWriter();
        // control is only reached on the abort-cancelled async/sync flush path, none of which these
        // sync budget tests exercise (the throw precedes the flush; success cases leave abort unset).
        var writer = new ProtocolDataWriter(sink, Encoding.UTF8, static () => { }, default, null!);
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
    public void ZeroSizeHint_ReturnsNonEmptyBuffer_BeforeAndAfterFlush()
    {
        var (writer, _) = NewWriter();

        Assert.IsGreaterThan(0, writer.GetMemory().Length);
        Assert.IsGreaterThan(0, writer.GetSpan().Length);

        writer.Flush();

        Assert.IsGreaterThan(0, writer.GetMemory().Length);
        Assert.IsGreaterThan(0, writer.GetSpan().Length);
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

    [TestMethod]
    public void Padding_AccountsForAbortedSerializerLocalBytes()
    {
        const int bodyLength = 256 * 1024;
        var (writer, sink) = NewWriter();
        writer.StartMessage((byte)'B', bodyLength);
        var serializer = new PgWriter(writer).Init(flushMode: FlushMode.Blocking);
        serializer.WriteBytes(new byte[64 * 1024 + 1]);
        serializer.AbortWrite();

        var padded = writer.CompleteCurrentMessageWithPadding();
        writer.Flush();

        Assert.AreEqual(bodyLength + sizeof(byte) + sizeof(uint), sink.ToArray().Length);
        Assert.IsGreaterThan(0, padded);
    }

    [TestMethod]
    public void ParameterWriterState_IsCachedPerTokenBearingShell()
    {
        var (writer, _) = NewWriter();
        var strategy = new CountingParameterWriterStrategy();

        var first = writer.GetParameterWriterState(strategy);
        var second = writer.GetParameterWriterState(strategy);

        Assert.AreSame(first, second);
        Assert.AreEqual(1, strategy.CreateCount);
    }

    [TestMethod]
    public void ParameterWriterState_IsRecreatedAfterClientEncodingChanges()
    {
        var (writer, _) = NewWriter();
        var strategy = new CountingParameterWriterStrategy();
        var first = writer.GetParameterWriterState(strategy);

        writer.ClientEncoding = Encoding.Latin1;
        var second = writer.GetParameterWriterState(strategy);

        Assert.AreNotSame(first, second);
        Assert.AreEqual(2, strategy.CreateCount);
    }

    sealed class CountingParameterWriterStrategy : ParameterWriterStrategy
    {
        public int CreateCount { get; private set; }

        public override object CreateState(IOutputWriter output, Encoding textEncoding)
        {
            CreateCount++;
            return new object();
        }

        public override void Write(object state, in Parameter parameter) => throw new NotSupportedException();
        public override ValueTask WriteAsync(object state, Parameter parameter,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
