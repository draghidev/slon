using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

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
    public void Padding_CanBeEmittedInBoundedChunksAcrossFlushes()
    {
        var (writer, sink) = NewWriter();
        writer.StartMessage(totalLength: 12);
        writer.WriteRaw(new byte[2]);

        Assert.AreEqual(10, writer.CurrentMessagePaddingLength);
        Assert.AreEqual(4, writer.CompleteCurrentMessageWithPadding(4));
        writer.Flush();
        Assert.AreEqual(6, writer.CurrentMessagePaddingLength);
        Assert.AreEqual(6, writer.CompleteCurrentMessageWithPadding(10));
        writer.Flush();

        Assert.AreEqual(0, writer.CurrentMessagePaddingLength);
        Assert.AreEqual(12, sink.ToArray().Length);
    }

    [TestMethod]
    public void Terminate_WritesHeaderOnlyFrontendMessage()
    {
        var (writer, sink) = NewWriter();

        writer.WriteTerminate();
        writer.Flush();

        CollectionAssert.AreEqual(
            new byte[] { (byte)'X', 0, 0, 0, 4 },
            sink.ToArray());
    }

    [TestMethod]
    public void ParameterWriterState_IsCachedPerTokenBearingShell()
    {
        var (writer, _) = NewWriter();
        var writerComponent = new CountingParameterWriter();

        var first = writer.GetParameterWriterState(writerComponent);
        var second = writer.GetParameterWriterState(writerComponent);

        Assert.AreSame(first, second);
        Assert.AreEqual(1, writerComponent.CreateCount);
    }

    [TestMethod]
    public void ParameterWriterState_IsRecreatedAfterClientEncodingChanges()
    {
        var (writer, _) = NewWriter();
        var writerComponent = new CountingParameterWriter();
        var first = writer.GetParameterWriterState(writerComponent);

        writer.ClientEncoding = Encoding.Latin1;
        var second = writer.GetParameterWriterState(writerComponent);

        Assert.AreNotSame(first, second);
        Assert.AreEqual(2, writerComponent.CreateCount);
    }

    [TestMethod]
    public void LaterBindFailure_ReleasesEarlierParameterWriteState()
    {
        var (writer, _) = NewWriter();
        var writerComponent = new FailingBindParameterWriter();
        var source = new ParameterSource(new object(), writerComponent);

        var encoder = new PgEncoder(default, writer);
        Assert.ThrowsExactly<InvalidOperationException>(() =>
            encoder.WriteBind(parameters: source));
        Assert.IsTrue(writerComponent.FirstWriteState.IsDisposed);
    }

    sealed class CountingParameterWriter : ParameterWriter
    {
        public int CreateCount { get; private set; }

        internal override object CreateWriterStateCore(IOutputWriter output, Encoding textEncoding)
        {
            CreateCount++;
            return new object();
        }
        internal override int GetParameterCountCore(object source)
            => throw new NotSupportedException();

        internal override PgTypeId GetParameterTypeCore(object source, int index)
            => throw new NotSupportedException();
        private protected override object BeginWriteStateCore(object source, int count)
            => throw new NotSupportedException();
        private protected override int GetSizeCore(object writeState, int parameterIndex)
            => throw new NotSupportedException();
        private protected override void WriteCore(object writerState, object source,
            object writeState, int parameterIndex)
            => throw new NotSupportedException();
        private protected override ValueTask WriteAsyncCore(object writerState, object source,
            object writeState, int parameterIndex,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }

    sealed class FailingBindParameterWriter : ParameterWriter
    {
        public DisposableState FirstWriteState { get; } = new();

        internal override object CreateWriterStateCore(
            IOutputWriter output, Encoding textEncoding) => this;
        internal override int GetParameterCountCore(object source) => 2;
        internal override PgTypeId GetParameterTypeCore(object source, int index)
            => throw new NotSupportedException();
        private protected override object BeginWriteStateCore(object source, int count) => new BindingState();

        private protected override int GetSizeCore(object writeState, int parameterIndex)
            => ((BindingState)writeState).Sizes[parameterIndex];

        private protected override void BindCore(object writerState, object source,
            object writeState, int parameterIndex)
        {
            if (parameterIndex is 1)
                throw new InvalidOperationException("Bind failed.");
            var binding = (BindingState)writeState;
            binding.Sizes[parameterIndex] = sizeof(int);
            binding.WriteState = FirstWriteState;
        }

        private protected override void EndWriteCore(object writeState, int count)
            => ((BindingState)writeState).WriteState?.Dispose();

        private protected override void WriteCore(object writerState, object source,
            object writeState, int parameterIndex)
            => throw new AssertFailedException("Writing must not start after Bind fails.");

        private protected override ValueTask WriteAsyncCore(object writerState, object source,
            object writeState, int parameterIndex,
            CancellationToken cancellationToken = default)
            => throw new AssertFailedException("Writing must not start after Bind fails.");

        internal sealed class BindingState
        {
            public int[] Sizes { get; } = new int[2];
            public IDisposable? WriteState { get; set; }
        }
    }

    sealed class DisposableState : IDisposable
    {
        public bool IsDisposed { get; private set; }
        public void Dispose() => IsDisposed = true;
    }
}
