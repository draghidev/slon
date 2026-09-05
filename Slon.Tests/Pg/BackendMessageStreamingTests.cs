using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using Slon.Pipelines;
using Slon.Pg.Protocol;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Tests.Pg;

// Backend-message framing across fragmented, extended and sliding buffers, including terminal
// re-drive and preservation of messages following a partially consumed DataRow.
[TestClass]
public class BackendMessageStreamingTests
{
    sealed class SequenceSegment : ReadOnlySequenceSegment<byte>
    {
        public SequenceSegment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public SequenceSegment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new SequenceSegment(memory)
            {
                RunningIndex = RunningIndex + Memory.Length
            };
            Next = next;
            return next;
        }

        public ReadOnlySequence<byte> To(SequenceSegment end)
            => new(this, 0, end, end.Memory.Length);
    }

    sealed class TestMemoryManager(byte[] buffer) : MemoryManager<byte>
    {
        public override Span<byte> GetSpan() => buffer;
        public override MemoryHandle Pin(int elementIndex = 0)
            => throw new NotSupportedException();
        public override void Unpin() { }
        protected override void Dispose(bool disposing) { }
    }

    sealed class RejectRetiredSuppliedReadReader(PipeReader inner) : PipeReader
    {
        ReadResult _activeRead;
        bool _rejectAdvanceAtStart;
        long? _expectedAdvanceOffset;

        public Action? BeforeAdvance { get; set; }

        public void RejectAdvanceAtActiveStart() => _rejectAdvanceAtStart = true;
        public void ExpectAdvanceAtActiveOffset(long offset) => _expectedAdvanceOffset = offset;

        public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            var task = inner.ReadAsync(cancellationToken);
            Assert.IsTrue(task.IsCompletedSuccessfully);
            _activeRead = task.Result;
            return new(_activeRead);
        }

        public override bool TryRead(out ReadResult result)
        {
            if (!inner.TryRead(out result))
                return false;
            _activeRead = result;
            return true;
        }

        public override void AdvanceTo(SequencePosition consumed)
            => AdvanceTo(consumed, consumed);

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            BeforeAdvance?.Invoke();
            if (_rejectAdvanceAtStart && consumed.Equals(_activeRead.Buffer.Start))
                Assert.Fail("The supplied read was retired before its buffer was inspected.");
            _rejectAdvanceAtStart = false;
            if (_expectedAdvanceOffset is { } expectedOffset)
            {
                Assert.AreEqual(_activeRead.Buffer.GetPosition(expectedOffset), consumed,
                    "The read pipe retained bytes before the active result tenure.");
                _expectedAdvanceOffset = null;
            }
            inner.AdvanceTo(consumed, examined);
        }

        public override void CancelPendingRead() => inner.CancelPendingRead();
        public override void Complete(Exception? exception = null) => inner.Complete(exception);
        public override ValueTask CompleteAsync(Exception? exception = null) => inner.CompleteAsync(exception);
    }

    enum LifetimeAction
    {
        LoadCommandComplete,
        LoadReadyForQuery,
        MoveNext,
        Retire,
    }

    static byte[] BackendMessageBytes(BackendType type, int totalLength)
    {
        var bytes = new byte[totalLength];
        bytes[0] = (byte)type;
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(1), totalLength - 1);
        return bytes;
    }

    static byte[] BackendMessageBytes(BackendType type, ReadOnlySpan<byte> body)
    {
        var bytes = BackendMessageBytes(type, BackendHeader.ByteCount + body.Length);
        body.CopyTo(bytes.AsSpan(BackendHeader.ByteCount));
        return bytes;
    }

    static ReadOnlySequence<byte> Segmented(
        ReadOnlyMemory<byte> first, ReadOnlyMemory<byte> second)
    {
        var start = new SequenceSegment(first);
        return start.To(start.Append(second));
    }

    static async ValueTask<bool> ReadNextAsync(ProtocolReadPipe pipe)
    {
        pipe.PrepareRead();
        var read = await pipe.ReadAsync(CancellationToken.None);
        return pipe.CompleteRead(
            read, CancellationToken.None, out _);
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
        Assert.ThrowsExactly<InvalidOperationException>(() => context.TryExtend(0, out _));
    }

    [TestMethod]
    public void IndependentBackendMessage_ReconstructsAChainedBuffer()
    {
        var bytes = BackendMessageBytes(BackendType.DataRow, [1, 2, 3, 4, 5, 6]);
        var sequence = Segmented(bytes.AsMemory(0, 7), bytes.AsMemory(7));
        var message = BackendMessage.CreateIndependent(
            new BackendHeader(BackendType.DataRow, bytes.Length - 1), sequence);

        CollectionAssert.AreEqual(bytes.AsSpan(BackendHeader.ByteCount).ToArray(),
            message.GetSequence().ToArray());
        Assert.IsTrue(message.TryGetFirstSpan(0, out var first));
        CollectionAssert.AreEqual(bytes.AsSpan(BackendHeader.ByteCount, 2).ToArray(),
            first.ToArray());
    }

    [TestMethod]
    public void BackendMessageContext_CurrentThrowsOutsidePublicationWindow()
    {
        var context = new BackendMessageContext();

        Assert.IsFalse(context.TryGetCurrent(out _));
        Assert.ThrowsExactly<InvalidOperationException>(() => _ = context.Current);

        context.SetCursor(new BackendMessageCursor(
            new ReadOnlySequence<byte>(BackendMessageBytes(BackendType.CommandComplete, 6))));
        Assert.IsTrue(context.TryMoveNext());
        Assert.IsTrue(context.TryGetCurrent(out var current));
        Assert.AreEqual(BackendType.CommandComplete, current.Header.Type);
        var accessor = current.GetAccessor();
        Assert.IsFalse(context.TryMoveNext());
        Assert.IsTrue(context.TryGetCurrent(out current));
        Assert.AreEqual(BackendType.CommandComplete, current.Header.Type);
        Assert.AreEqual(BackendType.CommandComplete, accessor.Message.Header.Type);
        context.RetireCursor();
        Assert.IsFalse(context.TryGetCurrent(out _));
        Assert.ThrowsExactly<InvalidOperationException>(() => _ = context.Current);
        Assert.ThrowsExactly<InvalidOperationException>(() => _ = accessor.Message);
    }

    [TestMethod]
    public void BackendMessageCursor_AdvancesAcrossExactSegmentBoundary()
    {
        var first = BackendMessageBytes(BackendType.CommandComplete, 6);
        var second = BackendMessageBytes(BackendType.ReadyForQuery, 6);
        var cursor = new BackendMessageCursor(Segmented(first, second));

        Assert.IsTrue(cursor.TryReadNextInPlace(out var header, out var message, out _));
        Assert.AreEqual(BackendType.CommandComplete, header.Type);
        CollectionAssert.AreEqual(first, message.ToArray());
        Assert.IsTrue(cursor.TryReadNextInPlace(out header, out message, out _));
        Assert.AreEqual(BackendType.ReadyForQuery, header.Type);
        CollectionAssert.AreEqual(second, message.ToArray());
        Assert.IsFalse(cursor.TryReadNextInPlace(out _, out _, out _));
    }

    [TestMethod]
    public void BackendMessageCursor_CollapsesRemainingFinalSegmentToArrayBacking()
    {
        var first = BackendMessageBytes(BackendType.CommandComplete, 6);
        var second = BackendMessageBytes(BackendType.ReadyForQuery, 6);
        var cursor = new BackendMessageCursor(Segmented(first, second));

        Assert.IsTrue(cursor.TryReadNextInPlace(out _, out _, out _));
        Assert.IsTrue(cursor.TryReadNextInPlace(out _, out var remaining, out _));

        Assert.IsInstanceOfType<byte[]>(remaining.Start.GetObject());
        Assert.AreSame(second, remaining.Start.GetObject());
        CollectionAssert.AreEqual(second, remaining.ToArray());
    }

    [TestMethod]
    public void BackendMessageCursor_CollapsesFinalSegmentAfterStraddlingMessage()
    {
        var straddling = BackendMessageBytes(
            BackendType.DataRow, new byte[] { 1, 2, 3, 4, 5, 6 });
        var remainingMessage = BackendMessageBytes(BackendType.ReadyForQuery, 6);
        const int firstSegmentLength = 7;
        var finalSegment = new byte[straddling.Length - firstSegmentLength
            + remainingMessage.Length + 6];
        straddling.AsSpan(firstSegmentLength).CopyTo(finalSegment.AsSpan(3));
        remainingMessage.CopyTo(finalSegment.AsSpan(
            3 + straddling.Length - firstSegmentLength));
        var finalMemory = finalSegment.AsMemory(3,
            straddling.Length - firstSegmentLength + remainingMessage.Length);
        var cursor = new BackendMessageCursor(Segmented(
            straddling.AsMemory(0, firstSegmentLength), finalMemory));

        Assert.IsTrue(cursor.TryReadNextInPlace(out _, out var first, out _));
        Assert.IsFalse(first.IsSingleSegment);
        Assert.IsTrue(cursor.TryReadNextInPlace(out _, out var remaining, out _));

        Assert.AreSame(finalSegment, remaining.Start.GetObject());
        CollectionAssert.AreEqual(remainingMessage, remaining.ToArray());
    }

    [TestMethod]
    public void ContiguousMemory_StraddleIsProjectedOncePerResultTenure()
    {
        var bytes = BackendMessageBytes(
            BackendType.DataRow, new byte[] { 0, 1, 2, 3, 4, 5 });
        var context = new BackendMessageContext();
        context.SetCursor(new(Segmented(
            bytes.AsMemory(0, 7), bytes.AsMemory(7))));
        Assert.IsTrue(context.TryMoveNext());
        var field = context.Current.GetSequence();
        Assert.IsFalse(field.IsSingleSegment);

        var first = context.Current.GetContiguousMemory(field);
        var second = context.Current.GetContiguousMemory(field);
        CollectionAssert.AreEqual(field.ToArray(), first.ToArray());
        Assert.IsTrue(MemoryMarshal.TryGetArray(first, out var firstArray));
        Assert.IsTrue(MemoryMarshal.TryGetArray(second, out var secondArray));
        Assert.AreSame(firstArray.Array, secondArray.Array);

        context.ReleaseContiguousProjections();
        context.RetireCursor();
    }

    [TestMethod]
    public async Task MovingToNextRead_RetiresCurrentBeforeReturningItsStorage()
    {
        var pipe = new Pipe();
        var reader = new RejectRetiredSuppliedReadReader(pipe.Reader);
        var protocolPipe = new ProtocolReadPipe(reader,
            BackendMessageCursor.DefaultDataRowStreamingThreshold,
            ownsReader: true);

        await pipe.Writer.WriteAsync(BackendMessageBytes(BackendType.CommandComplete, 6));
        Assert.IsTrue(await ReadNextAsync(protocolPipe));
        Assert.IsTrue(protocolPipe.TryMoveNext());
        var accessor = protocolPipe.Current.GetAccessor();
        Assert.IsFalse(protocolPipe.TryMoveNext());

        var observedAdvance = false;
        reader.BeforeAdvance = () =>
        {
            observedAdvance = true;
            Assert.IsFalse(protocolPipe.TryGetCurrent(out _),
                "the publication must be retired before its backing storage is returned or refilled");
            Assert.ThrowsExactly<InvalidOperationException>(() => _ = accessor.Message);
            reader.BeforeAdvance = null;
        };

        await pipe.Writer.WriteAsync(BackendMessageBytes(BackendType.ReadyForQuery, 6));
        Assert.IsTrue(await ReadNextAsync(protocolPipe));
        Assert.IsTrue(observedAdvance);
        Assert.IsTrue(protocolPipe.TryMoveNext());
        Assert.AreEqual(BackendType.ReadyForQuery, protocolPipe.Current.Header.Type);

        await pipe.Writer.CompleteAsync();
        await protocolPipe.DisposeAsync();
    }

    [TestMethod]
    public void BackendMessageContext_CurrentLifetime_ExhaustiveShortSequences()
    {
        const int sequenceLength = 5;
        var actions = Enum.GetValues<LifetimeAction>();
        var sequenceCount = (int)Math.Pow(actions.Length, sequenceLength);

        for (var encoded = 0; encoded < sequenceCount; encoded++)
        {
            var context = new BackendMessageContext();
            BackendType? loaded = null;
            BackendType? current = null;
            var remaining = encoded;

            for (var step = 0; step < sequenceLength; step++)
            {
                var action = actions[remaining % actions.Length];
                remaining /= actions.Length;
                switch (action)
                {
                    case LifetimeAction.LoadCommandComplete:
                    case LifetimeAction.LoadReadyForQuery:
                        // ProtocolReadPipe retires the prior cursor before committing replacement
                        // storage. Model that ownership boundary rather than calling SetCursor as a
                        // replacement operation it is not.
                        context.RetireCursor();
                        current = null;
                        loaded = action is LifetimeAction.LoadCommandComplete
                            ? BackendType.CommandComplete
                            : BackendType.ReadyForQuery;
                        context.SetCursor(new(new ReadOnlySequence<byte>(
                            BackendMessageBytes(loaded.Value, 6))));
                        break;
                    case LifetimeAction.MoveNext:
                        Assert.AreEqual(loaded.HasValue, context.TryMoveNext());
                        if (loaded is { } next)
                        {
                            current = next;
                            loaded = null;
                        }
                        break;
                    case LifetimeAction.Retire:
                        context.RetireCursor();
                        loaded = null;
                        current = null;
                        break;
                    default:
                        Assert.Fail($"Unknown publication action: {action}");
                        break;
                }

                Assert.AreEqual(current.HasValue, context.TryGetCurrent(out var message),
                    $"sequence {encoded}, step {step}, action {action}");
                if (current is { } currentType)
                    Assert.AreEqual(currentType, message.Header.Type);
            }
        }
    }

    [TestMethod]
    public async Task RepeatedQueryFrames_WithSmallRecycledBuffers_NeverEnterMessageBodies()
    {
        var repetitions = StressEnv.Iterations(512, 100_000);
        var response = QueryResponseBytes();
        var responseLength = response.Sum(static message => message.Length);
        var wire = new byte[responseLength * repetitions];
        for (var repetition = 0; repetition < repetitions; repetition++)
        {
            var offset = repetition * responseLength;
            foreach (var message in response)
            {
                message.CopyTo(wire, offset);
                offset += message.Length;
            }
        }

        var reader = new DefaultStreamPipeReader(
            new MemoryStream(wire, writable: false),
            new StreamPipeReaderOptions(bufferSize: 1024, useZeroByteReads: false),
            supportCancelPending: false);
        var readPipe = new ProtocolReadPipe(reader,
            BackendMessageCursor.DefaultDataRowStreamingThreshold,
            ownsReader: true);
        var messageIndex = 0;
        while (await readPipe.MoveNextAsync(default))
        {
            while (readPipe.TryMoveNext())
            {
                Assert.AreEqual(response[messageIndex % response.Length][0],
                    (byte)readPipe.Current.Header.Type,
                    $"message {messageIndex}");
                messageIndex++;
            }
        }

        Assert.AreEqual(repetitions * response.Length, messageIndex);
        await readPipe.DisposeAsync();
    }

#if !NET11_0_OR_GREATER
    [TestMethod]
    public async Task RepeatedQueryFrames_ThroughDirectReads_NeverEnterMessageBodies()
    {
        var repetitions = StressEnv.Iterations(512, 100_000);
        var response = QueryResponseBytes();
        var responseLength = response.Sum(static message => message.Length);
        var wire = new byte[responseLength * repetitions];
        for (var repetition = 0; repetition < repetitions; repetition++)
        {
            var offset = repetition * responseLength;
            foreach (var message in response)
            {
                message.CopyTo(wire, offset);
                offset += message.Length;
            }
        }

        var reader = new DefaultStreamPipeReader(
            new MemoryStream(wire, writable: false),
            new StreamPipeReaderOptions(bufferSize: 1024, useZeroByteReads: false),
            supportCancelPending: false);
        var readPipe = new ProtocolReadPipe(reader,
            BackendMessageCursor.DefaultDataRowStreamingThreshold,
            ownsReader: true);
        var directReader = (StreamPipeReader)readPipe.PipeReader;
        var messageIndex = 0;
        while (true)
        {
            readPipe.PrepareRead();
            Assert.IsTrue(directReader.SupportsDirectRead);
            var read = directReader.BeginDirectRead(default);
            while (true)
            {
                var length = await read;
                if (!directReader.CompleteDirectRead(length, default, out read, out var result))
                {
                    continue;
                }
                if (readPipe.CompleteRead(
                        result, default, out var completed))
                {
                    ValidateMessages();
                    break;
                }
                if (completed)
                    goto done;
                break;
            }
        }

        done:
        Assert.AreEqual(repetitions * response.Length, messageIndex);
        await readPipe.DisposeAsync();

        void ValidateMessages()
        {
            while (readPipe.TryMoveNext())
            {
                Assert.AreEqual(response[messageIndex % response.Length][0],
                    (byte)readPipe.Current.Header.Type,
                    $"message {messageIndex}");
                messageIndex++;
            }
        }
    }
#endif

    static byte[][] QueryResponseBytes()
    {
        var rowDescriptionBody = new byte[29];
        BinaryPrimitives.WriteInt16BigEndian(rowDescriptionBody, 1);
        "?column?\0"u8.CopyTo(rowDescriptionBody.AsSpan(2));
        BinaryPrimitives.WriteUInt32BigEndian(rowDescriptionBody.AsSpan(11), 0);
        BinaryPrimitives.WriteInt16BigEndian(rowDescriptionBody.AsSpan(15), 0);
        BinaryPrimitives.WriteUInt32BigEndian(rowDescriptionBody.AsSpan(17), 23);
        BinaryPrimitives.WriteInt16BigEndian(rowDescriptionBody.AsSpan(21), 4);
        BinaryPrimitives.WriteInt32BigEndian(rowDescriptionBody.AsSpan(23), -1);
        BinaryPrimitives.WriteInt16BigEndian(rowDescriptionBody.AsSpan(27), 0);
        return
        [
            BackendMessageBytes(BackendType.ParseComplete, []),
            BackendMessageBytes(BackendType.BindComplete, []),
            BackendMessageBytes(BackendType.RowDescription, rowDescriptionBody),
            BackendMessageBytes(BackendType.DataRow, [0, 1, 0, 0, 0, 1, (byte)'1']),
            BackendMessageBytes(BackendType.CommandComplete, "SELECT 1\0"u8),
            BackendMessageBytes(BackendType.ReadyForQuery, [(byte)'I'])
        ];
    }

    [TestMethod]
    public async Task Eof_InvalidatesPublishedBackendMessage()
    {
        var pipe = new Pipe();
        var readPipe = new ProtocolReadPipe(pipe.Reader,
            BackendMessageCursor.DefaultDataRowStreamingThreshold);
        await pipe.Writer.WriteAsync(BackendMessageBytes(BackendType.ReadyForQuery, 6));

        Assert.IsTrue(await readPipe.MoveNextAsync(CancellationToken.None));
        Assert.IsTrue(readPipe.TryMoveNext());
        var accessor = readPipe.Current.GetAccessor();
        Assert.IsFalse(readPipe.TryMoveNext());

        await pipe.Writer.CompleteAsync();
        Assert.IsFalse(await readPipe.MoveNextAsync(CancellationToken.None));
        Assert.IsFalse(readPipe.TryGetCurrent(out _));
        Assert.ThrowsExactly<InvalidOperationException>(() => _ = readPipe.Current);
        Assert.ThrowsExactly<InvalidOperationException>(() => _ = accessor.Message);
        await readPipe.DisposeAsync();
    }

    [TestMethod]
    public async Task EndingResultRetention_ReexposesBufferedSuccessorMessages()
    {
        var first = BackendMessageBytes(BackendType.DataRow, [0, 0]);
        var terminal = BackendMessageBytes(BackendType.CommandComplete, "SELECT 1\0"u8);
        var ready = BackendMessageBytes(BackendType.ReadyForQuery, [(byte)'I']);
        var successor = BackendMessageBytes(BackendType.BindComplete, []);
        var wire = new byte[first.Length + terminal.Length + ready.Length + successor.Length];
        var offset = 0;
        foreach (var message in (byte[][])[first, terminal, ready, successor])
        {
            message.CopyTo(wire, offset);
            offset += message.Length;
        }

        var pipe = new Pipe();
        var readPipe = new ProtocolReadPipe(pipe.Reader,
            BackendMessageCursor.DefaultDataRowStreamingThreshold);
        await pipe.Writer.WriteAsync(wire);

        Assert.IsTrue(await readPipe.MoveNextAsync(default));
        Assert.IsTrue(readPipe.TryMoveNext());
        Assert.AreEqual(BackendType.DataRow, readPipe.Current.Header.Type);
        readPipe.EnableResultRetention();
        Assert.IsTrue(readPipe.TryMoveNext());
        Assert.AreEqual(BackendType.CommandComplete, readPipe.Current.Header.Type);
        Assert.IsTrue(readPipe.TryMoveNext());
        Assert.AreEqual(BackendType.ReadyForQuery, readPipe.Current.Header.Type);

        readPipe.EndResultRetention();
        Assert.IsTrue(readPipe.TryMoveNext());
        Assert.AreEqual(BackendType.BindComplete, readPipe.Current.Header.Type);

        await pipe.Writer.CompleteAsync();
        await readPipe.DisposeAsync();
    }

    [TestMethod]
    public async Task BeginningNewResultRetention_DropsPriorResultPrefixAtNextRead()
    {
        var prior = BackendMessageBytes(BackendType.DataRow, [0, 1]);
        var retained = BackendMessageBytes(BackendType.DataRow, [2, 3]);
        var terminal = BackendMessageBytes(BackendType.CommandComplete, "SELECT 1\0"u8);
        var firstGrant = new byte[prior.Length + retained.Length];
        prior.CopyTo(firstGrant, 0);
        retained.CopyTo(firstGrant, prior.Length);

        var pipe = new Pipe();
        var reader = new RejectRetiredSuppliedReadReader(pipe.Reader);
        var readPipe = new ProtocolReadPipe(reader,
            BackendMessageCursor.DefaultDataRowStreamingThreshold);
        await pipe.Writer.WriteAsync(firstGrant);

        Assert.IsTrue(await readPipe.MoveNextAsync(default));
        Assert.IsTrue(readPipe.TryMoveNext());
        readPipe.EnableResultRetention();
        readPipe.EndResultRetention();
        Assert.IsTrue(readPipe.TryMoveNext());
        readPipe.EnableResultRetention();
        Assert.IsFalse(readPipe.TryMoveNext());

        reader.ExpectAdvanceAtActiveOffset(prior.Length);
        await pipe.Writer.WriteAsync(terminal);
        Assert.IsTrue(await readPipe.MoveNextAsync(default));
        Assert.IsTrue(readPipe.TryMoveNext());
        Assert.AreEqual(BackendType.CommandComplete, readPipe.CurrentType);

        readPipe.EndResultRetention();
        await pipe.Writer.CompleteAsync();
        await readPipe.DisposeAsync();
    }

    [TestMethod]
    public async Task IdleRelease_UnexaminesAndReacquiresBufferedSuffix()
    {
        var completed = BackendMessageBytes(BackendType.ReadyForQuery, [(byte)'I']);
        var suffix = BackendMessageBytes(BackendType.NotificationResponse, [1, 2, 3]);
        var wire = new byte[completed.Length + suffix.Length];
        completed.CopyTo(wire, 0);
        suffix.CopyTo(wire, completed.Length);
        var pipe = new Pipe();
        var readPipe = new ProtocolReadPipe(pipe.Reader,
            BackendMessageCursor.DefaultDataRowStreamingThreshold);
        await pipe.Writer.WriteAsync(wire);

        Assert.IsTrue(await readPipe.MoveNextAsync(default));
        Assert.IsTrue(readPipe.TryMoveNext());
        Assert.AreEqual(BackendType.ReadyForQuery, readPipe.CurrentType);

        readPipe.ReleaseReadBufferAtIdle();

        Assert.IsTrue(await readPipe.MoveNextAsync(default));
        Assert.IsTrue(readPipe.TryMoveNext());
        Assert.AreEqual(BackendType.NotificationResponse, readPipe.CurrentType);
        await pipe.Writer.CompleteAsync();
        await readPipe.DisposeAsync();
    }

    [TestMethod]
    public async Task BackendBodyReader_ExtendsPrefixThenSlides()
    {
        var pipe = new Pipe();
        var wire = BackendMessageBytes(BackendType.DataRow, 32);
        for (var i = BackendHeader.ByteCount; i < wire.Length; i++)
            wire[i] = (byte)i;

        var decoder = new PgDecoder(
            pipe.Reader, 8, CancellationToken.None, Timeout.InfiniteTimeSpan);
        decoder.Pipe.BindDecoder(decoder);

        await pipe.Writer.WriteAsync(wire.AsMemory(0, 8));
        Assert.IsTrue(await ReadNextAsync(decoder.Pipe));
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
    public async Task BackendReadPipe_ExtendedRowAdvancesToTrailingMessage()
    {
        var bind = BackendMessageBytes(BackendType.BindComplete, BackendHeader.ByteCount);
        var row = BackendMessageBytes(BackendType.DataRow, 128 * 1024);
        var complete = BackendMessageBytes(BackendType.CommandComplete, BackendHeader.ByteCount);
        var wire = new byte[bind.Length + row.Length + complete.Length];
        bind.CopyTo(wire, 0);
        row.CopyTo(wire, bind.Length);
        complete.CopyTo(wire, bind.Length + row.Length);
        var pipe = new Pipe(new PipeOptions(
            pauseWriterThreshold: 256 * 1024,
            resumeWriterThreshold: 128 * 1024));
        var decoder = new PgDecoder(pipe.Reader,
            BackendMessageCursor.DefaultDataRowStreamingThreshold,
            CancellationToken.None, Timeout.InfiniteTimeSpan);
        decoder.Pipe.BindDecoder(decoder);

        var initialLength = bind.Length
            + BackendMessageCursor.DefaultDataRowStreamingThreshold;
        await pipe.Writer.WriteAsync(wire.AsMemory(0, initialLength));
        Assert.IsTrue(await decoder.Pipe.MoveNextAsync(default));
        Assert.IsTrue(decoder.Pipe.TryMoveNext());
        Assert.AreEqual(BackendType.BindComplete, decoder.Pipe.Current.Header.Type);
        Assert.IsTrue(decoder.Pipe.TryMoveNext());
        Assert.AreEqual(BackendType.DataRow, decoder.Pipe.Current.Header.Type);
        var body = decoder.Pipe.Current.OpenBodyReader();
        await pipe.Writer.WriteAsync(wire.AsMemory(initialLength));
        while (!body.IsComplete)
            Assert.IsTrue(body.TryExtend());

        Assert.IsFalse(decoder.Pipe.TryMoveNext());
        Assert.IsTrue(await decoder.Pipe.MoveNextAsync(default));
        Assert.IsTrue(decoder.Pipe.TryMoveNext());
        Assert.AreEqual(BackendType.CommandComplete, decoder.Pipe.Current.Header.Type);
        await pipe.Writer.CompleteAsync();
        await ((IAsyncDisposable)decoder).DisposeAsync();
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
    public void BackendCursor_WaitsForUsefulPartialDataRowPrefix()
    {
        var rowLength = 128 * 1024;
        var wire = BackendMessageBytes(BackendType.DataRow, rowLength);

        var smallPrefix = new ReadOnlySequence<byte>(wire.AsMemory(0, 32));
        var cursor = new BackendMessageCursor(smallPrefix);
        Assert.IsFalse(cursor.TryReadNextInPlace(out _, out _, out _));
        Assert.AreEqual(BackendMessageCursor.DefaultDataRowStreamingThreshold,
            cursor.RequiredBufferedLength);

        var usefulPrefix = new ReadOnlySequence<byte>(
            wire.AsMemory(0, BackendMessageCursor.DefaultDataRowStreamingThreshold));
        cursor = new(usefulPrefix);
        Assert.IsTrue(cursor.TryReadNextInPlace(out var rowHeader, out var partialRow, out _));
        Assert.AreEqual(BackendType.DataRow, rowHeader.Type);
        Assert.AreEqual(BackendMessageCursor.DefaultDataRowStreamingThreshold, partialRow.Length);
        Assert.IsFalse(new BackendMessage(rowHeader, partialRow, new BackendMessageContext(), 0).Buffered);
    }

    [TestMethod]
    public void BackendCursor_FramesUnknownMessageType()
    {
        var wire = BackendHeaderBytes((BackendType)(byte)'o', 4);
        var cursor = new BackendMessageCursor(new ReadOnlySequence<byte>(wire));

        Assert.IsTrue(cursor.TryReadNextInPlace(out var header, out _, out _));
        Assert.AreEqual((BackendType)(byte)'o', header.Type);
    }

    [TestMethod]
    public void BackendCursor_FramesMemoryManagerBackedMessage()
    {
        using var manager = new TestMemoryManager(
            BackendMessageBytes(BackendType.CommandComplete, 8));
        var cursor = new BackendMessageCursor(
            new ReadOnlySequence<byte>(manager.Memory));

        Assert.IsTrue(cursor.TryReadNextInPlace(
            out var header, out var message, out var bufferedLength));
        Assert.AreEqual(BackendType.CommandComplete, header.Type);
        Assert.AreEqual(8, message.Length);
        Assert.AreEqual(8u, bufferedLength);
    }

    [TestMethod]
    public void BackendCursor_RejectsMessageBeyondPostgreSqlAllocationLimit()
    {
        var wire = BackendHeaderBytes(BackendType.DataRow, 0x3FFF_FFFF);
        var cursor = new BackendMessageCursor(new ReadOnlySequence<byte>(wire));

        Assert.ThrowsExactly<PgFramingException>(() =>
            cursor.TryReadNextInPlace(out _, out _, out _));
    }

    static byte[] BackendHeaderBytes(BackendType type, int length)
    {
        var bytes = new byte[BackendHeader.ByteCount];
        bytes[0] = (byte)type;
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(1), length);
        return bytes;
    }
}
