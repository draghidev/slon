using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Tests.Pg;

// Protocol termination tests driven by a synthetic transport. These deliberately avoid the
// private-connection lane: no PostgreSQL connection is opened.
[TestClass]
public class ProtocolTerminalTransportTests
{
    static byte[] Handshake()
    {
        var bytes = new byte[64];
        var offset = 0;
        bytes[offset++] = (byte)'R';
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(offset), 8); offset += 4;
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(offset), 0); offset += 4;
        bytes[offset++] = (byte)'K';
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(offset), 12); offset += 4;
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(offset), 4321); offset += 4;
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(offset), 8765); offset += 4;
        bytes[offset++] = (byte)'Z';
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(offset), 5); offset += 4;
        bytes[offset++] = (byte)'I';
        return bytes.AsSpan(0, offset).ToArray();
    }

    static byte[] ErrorResponse(string severity, string sqlState, string message)
    {
        using var body = new MemoryStream();
        WriteField('S', severity);
        WriteField('V', severity);
        WriteField('C', sqlState);
        WriteField('M', message);
        body.WriteByte(0);

        var fields = body.ToArray();
        var result = new byte[BackendHeader.ByteCount + fields.Length];
        result[0] = (byte)'E';
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(1), sizeof(int) + fields.Length);
        fields.CopyTo(result, BackendHeader.ByteCount);
        return result;

        void WriteField(char type, string value)
        {
            body.WriteByte((byte)type);
            body.Write(Encoding.UTF8.GetBytes(value));
            body.WriteByte(0);
        }
    }

    static async Task<Exception> ObserveFailure(CommandFlow flow)
    {
        var enumerator = flow.GetAsyncEnumerator();
        try
        {
            while (await enumerator.MoveNextAsync())
                enumerator.Current.GetCommandComplete();
        }
        catch (Exception exception)
        {
            return exception;
        }
        finally
        {
            try { await enumerator.DisposeAsync(); }
            catch { }
        }
        return new AssertFailedException("The flow completed without observing the injected failure.");
    }

    [TestMethod]
    public async Task ExternalForcefulCompletion_IsNotClassifiedAsCollateral()
    {
        var options = PgTestPool.NewOptions();
        var transport = new ControlledEofTransport(Handshake());
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);

        var flow = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(flow));
        var e = flow.GetAsyncEnumerator();
        var move = e.MoveNextAsync().AsTask();
        await transport.ReadParked;

        var supplied = new InvalidOperationException("external shutdown");
        var completion = protocol.CompleteAsync(supplied);
        transport.CompleteServerOutput();

        var observed = await Assert.ThrowsExactlyAsync<PgClientClosedException>(() => move);
        Assert.AreSame(supplied, observed.InnerException);
        await completion;
    }

    [TestMethod]
    [DataRow("FATAL", "ZZ999")]
    [DataRow("PANIC", "XX000")]
    public async Task FatalOrPanicFollowedByEof_IsCollateralForEveryFlow(string severity, string sqlState)
    {
        var options = PgTestPool.NewOptions();
        var transport = new ControlledEofTransport(Handshake());
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);

        var observing = new CommandFlow(async: true, Command.Create("select 1"));
        var successor = new CommandFlow(async: true, Command.Create("select 2"));
        Assert.IsTrue(protocol.TryQueue(observing));
        Assert.IsTrue(protocol.TryQueue(successor));
        var observingFailure = ObserveFailure(observing);
        var successorFailure = ObserveFailure(successor);
        await transport.ReadParked;

        await transport.WriteServerOutputAsync(ErrorResponse(severity, sqlState, "the backend session terminated"));
        transport.CompleteServerOutput();

        foreach (var failure in new[] { await observingFailure, await successorFailure })
        {
            var collateral = Assert.IsInstanceOfType<PgCollateralException>(failure);
            Assert.AreEqual(PgCollateralKind.BackendTermination, collateral.Kind);
            var backend = Assert.IsInstanceOfType<PgErrorException>(collateral.InnerException);
            Assert.AreEqual(sqlState, backend.SqlState);
        }
        await protocol.Completion;
    }

    [TestMethod]
    public async Task OrdinaryErrorFollowedByEof_IsNotBackendTermination()
    {
        var options = PgTestPool.NewOptions();
        var transport = new ControlledEofTransport(Handshake());
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);

        var observing = new CommandFlow(async: true, Command.Create("select 1"));
        var successor = new CommandFlow(async: true, Command.Create("select 2"));
        Assert.IsTrue(protocol.TryQueue(observing));
        Assert.IsTrue(protocol.TryQueue(successor));
        var observingFailure = ObserveFailure(observing);
        var successorFailure = ObserveFailure(successor);
        await transport.ReadParked;

        await transport.WriteServerOutputAsync(ErrorResponse("ERROR", "22012", "division by zero"));
        transport.CompleteServerOutput();

        var backend = Assert.IsInstanceOfType<PgErrorException>(await observingFailure);
        Assert.AreEqual("22012", backend.SqlState);
        var successorException = await successorFailure;
        var collateral = Assert.IsInstanceOfType<PgCollateralException>(
            successorException, successorException.ToString());
        Assert.AreEqual(PgCollateralKind.ProtocolFailure, collateral.Kind);
        Assert.IsInstanceOfType<PgProtocolException>(collateral.InnerException);
        await protocol.Completion;
    }

    [TestMethod]
    public async Task TransportReset_CondemnsWithoutRecoveryWrite()
    {
        var options = PgTestPool.NewOptions();
        var transport = new ControlledEofTransport(Handshake());
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        await protocol.StartAsync(options, transport);

        var observing = new CommandFlow(async: true, Command.Create("select 1"));
        Assert.IsTrue(protocol.TryQueue(observing));
        var observingFailure = ObserveFailure(observing);
        await transport.ReadParked;

        transport.ResetServerOutput();

        await observingFailure;
        await protocol.Completion;
        Assert.IsGreaterThan(0, transport.ConnectionLostChecks);
        Assert.AreEqual(0, transport.RecoveryMessagesAfterConnectionLost,
            "A known-dead transport must not receive recovery ROLLBACK/Sync bytes.");
    }

    sealed class ControlledEofTransport : TransportConnection
    {
        readonly Pipe _toClient = new();
        readonly Pipe _toServer = new(new PipeOptions(
            pauseWriterThreshold: 1 << 30,
            resumeWriterThreshold: 1 << 29));
        readonly CancellationIgnoringReader _reader;
        readonly ObservingWriter _writer;
        readonly IOException _reset = new("connection reset", new SocketException((int)SocketError.ConnectionReset));

        public ControlledEofTransport(byte[] handshake)
        {
            _reader = new(_toClient.Reader);
            _writer = new(_toServer.Writer);
            _toClient.Writer.WriteAsync(handshake).AsTask().GetAwaiter().GetResult();
        }

        public Task ReadParked => _reader.ReadParked;
        public override PipeReader Reader => _reader;
        public override PipeWriter Writer => _writer;
        public override void WaitWritable() { }

        public int ConnectionLostChecks { get; private set; }
        public int RecoveryMessagesAfterConnectionLost => _writer.RecoveryMessagesAfterConnectionLost;

        public override bool IsConnectionLost(Exception exception)
        {
            ConnectionLostChecks++;
            return ReferenceEquals(exception, _reset);
        }

        public ValueTask<FlushResult> WriteServerOutputAsync(byte[] bytes)
            => _toClient.Writer.WriteAsync(bytes);

        public void CompleteServerOutput() => _toClient.Writer.Complete();

        public void ResetServerOutput()
        {
            _writer.ConnectionLost = true;
            _toClient.Writer.Complete(_reset);
        }

        sealed class ObservingWriter(PipeWriter inner) : PipeWriter
        {
            Memory<byte> _memory;

            public bool ConnectionLost { get; set; }
            public int RecoveryMessagesAfterConnectionLost { get; private set; }
            public override bool CanGetUnflushedBytes => inner.CanGetUnflushedBytes;
            public override long UnflushedBytes => inner.UnflushedBytes;

            public override void Advance(int bytes)
            {
                if (ConnectionLost && bytes >= BackendHeader.ByteCount
                    && _memory.Span[0] is (byte)'Q' or (byte)'S')
                {
                    RecoveryMessagesAfterConnectionLost++;
                }
                inner.Advance(bytes);
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
                => _memory = inner.GetMemory(sizeHint);

            public override Span<byte> GetSpan(int sizeHint = 0)
                => GetMemory(sizeHint).Span;
            public override void CancelPendingFlush() => inner.CancelPendingFlush();
            public override void Complete(Exception? exception = null) => inner.Complete(exception);
            public override ValueTask CompleteAsync(Exception? exception = null) => inner.CompleteAsync(exception);
            public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
                => inner.FlushAsync(cancellationToken);
        }

        sealed class CancellationIgnoringReader(PipeReader inner) : PipeReader
        {
            readonly TaskCompletionSource _readParked = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public Task ReadParked => _readParked.Task;

            public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
            {
                var read = inner.ReadAsync(CancellationToken.None);
                if (read.IsCompletedSuccessfully)
                    return read;

                _readParked.TrySetResult();
                return Core(this, read);

                static async ValueTask<ReadResult> Core(CancellationIgnoringReader self, ValueTask<ReadResult> read)
                {
                    Interlocked.Exchange(ref self._readActive, 1);
                    try
                    {
                        return await read.ConfigureAwait(false);
                    }
                    finally
                    {
                        Interlocked.Exchange(ref self._readActive, 0);
                    }
                }
            }

            public override bool TryRead(out ReadResult result) => inner.TryRead(out result);
            public override void AdvanceTo(SequencePosition consumed) => inner.AdvanceTo(consumed);
            public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
                => inner.AdvanceTo(consumed, examined);
            public override void CancelPendingRead() { }
            public override void Complete(Exception? exception = null)
            {
                ThrowIfReadActive();
                inner.Complete(exception);
            }

            public override ValueTask CompleteAsync(Exception? exception = null)
            {
                ThrowIfReadActive();
                return inner.CompleteAsync(exception);
            }

            int _readActive;

            void ThrowIfReadActive()
            {
                if (Volatile.Read(ref _readActive) is not 0)
                    throw new InvalidOperationException("Reader completion raced its active ReadAsync.");
            }
        }
    }
}
