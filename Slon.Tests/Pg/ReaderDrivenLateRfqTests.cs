using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Text;
using Slon.Transport;

namespace Slon.Tests.Pg;

// The reader-driven drain reads the flow's own ReadyForQuery after the command's terminal message.
// When that read has to wait for the wire, the drain must consume the RFQ from the read that
// delivered it; a further move-next waits for a message the backend never sends.
[TestClass]
public class ReaderDrivenLateRfqTests
{
    static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    static async Task<(PgClientProtocol Protocol, LateRfqTransport Transport)> NewLateRfqProtocolAsync()
    {
        var options = PgTestPool.NewOptions();
        var transport = new LateRfqTransport(await SocketStreamConnection.ConnectAsync(options.EndPoint));
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options)
        {
            BackendProvider = DefaultPostgreSqlBackendProvider.Instance,
            CancellationTimeout = TimeSpan.FromMinutes(1)
        });
        await protocol.StartAsync(options, transport);
        return (protocol, transport);
    }

    static async Task<CommandDescriptor> Prepare(PgClientProtocol protocol, string sql, EncodedCString name)
    {
        var results = protocol.Queue(new CommandFlow(async: true,
            Command.Create(sql, commandName: name) with { DescribeOnly = true })).GetAsyncEnumerator();
        CommandDescriptor descriptor = default;
        while (await results.MoveNextAsync())
            descriptor = results.Current.GetMetadata().ToPreparedDescriptor();
        await results.DisposeAsync();
        return descriptor;
    }

    [ConnectionCreatingTestMethod]
    public async Task Streaming_LateRfq_CompletesTheDrain()
    {
        var (protocol, transport) = await NewLateRfqProtocolAsync();
        await using var _ = protocol;
        var descriptor = await Prepare(protocol, "select 1", "late_rfq_stream");

        var results = protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(descriptor))).GetAsyncEnumerator();
        Assert.IsTrue(await results.MoveNextAsync().AsTask().WaitAsync(Patience));
        await foreach (var row in results.Current)
            Assert.AreEqual(1, row.GetValue<int>(0));
        Assert.IsFalse(await results.MoveNextAsync().AsTask().WaitAsync(Patience));
        await results.DisposeAsync();

        Assert.IsGreaterThan(0, transport.SplitCount);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    [ConnectionCreatingTestMethod]
    public async Task Collect_LateRfq_CompletesTheDrain()
    {
        var (protocol, transport) = await NewLateRfqProtocolAsync();
        await using var _ = protocol;
        var descriptor = await Prepare(protocol, "select 1", "late_rfq_collect");

        var values = new List<int>();
        await protocol.Queue(new ReaderDrivenCommandFlow(Command.Create(descriptor)))
            .CollectAsync(values, static (state, row) => ((List<int>)state!).Add(row.GetValue<int>(0)))
            .AsTask().WaitAsync(Patience);

        CollectionAssert.AreEqual(new[] { 1 }, values);
        Assert.IsGreaterThan(0, transport.SplitCount);
        await PgTestPool.RunAsync(protocol, "select 1");
    }

    // Forwards the inner transport's reads, holding back a trailing ReadyForQuery so it arrives on a
    // separate read that completes after the consumer already parked on it.
    sealed class LateRfqTransport : TransportConnection
    {
        const int ReadyForQueryLength = 6;
        readonly TransportConnection _inner;
        readonly Pipe _toClient = new(new PipeOptions(pauseWriterThreshold: 0));
        int _splitCount;

        public LateRfqTransport(TransportConnection inner)
        {
            _inner = inner;
            _ = PumpAsync();
        }

        public int SplitCount => Volatile.Read(ref _splitCount);
        public override PipeReader Reader => _toClient.Reader;
        public override PipeWriter Writer => _inner.Writer;
        public override void WaitUntilWritable(TimeSpan timeout) => _inner.WaitUntilWritable(timeout);
        public override bool IsConnectionLost(Exception exception) => _inner.IsConnectionLost(exception);
        public override void Abort() => _inner.Abort();

        async Task PumpAsync()
        {
            var reader = _inner.Reader;
            var writer = _toClient.Writer;
            try
            {
                while (true)
                {
                    var result = await reader.ReadAsync();
                    var buffer = result.Buffer;
                    if (buffer.Length > 0)
                    {
                        var split = EndsWithReadyForQuery(buffer) ? buffer.Length - ReadyForQueryLength : buffer.Length;
                        if (split > 0)
                        {
                            foreach (var segment in buffer.Slice(0, split))
                                await writer.WriteAsync(segment);
                        }
                        if (split != buffer.Length)
                        {
                            await Task.Delay(20);
                            Interlocked.Increment(ref _splitCount);
                            foreach (var segment in buffer.Slice(split))
                                await writer.WriteAsync(segment);
                        }
                    }
                    reader.AdvanceTo(buffer.End);
                    if (result.IsCompleted || result.IsCanceled)
                        break;
                }
                await writer.CompleteAsync();
            }
            catch (Exception ex)
            {
                await writer.CompleteAsync(ex);
            }
        }

        static bool EndsWithReadyForQuery(in ReadOnlySequence<byte> buffer)
        {
            if (buffer.Length < ReadyForQueryLength)
                return false;
            Span<byte> tail = stackalloc byte[ReadyForQueryLength];
            buffer.Slice(buffer.Length - ReadyForQueryLength).CopyTo(tail);
            return tail[0] == (byte)'Z' && BinaryPrimitives.ReadInt32BigEndian(tail.Slice(1)) == ReadyForQueryLength - 1;
        }
    }
}
