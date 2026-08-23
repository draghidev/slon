using System.Threading.Channels;
using System.Threading.Tasks.Sources;
using BenchmarkDotNet.Attributes;
using Npgsql;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pooling;
using Slon.Text;

namespace Slon.Benchmark;

public class PgClientRequestBenchmark : ClientBenchmark
{
    const int ObjectPoolSize = Commands;
    const string CommandText = "SELECT 'fortune data here as a string' FROM generate_series(1,10)";
    static readonly EncodedCString StatementName = "p";
    static readonly CommandFlowOptions Command = new() { Commands = new(new Command { Descriptor = CommandDescriptor.Create(CommandText, default, StatementName) }) };
    static readonly CommandFlowOptions PreparedCommand = new() { Commands = new(new Command { Descriptor = CommandDescriptor.CreatePrepared(StatementName, default, null!) }) };

    static readonly bool RpsBench = true;

    // [Benchmark(Baseline = true)]
    [ArgumentsSource(nameof(NpgsqlMultiplexingPooledArgs))]
    public async ValueTask NpgsqlMultiplexingPooled(NpgsqlArgument commands)
    {
        commands.CtsHolder.Cts = new();
        if (!RpsBench)
            commands.CtsHolder.Cts.Cancel();
        else
            commands.CtsHolder.Cts.CancelAfter(TimeSpan.FromSeconds(1));
        for (var i = 0; i < commands.WorkItems.Length; i++)
        {
            var workitem = commands.WorkItems[i];
            workitem.Reset();
            ThreadPool.UnsafeQueueUserWorkItem(workitem, preferLocal: false);
        }

        var totalCount = 0;
        for (var i = 0; i < commands.WorkItems.Length; i++)
        {
            totalCount += await commands.WorkItems[i].Task;
        }
        if (RpsBench)
            Console.WriteLine(totalCount);
    }

    [Benchmark]
    [ArgumentsSource(nameof(PipelinedPooled))]
    public async ValueTask FlowPoolPipelinedObjectPool(Argument commands)
    {
        commands.CtsHolder.Cts = new();
        if (!RpsBench)
            commands.CtsHolder.Cts.Cancel();
        else
            commands.CtsHolder.Cts.CancelAfter(TimeSpan.FromSeconds(1));
        for (var i = 0; i < commands.WorkItems.Length; i++)
        {
            var workitem = commands.WorkItems[i];
            workitem.Reset();
            ThreadPool.UnsafeQueueUserWorkItem(workitem, preferLocal: false);
        }

        var totalCount = 0;
        for (var i = 0; i < commands.WorkItems.Length; i++)
        {
            totalCount += await commands.WorkItems[i].Task;
        }
        if (RpsBench)
            Console.WriteLine(totalCount);
    }

    public readonly struct FortuneUtf8(int id, byte[] message) : IComparable<FortuneUtf8>, IComparable
    {
        public int Id { get; } = id;
        public byte[] Message { get; } = message;

        // Performance critical, using culture insensitive comparison
        public int CompareTo(FortuneUtf8 other) => Message.AsSpan().SequenceCompareTo(other.Message.AsSpan());
        public int CompareTo(object? obj) => throw new InvalidOperationException("The non-generic CompareTo should not be used");
    }

    // Simulate response work.
    static void ResponseWork()
    {
        var additionalFortune = "Message from fortunes."u8.ToArray();

        var result = new List<FortuneUtf8>();
        result.Add(new(1, "fortune: No such file or directory"u8.ToArray()));
        result.Add(new(2, "A computer scientist is someone who fixes things that aren't broken."u8.ToArray()));
        result.Add(new(3, "After enough decimal places, nobody gives a damn."u8.ToArray()));
        result.Add(new(4, "A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1"u8.ToArray()));
        result.Add(new(5, "A computer program does what you tell it to do, not what you want it to do."u8.ToArray()));
        result.Add(new(6, "Emacs is a nice operating system, but I prefer UNIX. — Tom Christaensen"u8.ToArray()));
        result.Add(new(7,"Any program that runs right is obsolete."u8.ToArray()));
        result.Add(new(8, "A list is only as strong as its weakest link. — Donald Knuth"u8.ToArray()));
        result.Add(new(9, "Feature: A bug with seniority."u8.ToArray()));
        result.Add(new(10, "Computers make very fast, very accurate mistakes."u8.ToArray()));
        result.Add(new(11, """<script>alert(""This should not be displayed in a browser alert box."");</script>"""u8.ToArray()));
        result.Add(new(12, "フレームワークのベンチマーク"u8.ToArray()));

        result.Add(new(id: 0, additionalFortune));
        result.Sort();

    }

    internal class NpgsqlRequest(ObjectPool<NpgsqlConnection, NpgsqlRequest.PoolPolicy> connectionPool, CtsHolder holder) : IThreadPoolWorkItem, IValueTaskSource<int>
    {
        ManualResetValueTaskSourceCore<int> _taskSourceCore;
        int _count;

        public async void Execute()
        {
            var cancellationToken = holder.Cts.Token;
            try
            {
                do
                {
                    var conn = connectionPool.Get();
                    await conn.OpenAsync();
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = CommandText;
                    await using var reader = await cmd.ExecuteReaderAsync();
                    cmd.Dispose();
                    await conn.CloseAsync();
                    connectionPool.Return(conn);

                    ResponseWork();
                    _count++;
                } while (!cancellationToken.IsCancellationRequested);

                _taskSourceCore.SetResult(_count);
            }
            catch (Exception ex)
            {
                _taskSourceCore.SetException(ex);
            }
        }

        public ValueTask<int> Task => new(this, _taskSourceCore.Version);
        public void Reset()
        {
            _taskSourceCore.Reset();
            _count = 0;
        }

        int IValueTaskSource<int>.GetResult(short token) => _taskSourceCore.GetResult(token);
        ValueTaskSourceStatus IValueTaskSource<int>.GetStatus(short token) => _taskSourceCore.GetStatus(token);
        void IValueTaskSource<int>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => _taskSourceCore.OnCompleted(continuation, state, token, flags);

        internal readonly struct PoolPolicy(Func<NpgsqlConnection> factory) : IPooledObjectPolicy<NpgsqlConnection>
        {
            public NpgsqlConnection Create() => factory();
            public bool Return(NpgsqlConnection obj) => true;
        }
    }

    internal class Request(ObjectPool<CommandFlow, PgClientFlowPolicy<CommandFlow>> objectPool, CtsHolder holder, ChannelWriter<CommandFlow> channelWriter) : IThreadPoolWorkItem, IValueTaskSource<int>
    {
        ManualResetValueTaskSourceCore<int> _taskSourceCore;
        int _count;

        public async void Execute()
        {
            var cancellationToken = holder.Cts.Token;
            try
            {
                do
                {
                    var flow = objectPool.Get();
                    if (flow.IsStarted)
                        await flow.WaitForComplete();
                    flow.Reset();
                    flow.Initialize(async: true, PreparedCommand);
                    channelWriter.TryWrite(flow);
                    await using (var reader = flow.GetAsyncEnumerator())
                    {
                    }

                    objectPool.Return(flow);

                    ResponseWork();
                    _count++;
                } while (!cancellationToken.IsCancellationRequested);

                _taskSourceCore.SetResult(_count);
            }
            catch (Exception ex)
            {
                _taskSourceCore.SetException(ex);
            }
        }

        public ValueTask<int> Task => new(this, _taskSourceCore.Version);
        public void Reset()
        {
            _taskSourceCore.Reset();
            _count = 0;
        }

        int IValueTaskSource<int>.GetResult(short token) => _taskSourceCore.GetResult(token);
        ValueTaskSourceStatus IValueTaskSource<int>.GetStatus(short token) => _taskSourceCore.GetStatus(token);
        void IValueTaskSource<int>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => _taskSourceCore.OnCompleted(continuation, state, token, flags);
    }

    public IEnumerable<object> NpgsqlMultiplexingPooledArgs()
    {
        var dataSource = InitNpgsql(builder => builder.ConnectionStringBuilder.MaxAutoPrepare = 1);
        var objectPool = new ObjectPool<NpgsqlConnection, NpgsqlRequest.PoolPolicy>(new(() =>
        {
            var conn = dataSource.CreateConnection();
            conn.CreateCommand().Dispose();
            return conn;
        }), ObjectPoolSize);
        var holder = new CtsHolder();

        yield return new NpgsqlArgument
        {
            WorkItems = Enumerable.Range(0, Commands).Select(i => new NpgsqlRequest(objectPool, holder)).ToArray(),
            CtsHolder = holder
        };
    }

    public IEnumerable<object> PipelinedPooled()
    {
        var pool = InitSlonPool(async (protocol, cancellationToken) =>
        {
            // Prepare
            var flow = new CommandFlow(async: true, Command);
            protocol.TryQueue(flow);
            await using var reader = flow.GetAsyncEnumerator(cancellationToken);
        });
        var objectPool = new ObjectPool<CommandFlow, PgClientFlowPolicy<CommandFlow>>(
            new(() => new CommandFlow(async: true)), ObjectPoolSize);
        var holder = new CtsHolder();

        var channel = Channel.CreateUnbounded<CommandFlow>(new UnboundedChannelOptions { SingleReader = true });

        _ = QueueLoop(channel.Reader, pool);

        yield return new Argument
        {
            WorkItems = Enumerable.Range(0, Commands).Select(i => new Request(objectPool, holder, channel.Writer)).ToArray(),
            CtsHolder = holder,
        };

        static async Task QueueLoop(ChannelReader<CommandFlow> reader, ConnectionPool<PgConnection> pool)
        {
            PgConnection? conn = null;
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                var count = 0;
                while (reader.TryRead(out var flow))
                {
                    if (conn is not null && conn.Protocol.TryQueue(
                            flow, FlowEnqueueOptions.RequireExistingPipeline))
                    {
                        count = ++count % 8;
                        if (count is 0)
                            conn = null;
                        continue;
                    }
                    conn = await pool.GetAsync(
                        static (ctx, flow) => ctx.Connection.Protocol.TryQueue(flow,
                            ctx.IsIdleCandidate ? FlowEnqueueOptions.None : FlowEnqueueOptions.RequireExistingPipeline),
                        flow, TimeSpan.FromSeconds(30), CancellationToken.None).ConfigureAwait(false);
                    count = 1;
                }
            }
        }
    }

    internal class CtsHolder
    {
        internal CancellationTokenSource Cts { get; set; } = null!;
    }

    public readonly struct NpgsqlArgument
    {
        internal NpgsqlRequest[] WorkItems { get; init; }
        internal CtsHolder CtsHolder { get; init; }
        public override string ToString() => WorkItems.Length.ToString();
    }

    // Public wrapper struct to keep the internal types internal.
    public readonly struct Argument
    {
        internal Request[] WorkItems { get; init; }
        internal CtsHolder CtsHolder { get; init; }
        public override string ToString() => WorkItems.Length.ToString();
    }
}

readonly struct PgClientFlowPolicy<T>(Func<T> factory) : IPooledObjectPolicy<T> where T : PgClientFlow
{
    public T Create() => factory();
    public bool Return(T obj)
    {
        // obj.Reset();
        return true;
    }
}
