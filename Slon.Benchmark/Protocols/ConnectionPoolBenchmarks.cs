using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Slon.Benchmark;
using Slon.Pools;

namespace Slon.Protocols.Benchmark;

[ThreadingDiagnoser]
[MemoryDiagnoser]
public class ConnectionPoolBenchmarks
{
    ConnectionPool<EmptyProtocol<PooledUserCompleted>> _connectionPool = null!;
    EmptyFlow<PooledUserCompleted>[] _flows = null!;

    [Params(1)]
    public int Connections { get; set; }

    [Params(1_000_000)]
    public int Flows { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _connectionPool = new(new EmptyProtocolFactory(), new() { MaxConnections = Connections });
        _flows = new EmptyFlow<PooledUserCompleted>[Flows];
        for (var i = 0; i < _flows.Length; i++)
        {
            _flows[i] = new(i);
            _flows[i].Initialize(TimeSpan.FromSeconds(3));
        }
        _connectionPool.OpenAllConnectionsAsync(TimeSpan.Zero, CancellationToken.None).GetAwaiter().GetResult();
    }

    [Benchmark]
    public async ValueTask ScheduleAsync()
    {
        for (var i = 0; i < _flows.Length; i++)
        {
            var flow = _flows[i];
            flow.Reset();
            await _connectionPool.GetAsync(
                static (ctx, flow) => ctx.Connection.TryQueue(flow, mustPipeline: !ctx.IsIdleCandidate), flow,
                TimeSpan.Zero, CancellationToken.None);
        }

        for (var i = 0; i < _flows.Length; i++)
        {
            var flow = _flows[i];
            flow.UserCompleted();
            await flow.WaitForComplete();
        }
    }

    sealed class EmptyProtocolFactory(EmptyProtocolOptions? options = null) : IPoolConnectionFactory<EmptyProtocol<PooledUserCompleted>>
    {
        public EmptyProtocol<PooledUserCompleted> Create(TimeSpan timeout)
            => new(options, null);

        public ValueTask<EmptyProtocol<PooledUserCompleted>> CreateAsync(CancellationToken cancellationToken)
            => new(new EmptyProtocol<PooledUserCompleted>(options, null));

        EmptyProtocol<PooledUserCompleted> IPoolConnectionFactory<EmptyProtocol<PooledUserCompleted>>.Create(ConnectionPoolContext<EmptyProtocol<PooledUserCompleted>> poolContext, TimeSpan timeout)
            => new(options, poolContext);

        ValueTask<EmptyProtocol<PooledUserCompleted>> IPoolConnectionFactory<EmptyProtocol<PooledUserCompleted>>.CreateAsync(ConnectionPoolContext<EmptyProtocol<PooledUserCompleted>> poolContext, CancellationToken cancellationToken)
            => new(new EmptyProtocol<PooledUserCompleted>(options, poolContext));
    }
}
