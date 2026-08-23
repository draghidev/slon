using System.Data;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;

namespace Slon.Benchmark;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5, invocationCount: 1_000_000)]
[IterationCount(5)]
public class AdoCommandFlowFactoryBenchmark : ClientBenchmark
{
    SlonDataSource _dataSource = null!;
    SlonBatch _batch = null!;
    SlonBatch _unpreparedBatch = null!;
    SlonCommand _commandOwner = null!;
    SlonBatchCommand _preparedCommand = null!;
    SlonBatchCommand _untrackedCommand = null!;
    SlonDataSource.PgDbDependencies _dependencies = null!;
    PgConnection _pgConnection = null!;
    TrackedCommand _tracked = null!;
    TrackerContext _preparedTracker;
    AdoCommandFlowOptions _optionsSink;
    CommandDescriptor _descriptorSink;

    [GlobalSetup]
    public async Task Setup()
    {
        _dataSource = new(new()
        {
            EndPoint = Options.EndPoint,
            Username = Options.Username,
            Password = Options.Password,
            Database = Options.Database,
            PoolSize = Connections
        });
        await using (var connection = await _dataSource.OpenConnectionAsync())
        {
        }

        _batch = new(_dataSource);
        _commandOwner = new(_dataSource, "select 1");
        _batch.BatchCommands.Add(new() { CommandText = "select 1" });
        await _batch.PrepareAsync();
        _dependencies = _dataSource.GetDbDependencies();
        _pgConnection = await CreateProtocolFactory().CreateAsync();
        _preparedCommand = _batch.BatchCommands[0];
        _tracked = ((IAdoCommand)_preparedCommand).Tracked!;
        _pgConnection.SetTracked(_tracked);
        _preparedTracker = TrackerContext.Create(
            _dependencies.CommandsTracker, _tracked);
        _untrackedCommand = new()
        {
            CommandText = "select 1",
            AllowAutoPreparation = false
        };
        _unpreparedBatch = new(_dataSource);
        _unpreparedBatch.BatchCommands.Add(_untrackedCommand);
    }

    [Benchmark]
    public void CreatePreparedBatchOptions()
        => _optionsSink = GetBatchCore(_batch).CreateAdoCommandFlowOptions(
            [], CommandBehavior.Default, _dependencies, pgConnection: _pgConnection);

    [Benchmark]
    public void CreatePreparedCommandOptionsCanonical()
    {
        var factory = new AdoCommandFlowFactory<SlonBatchCommand>(
            _commandOwner, MemoryMarshal.CreateSpan(ref _preparedCommand, 1), _dependencies);
        _optionsSink = factory.Create(
            [], CommandBehavior.Default, explicitlyPrepared: true, enableErrorBarriers: false,
            TimeSpan.Zero, pgConnection: _pgConnection);
    }

    [Benchmark]
    public void CreateUnpreparedCommandOptionsCanonical()
    {
        var factory = new AdoCommandFlowFactory<SlonBatchCommand>(
            _commandOwner, MemoryMarshal.CreateSpan(ref _untrackedCommand, 1), _dependencies);
        _optionsSink = factory.Create(
            [], CommandBehavior.Default, explicitlyPrepared: false, enableErrorBarriers: false,
            TimeSpan.Zero, pgConnection: _pgConnection);
    }

    [Benchmark]
    public int GetConnectionPreparedStatus()
        => (int)_pgConnection.GetTrackedStatus(_tracked);

    [Benchmark]
    public bool GetPreparedDescriptor()
        => _tracked.TryGetPreparedDescriptor(out _descriptorSink);

    [Benchmark]
    public void CreateUnpreparedBatchOptions()
        => _optionsSink = GetBatchCore(_unpreparedBatch).CreateAdoCommandFlowOptions(
            [], CommandBehavior.Default, _dependencies, pgConnection: _pgConnection);

    [Benchmark]
    public int CreatePreparedCommand()
        => AdoCommandFactory.CreateCommand(
            _preparedCommand, enableErrorBarriers: false, CommandBehavior.Default,
            _preparedTracker, dbParameters: null, TimeSpan.Zero, preparing: false,
            _dependencies.SerializerOptions, _dependencies.ParameterWriter)
            .Item1.Descriptor.ParameterTypes.Count;

    [Benchmark]
    public void CreatePreparedCommandOptions()
    {
        var result = AdoCommandFactory.CreateCommand(
            _preparedCommand, enableErrorBarriers: false, CommandBehavior.Default,
            _preparedTracker, dbParameters: null, TimeSpan.Zero, preparing: false,
            _dependencies.SerializerOptions, _dependencies.ParameterWriter);
        _optionsSink = new AdoCommandFlowOptions
        {
            Commands = new(result.Item1)
        };
    }

    [Benchmark]
    public int CreateUntrackedCommand()
        => AdoCommandFactory.CreateCommand(
            _untrackedCommand, enableErrorBarriers: false, CommandBehavior.Default,
            default, dbParameters: null, TimeSpan.Zero, preparing: false,
            _dependencies.SerializerOptions, _dependencies.ParameterWriter)
            .Item1.Descriptor.ParameterTypes.Count;

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _batch.DisposeAsync();
        await _unpreparedBatch.DisposeAsync();
        await _commandOwner.DisposeAsync();
        await _pgConnection.CompleteAsync();
        await _dataSource.DisposeAsync();
    }

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_batchCore")]
    static extern ref AdoBatchCore<SlonBatchCommand> GetBatchCore(SlonBatch batch);
}
