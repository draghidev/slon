using System.Buffers;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using BenchmarkDotNet.Attributes;
using Npgsql;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon.Benchmark;

[MemoryDiagnoser]
public class AdoParameterBenchmark : ClientBenchmark
{
    SlonDataSource _dataSource = null!;
    NpgsqlDataSource _npgsqlDataSource = null!;
    NpgsqlCommand _npgsqlCommand = null!;
    NpgsqlParameter<int>[] _npgsqlParameters = null!;
    Action<NpgsqlParameter> _npgsqlBind = null!;
    SlonCommand _ownedCommand = null!;
    SlonCommand _externalCommand = null!;
    SlonParameter<int>[] _ownedParameters = null!;
    SlonParameter<int>[] _externalParameterValues = null!;
    SlonParameters _externalParameters = null!;
    ParameterSource _parameterSource;
    ParameterWriter _parameterWriter = null!;
    object _parameterWriterState = null!;
    PocoValues _pocoValues = null!;
    PocoParameterWriter _pocoWriter = null!;
    ParameterSource _pocoSource;
    object _pocoWriterState = null!;
    int _value;

    [Params(1, 10)]
    public int ParameterCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _dataSource = new SlonDataSource(new()
        {
            EndPoint = Options.EndPoint,
            Username = Options.Username,
            Password = Options.Password,
            Database = Options.Database,
            PoolSize = Connections
        });

        var commandText = $"select {string.Join(" + ",
            Enumerable.Range(1, ParameterCount).Select(static i => $"${i}::int"))}";

        _npgsqlDataSource = InitNpgsql(static _ => { });
        _npgsqlCommand = _npgsqlDataSource.CreateCommand(commandText);
        _npgsqlParameters = new NpgsqlParameter<int>[ParameterCount];
        for (var i = 0; i < _npgsqlParameters.Length; i++)
        {
            _npgsqlParameters[i] = new() { TypedValue = 0 };
            _npgsqlCommand.Parameters.Add(_npgsqlParameters[i]);
        }
        await _npgsqlCommand.ExecuteScalarAsync();
        _npgsqlBind = CreateNpgsqlBindDelegate();

        _ownedCommand = _dataSource.CreateCommand(commandText);
        _ownedParameters = new SlonParameter<int>[ParameterCount];
        for (var i = 0; i < _ownedParameters.Length; i++)
        {
            _ownedParameters[i] = new(0);
            _ownedCommand.Parameters.Add(_ownedParameters[i]);
        }
        await _ownedCommand.PrepareAsync();

        _externalCommand = _dataSource.CreateCommand(commandText);
        for (var i = 0; i < ParameterCount; i++)
            _externalCommand.Parameters.Add(new SlonParameter(null));
        await _externalCommand.PrepareAsync();

        _externalParameterValues = new SlonParameter<int>[ParameterCount];
        _externalParameters = new(ParameterCount);
        for (var i = 0; i < _externalParameterValues.Length; i++)
        {
            _externalParameterValues[i] = new(0);
            _externalParameters.Add(_externalParameterValues[i]);
        }
        await _externalCommand.ExecuteScalarAsync(_externalParameters);

        _parameterWriter = _dataSource.GetDbDependencies().ParameterWriter;
        _parameterSource = new(_externalParameters, _parameterWriter);
        _parameterWriterState = _parameterWriter.CreateWriterStateCore(new BufferOutputWriter(), Encoding.UTF8);

        _pocoValues = new(ParameterCount);
        _pocoWriter = new(_dataSource.GetDbDependencies().SerializerOptions
            .GetTypeInfo(typeof(int), pgTypeId: null));
        _pocoSource = new(_pocoValues, _pocoWriter);
        _pocoWriterState = _pocoWriter.CreateWriterStateCore(new BufferOutputWriter(), Encoding.UTF8);

        // Warm the shared array pool before measuring materialization.
        using var warmLease = _parameterWriter.BeginWriteCore(
            _parameterSource.State!, _parameterSource.Count);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _ownedCommand.DisposeAsync();
        await _externalCommand.DisposeAsync();
        await _dataSource.DisposeAsync();
        await _npgsqlCommand.DisposeAsync();
        await _npgsqlDataSource.DisposeAsync();
    }

    [Benchmark]
    public int BeginParameterWrite()
    {
        var count = _parameterSource.Count;
        using var lease = _parameterWriter.BeginWriteCore(_parameterSource.State!, count);
        return count;
    }

    [Benchmark]
    public int BeginAndBindParameterWrite()
    {
        var source = _parameterSource.State!;
        var count = _parameterSource.Count;
        using var lease = _parameterWriter.BeginWriteCore(source, count);
        var totalSize = 0;
        for (var i = 0; i < count; i++)
        {
            lease.Bind(_parameterWriterState, i);
            totalSize += lease.GetSize(i);
        }
        return totalSize;
    }

    [Benchmark]
    public int BeginAndBindChangedValues()
    {
        SetValues(_externalParameterValues);
        return BeginAndBindParameterWrite();
    }

    [Benchmark]
    public int BeginAndBindPocoChangedValues()
    {
        var value = ++_value;
        _pocoValues.Set(ParameterCount, value);
        var source = _pocoSource.State!;
        var count = _pocoSource.Count;
        using var lease = _pocoWriter.BeginWriteCore(source, count);
        var totalSize = 0;
        for (var i = 0; i < count; i++)
        {
            lease.Bind(_pocoWriterState, i);
            totalSize += lease.GetSize(i);
        }
        return totalSize;
    }

    [Benchmark]
    public int NpgsqlBindChangedValues()
    {
        var value = ++_value;
        for (var i = 0; i < _npgsqlParameters.Length; i++)
        {
            _npgsqlParameters[i].TypedValue = value;
            _npgsqlBind(_npgsqlParameters[i]);
        }
        return _npgsqlParameters.Length;
    }

    [Benchmark(Baseline = true)]
    public Task<object?> ExecuteOwnedParameter()
    {
        SetValues(_ownedParameters);
        return _ownedCommand.ExecuteScalarAsync(CancellationToken.None);
    }

    [Benchmark]
    public ValueTask<object?> ExecuteExternalParameters()
    {
        SetValues(_externalParameterValues);
        return _externalCommand.ExecuteScalarAsync(_externalParameters, CancellationToken.None);
    }

    void SetValues(SlonParameter<int>[] parameters)
    {
        var value = ++_value;
        for (var i = 0; i < parameters.Length; i++)
            parameters[i].Value = value;
    }

    static Action<NpgsqlParameter> CreateNpgsqlBindDelegate()
    {
        // Npgsql's Bind surface is internal. Pay reflection and IL generation once so the benchmarked
        // delegate call measures Bind rather than MethodInfo.Invoke.
        var bind = typeof(NpgsqlParameter).GetMethod("Bind", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(typeof(NpgsqlParameter).FullName, "Bind");
        var parameterTypes = bind.GetParameters();
        var dataFormatType = parameterTypes[0].ParameterType.GetElementType()!;
        var sizeType = parameterTypes[1].ParameterType.GetElementType()!;
        var requiredFormatType = parameterTypes[2].ParameterType;
        var method = new DynamicMethod("BindNpgsqlParameter", null, [typeof(NpgsqlParameter)],
            typeof(AdoParameterBenchmark).Module, skipVisibility: true);
        var il = method.GetILGenerator();
        var format = il.DeclareLocal(dataFormatType);
        var size = il.DeclareLocal(sizeType);
        var requiredFormat = il.DeclareLocal(requiredFormatType);
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldloca_S, format);
        il.Emit(OpCodes.Ldloca_S, size);
        il.Emit(OpCodes.Ldloca_S, requiredFormat);
        il.Emit(OpCodes.Initobj, requiredFormatType);
        il.Emit(OpCodes.Ldloc, requiredFormat);
        il.Emit(OpCodes.Call, bind);
        il.Emit(OpCodes.Ret);
        return method.CreateDelegate<Action<NpgsqlParameter>>();
    }

    sealed class BufferOutputWriter : IOutputWriter
    {
        readonly ArrayBufferWriter<byte> _writer = new();

        public long UnflushedBytes => _writer.WrittenCount;

        public void Advance(int count) => _writer.Advance(count);
        public Memory<byte> GetMemory(int sizeHint = 0) => _writer.GetMemory(sizeHint);
        public Span<byte> GetSpan(int sizeHint = 0) => _writer.GetSpan(sizeHint);
        public void Flush(TimeSpan timeout = default) { }
        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => default;
    }

    sealed class PocoValues(int count)
    {
        internal int Count { get; } = count;
        internal int P0, P1, P2, P3, P4, P5, P6, P7, P8, P9;

        internal void Set(int count, int value)
        {
            if (count is 1)
            {
                P0 = value;
                return;
            }
            (P0, P1, P2, P3, P4, P5, P6, P7, P8, P9)
                = (value, value, value, value, value, value, value, value, value, value);
        }
    }

    sealed class PocoParameterWriter(PgTypeInfo typeInfo) : PgSerializerParameterWriter<PocoValues>
    {
        public override int GetParameterCount(PocoValues source) => source.Count;
        public override PgTypeId GetParameterType(PocoValues source, int index) => typeInfo.PgTypeId;

        protected override void ApplyParameter(PocoValues source, int parameterIndex,
            PgParameterValueOperation operation)
        {
            switch (parameterIndex)
            {
            case 0: operation.Apply(typeInfo, source.P0); break;
            case 1: operation.Apply(typeInfo, source.P1); break;
            case 2: operation.Apply(typeInfo, source.P2); break;
            case 3: operation.Apply(typeInfo, source.P3); break;
            case 4: operation.Apply(typeInfo, source.P4); break;
            case 5: operation.Apply(typeInfo, source.P5); break;
            case 6: operation.Apply(typeInfo, source.P6); break;
            case 7: operation.Apply(typeInfo, source.P7); break;
            case 8: operation.Apply(typeInfo, source.P8); break;
            case 9: operation.Apply(typeInfo, source.P9); break;
            default: throw new ArgumentOutOfRangeException(nameof(parameterIndex));
            }
        }
    }
}
