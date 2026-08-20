using System.Buffers;
using BenchmarkDotNet.Attributes;
using Npgsql;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Serialization;
using Slon.Pg.Serialization.Converters;

namespace Slon.Benchmark;

// Measures the complete buffered ADO getter path after a row has arrived. Re-reading the same
// buffered field deliberately removes network and command scheduling from the serializer profile.
[MemoryDiagnoser]
public class AdoFieldReadBenchmark : ClientBenchmark
{
    SlonDataSource _dataSource = null!;
    SlonCommand _command = null!;
    SlonDataReader _reader = null!;
    NpgsqlDataSource _npgsqlDataSource = null!;
    NpgsqlCommand _npgsqlCommand = null!;
    NpgsqlDataReader _npgsqlReader = null!;

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
        _command = _dataSource.CreateCommand("select 42::int4, 'field-value'::text");
        _reader = await _command.ExecuteReaderAsync();
        if (!await _reader.ReadAsync())
            throw new InvalidOperationException("Slon did not return the benchmark row.");

        _npgsqlDataSource = InitNpgsql(static _ => { });
        _npgsqlCommand = _npgsqlDataSource.CreateCommand(
            "select 42::int4, 'field-value'::text");
        _npgsqlReader = await _npgsqlCommand.ExecuteReaderAsync();
        if (!await _npgsqlReader.ReadAsync())
            throw new InvalidOperationException("Npgsql did not return the benchmark row.");
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _reader.DisposeAsync();
        await _command.DisposeAsync();
        await _dataSource.DisposeAsync();
        await _npgsqlReader.DisposeAsync();
        await _npgsqlCommand.DisposeAsync();
        await _npgsqlDataSource.DisposeAsync();
    }

    [Benchmark(Baseline = true)]
    public int SlonInt32() => _reader.GetInt32(0);

    [Benchmark]
    public int NpgsqlInt32() => _npgsqlReader.GetInt32(0);

    [Benchmark]
    public string SlonString() => _reader.GetString(1);

    [Benchmark]
    public string NpgsqlString() => _npgsqlReader.GetString(1);
}

// Isolates the reusable transport cursor and converter boundary from ADO row navigation. This
// catches overhead which can disappear into the larger getter profile and separately exercises
// the refill path used by sequential rows.
[MemoryDiagnoser]
public class FieldReadBenchmark
{
    static readonly byte[] IntBytes = [0, 0, 0, 42];
    static readonly byte[] TextBytes = "field-value"u8.ToArray();

    readonly ReusableFieldReader _reader = new();
    readonly WindowInputReader _windowReader = new(IntBytes, windowSize: 2);
    readonly PgConverter _intConverter = new Int4Converter<int>();
    readonly PgConverter _textConverter = TextConverter.CreateStringConverter();

    [Benchmark(Baseline = true)]
    public int ReusableInt32()
    {
        var reader = _reader.Open(IntBytes);
        return _intConverter.Read<int>(reader);
    }

    [Benchmark]
    public int ReusableSplitInt32()
    {
        _windowReader.Reset();
        var reader = _reader.Open(_windowReader, IntBytes.Length);
        return _intConverter.Read<int>(reader);
    }

    [Benchmark]
    public string ReusableString()
    {
        var reader = _reader.Open(TextBytes);
        return _textConverter.Read<string>(reader);
    }

    [Benchmark]
    public int FreshCursorInt32()
        => _intConverter.Read<int>(new PgReader(IntBytes));

    sealed class ReusableFieldReader : PgFieldReader
    {
        internal PgReader Open(ReadOnlyMemory<byte> buffer)
        {
            Initialize(buffer);
            return new(this, PgConversionContext.Empty);
        }

        internal PgReader Open(IInputReader source, int fieldSize)
        {
            Initialize(source, source.Buffer, fieldSize, releasePrefix: 0);
            return new(this, PgConversionContext.Empty);
        }
    }

    sealed class WindowInputReader(byte[] input, int windowSize) : IInputReader
    {
        int _offset;

        public ReadOnlySequence<byte> Buffer { get; private set; }
        public bool IsComplete { get; private set; }

        internal void Reset()
        {
            _offset = 0;
            Publish();
        }

        public void AdvanceTo(SequencePosition consumed, long consumedLength)
        {
            var actual = Buffer.Slice(0, consumed).Length;
            if (actual != consumedLength)
                throw new InvalidOperationException("The cursor released a different prefix length.");
            _offset += checked((int)consumedLength);
        }

        public void Read() => Publish();

        public ValueTask ReadAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Publish();
            return default;
        }

        void Publish()
        {
            var count = Math.Min(windowSize, input.Length - _offset);
            Buffer = new(input.AsMemory(_offset, count));
            IsComplete = _offset + count == input.Length;
        }
    }
}
