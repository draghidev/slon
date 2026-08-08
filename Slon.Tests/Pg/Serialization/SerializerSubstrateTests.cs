using System.Buffers;
using System.Text;
using Slon.Buffers;
using Slon.Pg.Serialization;
using Slon.Pg.Serialization.Converters;

namespace Slon.Tests.Pg.Serialization;

[TestClass]
public class SerializerSubstrateTests
{
    enum TestEnum : int
    {
        Value = 42
    }

    [TestMethod]
    public void PrimitiveConverters_RoundTripBinaryValues()
    {
        Assert.AreEqual(true, RoundTrip(new BoolConverter(), true));
        Assert.AreEqual((short)-1234, RoundTrip(new Int2Converter<short>(), (short)-1234));
        Assert.AreEqual(123456789, RoundTrip(new Int4Converter<int>(), 123456789));
        Assert.AreEqual(-1234567890123456789L,
            RoundTrip(new Int8Converter<long>(), -1234567890123456789L));
        Assert.AreEqual(1.25f, RoundTrip(new RealConverter<float>(), 1.25f));
        Assert.AreEqual(-123.5d, RoundTrip(new DoubleConverter<double>(), -123.5d));

        var guid = Guid.NewGuid();
        Assert.AreEqual(guid, RoundTrip(new GuidUuidConverter(), guid));
    }

    [TestMethod]
    public void UnderlyingNumericConverter_RoundTripsEnumWithoutBoxingContractChange()
    {
        PgConverter converter = new Int4Converter<int>();
        var output = new ArrayBufferWriter<byte>();
        var writer = new PgWriter(output);
        converter.Write(writer, TestEnum.Value);
        writer.EndWrite(sizeof(int));

        var value = converter.Read<TestEnum>(new PgReader(output.WrittenMemory));
        Assert.AreEqual(TestEnum.Value, value);
    }
    [TestMethod]
    public async Task PgWriter_ResumesAcrossTinyOutputWindowsAndValidatesSize()
    {
        var output = new TinyOutputWriter(3);
        var writer = new PgWriter(output).Init(flushMode: FlushMode.NonBlocking);
        var bytes = Enumerable.Range(0, 17).Select(static x => (byte)x).ToArray();

        await writer.WriteBytesAsync(bytes);
        writer.EndWrite(bytes.Length);
        await output.FlushAsync();

        CollectionAssert.AreEqual(bytes, output.ToArray());
        Assert.ThrowsExactly<InvalidOperationException>(() => writer.EndWrite(1));
    }

    [TestMethod]
    public void PgWriter_AbortRevokesPartialWriteStateBeforeReuse()
    {
        var output = new ArrayBufferWriter<byte>();
        var writer = new PgWriter(output).Init(writeState: new object());
        writer.WriteByte(1);

        writer.AbortWrite();
        writer.Init();
        writer.WriteInt32(42);
        writer.EndWrite(sizeof(int));

        CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 42 }, output.WrittenSpan.ToArray());
        Assert.IsNull(writer.WriteState);
    }

    [TestMethod]
    public async Task PgReader_StreamsAcrossInputWindows()
    {
        var bytes = Enumerable.Range(0, 31).Select(static x => (byte)x).ToArray();
        var source = new TinyInputReader(bytes, 3);
        await using var reader = new PgReader(source, bytes.Length);

        Assert.AreEqual((byte)0, reader.ReadByte());
        Assert.AreEqual((ushort)0x0102, reader.ReadUInt16());
        var destination = new byte[19];
        await reader.ReadBytesAsync(destination);
        CollectionAssert.AreEqual(bytes[3..22], destination);
        await reader.ConsumeAsync(4);
        Assert.AreEqual(5, reader.CurrentRemaining);
        Assert.AreEqual(0x1a1b1c1dU, reader.ReadUInt32());
        Assert.AreEqual((byte)30, reader.ReadByte());
        Assert.AreEqual(0, reader.CurrentRemaining);
    }

    [TestMethod]
    public async Task PgReaderAndWriter_StreamAdaptersRetainFieldBounds()
    {
        var bytes = Enumerable.Range(0, 17).Select(static x => (byte)x).ToArray();
        var source = new TinyInputReader(bytes, 2);
        await using var reader = new PgReader(source, bytes.Length);
        await using (var stream = reader.GetStream(9))
        {
            var first = new byte[4];
            Assert.AreEqual(4, await stream.ReadAsync(first));
            CollectionAssert.AreEqual(bytes[..4], first);
        }
        Assert.AreEqual(8, reader.CurrentRemaining);

        var output = new TinyOutputWriter(4);
        var writer = new PgWriter(output).Init(flushMode: FlushMode.NonBlocking);
        await using (var stream = writer.GetStream())
            await stream.WriteAsync(bytes);
        writer.EndWrite(bytes.Length);
        await output.FlushAsync();
        CollectionAssert.AreEqual(bytes, output.ToArray());
    }

    [TestMethod]
    public async Task PgReader_NestedScopesEnforceConverterConsumption()
    {
        await using var reader = new PgReader(new byte[] { 1, 2, 3, 4, 5 });
        using (reader.BeginNestedRead(2, Size.Create(2)))
        {
            Assert.AreEqual((byte)1, reader.ReadByte());
            Assert.AreEqual((byte)2, reader.ReadByte());
        }
        Assert.AreEqual(3, reader.CurrentRemaining);

        var upperBound = reader.BeginNestedRead(2, Size.CreateUpperBound(2));
        Assert.AreEqual((byte)3, reader.ReadByte());
        upperBound.Dispose();
        Assert.AreEqual((byte)5, reader.ReadByte());
    }

    [TestMethod]
    public async Task TextConverter_UsesAsyncStreamingInputAndOutput()
    {
        const string value = "héllo 🌍";
        var encoding = Encoding.UTF8;
        var bytes = encoding.GetBytes(value);
        var context = new PgConversionContext { TextEncoding = encoding };
        var converter = TextConverter.CreateStringConverter();

        await using var reader = new PgReader(new TinyInputReader(bytes, 2), bytes.Length, context);
        Assert.AreEqual(value, await converter.ReadAsync(reader));

        var output = new TinyOutputWriter(8);
        var writer = new PgWriter(output, context).Init(context, FlushMode.NonBlocking);
        await converter.WriteAsync(writer, value);
        writer.EndWrite(bytes.Length);
        await output.FlushAsync();
        CollectionAssert.AreEqual(bytes, output.ToArray());
    }

    static T RoundTrip<T>(PgConverter<T> converter, T value)
    {
        var output = new ArrayBufferWriter<byte>();
        var writer = new PgWriter(output);
        converter.Write(writer, value);
        var descriptor = converter.GetDescriptor(
            new() { ConversionContext = PgConversionContext.Empty });
        writer.EndWrite(descriptor.BufferRequirements.Write);
        return converter.Read(new PgReader(output.WrittenMemory));
    }

    sealed class TinyOutputWriter(int capacity) : IOutputWriter
    {
        readonly byte[] _window = new byte[capacity];
        readonly List<byte> _output = [];
        int _written;

        public long UnflushedBytes => _written;

        public void Advance(int count)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(count, capacity - _written);
            _written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            if (sizeHint > capacity - _written)
                throw new InvalidOperationException("Requested window is larger than the test sink.");
            return _window.AsMemory(_written, capacity - _written);
        }

        public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

        public void Flush(TimeSpan timeout = default)
        {
            _output.AddRange(_window.AsSpan(0, _written));
            _written = 0;
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default)
        {
            Flush();
            return default;
        }

        public byte[] ToArray() => [.. _output];
    }

    sealed class TinyInputReader : IInputReader
    {
        readonly byte[] _input;
        readonly int _windowSize;
        int _offset;

        internal TinyInputReader(byte[] input, int windowSize)
        {
            _input = input;
            _windowSize = windowSize;
            Publish();
        }

        public ReadOnlySequence<byte> Buffer { get; private set; }
        public bool IsComplete { get; private set; }

        public void AdvanceTo(SequencePosition consumed, long consumedLength)
        {
            Assert.AreEqual(consumedLength, Buffer.Slice(0, consumed).Length);
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
            var count = Math.Min(_windowSize, _input.Length - _offset);
            Buffer = new(_input.AsMemory(_offset, count));
            IsComplete = _offset + count == _input.Length;
        }
    }
}
