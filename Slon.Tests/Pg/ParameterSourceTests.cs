using System.Collections.Immutable;
using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon.Tests.Pg;

[TestClass]
public class ParameterSourceTests
{
    static (PgEncoder Encoder, ProtocolDataWriter Writer, BufferOutputWriter Output) NewEncoder()
    {
        var output = new BufferOutputWriter();
        var writer = new ProtocolDataWriter(output, Encoding.UTF8, static () => { }, default, null!);
        var flow = new CommandFlow(async: false, ReadOnlySpan<Command>.Empty);
        return (new(flow.GetExecutionControl(null!), writer), writer, output);
    }

    [TestMethod]
    public void ObjectState_RejectsParameterArray()
    {
        object parameters = Array.Empty<Parameter>();

        Assert.ThrowsExactly<ArgumentException>(() =>
            new ParameterSource(parameters, new TestParameterWriter()));
    }

    [TestMethod]
    public void WriterState_BeginsSeparateWriteState()
    {
        object?[] values = [1, null, DBNull.Value, 4];
        var writer = new TestParameterWriter();
        var source = new ParameterSource(values, writer);

        var count = source.Count;
        using var lease = source.Writer!.BeginWriteCore(source.State!, count);

        Assert.AreEqual(values.Length, count);
        Assert.AreSame(values, source.State);
        Assert.AreSame(writer, source.Writer);
        Assert.AreEqual(1, writer.BeginCount);
    }

    [TestMethod]
    public void DirectBytes_AreWrittenWithoutAParameterWriter()
    {
        byte[] bytes = [1, 2, 3, 4];
        var source = new ParameterSource(ImmutableArray.Create(Parameter.Create(bytes, (Oid)17u)));
        var (encoder, writer, output) = NewEncoder();

        encoder.WriteBind(parameters: source);
        writer.Flush();

        CollectionAssert.AreEqual(bytes, output.ToArray()[17..(17 + bytes.Length)]);
    }

    [TestMethod]
    public async Task DirectStream_WritesItsCapturedRemainingLength()
    {
        byte[] bytes = [1, 2, 3, 4];
        await using var stream = new MemoryStream(bytes);
        stream.Position = 1;
        var source = new ParameterSource(ImmutableArray.Create(Parameter.Create(stream, (Oid)17u)));
        var (encoder, writer, output) = NewEncoder();

        await encoder.WriteBindAsync(parameters: source);
        writer.Flush();

        CollectionAssert.AreEqual(bytes[1..], output.ToArray()[17..(16 + bytes.Length)]);
    }

    [TestMethod]
    public void SerializerWriter_BindsLowLevelValuesWithoutAdoParameters()
    {
        var options = new PgSerializerOptions(PgTypeCatalog.Default);
        var values = new LowLevelValues(42, options.GetTypeInfo(typeof(int), pgTypeId: null));
        var source = new ParameterSource(values, LowLevelParameterWriter.Instance);
        var (encoder, writer, output) = NewEncoder();

        encoder.WriteBind(parameters: source);
        writer.Flush();

        CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 42 }, output.ToArray()[17..21]);
    }

    [TestMethod]
    public void SerializerBinding_ReleaseDetachesWriteState()
    {
        var state = new DisposableState();
        PgSerializerParameterBinding[] bindings = [default];

        bindings[0].Set(4, state);
        foreach (ref var binding in bindings.AsSpan())
            binding.Release();
        bindings[0].Set(4, null);
        bindings[0].Release();

        Assert.AreEqual(1, state.DisposeCount);
    }

    sealed class TestParameterWriter : ParameterWriter
    {
        internal object WriteState { get; } = new();
        internal int BeginCount { get; private set; }

        internal override object CreateWriterStateCore(IOutputWriter output, Encoding textEncoding)
            => throw new NotSupportedException();
        internal override int GetParameterCountCore(object source) => ((object?[])source).Length;
        internal override PgTypeId GetParameterTypeCore(object source, int index)
            => throw new NotSupportedException();

        private protected override object BeginWriteStateCore(object source, int count)
        {
            BeginCount++;
            return WriteState;
        }

        private protected override int GetSizeCore(object writeState, int parameterIndex) => 0;

        private protected override void WriteCore(object writerState, object source,
            object writeState, int parameterIndex)
            => throw new NotSupportedException();

        private protected override ValueTask WriteAsyncCore(object writerState, object source,
            object writeState, int parameterIndex,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }

    sealed class LowLevelValues(int value, PgTypeInfo typeInfo)
    {
        internal int Value { get; } = value;
        internal PgTypeInfo TypeInfo { get; } = typeInfo;
    }

    sealed class DisposableState : IDisposable
    {
        internal int DisposeCount { get; private set; }
        public void Dispose() => DisposeCount++;
    }

    sealed class LowLevelParameterWriter : PgSerializerParameterWriter<LowLevelValues>
    {
        internal static LowLevelParameterWriter Instance { get; } = new();

        public override int GetParameterCount(LowLevelValues source) => 1;

        public override PgTypeId GetParameterType(LowLevelValues source, int index)
            => index is 0 ? source.TypeInfo.PgTypeId : throw new ArgumentOutOfRangeException(nameof(index));

        protected override void ApplyParameter(LowLevelValues source, int parameterIndex,
            PgParameterValueOperation operation)
        {
            if (parameterIndex is not 0)
                throw new ArgumentOutOfRangeException(nameof(parameterIndex));
            operation.Apply(source.TypeInfo, source.Value);
        }
    }
}
