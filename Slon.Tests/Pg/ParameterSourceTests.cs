using System.Collections.Immutable;
using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
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

        Assert.ThrowsExactly<ArgumentException>(() => new ParameterSource(parameters, 0));
    }

    [TestMethod]
    public void StrategyState_BeginsSeparateWriteState()
    {
        object?[] values = [1, null, DBNull.Value, 4];
        var strategy = new TestParameterWriterStrategy();
        var source = new ParameterSource(values, values.Length);

        var count = source.Count;
        using var lease = strategy.BeginWrite(source.State!, count);

        Assert.AreEqual(values.Length, count);
        Assert.AreSame(values, source.State);
        Assert.AreSame(strategy.WriteState, lease.State);
    }

    [TestMethod]
    public void DirectBytes_AreWrittenWithoutAParameterStrategy()
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

    sealed class TestParameterWriterStrategy : ParameterWriterStrategy
    {
        internal object WriteState { get; } = new();

        public override object CreateWriterState(IOutputWriter output, Encoding textEncoding)
            => throw new NotSupportedException();
        public override PgTypeId GetParameterType(object source, int index)
            => throw new NotSupportedException();

        public override WriteLease BeginWrite(object source, int count)
            => new(WriteState, count, this);

        public override int GetSize(object writeState, int parameterIndex) => 0;

        public override void Write(object writerState, object source, object writeState, int parameterIndex)
            => throw new NotSupportedException();

        public override ValueTask WriteAsync(object writerState, object source, object writeState,
            int parameterIndex,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
