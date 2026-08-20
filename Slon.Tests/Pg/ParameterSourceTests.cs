using System.Text;
using Slon.Buffers;
using Slon.Pg;
using Slon.Pg.Types;

namespace Slon.Tests.Pg;

[TestClass]
public class ParameterSourceTests
{
    [TestMethod]
    public void ObjectState_RejectsParameterArray()
    {
        object parameters = Array.Empty<Parameter>();

        Assert.ThrowsExactly<ArgumentException>(() => new ParameterSource(parameters));
    }

    [TestMethod]
    public void StrategyState_MaterializesEveryParameterInOrder()
    {
        object?[] values = [1, null, DBNull.Value, 4];
        var strategy = new MaterializingStrategy();
        var source = new ParameterSource(values);

        using var lease = source.Materialize(strategy);

        Assert.AreEqual(values.Length, lease.Buffer.Length);
        for (var i = 0; i < values.Length; i++)
            Assert.AreSame(values[i], lease.Buffer[i].Value);
    }

    sealed class MaterializingStrategy : ParameterWriterStrategy
    {
        public override object CreateState(IOutputWriter output, Encoding textEncoding)
            => throw new NotSupportedException();

        public override int GetParameterCount(object source) => ((object?[])source).Length;

        public override void Materialize(object source, Span<Parameter> destination)
        {
            var values = (object?[])source;
            for (var i = 0; i < values.Length; i++)
                destination[i] = Parameter.Create(values[i], new PgTypeId(DataTypeNames.Int4));
        }

        public override void Write(object state, int parameterIndex, in Parameter parameter)
            => throw new NotSupportedException();

        public override ValueTask WriteAsync(object state, int parameterIndex, Parameter parameter,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
