using Slon.Pg;

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
}
