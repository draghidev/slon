using System.Data;
using System.Data.Common;

namespace Slon.Tests.Ado;

[TestClass]
public class SlonParameterTests
{
    [TestMethod]
    public void PackedFlags_HaveAdoDefaultsAndRemainIndependent()
    {
        var parameter = new SlonParameter();

        Assert.AreEqual(ParameterDirection.Input, parameter.Direction);
        Assert.IsFalse(parameter.IsNullable);

        parameter.IsNullable = true;
        parameter.Direction = ParameterDirection.Output;

        Assert.IsTrue(parameter.IsNullable);
        Assert.AreEqual(ParameterDirection.Output, parameter.Direction);
    }

    [TestMethod]
    public void GenericValueType_CachesBoxForObjectFacingReads()
    {
        var parameter = new SlonParameter<int>(42);
        DbParameter dbParameter = parameter;

        var first = dbParameter.Value;
        Assert.AreSame(first, dbParameter.Value);

        parameter.Value = 43;
        Assert.AreEqual(43, dbParameter.Value);
        Assert.AreNotSame(first, dbParameter.Value);
    }

    [TestMethod]
    public void GenericParameter_ClonesThroughBothHierarchySurfaces()
    {
        var parameter = new SlonParameter<int>("value", 42);

        var typedClone = parameter.Clone();
        SlonParameter baseClone = ((SlonParameter)parameter).Clone();

        Assert.AreEqual(42, typedClone.Value);
        Assert.IsInstanceOfType<SlonParameter<int>>(baseClone);
        Assert.AreEqual(42, ((SlonParameter<int>)baseClone).Value);
    }
}
