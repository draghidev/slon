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
        Assert.AreEqual(DataRowVersion.Current, parameter.SourceVersion);

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

    [TestMethod]
    public void CloneOfCollectedParameter_CanBeRenamed()
    {
        var parameter = new SlonParameter("value", 42);
        _ = new SlonParameters { parameter };

        var clone = parameter.Clone();
        clone.ParameterName = "renamed";

        Assert.AreEqual("renamed", clone.ParameterName);
        Assert.ThrowsExactly<InvalidOperationException>(() => parameter.ParameterName = "renamed");
    }

    [TestMethod]
    public void SparseProperties_CanReturnToTheirDefaults()
    {
        var parameter = new SlonParameter
        {
            SourceColumn = "source",
            SourceColumnNullMapping = true,
            SourceVersion = DataRowVersion.Original
        };

        parameter.SourceColumn = "";
        parameter.SourceColumnNullMapping = false;
        parameter.SourceVersion = DataRowVersion.Current;

        Assert.AreEqual("", parameter.SourceColumn);
        Assert.IsFalse(parameter.SourceColumnNullMapping);
        Assert.AreEqual(DataRowVersion.Current, parameter.SourceVersion);
    }

    [TestMethod]
    public void ResetDbType_PreservesExplicitFacets()
    {
        var parameter = new SlonParameter
        {
            DbType = DbType.Int32,
            Precision = 3,
            Scale = 2,
            Size = 10
        };

        parameter.ResetDbType();

        Assert.AreEqual(DbType.String, parameter.DbType);
        Assert.AreEqual((byte)3, parameter.Precision);
        Assert.AreEqual((byte)2, parameter.Scale);
        Assert.AreEqual(10, parameter.Size);
    }

    [TestMethod]
    public void Collection_UsesAdoNullSemantics()
    {
        DbParameterCollection parameters = new SlonParameters();
        var parameter = new SlonParameter("value", 42);
        parameters.Add(parameter);

        Assert.IsFalse(parameters.Contains((object)null!));
        Assert.IsFalse(parameters.Contains((string)null!));
        Assert.AreEqual(-1, parameters.IndexOf((object)null!));
        Assert.AreEqual(-1, parameters.IndexOf((string)null!));
        Assert.ThrowsExactly<ArgumentNullException>(() => parameters[0] = null!);
        Assert.ThrowsExactly<ArgumentNullException>(() => parameters["value"] = null!);
    }

    [TestMethod]
    public void Collection_CopyToChecksAndCopiesTheAvailableRange()
    {
        DbParameterCollection parameters = new SlonParameters
        {
            new SlonParameter("first", 1),
            new SlonParameter("second", 2)
        };
        var destination = new object?[3];

        parameters.CopyTo(destination, 1);
        Assert.AreSame(parameters[0], destination[1]);
        Assert.AreSame(parameters[1], destination[2]);
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => parameters.CopyTo(new object?[1], 0));
    }
}
