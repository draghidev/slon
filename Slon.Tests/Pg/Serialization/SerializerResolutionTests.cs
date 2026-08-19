using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon.Tests.Pg.Serialization;

[TestClass]
public class SerializerResolutionTests
{
    enum TestEnum : int
    {
        Value = 42
    }

    [TestMethod]
    public void ResolveCatalogBackedScalarAndEnumMappings()
    {
        var options = new PgSerializerOptions(PgTypeCatalog.Default);

        var intInfo = options.GetTypeInfo(typeof(int));
        Assert.AreEqual(typeof(int), intInfo.Type);
        Assert.AreEqual((Oid)23u, intInfo.PgTypeId.Oid);

        var byOid = options.GetTypeInfo(typeof(int), (Oid)23u);
        Assert.AreSame(intInfo.Converter, byOid.Converter);

        var enumInfo = options.GetTypeInfo(typeof(TestEnum));
        Assert.AreEqual(typeof(TestEnum), enumInfo.Type);
        Assert.AreEqual((Oid)23u, enumInfo.PgTypeId.Oid);
    }

    [TestMethod]
    [DataRow(25u)]
    [DataRow(1043u)]
    [DataRow(1042u)]
    [DataRow(19u)]
    public void StringMappingAcceptsPostgreSqlCharacterTypes(uint oid)
    {
        var options = new PgSerializerOptions(PgTypeCatalog.Default);

        var info = options.GetTypeInfo(typeof(string), (Oid)oid);

        Assert.AreEqual(typeof(string), info.Type);
        Assert.AreEqual((Oid)oid, info.PgTypeId.Oid);
    }

    [TestMethod]
    public void StringMappingDefaultsToText()
    {
        var options = new PgSerializerOptions(PgTypeCatalog.Default);

        Assert.AreEqual((Oid)25u, options.GetTypeInfo(typeof(string)).PgTypeId.Oid);
    }
}
