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
}
