using System.Data;
using Slon.Pg;
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

    [TestMethod]
    public void ParameterCollectionCachesNakedValueResolutionPerSerializerGraph()
    {
        var options = new PgSerializerOptions(PgTypeCatalog.Default);
        var parameters = SlonParameters.Create(41);

        var first = parameters.GetOrResolveTypeInfo(
                0, options, preparedTypeId: null, allowUnspecified: false)
            .CreateParameter(41, parameterIndex: 0);
        var constrained = parameters.GetOrResolveTypeInfo(0, options, (Oid)23u, allowUnspecified: false)
            .CreateParameter(41, parameterIndex: 0);
        Assert.AreSame(first.TypeResolution, constrained.TypeResolution);

        ((IDataParameterCollection)parameters)[SlonParameters.PositionalName] = 42;
        var replacedValue = parameters.GetOrResolveTypeInfo(0, options, (Oid)23u, allowUnspecified: false)
            .CreateParameter(42, parameterIndex: 0);
        Assert.AreSame(first.TypeResolution, replacedValue.TypeResolution);

        var otherOptions = new PgSerializerOptions(PgTypeCatalog.Default);
        var otherGraph = parameters.GetOrResolveTypeInfo(
                0, otherOptions, (Oid)23u, allowUnspecified: false)
            .CreateParameter(42, parameterIndex: 0);
        Assert.AreNotSame(first.TypeResolution, otherGraph.TypeResolution);
    }

    [TestMethod]
    public void ParameterCollectionValidatesParameterResolutionWithItsTypeRevision()
    {
        var options = new PgSerializerOptions(PgTypeCatalog.Default);
        VerifyObjectParameter(new SlonParameter(41));
        VerifyObjectParameter(new SlonParameter<object>(41));

        var typedValue = new SlonParameter<int>(41);
        var typedParameters = new SlonParameters { typedValue };
        var typedFirst = Resolve(typedParameters, typedValue);
        typedValue.Value = 42;
        Assert.AreSame(typedFirst.TypeResolution,
            Resolve(typedParameters, typedValue).TypeResolution);

        void VerifyObjectParameter(SlonParameter value)
        {
            var parameters = new SlonParameters { value };
            var first = Resolve(parameters, value);
            value.Value = 42;
            Assert.AreSame(first.TypeResolution, Resolve(parameters, value).TypeResolution);

            value.Value = "forty-two";
            var changedType = Resolve(parameters, value);
            Assert.AreNotSame(first.TypeResolution, changedType.TypeResolution);
            Assert.AreEqual((Oid)25u, changedType.PgTypeId.Oid);

            value.SlonDbType = SlonDbTypes.Varchar;
            var requestedType = Resolve(parameters, value);
            Assert.AreNotSame(changedType.TypeResolution, requestedType.TypeResolution);
            Assert.AreEqual((Oid)1043u, requestedType.PgTypeId.Oid);
        }

        Parameter Resolve(SlonParameters parameters, SlonParameter value)
            => parameters.GetOrResolveTypeInfo(
                    0, options, preparedTypeId: null, allowUnspecified: false)
                .CreateParameter(value, parameterIndex: 0);
    }
}
