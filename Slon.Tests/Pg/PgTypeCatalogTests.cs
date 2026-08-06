using Slon.Pg;
using Slon.Pg.Types;

namespace Slon.Tests.Pg;

[TestClass]
public class PgTypeCatalogTests
{
    [TestMethod]
    public void DataTypeName_UnqualifiedQueriesRemainDistinctFromCatalogIdentities()
    {
        var custom = DataTypeName.FromDisplayName("custom_type");
        var builtIn = DataTypeName.FromDisplayName("integer");

        Assert.IsTrue(custom.IsUnqualified);
        Assert.AreEqual("custom_type", custom.DisplayName);
        Assert.IsFalse(builtIn.IsUnqualified);
        Assert.AreEqual(DataTypeNames.Int4, builtIn);
        Assert.ThrowsExactly<ArgumentException>(
            () => new PgTypeCatalogBuilder([PgType.CreateBase(custom)]));
    }

    [TestMethod]
    public void Build_LinksRecursiveCompositeFieldsToFinalEntries()
    {
        var selfId = new PgTypeId((Oid)8100);
        var field = new PgCompositeFieldType(new Field("next", selfId, -1));
        var composite = PgType.CreateComposite([field], new("public.node"), oid: 8100);

        var catalog = new PgTypeCatalogBuilder([composite]).Build();
        var surviving = catalog.GetPgType(selfId);

        Assert.AreEqual(surviving, surviving.CompositeFields[0].Type);
    }

    [TestMethod]
    public void Build_LinksSnapshotOwnedFieldsWithoutRetargetingBaseline()
    {
        var originalTarget = PgType.CreateBase(new("public.original"), oid: 8110);
        var composite = PgType.CreateComposite(
            [new PgCompositeFieldType(new Field("value", new((Oid)8110), -1))],
            new("public.container"), oid: 8111);
        var baseline = new PgTypeCatalogBuilder([originalTarget, composite]).Build();

        var replacement = PgType.CreateBase(new("public.replacement"), oid: 8110);
        var builder = new PgTypeCatalogBuilder(baseline);
        builder.Add(replacement);
        var amended = builder.Build();

        Assert.AreEqual(new DataTypeName("public.original"),
            baseline.GetPgType((Oid)8111).CompositeFields[0].Type.DataTypeName);
        Assert.AreEqual(new DataTypeName("public.replacement"),
            amended.GetPgType((Oid)8111).CompositeFields[0].Type.DataTypeName);
        Assert.AreNotSame(baseline.GetPgType((Oid)8111).CompositeFields[0],
            amended.GetPgType((Oid)8111).CompositeFields[0]);
    }

    [TestMethod]
    public void CompositeField_EqualityDoesNotDependOnCatalogLink()
    {
        var declaration = new Field("value", new((Oid)8115), -1);
        var first = new PgCompositeFieldType(declaration);
        var second = new PgCompositeFieldType(declaration);
        var hash = first.GetHashCode();

        first.Link(PgType.CreateBase(new("public.first"), 8115));
        second.Link(PgType.CreateBase(new("public.second"), 8115));

        Assert.AreEqual(first, second);
        Assert.AreEqual(hash, first.GetHashCode());
    }

    static readonly PgBackendInfo BackendInfo = new PgBackendInfoBuilder(
        new Dictionary<string, string>
        {
            ["server_version"] = "17.0",
            ["server_encoding"] = "UTF8"
        }).Build();

    [TestMethod]
    public void Build_PrecomputesArrayAndMultirangeRelationships()
    {
        var element = PgType.CreateBase(new("public.measurement"), oid: 8000);
        var array = PgType.CreateArray(element, oid: 8001);
        var range = PgType.CreateRange(element, new("public.measurement_range"), oid: 8002);
        var multirange = PgType.CreateMultirange(range, new("public.measurement_multirange"), oid: 8003);
        var catalog = new PgTypeCatalogBuilder([element, array, range, multirange]).Build();

        Assert.AreEqual((Oid)8001, catalog.GetArrayOid(new PgTypeId((Oid)8000)));
        Assert.AreEqual(array.DataTypeName, catalog.GetArrayDataTypeName(element.DataTypeName));
        Assert.AreEqual(element.DataTypeName, catalog.GetElementDataTypeName(array.Oid!.Value));
        Assert.AreEqual((Oid)8000, catalog.GetElementOid(array.DataTypeName));
        Assert.IsTrue(catalog.TryGetMultiRangeIdentifiers(range.DataTypeName, out var id, out var name));
        Assert.AreEqual(new PgTypeId((Oid)8003), id);
        Assert.AreEqual(multirange.DataTypeName, name);
    }

    [TestMethod]
    public void Build_PortableCatalogResolvesNamesWithoutOids()
    {
        var element = PgType.CreateBase(new("portable.value"));
        var array = PgType.CreateArray(element);
        var catalog = new PgTypeCatalogBuilder([element, array]).Build();

        Assert.IsTrue(catalog.IsPortable);
        Assert.IsTrue(catalog.TryGetArrayIdentifiers("portable.value", out var id, out var name));
        Assert.AreEqual(new PgTypeId(array.DataTypeName), id);
        Assert.AreEqual(array.DataTypeName, name);
        Assert.ThrowsExactly<InvalidOperationException>(() => catalog.GetArrayOid(element.DataTypeName));
    }

    [TestMethod]
    public void UnqualifiedName_AmbiguityFallsBackToPgCatalog()
    {
        var pgCatalog = PgType.CreateBase(new("pg_catalog.widget"), oid: 8100);
        var custom = PgType.CreateBase(new("custom.widget"), oid: 8101);
        var catalog = new PgTypeCatalogBuilder([custom, pgCatalog]).Build();

        Assert.IsTrue(catalog.TryGetIdentifiers("widget", out var id, out var name));
        Assert.AreEqual(new PgTypeId((Oid)8100), id);
        Assert.AreEqual(pgCatalog.DataTypeName, name);
        Assert.IsTrue(catalog.TryGetIdentifiers("custom.widget", out id, out name));
        Assert.AreEqual(new PgTypeId((Oid)8101), id);
        Assert.AreEqual(custom.DataTypeName, name);
    }

    [TestMethod]
    public void UnqualifiedName_AmbiguousWithoutPgCatalogDoesNotResolve()
    {
        var catalog = new PgTypeCatalogBuilder([
            PgType.CreateBase(new("first.widget"), oid: 8200),
            PgType.CreateBase(new("second.widget"), oid: 8201),
        ]).Build();

        Assert.IsFalse(catalog.TryGetIdentifiers("widget", out _, out _));
        var exception = Assert.ThrowsExactly<KeyNotFoundException>(
            () => catalog.GetDataTypeName("widget"));
        StringAssert.Contains(exception.Message, "first.widget");
        StringAssert.Contains(exception.Message, "second.widget");
    }

    [TestMethod]
    public void Add_ReplacesWholeEntryByOidAndName()
    {
        var builder = new PgTypeCatalogBuilder([
            PgType.CreateBase(new("first.original"), oid: 8300),
            PgType.CreateBase(new("first.replaced_name"), oid: 8301),
        ]);

        var replacement = PgType.CreateBase(new("first.replaced_name"), oid: 8300);
        builder.Add(replacement);
        var catalog = builder.Build();

        Assert.AreEqual(1, catalog.Types.Count);
        Assert.AreEqual(replacement, catalog.GetPgType((Oid)8300));
        Assert.IsFalse(catalog.TryGetIdentifiers("first.original", out _, out _));
        Assert.ThrowsExactly<KeyNotFoundException>(() => catalog.GetPgType((Oid)8301));
    }

    [TestMethod]
    public void Add_RejectsMixedPortableAndOidBackedTypes()
    {
        var builder = new PgTypeCatalogBuilder([PgType.CreateBase(new("portable.value"))]);

        Assert.ThrowsExactly<InvalidOperationException>(
            () => builder.Add(PgType.CreateBase(new("pg_catalog.int4"), oid: 23)));
    }

    [TestMethod]
    public void Build_RelinksRelationshipsToReplacedEntry()
    {
        var element = PgType.CreateBase(new("public.value"), oid: 8400);
        var builder = new PgTypeCatalogBuilder([element, PgType.CreateArray(element, oid: 8401)]);
        var replacement = PgType.CreateBase(new("public.renamed_value"), oid: 8400);
        builder.Add(replacement);

        var catalog = builder.Build();

        Assert.AreEqual(replacement, catalog.GetPgType((Oid)8401).ElementType);
    }

    [TestMethod]
    public void Build_ResolvesComplexTypesIndependentlyOfPopulationOrder()
    {
        var builder = new PgTypeCatalogBuilder();

        // Deliberately add the graph from the outermost type inward. This is also a shape
        // PostgreSQL can produce naturally: a domain over an array gets its own array type.
        builder.AddArray(new("public._measurements"), 8705, new PgTypeId((Oid)8704));
        builder.AddMultirange(new("public.measurement_multirange"), 8704, new PgTypeId((Oid)8703));
        builder.AddRange(new("public.measurement_range"), 8703, new PgTypeId((Oid)8702));
        builder.AddDomain(new("public.measurements"), 8702, new PgTypeId((Oid)8701));
        builder.AddArray(new("public._measurement"), 8701, new PgTypeId((Oid)8700));
        builder.Add(PgType.CreateBase(new("public.measurement"), oid: 8700));

        var catalog = builder.Build();
        var outerArray = catalog.GetPgType((Oid)8705);

        Assert.AreEqual(PgTypeKind.Array, outerArray.Kind);
        Assert.AreEqual(PgTypeKind.Multirange, outerArray.ElementType.Kind);
        Assert.AreEqual(PgTypeKind.Range, outerArray.ElementType.RangeType.Kind);
        Assert.AreEqual(PgTypeKind.Domain, outerArray.ElementType.ElementType.Kind);
        Assert.AreEqual(PgTypeKind.Array, outerArray.ElementType.ElementType.UnderlyingType.Kind);
        Assert.AreEqual(new DataTypeName("public.measurement"),
            outerArray.ElementType.ElementType.UnderlyingType.ElementType.DataTypeName);
    }

    [TestMethod]
    public async Task Factory_PluginsConfigureBeforeLoadingAndLastRegistrationWins()
    {
        var original = PgType.CreateBase(new("extension.value"), oid: 8500);
        var firstReplacement = PgType.CreateBase(new("extension.first"), oid: 8500);
        var lastReplacement = PgType.CreateBase(new("extension.last"), oid: 8500);
        var factory = new RecordingFactory(original);

        var catalog = await factory.CreateAsync(Context(),
        [
            new TestPlugin("first", firstReplacement, loadTableComposites: true),
            new TestPlugin("second", lastReplacement)
        ]);

        CollectionAssert.AreEqual(new[] { "first", "second" }, factory.Options.Schemas.ToArray());
        Assert.IsTrue(factory.Options.LoadTableComposites);
        Assert.AreEqual(lastReplacement, catalog.GetPgType((Oid)8500));
    }

    [TestMethod]
    public async Task Factory_ReexecutionReconfiguresReloadAndReappliesPlugins()
    {
        var factory = new RecordingFactory(PgType.CreateBase(new("public.first"), oid: 8600));
        var plugin = new CountingPlugin();

        var first = await factory.CreateAsync(Context(), [plugin]);
        factory.Baseline = PgType.CreateBase(new("public.second"), oid: 8601);
        var second = await factory.CreateAsync(Context(), [plugin]);

        Assert.AreEqual(2, plugin.ConfigureCount);
        Assert.AreEqual(2, plugin.ApplyCount);
        Assert.AreNotSame(first, second);
        Assert.AreEqual(new DataTypeName("public.second"), second.GetPgType((Oid)8601).DataTypeName);
    }

    sealed class RecordingFactory(PgType baseline) : PgTypeCatalogFactory
    {
        public PgType Baseline { get; set; } = baseline;
        public PgTypeLoadingOptions Options { get; private set; } = null!;

        protected override void Populate(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options)
        {
            Options = options;
            builder.Add(Baseline);
        }

        protected override ValueTask PopulateAsync(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options, CancellationToken cancellationToken)
        {
            Populate(builder, context, options);
            return ValueTask.CompletedTask;
        }
    }

    [TestMethod]
    public void TypeLoadingRequirements_AreMonotonic()
    {
        var options = new PgTypeLoadingOptionsBuilder();
        options.EnableTableCompositesLoading();
        options.EnableTableCompositesLoading(enable: false);

        Assert.IsTrue(options.Build().LoadTableComposites);
    }

    sealed class TestPlugin(string schema, PgType replacement, bool loadTableComposites = false)
        : PgTypeCatalogPlugin
    {
        public override void Configure(PgTypeLoadingOptionsBuilder options)
        {
            options.AddTypeLoadingSchema(schema);
            if (loadTableComposites)
                options.EnableTableCompositesLoading();
        }

        public override void Apply(PgTypeCatalogBuilder builder)
            => builder.Add(replacement);
    }

    sealed class CountingPlugin : PgTypeCatalogPlugin
    {
        public int ConfigureCount { get; private set; }
        public int ApplyCount { get; private set; }

        public override void Configure(PgTypeLoadingOptionsBuilder options)
        {
            ConfigureCount++;
            options.AddTypeLoadingSchema("reload");
        }

        public override void Apply(PgTypeCatalogBuilder builder)
            => ApplyCount++;
    }

    static PgTypeCatalogFactoryContext Context() => new(BackendInfo);
}
