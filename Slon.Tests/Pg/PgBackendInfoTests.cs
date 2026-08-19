using Slon.Pg;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Types;

namespace Slon.Tests.Pg;

[TestClass]
public class PgBackendInfoTests
{
    [TestMethod]
    public void ConfiguredProfileControlsBackendBehavior()
    {
        Assert.AreSame(PostgreSqlBackendProvider.Instance, PgBackendProviders.Create(null));
        Assert.IsInstanceOfType<ConfiguredBackendProvider>(PgBackendProviders.Create(CreateQuestDbProfile()));
    }

    [TestMethod]
    public void QuestDbProfileUsesExplicitFeaturesAndPrebuiltTypes()
    {
        var provider = PgBackendProviders.Create(CreateQuestDbProfile());

        var info = provider.CreateBackendInfo(new Dictionary<string, string>
        {
            ["server_version"] = "11.3"
        });

        Assert.IsTrue(info.Capabilities.HasIntegerDateTimes);
        Assert.IsFalse(info.Capabilities.SupportsRangeTypes);
        Assert.IsFalse(provider.CreateTypeCatalogFactory(info).RequiresProtocol);
    }

    [TestMethod]
    public void QuestDbProfileUsesDiscardAllForCompleteReset()
    {
        var provider = PgBackendProviders.Create(CreateQuestDbProfile());
        var info = provider.CreateBackendInfo(new Dictionary<string, string>
        {
            ["server_version"] = "11.3"
        });

        Assert.AreEqual("DISCARD ALL", provider.ResolveScopeResetCommand(new ScopeResetOptions(), info));

        var disabled = new ScopeResetOptions
        {
            CloseCursors = false,
            ResetSessionAuthorization = false,
            ResetParameters = false,
            ClearListeners = false,
            ReleaseAdvisoryLocks = false,
            DropTemporaryObjects = false
        };
        Assert.IsNull(provider.ResolveScopeResetCommand(disabled, info));

        disabled.CloseCursors = true;
        Assert.ThrowsExactly<NotSupportedException>(() => provider.ResolveScopeResetCommand(disabled, info));
    }

    static PostgreSqlCompatibilityProfile CreateQuestDbProfile()
        => new()
        {
            Features = PostgreSqlCompatibilityFeatures.IntegerDateTimes,
            LoadTypesFromCatalog = false,
            CompleteScopeResetCommand = "DISCARD ALL"
        };

    [TestMethod]
    public void Builder_SeedsPostgreSqlCapabilitiesFromVersionAndParameters()
    {
        var info = new PgBackendInfoBuilder(new Dictionary<string, string>
        {
            ["server_version"] = "14.12 (test build)",
            ["server_encoding"] = "UTF8",
            ["integer_datetimes"] = "off",
        }).Build();

        Assert.AreEqual(new Version(14, 12), info.ServerVersion);
        Assert.AreEqual("14.12 (test build)", info.ServerVersionString);
        Assert.IsTrue(info.Capabilities.SupportsRangeTypes);
        Assert.IsTrue(info.Capabilities.SupportsMultirangeTypes);
        Assert.IsTrue(info.Capabilities.SupportsEnumTypes);
        Assert.IsTrue(info.Capabilities.HasEnumSortOrder);
        Assert.IsTrue(info.Capabilities.HasTypeCategory);
        Assert.IsFalse(info.Capabilities.HasIntegerDateTimes);
        Assert.AreEqual("14.12 (test build)", info.StartupParameters["server_version"]);
    }

    [TestMethod]
    [DataRow("6.3", false, false, false)]
    [DataRow("6.4", true, false, false)]
    [DataRow("7.1", true, false, false)]
    [DataRow("7.2", true, true, false)]
    [DataRow("7.3", true, true, true)]
    public void Builder_VersionGatesLegacySessionFeatures(
        string version,
        bool supportsUnlisten,
        bool supportsResetAll,
        bool supportsSessionAuthorization)
    {
        var capabilities = CreateInfo(version, "UTF8", "on").Capabilities;

        Assert.AreEqual(supportsUnlisten, capabilities.SupportsUnlisten);
        Assert.AreEqual(supportsResetAll, capabilities.SupportsResetAll);
        Assert.AreEqual(supportsSessionAuthorization, capabilities.SupportsSessionAuthorization);
    }

    [TestMethod]
    public void Builder_AllowsDialectToOverrideVersionDerivedCapabilities()
    {
        var builder = new PgBackendInfoBuilder(new Dictionary<string, string>
        {
            ["server_version"] = "17",
            ["server_encoding"] = "UTF8",
            ["integer_datetimes"] = "on",
        });
        builder.Capabilities = builder.Capabilities with
        {
            SupportsMultirangeTypes = false,
            SupportsListen = false,
        };

        var info = builder.Build();

        Assert.AreEqual(new Version(17, 0), info.ServerVersion);
        Assert.IsFalse(info.Capabilities.SupportsMultirangeTypes);
        Assert.IsFalse(info.Capabilities.SupportsListen);
    }

    [TestMethod]
    public void Builder_PreservesStartupFactsIndependentlyOfTheCallerDictionary()
    {
        var parameters = new Dictionary<string, string>
        {
            ["crdb_version"] = "CockroachDB CCL v24.3",
            ["server_version"] = "13.0.0",
            ["server_encoding"] = "UTF8"
        };
        var info = new PgBackendInfoBuilder(parameters).Build();

        parameters["crdb_version"] = "mutated";

        Assert.AreEqual("CockroachDB CCL v24.3", info.StartupParameters["crdb_version"]);
    }

    [TestMethod]
    public void StandaloneCompatibilityMatchesModernPostgreSqlCapabilities()
    {
        var modern = new PgBackendInfoBuilder(new Dictionary<string, string>
        {
            ["server_version"] = int.MaxValue.ToString(),
            ["server_encoding"] = "UTF8",
            ["integer_datetimes"] = "on",
        }).Build();

        Assert.AreEqual(modern.Capabilities, PgBackendCapabilities.PostgreSqlCompatibility);
    }

    [TestMethod]
    [DataRow("14.")]
    [DataRow(".5")]
    [DataRow("not-a-version")]
    public void Builder_InvalidServerVersionHasContext(string serverVersion)
    {
        var exception = Assert.ThrowsExactly<FormatException>(() => new PgBackendInfoBuilder(
            new Dictionary<string, string>
            {
                ["server_version"] = serverVersion,
                ["server_encoding"] = "UTF8",
            }));

        StringAssert.Contains(exception.Message, "server_version");
        StringAssert.Contains(exception.Message, serverVersion);
    }

    [TestMethod]
    public void Provider_RejectsConnectionWithDifferentCapabilityShape()
    {
        var provider = PostgreSqlBackendProvider.Instance;
        var expected = CreateInfo("14.1", "UTF8", "on");
        var actual = CreateInfo("13.9", "UTF8", "on");

        var exception = Assert.ThrowsExactly<InvalidOperationException>(
            () => provider.ValidateConnectionCompatibility(expected, actual));
        StringAssert.Contains(exception.Message, nameof(PgBackendCapabilities.SupportsMultirangeTypes));
    }

    [TestMethod]
    public void Provider_AllowsDifferentVersionsAndServerEncodingsWithTheSameCapabilityShape()
    {
        var provider = PostgreSqlBackendProvider.Instance;
        var expected = CreateInfo("17.1", "UTF8", "on");
        var actual = CreateInfo("17.2 (rolling upgrade)", "LATIN1", "on");

        provider.ValidateConnectionCompatibility(expected, actual);
    }

    [TestMethod]
    public void Provider_AllowsConnectionLocalCapabilityDifferences()
    {
        var provider = PostgreSqlBackendProvider.Instance;
        var expected = CreateInfo("17.1", "UTF8", "on");
        var builder = new PgBackendInfoBuilder(expected.StartupParameters);
        builder.Capabilities = builder.Capabilities with
        {
            SupportsListen = false,
            SupportsUnlisten = false,
            SupportsNotifications = false,
            SupportsAdvisoryLocks = false
        };

        provider.ValidateConnectionCompatibility(expected, builder.Build());
    }

    [TestMethod]
    public void PostgreSqlFactory_ConfiguredSchemasStillDiscoverPluginTypes()
    {
        var options = new PgTypeLoadingOptions
        {
            Schemas = ["application", "odd\\schema'name"],
            LoadTableComposites = false
        };

        var query = PostgreSqlTypeCatalogFactory.BuildTypeQuery(options,
            new PgBackendCapabilities { HasTypeCategory = true });
        StringAssert.Contains(query, "OR t.typcategory = 'U'");
        StringAssert.Contains(query, "OR (et.typarray = t.oid AND et.typcategory = 'U')");
        StringAssert.Contains(query, "E'odd\\\\schema''name'");

        var legacyQuery = PostgreSqlTypeCatalogFactory.BuildTypeQuery(options,
            new PgBackendCapabilities { HasTypeCategory = false });
        Assert.IsFalse(legacyQuery.Contains("typcategory", StringComparison.Ordinal));

        var capabilities = new PgBackendCapabilities
        {
            SupportsEnumTypes = true,
            HasTypeCategory = true
        };
        StringAssert.Contains(PostgreSqlTypeCatalogFactory.BuildEnumQuery(options, capabilities),
            "OR t.typcategory = 'U'");
        StringAssert.Contains(PostgreSqlTypeCatalogFactory.BuildCompositeQuery(options, capabilities),
            "OR t.typcategory = 'U'");
    }

    [TestMethod]
    public void PostgreSqlFactory_AbsentRangeColumnsRetainOidBinaryShape()
    {
        var options = new PgTypeLoadingOptions
        {
            Schemas = [],
            LoadTableComposites = false
        };

        var withoutEither = PostgreSqlTypeCatalogFactory.BuildTypeQuery(options, new());
        StringAssert.Contains(withoutEither, "0::oid, 0::oid FROM");

        var withoutMultiranges = PostgreSqlTypeCatalogFactory.BuildTypeQuery(options,
            new PgBackendCapabilities { SupportsRangeTypes = true });
        StringAssert.Contains(withoutMultiranges, "COALESCE(r.rngsubtype, 0), 0::oid FROM");

        var withoutRanges = PostgreSqlTypeCatalogFactory.BuildTypeQuery(options,
            new PgBackendCapabilities { SupportsMultirangeTypes = true });
        StringAssert.Contains(withoutRanges, "0::oid, COALESCE(m.rngtypid, 0) FROM");
    }

    static PgBackendInfo CreateInfo(string version, string encoding, string integerDateTimes)
        => new PgBackendInfoBuilder(new Dictionary<string, string>
        {
            ["server_version"] = version,
            ["server_encoding"] = encoding,
            ["integer_datetimes"] = integerDateTimes
        }).Build();

}
