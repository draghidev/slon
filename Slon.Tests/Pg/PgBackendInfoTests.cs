using Slon.Pg;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Types;

namespace Slon.Tests.Pg;

[TestClass]
public class PgBackendInfoTests
{

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
        Assert.AreEqual("UTF8", info.ServerEncoding);
        Assert.IsTrue(info.Capabilities.SupportsRangeTypes);
        Assert.IsTrue(info.Capabilities.SupportsMultirangeTypes);
        Assert.IsTrue(info.Capabilities.SupportsEnumTypes);
        Assert.IsTrue(info.Capabilities.HasEnumSortOrder);
        Assert.IsTrue(info.Capabilities.HasTypeCategory);
        Assert.IsFalse(info.Capabilities.HasIntegerDateTimes);
        Assert.AreEqual("14.12 (test build)", info.StartupParameters["server_version"]);
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
        StringAssert.Contains(exception.Message, "Expected server encoding 'UTF8'");
        StringAssert.Contains(exception.Message, "actual server encoding 'UTF8'");
    }

    [TestMethod]
    public void Provider_AllowsDifferentVersionsWithTheSameCapabilityShape()
    {
        var provider = PostgreSqlBackendProvider.Instance;
        var expected = CreateInfo("17.1", "UTF8", "on");
        var actual = CreateInfo("17.2 (rolling upgrade)", "utf8", "on");

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
    [DataRow(false)]
    [DataRow(true)]
    public async Task DataSource_BootstrapConnectionBecomesFirstPooledConnection(bool async)
    {
        var events = new List<string>();
        var provider = new RecordingProvider(events);
        await using var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = provider,
            TypeCatalogPlugins = [new RecordingPlugin("user", events)]
        });

        var connection = async
            ? await dataSource.OpenConnectionAsync(CancellationToken.None)
            : dataSource.OpenConnection();
        await using var _ = connection.ConfigureAwait(false);

        Assert.AreEqual(1, provider.BackendInfoBuilds,
            "the connection used for bootstrap should be retained as the first pooled connection");
        Assert.AreEqual(1, provider.CatalogFactoryBuilds);
        CollectionAssert.AreEqual(new[]
        {
            "configure:dialect", "configure:user", "apply:dialect", "apply:user"
        }, events);
    }

    [TestMethod]
    public async Task DataSource_DirectTypeLoadingOptionsFeedTheFactory()
    {
        var factory = new OptionsRecordingFactory();
        var schemas = new[] { "application" };
        await using var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(factory),
            TypeLoadingSchemas = schemas,
            LoadTableComposites = true
        });
        schemas[0] = "mutated";

        await using (var connection = await dataSource.OpenConnectionAsync()) { }

        CollectionAssert.AreEqual(new[] { "application" }, factory.Options!.Schemas.ToArray());
        Assert.IsTrue(factory.Options.LoadTableComposites);
    }

    [TestMethod]
    public async Task Bootstrap_ProtocolFactoryMustQueueItsLoadFlow()
    {
        await using var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(new NonQueuingProtocolFactory())
        });

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            async () => await dataSource.OpenConnectionAsync());
        StringAssert.Contains(exception.Message, "without queuing its load flow");
    }

    [TestMethod]
    public async Task ReloadTypes_PrebuiltCatalogIsANoOp()
    {
        await using var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(
                PgTypeCatalogFactory.FromBaseline(PgTypeCatalog.Default))
        });
        await using (var connection = await dataSource.OpenConnectionAsync()) { }
        var before = dataSource.GetDbDependencies(initializedOnly: true);

        dataSource.ReloadTypes();
        await dataSource.ReloadTypesAsync();

        Assert.AreSame(before, dataSource.GetDbDependencies(initializedOnly: true),
            "a prebuilt reload must neither rebuild the catalog nor publish a fake revision");
    }

    [TestMethod]
    public async Task ReloadTypes_OnFreshDataSourceIsSatisfiedByInitialization()
    {
        var factory = new ReloadableFactory(PgType.CreateBase(new("public.initial"), oid: 8803));
        await using var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(factory)
        });

        await dataSource.ReloadTypesAsync();

        Assert.AreEqual(1, factory.PopulateCount);
        Assert.AreEqual(0, dataSource.GetDbDependencies(initializedOnly: true).Revision);
    }

    [TestMethod]
    public async Task FirstOpenCancellationDetachesFromSharedInitialization()
    {
        var factory = new ReloadableFactory(PgType.CreateBase(new("public.initial"), oid: 8804));
        factory.Arm();
        await using var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(factory)
        });
        using var cancellation = new CancellationTokenSource();

        var firstOpen = dataSource.OpenConnectionAsync(cancellation.Token).AsTask();
        await factory.Entered.WaitAsync(TestTimeout.Hang);
        cancellation.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await firstOpen);

        factory.Release();
        await using var connection = await dataSource.OpenConnectionAsync();
        Assert.AreEqual(1, factory.PopulateCount,
            "caller cancellation must not cancel or duplicate shared initialization");
    }

    [TestMethod]
    public async Task PostgreSqlFactory_LoadsEnumAndCompositeMetadataWithoutRowMultiplication()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var enumName = "slon_enum_" + suffix;
        var domainName = "slon_domain_" + suffix;
        var compositeName = "slon_composite_" + suffix;

        try
        {
            await AdoTestPool.ExecuteNonQueryAsync(
                $"CREATE TYPE {enumName} AS ENUM ('first', 'second')");
            await AdoTestPool.ExecuteNonQueryAsync(
                $"CREATE DOMAIN {domainName} AS integer NOT NULL");
            await AdoTestPool.ExecuteNonQueryAsync(
                $"CREATE TYPE {compositeName} AS " +
                $"(number integer, value {enumName}, constrained {domainName}, label varchar(12))");

            await using var dataSource = AdoTestPool.NewIsolatedDataSource(
                options => options with { PoolSize = 1 });
            await using (var connection = await dataSource.OpenConnectionAsync()) { }

            var enumType = dataSource.TypeCatalog.GetPgType(
                dataSource.TypeCatalog.TryGetIdentifiers(enumName, out var enumId, out _)
                    ? enumId
                    : throw new AssertFailedException("enum type was not loaded"));
            CollectionAssert.AreEqual(new[] { "first", "second" }, enumType.EnumVariants);

            Assert.IsTrue(dataSource.TypeCatalog.TryGetIdentifiers(compositeName, out var compositeId, out _));
            var fields = dataSource.TypeCatalog.GetPgType(compositeId).CompositeFields;
            Assert.AreEqual(4, fields.Length);
            Assert.AreEqual("number", fields[0].Field.Name);
            Assert.AreEqual("value", fields[1].Field.Name);
            Assert.AreEqual(enumId.Oid, fields[1].Type.Oid);
            Assert.IsTrue(dataSource.TypeCatalog.TryGetIdentifiers(domainName, out var domainId, out _));
            Assert.AreEqual(domainId.Oid, fields[2].Type.Oid);
            Assert.IsTrue(fields[2].Type.IsDomainNotNull);
            Assert.AreEqual("label", fields[3].Field.Name);
            Assert.AreEqual(16, fields[3].Field.TypeModifier,
                "varchar(12) typmod includes PostgreSQL's four-byte varlena header");

            var beforeReload = dataSource.GetDbDependencies(initializedOnly: true);
            await dataSource.ReloadTypesAsync();
            var afterReload = dataSource.GetDbDependencies(initializedOnly: true);
            Assert.AreNotSame(beforeReload, afterReload);
            CollectionAssert.AreEqual(new[] { "first", "second" },
                afterReload.TypeCatalog.GetPgType(enumId).EnumVariants);
        }
        finally
        {
            await AdoTestPool.ExecuteNonQueryAsync($"DROP TYPE IF EXISTS {compositeName}");
            await AdoTestPool.ExecuteNonQueryAsync($"DROP DOMAIN IF EXISTS {domainName}");
            await AdoTestPool.ExecuteNonQueryAsync($"DROP TYPE IF EXISTS {enumName}");
        }
    }

    [TestMethod]
    public async Task ReloadTypes_IsSingleFlightAndPublishesOnlyACompleteCatalog()
    {
        var factory = new ReloadableFactory(PgType.CreateBase(new("public.initial"), oid: 8800));
        await using var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(factory)
        });
        await using (var connection = await dataSource.OpenConnectionAsync()) { }

        var initial = dataSource.GetDbDependencies(initializedOnly: true);
        factory.Baseline = PgType.CreateBase(new("public.reloaded"), oid: 8801);
        factory.Arm();

        var first = dataSource.ReloadTypesAsync().AsTask();
        await factory.Entered.WaitAsync(TestTimeout.Hang);
        var second = dataSource.ReloadTypesAsync().AsTask();
        factory.Release();
        await Task.WhenAll(first, second);

        var reloaded = dataSource.GetDbDependencies(initializedOnly: true);
        Assert.AreNotSame(initial, reloaded);
        Assert.AreSame(initial.BackendInfo, reloaded.BackendInfo);
        Assert.AreNotSame(initial.TypeCatalog, reloaded.TypeCatalog);
        Assert.AreEqual(initial.Revision + 1, reloaded.Revision);
        Assert.AreEqual(new DataTypeName("public.reloaded"),
            reloaded.TypeCatalog.GetPgType((Oid)8801).DataTypeName);
        Assert.AreEqual(2, factory.PopulateCount,
            "one bootstrap population plus one shared reload population");

        factory.Baseline = PgType.CreateBase(new("public.sync_reloaded"), oid: 8802);
        dataSource.ReloadTypes();
        var syncReloaded = dataSource.GetDbDependencies(initializedOnly: true);
        Assert.AreEqual(reloaded.Revision + 1, syncReloaded.Revision);
        Assert.AreEqual(new DataTypeName("public.sync_reloaded"),
            syncReloaded.TypeCatalog.GetPgType((Oid)8802).DataTypeName);

        factory.Fail = true;
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await dataSource.ReloadTypesAsync());
        Assert.AreSame(syncReloaded, dataSource.GetDbDependencies(initializedOnly: true));
    }

    [TestMethod]
    public async Task ReloadTypes_UsesTheMatchingFactoryContract()
    {
        var factory = new ReloadPathFactory();
        await using var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(factory)
        });

        await using (var connection = await dataSource.OpenConnectionAsync()) { }
        Assert.AreEqual(0, factory.SyncPopulates);
        Assert.AreEqual(1, factory.AsyncPopulates);

        dataSource.ReloadTypes();
        Assert.AreEqual(1, factory.SyncPopulates);
        Assert.AreEqual(1, factory.AsyncPopulates);

        await dataSource.ReloadTypesAsync();
        Assert.AreEqual(1, factory.SyncPopulates);
        Assert.AreEqual(2, factory.AsyncPopulates);
    }

    [TestMethod]
    public async Task Dispose_CancelsBootstrapBeforeWaitingForLifecycleQuiescence()
    {
        var factory = new ReloadableFactory(PgType.CreateBase(new("public.initial"), oid: 8891));
        factory.Arm();
        var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(factory)
        });

        var open = dataSource.OpenConnectionAsync(CancellationToken.None).AsTask();
        await factory.Entered.WaitAsync(TestTimeout.Hang);
        var dispose = dataSource.DisposeAsync().AsTask();

        await dispose.WaitAsync(TestTimeout.Hang);
        try
        {
            await open;
            Assert.Fail("bootstrap should have observed datasource shutdown");
        }
        catch (OperationCanceledException)
        {
        }
    }

    [TestMethod]
    public async Task Dispose_CancelsTypeReloadBeforeWaitingForLifecycleQuiescence()
    {
        var factory = new ReloadableFactory(PgType.CreateBase(new("public.initial"), oid: 8892));
        var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(factory)
        });

        await using (var connection = await dataSource.OpenConnectionAsync()) { }
        factory.Arm();
        var reload = dataSource.ReloadTypesAsync().AsTask();
        await factory.Entered.WaitAsync(TestTimeout.Hang);
        var dispose = dataSource.DisposeAsync().AsTask();

        await dispose.WaitAsync(TestTimeout.Hang);
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await reload);
    }

    [TestMethod]
    public async Task Dispose_CancelsSynchronousTypeReloadBeforeWaitingForLifecycleQuiescence()
    {
        var factory = new SyncCancellationFactory();
        var dataSource = new SlonDataSource(AdoTestPool.NewOptions() with
        {
            BackendProvider = new ReloadableProvider(factory)
        });

        await using (var connection = await dataSource.OpenConnectionAsync()) { }
        factory.Arm();
        var reload = Task.Run(dataSource.ReloadTypes);
        await factory.Entered.WaitAsync(TestTimeout.Hang);
        var dispose = dataSource.DisposeAsync().AsTask();

        await dispose.WaitAsync(TestTimeout.Hang);
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await reload);
    }

    static PgBackendInfo CreateInfo(string version, string encoding, string integerDateTimes)
        => new PgBackendInfoBuilder(new Dictionary<string, string>
        {
            ["server_version"] = version,
            ["server_encoding"] = encoding,
            ["integer_datetimes"] = integerDateTimes
        }).Build();

    sealed class RecordingProvider(List<string> events) : PgBackendProvider
    {
        public int BackendInfoBuilds { get; private set; }
        public int CatalogFactoryBuilds { get; private set; }

        public override PgBackendInfo CreateBackendInfo(IReadOnlyDictionary<string, string> serverParameters)
        {
            BackendInfoBuilds++;
            return new PgBackendInfoBuilder(serverParameters).Build();
        }

        public override PgTypeCatalogFactory CreateTypeCatalogFactory(PgBackendInfo backendInfo)
        {
            CatalogFactoryBuilds++;
            return PgTypeCatalogFactory.FromBaseline(PgTypeCatalog.Default);
        }

        public override IReadOnlyList<PgTypeCatalogPlugin> CreateTypeCatalogPlugins(PgBackendInfo backendInfo)
            => [new RecordingPlugin("dialect", events)];
    }

    sealed class RecordingPlugin(string name, List<string> events) : PgTypeCatalogPlugin
    {
        public override void Configure(PgTypeLoadingOptionsBuilder options)
            => events.Add($"configure:{name}");

        public override void Apply(PgTypeCatalogBuilder builder)
            => events.Add($"apply:{name}");
    }

    sealed class ReloadableProvider(PgTypeCatalogFactory factory) : PgBackendProvider
    {
        public override PgBackendInfo CreateBackendInfo(IReadOnlyDictionary<string, string> serverParameters)
            => new PgBackendInfoBuilder(serverParameters).Build();

        public override PgTypeCatalogFactory CreateTypeCatalogFactory(PgBackendInfo backendInfo)
            => factory;
    }

    sealed class ReloadableFactory(PgType baseline) : PgTypeCatalogFactory
    {
        TaskCompletionSource? _entered;
        TaskCompletionSource? _release;

        public PgType Baseline { get; set; } = baseline;
        public bool Fail { get; set; }
        public int PopulateCount { get; private set; }
        public Task Entered => _entered?.Task ?? Task.CompletedTask;
        internal override bool RequiresProtocol => false;

        public void Arm()
        {
            _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void Release() => _release!.TrySetResult();

        protected override void Populate(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options)
        {
            PopulateCount++;
            if (Fail)
                throw new InvalidOperationException("reload failed");
            builder.Add(Baseline);
        }

        protected override async ValueTask PopulateAsync(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options,
            CancellationToken cancellationToken)
        {
            PopulateCount++;
            if (_entered is { } entered)
            {
                entered.TrySetResult();
                await _release!.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                _entered = null;
                _release = null;
            }
            if (Fail)
                throw new InvalidOperationException("reload failed");
            builder.Add(Baseline);
        }
    }

    sealed class SyncCancellationFactory : PgTypeCatalogFactory
    {
        TaskCompletionSource? _entered;

        internal override bool RequiresProtocol => false;
        public Task Entered => _entered?.Task ?? Task.CompletedTask;

        public void Arm()
            => _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override void Populate(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options)
        {
            if (_entered is { } entered)
            {
                entered.TrySetResult();
                context.StoppingToken.WaitHandle.WaitOne();
                context.StoppingToken.ThrowIfCancellationRequested();
            }
            foreach (var type in PgTypeCatalog.Default.Types)
                builder.Add(type);
        }

        protected override ValueTask PopulateAsync(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options,
            CancellationToken cancellationToken)
        {
            foreach (var type in PgTypeCatalog.Default.Types)
                builder.Add(type);
            return ValueTask.CompletedTask;
        }
    }

    sealed class OptionsRecordingFactory : PgTypeCatalogFactory
    {
        internal override bool RequiresProtocol => false;
        public PgTypeLoadingOptions? Options { get; private set; }

        protected override void Populate(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options)
        {
            Options = options;
            foreach (var type in PgTypeCatalog.Default.Types)
                builder.Add(type);
        }

        protected override ValueTask PopulateAsync(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options,
            CancellationToken cancellationToken)
        {
            Populate(builder, context, options);
            return ValueTask.CompletedTask;
        }
    }

    sealed class NonQueuingProtocolFactory : PgTypeCatalogFactory
    {
        protected override void Populate(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options)
        { }

        protected override ValueTask PopulateAsync(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options,
            CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }

    sealed class ReloadPathFactory : PgTypeCatalogFactory
    {
        public int SyncPopulates { get; private set; }
        public int AsyncPopulates { get; private set; }

        protected override void Populate(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options)
        {
            SyncPopulates++;
            foreach (var result in context.Queue(
                         new CommandFlow(async: false, Command.Create("select 1"))))
            {
                foreach (var _ in result) { }
            }
            builder.Add(PgType.CreateBase(new("public.reload_path"), oid: 8890));
        }

        protected override async ValueTask PopulateAsync(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options,
            CancellationToken cancellationToken)
        {
            AsyncPopulates++;
            await foreach (var result in context.Queue(
                               new CommandFlow(async: true, Command.Create("select 1")),
                               cancellationToken))
            {
                await foreach (var _ in result.WithCancellation(cancellationToken)) { }
            }
            builder.Add(PgType.CreateBase(new("public.reload_path"), oid: 8890));
        }
    }
}
