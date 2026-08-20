namespace Slon.Tests;

[TestClass]
public class PreparedCommandTests
{
    [TestMethod]
    public async Task Prepare_MakesZeroParameterCommandImmutableAndConcurrentlyReusable()
    {
        await using var command = AdoTestPool.CreateCommand("select 42");

        await command.PrepareAsync();
        Assert.IsTrue(command.IsReadOnly);
        Assert.ThrowsExactly<InvalidOperationException>(() => command.CommandText = "select 43");
        command.CommandTimeout = 1;
        Assert.ThrowsExactly<NotSupportedException>(() => command.Cancel());
        Assert.ThrowsExactly<NotSupportedException>(() => command.CancelAsync());

        var executions = Enumerable.Range(0, 16)
            .Select(_ => command.ExecuteScalarAsync(CancellationToken.None));
        var results = await Task.WhenAll(executions);

        Assert.IsTrue(results.All(static result => result is 42));
    }

    [TestMethod]
    public async Task PreparedParameters_AreInferredAndRemainExecutionLocal()
    {
        await using var command = AdoTestPool.CreateCommand("select $1::int");
        command.Parameters.Add(new SlonParameter(null));
        await command.PrepareAsync();

        var executions = Enumerable.Range(0, 16)
            .Select(value => command.ExecuteScalarAsync(SlonParameters.Create(value)).AsTask());
        var results = await Task.WhenAll(executions);

        Assert.IsTrue(command.IsReadOnly);
        for (var i = 0; i < results.Length; i++)
            Assert.AreEqual(i, results[i]);
        Assert.ThrowsExactly<InvalidOperationException>(() => command.CommandText = "select $1::bigint");
    }

    [TestMethod]
    public async Task Prepare_KeepsCommandParameterValuesMutable()
    {
        await using var command = AdoTestPool.CreateCommand("select $1::int");
        var parameter = new SlonParameter<int>(1);
        command.Parameters.Add(parameter);
        await command.PrepareAsync();

        Assert.AreEqual(1, await command.ExecuteScalarAsync(CancellationToken.None));
        parameter.Value = 2;
        Assert.AreEqual(2, await command.ExecuteScalarAsync(CancellationToken.None));

        Assert.AreEqual(3,
            await command.ExecuteScalarAsync(SlonParameters.Create(3), CancellationToken.None));
    }

    [TestMethod]
    public async Task Prepare_UsesLocalParameterTypesAsOverloadHints()
    {
        await using var command = AdoTestPool.CreateCommand("select $1");
        command.Parameters.Add(new SlonParameter<int>());

        await command.PrepareAsync();

        Assert.AreEqual(42,
            await command.ExecuteScalarAsync(SlonParameters.Create(42), CancellationToken.None));
    }

    [TestMethod]
    public async Task Prepare_InfersOnlyUnresolvedParameterSlots()
    {
        await using var command = AdoTestPool.CreateCommand("select $1::int + $2");
        command.Parameters.Add(new SlonParameter(null));
        command.Parameters.Add(new SlonParameter<int>());

        await command.PrepareAsync();

        var parameters = new SlonParameters();
        parameters.Add(40);
        parameters.Add(2);
        Assert.AreEqual(42,
            await command.ExecuteScalarAsync(parameters, CancellationToken.None));
    }

    [TestMethod]
    public async Task PreparedParameterRejectsConflictingRequestedType()
    {
        await using var command = AdoTestPool.CreateCommand("select $1::int");
        command.Parameters.Add(new SlonParameter(null));
        await command.PrepareAsync();

        var parameter = new SlonParameter("42") { SlonDbType = SlonDbTypes.Text };
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => command.ExecuteScalarAsync(new SlonParameters { parameter }).AsTask());
    }

    [TestMethod]
    public async Task Prepare_ReportsUnresolvedParameterAmbiguity()
    {
        await using var command = AdoTestPool.CreateCommand("select $1 + $2");
        command.Parameters.Add(new SlonParameter(null));
        command.Parameters.Add(new SlonParameter(null));

        var exception = await Assert.ThrowsAsync<AggregateException>(async () => await command.PrepareAsync());

        Assert.IsInstanceOfType<PostgreSqlException>(exception.InnerException);
        Assert.AreEqual("42725", ((PostgreSqlException)exception.InnerException).SqlState);
        Assert.IsFalse(command.IsReadOnly);
    }

    [TestMethod]
    public async Task DisposeAsyncClosesDataSourcePreparedStatement()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(options => options with { PoolSize = 1 });
        var command = dataSource.CreateCommand("select 42");
        await command.PrepareAsync();

        Assert.AreEqual(1, await CountPreparedStatements(dataSource, "_dp"));

        await command.DisposeAsync();
        Assert.AreEqual(0, await CountPreparedStatements(dataSource, "_dp"));
    }

    [TestMethod]
    public async Task ExplicitPrepareDoesNotAdoptPriorAutoPreparation()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(options => options with
        {
            PoolSize = 1,
            MaxActiveAutoPreparations = 1,
            AutoPreparationMinimumUses = 1
        });
        var command = dataSource.CreateCommand("select 42");
        _ = await command.ExecuteScalarAsync();
        _ = await command.ExecuteScalarAsync();

        await command.PrepareAsync();
        Assert.AreEqual(1, await CountPreparedStatements(dataSource, "_ap"));
        Assert.AreEqual(1, await CountPreparedStatements(dataSource, "_dp"));

        await command.DisposeAsync();
        Assert.AreEqual(1, await CountPreparedStatements(dataSource, "_ap"));
        Assert.AreEqual(0, await CountPreparedStatements(dataSource, "_dp"));
    }

    [TestMethod]
    public async Task DisableAutoPreparationDoesNotDisableExplicitPrepare()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(options => options with
        {
            PoolSize = 1,
            MaxActiveAutoPreparations = 1,
            AutoPreparationMinimumUses = 1
        });
        var command = dataSource.CreateCommand("select 42");
        command.DisableAutoPreparation = true;

        _ = await command.ExecuteScalarAsync();
        _ = await command.ExecuteScalarAsync();
        Assert.AreEqual(0, await CountPreparedStatements(dataSource, "_ap"));

        await command.PrepareAsync();
        Assert.AreEqual(1, await CountPreparedStatements(dataSource, "_dp"));
        await command.DisposeAsync();
    }

    [TestMethod]
    public async Task AutoPreparationDoesNotTurnCommandIntoReusableTemplate()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(options => options with
        {
            PoolSize = 1,
            MaxActiveAutoPreparations = 1,
            AutoPreparationMinimumUses = 1
        });
        await using var command = dataSource.CreateCommand("select $1::int");
        command.Parameters.Add(42);
        _ = await command.ExecuteScalarAsync();
        _ = await command.ExecuteScalarAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => command.ExecuteScalarAsync(SlonParameters.Create(43)).AsTask());
        Assert.IsFalse(command.IsReadOnly);
    }

    [TestMethod]
    public void DataSourcePreparedTemplatesHaveDistinctNames()
    {
        using var tracker = new CommandTracker(maxAuto: 0, autoMinimumUses: 0);
        var descriptor = Slon.Pg.CommandDescriptor.Create("select 42");

        var first = tracker.Track(descriptor, owningInstance: new object()).Tracked;
        var second = tracker.Track(descriptor, owningInstance: new object()).Tracked;

        Assert.IsNotNull(first);
        Assert.IsNotNull(second);
        Assert.AreNotEqual(first.CommandName, second.CommandName);
    }

    [TestMethod]
    public async Task Prepare_MakesBatchCommandShapeImmutable()
    {
        await using var batch = AdoTestPool.CreateBatch();
        var command = batch.CreateBatchCommand("select 42");
        batch.BatchCommands.Add(command);

        await batch.PrepareAsync();

        Assert.IsTrue(batch.IsReadOnly);
        Assert.ThrowsExactly<InvalidOperationException>(() => command.CommandText = "select 43");
    }

    static async Task<int> CountPreparedStatements(SlonDataSource dataSource, string prefix)
    {
        await using var connection = (SlonConnection)await dataSource.OpenConnectionAsync();
        await using var command = new SlonCommand(connection,
            $"select count(*)::int from pg_prepared_statements where left(name, 3) = '{prefix}'")
        {
            DisableAutoPreparation = true
        };
        return (int)(await command.ExecuteScalarAsync())!;
    }
}
