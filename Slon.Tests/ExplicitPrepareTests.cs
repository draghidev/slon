namespace Slon.Tests;

[TestClass]
public class ExplicitPrepareTests
{
    [TestMethod]
    public async Task DisposeAsyncClosesOwnedPreparedStatement()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var connection = dataSource.CreateConnection();
        await connection.OpenAsync();
        var command = new SlonCommand(connection, "SELECT 1");

        await command.PrepareAsync();
        Assert.AreEqual(1, await CountExplicitStatements(connection));

        await command.DisposeAsync();
        Assert.AreEqual(0, await CountExplicitStatements(connection));
    }

    [TestMethod]
    public void DisposeClosesOwnedPreparedStatement()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var connection = dataSource.CreateConnection();
        connection.Open();
        var command = new SlonCommand(connection, "SELECT 1");

        command.Prepare();
        Assert.AreEqual(1, CountExplicitStatementsSync(connection));

        command.Dispose();
        Assert.AreEqual(0, CountExplicitStatementsSync(connection));
    }

    [TestMethod]
    public async Task FailedBatchPrepareClosesStatementsPreparedBeforeTheError()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var connection = dataSource.CreateConnection();
        await connection.OpenAsync();
        await using var batch = new SlonBatch(connection) { EnableErrorBarriers = true };
        batch.BatchCommands.Add(batch.CreateBatchCommand("SELECT 1"));
        batch.BatchCommands.Add(batch.CreateBatchCommand("THIS IS NOT SQL"));

        await Assert.ThrowsExactlyAsync<AggregateException>(() => batch.PrepareAsync());
        Assert.AreEqual(0, await CountExplicitStatements(connection));
    }

    static async Task<int> CountExplicitStatements(SlonConnection connection)
    {
        await using var command = new SlonCommand(connection,
            "SELECT count(*)::int FROM pg_prepared_statements WHERE left(name, 3) = '_cp'");
        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        return reader.GetInt32(0);
    }

    static int CountExplicitStatementsSync(SlonConnection connection)
    {
        using var command = new SlonCommand(connection,
            "SELECT count(*)::int FROM pg_prepared_statements WHERE left(name, 3) = '_cp'");
        using var reader = command.ExecuteReader();
        Assert.IsTrue(reader.Read());
        return reader.GetInt32(0);
    }
}
