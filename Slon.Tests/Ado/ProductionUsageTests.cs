namespace Slon.Tests;

[TestClass]
public class ProductionUsageTests
{
    [TestMethod]
    public async Task DataSourceCommands_ExecuteSynchronouslyAndAsynchronously()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();

        using (var command = dataSource.CreateCommand("select 41"))
            Assert.AreEqual(41, command.ExecuteScalar());

        await using (var command = dataSource.CreateCommand("select 42"))
            Assert.AreEqual(42, await command.ExecuteScalarAsync(CancellationToken.None));
    }

    [TestMethod]
    public void DataSourceBatch_ExposesEveryResultSynchronously()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var batch = dataSource.CreateBatch();
        batch.BatchCommands.Add(batch.CreateBatchCommand("select 1"));
        batch.BatchCommands.Add(batch.CreateBatchCommand("select 2"));
        batch.BatchCommands.Add(batch.CreateBatchCommand("select 3"));

        using var reader = batch.ExecuteReader();
        for (var expected = 1; expected <= 3; expected++)
        {
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(expected, reader.GetInt32(0));
            Assert.IsFalse(reader.Read());
            Assert.AreEqual(expected != 3, reader.NextResult());
        }
    }

    [TestMethod]
    public void SynchronousTransactions_CommitRollbackAndDispose()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var connection = dataSource.OpenConnection();
        using var command = connection.CreateCommand("create temp table production_usage (value int)");
        command.ExecuteNonQuery();

        using (var transaction = connection.BeginTransaction())
        {
            command.CommandText = "insert into production_usage values (1)";
            command.ExecuteNonQuery();
            transaction.Commit();
        }

        using (var transaction = connection.BeginTransaction())
        {
            command.CommandText = "insert into production_usage values (10)";
            command.ExecuteNonQuery();
            transaction.Rollback();
        }

        using (connection.BeginTransaction())
        {
            command.CommandText = "insert into production_usage values (100)";
            command.ExecuteNonQuery();
        }

        command.CommandText = "select sum(value)::int from production_usage";
        Assert.AreEqual(1, command.ExecuteScalar());
    }
}
