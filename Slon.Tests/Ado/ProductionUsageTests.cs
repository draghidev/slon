namespace Slon.Tests;

using System.Data;

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
    public async Task ExecuteNonQuery_ReturnsNegativeOneForSelect()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();

        using (var command = dataSource.CreateCommand("select 1"))
            Assert.AreEqual(-1, command.ExecuteNonQuery());

        await using (var command = dataSource.CreateCommand("select 1"))
            Assert.AreEqual(-1, await command.ExecuteNonQueryAsync(CancellationToken.None));
    }

    [TestMethod]
    public async Task SingleRow_DrainsRemainingRowsWhenEnumerationEnds()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();

        using (var command = dataSource.CreateCommand("select generate_series(1, 3)"))
        using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
        {
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32(0));
            Assert.IsFalse(reader.Read());
        }

        await using (var command = dataSource.CreateCommand("select generate_series(1, 3)"))
        await using (var reader = await command.ExecuteReaderAsync(
            CommandBehavior.SingleRow, CancellationToken.None))
        {
            Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
            Assert.AreEqual(1, reader.GetInt32(0));
            Assert.IsFalse(await reader.ReadAsync(CancellationToken.None));
        }
    }

    [TestMethod]
    public void Reader_PreservesRecordsAffectedAndHandlesPastEndByteOffset()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var command = dataSource.CreateCommand("select decode('0102', 'hex')");
        var reader = command.ExecuteReader();

        Assert.IsTrue(reader.Read());
        var buffer = new byte[2];
        Assert.AreEqual(0, reader.GetBytes(0, 2, buffer, 0, buffer.Length));
        Assert.IsFalse(reader.Read());
        reader.Close();
        Assert.AreEqual(-1, reader.RecordsAffected);
        reader.Dispose();
        Assert.AreEqual(-1, reader.RecordsAffected);
    }

    [TestMethod]
    public void HasRows_PrefetchesWithoutLosingTheFirstRow()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();

        using (var command = dataSource.CreateCommand("select value from (values (1), (2)) v(value)"))
        using (var reader = command.ExecuteReader())
        {
            Assert.IsTrue(reader.HasRows);
            Assert.IsTrue(reader.HasRows);
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32(0));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(2, reader.GetInt32(0));
            Assert.IsFalse(reader.Read());
        }

        using (var command = dataSource.CreateCommand("select 1 where false"))
        using (var reader = command.ExecuteReader())
        {
            Assert.IsFalse(reader.HasRows);
            Assert.IsFalse(reader.Read());
        }
    }

    [TestMethod]
    public async Task BufferedFieldOperations_RespectPreCanceledTokens()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var command = dataSource.CreateCommand("select 1");
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        var token = new CancellationToken(canceled: true);

        await Assert.ThrowsAsync<OperationCanceledException>(
            async () => await reader.GetFieldValueAsync<int>(0, token));
        await Assert.ThrowsAsync<OperationCanceledException>(
            async () => await reader.IsDBNullAsync(0, token));
    }

    [TestMethod]
    public void Reader_UsesAdoStateAndOrdinalExceptions()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();

        using (var command = dataSource.CreateCommand("select 1"))
        using (var reader = command.ExecuteReader())
        {
            Assert.ThrowsExactly<IndexOutOfRangeException>(() => reader.GetName(1));
            reader.Dispose();
            Assert.ThrowsExactly<ObjectDisposedException>(() => reader.Read());
            Assert.ThrowsExactly<ObjectDisposedException>(() => reader.NextResult());
            Assert.ThrowsExactly<ObjectDisposedException>(() => _ = reader.FieldCount);
        }

        using (var command = dataSource.CreateCommand("select 1"))
        using (var reader = command.ExecuteReader())
        {
            reader.Close();
            Assert.ThrowsExactly<InvalidOperationException>(() => reader.Read());
        }

        using (var setup = dataSource.CreateCommand("create temp table reader_no_columns (value int)"))
            setup.ExecuteNonQuery();

        using (var command = dataSource.CreateCommand("delete from reader_no_columns where false"))
        using (var reader = command.ExecuteReader())
        {
            // A command without columns has no metadata surface; its zero field count is not an ordinal range.
            Assert.ThrowsExactly<InvalidOperationException>(() => reader.GetName(0));
            Assert.ThrowsExactly<InvalidOperationException>(() => reader.GetFieldType(0));
            Assert.ThrowsExactly<InvalidOperationException>(() => reader.IsDBNull(0));
        }
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
    public void MutableBatch_PublishesPerCommandRecordsAffected()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var batch = dataSource.CreateBatch();
        var create = batch.CreateBatchCommand("create temp table batch_records_affected (value int)");
        var insert = batch.CreateBatchCommand("insert into batch_records_affected values (1), (2)");
        var select = batch.CreateBatchCommand("select * from batch_records_affected");
        batch.BatchCommands.Add(create);
        batch.BatchCommands.Add(insert);
        batch.BatchCommands.Add(select);

        using (var reader = batch.ExecuteReader())
            while (reader.NextResult()) { }

        Assert.AreEqual(0, create.RecordsAffected);
        Assert.AreEqual(2, insert.RecordsAffected);
        Assert.AreEqual(-1, select.RecordsAffected);
    }

    [TestMethod]
    public async Task MutableBatch_PublishesPerCommandRecordsAffectedAsynchronously()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var batch = dataSource.CreateBatch();
        var create = batch.CreateBatchCommand("create temp table async_batch_records_affected (value int)");
        var insert = batch.CreateBatchCommand("insert into async_batch_records_affected values (1), (2)");
        var select = batch.CreateBatchCommand("select * from async_batch_records_affected");
        batch.BatchCommands.Add(create);
        batch.BatchCommands.Add(insert);
        batch.BatchCommands.Add(select);

        await using (var reader = await batch.ExecuteReaderAsync(CancellationToken.None))
            while (await reader.NextResultAsync(CancellationToken.None)) { }

        Assert.AreEqual(0, create.RecordsAffected);
        Assert.AreEqual(2, insert.RecordsAffected);
        Assert.AreEqual(-1, select.RecordsAffected);
    }

    [TestMethod]
    public void PreparedBatch_DoesNotPublishExecutionStateToFrozenCommands()
    {
        using var batch = AdoTestPool.CreateBatch();
        var command = batch.CreateBatchCommand("select 1");
        batch.BatchCommands.Add(command);

        batch.ExecuteNonQuery();
        Assert.AreEqual(-1, command.RecordsAffected);

        batch.Prepare();
        Assert.AreEqual(0, command.RecordsAffected);
        batch.ExecuteNonQuery();
        Assert.AreEqual(0, command.RecordsAffected);
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
