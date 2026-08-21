namespace Slon.Tests;

using System.Data;
using System.Data.Common;
using System.Reflection;

[TestClass]
public class DataReaderTests
{
    [TestMethod]
    public async Task ColumnSchema_ProjectsProtocolAndSerializerMetadata()
    {
        await using var command = AdoTestPool.CreateCommand("select 1::integer as value, 'x'::text as label");
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);

        var columns = reader.GetColumnSchema();
        Assert.HasCount(2, columns);
        Assert.AreEqual("value", columns[0].ColumnName);
        Assert.AreEqual(0, columns[0].ColumnOrdinal);
        Assert.AreEqual(typeof(int), columns[0].DataType);
        Assert.AreEqual("integer", columns[0].DataTypeName);
        Assert.AreEqual(SlonDbTypes.Int4, columns[0].SlonDbType);
        Assert.AreEqual("label", columns[1].ColumnName);
        Assert.AreEqual(typeof(string), columns[1].DataType);
        Assert.AreEqual("text", columns[1].DataTypeName);
        Assert.AreEqual(SlonDbTypes.Text, columns[1].SlonDbType);

        var providerIndependent = ((IDbColumnSchemaGenerator)reader).GetColumnSchema();
        Assert.HasCount(2, providerIndependent);
        Assert.IsInstanceOfType<SlonDbColumn>(providerIndependent[0]);

        var asyncColumns = await reader.GetColumnSchemaAsync(CancellationToken.None);
        Assert.HasCount(2, asyncColumns);

        var schemaTable = reader.GetSchemaTable();
        Assert.IsNotNull(schemaTable);
        Assert.HasCount(2, schemaTable.Rows);
        Assert.AreEqual(typeof(int), schemaTable.Rows[0]["ProviderSpecificDataType"]);
    }

    [TestMethod]
    public async Task EnumerateCommandResults_ExposesResultsWithoutRows()
    {
        await using var batch = AdoTestPool.CreateBatch();
        batch.BatchCommands.Add(batch.CreateBatchCommand("set application_name = 'slon-enumerate-command-results'"));
        batch.BatchCommands.Add(batch.CreateBatchCommand("select 42"));

        await using var reader = await batch.ExecuteReaderAsync((CommandBehavior)64,
            CancellationToken.None);
        Assert.AreEqual(0, reader.FieldCount);
        Assert.IsFalse(await reader.ReadAsync(CancellationToken.None));

        Assert.IsTrue(await reader.NextResultAsync(CancellationToken.None));
        Assert.AreEqual(1, reader.FieldCount);
        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        Assert.AreEqual(42, reader.GetInt32(0));
        Assert.IsFalse(await reader.ReadAsync(CancellationToken.None));
        Assert.IsFalse(await reader.NextResultAsync(CancellationToken.None));

        await using var reflectedBatch = AdoTestPool.CreateBatch();
        reflectedBatch.BatchCommands.Add(reflectedBatch.CreateBatchCommand("select 1"));
        reflectedBatch.BatchCommands.Add(reflectedBatch.CreateBatchCommand("set application_name = 'slon-reflected-command-results'"));
        reflectedBatch.BatchCommands.Add(reflectedBatch.CreateBatchCommand("select 2"));

        await using var reflectedReader = await reflectedBatch.ExecuteReaderAsync(CancellationToken.None);
        typeof(SlonDataReader).GetProperty("EnumerateCommandResults",
                BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(reflectedReader, true);
        Assert.IsTrue(await reflectedReader.ReadAsync(CancellationToken.None));
        Assert.AreEqual(1, reflectedReader.GetInt32(0));
        Assert.IsFalse(await reflectedReader.ReadAsync(CancellationToken.None));

        Assert.IsTrue(await reflectedReader.NextResultAsync(CancellationToken.None));
        Assert.AreEqual(0, reflectedReader.FieldCount);
        Assert.IsFalse(await reflectedReader.ReadAsync(CancellationToken.None));

        Assert.IsTrue(await reflectedReader.NextResultAsync(CancellationToken.None));
        Assert.IsTrue(await reflectedReader.ReadAsync(CancellationToken.None));
        Assert.AreEqual(2, reflectedReader.GetInt32(0));
    }

    [TestMethod]
    public async Task ConcurrentDataSourceBatches_ExposeEveryResult()
    {
        var tasks = Enumerable.Range(0, 16).Select(async operationId =>
        {
            await using var batch = AdoTestPool.CreateBatch();
            for (var i = 0; i < 3; i++)
                batch.BatchCommands.Add(batch.CreateBatchCommand($"select {operationId * 3 + i}"));

            await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
            for (var i = 0; i < 3; i++)
            {
                Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
                Assert.AreEqual(operationId * 3 + i, reader.GetInt32(0));
                Assert.IsFalse(await reader.ReadAsync(CancellationToken.None));
                Assert.AreEqual(i != 2, await reader.NextResultAsync(CancellationToken.None));
            }
        });

        await Task.WhenAll(tasks);
    }

    const int FirstLength = 128 * 1024;
    const int SecondLength = FirstLength + 1;
    static readonly string Query = $"SELECT repeat('x', {FirstLength}), 42 UNION ALL SELECT repeat('y', {SecondLength}), 43";

    [TestMethod]
    public async Task Async_LargeRowsContinueWithinTheirMessageBoundary()
    {
        await using var command = AdoTestPool.CreateCommand(Query);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);

        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        var first = await reader.GetFieldValueAsync<string>(0, CancellationToken.None);
        Assert.AreEqual(FirstLength, first.Length);
        Assert.AreEqual('x', first[0]);
        Assert.AreEqual(42, reader.GetInt32(1));

        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        var second = await reader.GetFieldValueAsync<string>(0, CancellationToken.None);
        Assert.AreEqual(SecondLength, second.Length);
        Assert.AreEqual('y', second[0]);
        Assert.AreEqual(43, reader.GetInt32(1));
        Assert.IsFalse(await reader.ReadAsync(CancellationToken.None));
    }

    [TestMethod]
    public void Sync_LargeRowContinuesWithinItsMessageBoundary()
    {
        using var command = AdoTestPool.CreateCommand(Query);
        using var reader = command.ExecuteReader();

        Assert.IsTrue(reader.Read());
        Assert.AreEqual(FirstLength, reader.GetString(0).Length);
        Assert.AreEqual(42, reader.GetInt32(1));
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(SecondLength, reader.GetString(0).Length);
        Assert.AreEqual(43, reader.GetInt32(1));
        Assert.IsFalse(reader.Read());
    }

    [TestMethod]
    public async Task SequentialAccess_EmitsPartialRowsThatCanStillUseBufferedAccess()
    {
        await using var command = AdoTestPool.CreateCommand(Query);
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, CancellationToken.None);

        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        Assert.AreEqual(FirstLength, reader.GetString(0).Length);
        Assert.AreEqual(42, reader.GetInt32(1));
        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        Assert.AreEqual(SecondLength, reader.GetString(0).Length);
        Assert.AreEqual(43, reader.GetInt32(1));
        Assert.IsFalse(await reader.ReadAsync(CancellationToken.None));
    }

    [TestMethod]
    public async Task ExecuteNonQuery_ReturnsRowsAffected_PerStatementType()
    {
        var t = "slon_ra_" + Guid.NewGuid().ToString("N");
        await AdoTestPool.ExecuteNonQueryAsync($"CREATE TABLE {t} (x int)");
        try
        {
            Assert.AreEqual(5, await AdoTestPool.ExecuteNonQueryAsync($"INSERT INTO {t} VALUES (1),(2),(3),(4),(5)"), "INSERT");
            Assert.AreEqual(5, await AdoTestPool.ExecuteNonQueryAsync($"UPDATE {t} SET x = x + 1"), "UPDATE");           // x -> 2..6
            Assert.AreEqual(2, await AdoTestPool.ExecuteNonQueryAsync($"DELETE FROM {t} WHERE x > 4"), "DELETE");        // 5,6
            Assert.AreEqual(0, await AdoTestPool.ExecuteNonQueryAsync($"UPDATE {t} SET x = 0 WHERE x > 100"), "UPDATE 0"); // affects none
        }
        finally
        {
            await AdoTestPool.ExecuteNonQueryAsync($"DROP TABLE {t}");
        }
    }

    [TestMethod]
    public async Task ExecuteNonQuery_NonDataModifying_IsZero()
    {
        // SELECT / DDL don't count toward RecordsAffected.
        Assert.AreEqual(-1, await AdoTestPool.ExecuteNonQueryAsync("SELECT 1"), "SELECT");
        Assert.AreEqual(-1, await AdoTestPool.ExecuteNonQueryAsync("SELECT generate_series(1, 10)"), "SELECT 10 rows");
    }

    [TestMethod]
    public async Task BatchExecuteNonQuery_SumsAllCommandResults()
    {
        var t = "slon_batch_ra_" + Guid.NewGuid().ToString("N");
        await using var connection = await AdoTestPool.OpenConnectionAsync();
        await using var setup = new SlonCommand(connection, $"CREATE TABLE {t} (x int)");
        await setup.ExecuteNonQueryAsync();
        try
        {
            await using var batch = connection.CreateBatch();
            batch.BatchCommands.Add(batch.CreateBatchCommand($"INSERT INTO {t} VALUES (1),(2),(3)"));
            batch.BatchCommands.Add(batch.CreateBatchCommand($"UPDATE {t} SET x = x + 1"));
            batch.BatchCommands.Add(batch.CreateBatchCommand("SELECT generate_series(1, 10)"));
            batch.BatchCommands.Add(batch.CreateBatchCommand($"DELETE FROM {t} WHERE x = 4"));

            Assert.AreEqual(7, await batch.ExecuteNonQueryAsync());
        }
        finally
        {
            setup.CommandText = $"DROP TABLE {t}";
            await setup.ExecuteNonQueryAsync();
        }
    }
}
