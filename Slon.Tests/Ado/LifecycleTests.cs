namespace Slon.Tests;

using System.Data;

[TestClass]
public class LifecycleTests
{
    [TestMethod]
    public void CommandsAndBatches_RejectUseAfterDispose()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();

        var command = dataSource.CreateCommand("select 1");
        command.Dispose();
        Assert.ThrowsExactly<ObjectDisposedException>(() => command.ExecuteScalar());
        Assert.ThrowsExactly<ObjectDisposedException>(() => command.CommandText = "select 2");

        var batch = dataSource.CreateBatch();
        batch.BatchCommands.Add(batch.CreateBatchCommand("select 1"));
        var commands = batch.BatchCommands;
        batch.Dispose();
        Assert.ThrowsExactly<ObjectDisposedException>(() => batch.ExecuteScalar());
        Assert.ThrowsExactly<ObjectDisposedException>(() =>
            commands.Add(batch.CreateBatchCommand("select 2")));
    }

    [TestMethod]
    public async Task Transactions_RejectCompletionAfterCommitRollbackOrDispose()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var connection = await dataSource.OpenConnectionAsync(CancellationToken.None);

        var committed = connection.BeginTransaction();
        committed.Commit();
        Assert.IsNull(committed.Connection);
        Assert.ThrowsExactly<InvalidOperationException>(() => committed.Rollback());
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committed.CommitAsync());
        committed.Dispose();

        var rolledBack = connection.BeginTransaction();
        rolledBack.Rollback();
        Assert.IsNull(rolledBack.Connection);
        Assert.ThrowsExactly<InvalidOperationException>(() => rolledBack.Commit());
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => rolledBack.RollbackAsync());
        await rolledBack.DisposeAsync();

        var disposed = connection.BeginTransaction();
        await disposed.DisposeAsync();
        Assert.IsNull(disposed.Connection);
        Assert.ThrowsExactly<InvalidOperationException>(() => disposed.Rollback());
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => disposed.CommitAsync());
    }

    [TestMethod]
    public void ConnectionClose_InvalidatesTransactionAndPermitsTransactionAfterReopen()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var connection = dataSource.OpenConnection();
        var transaction = connection.BeginTransaction();
        using (var command = connection.CreateCommand("select 1"))
            command.ExecuteNonQuery();

        connection.Close();
        Assert.IsNull(transaction.Connection);
        Assert.ThrowsExactly<InvalidOperationException>(() => transaction.Rollback());

        connection.Open();
        using var successor = connection.BeginTransaction();
        successor.Rollback();
    }

    [TestMethod]
    public async Task ConnectionDispose_RollsBackActiveTransaction()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        var table = "lifecycle_connection_dispose_" + Guid.NewGuid().ToString("N");
        await using var outside = dataSource.CreateCommand($"create table {table} (value int)");
        await outside.ExecuteNonQueryAsync(CancellationToken.None);
        try
        {
            var connection = await dataSource.OpenConnectionAsync(CancellationToken.None);
            var transaction = await connection.BeginTransactionAsync(CancellationToken.None);
            await using (var command = connection.CreateCommand($"insert into {table} values (1)"))
                await command.ExecuteNonQueryAsync(CancellationToken.None);

            await connection.DisposeAsync();
            Assert.IsNull(transaction.Connection);
            await transaction.DisposeAsync();

            outside.CommandText = $"select count(*)::int from {table}";
            Assert.AreEqual(0, await outside.ExecuteScalarAsync(CancellationToken.None));
        }
        finally
        {
            outside.CommandText = $"drop table {table}";
            await outside.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    [TestMethod]
    public void ReaderAndCommandDisposal_DrainBeforeTransactionCommit()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var connection = dataSource.OpenConnection();
        using var setup = connection.CreateCommand("create temp table lifecycle_commit (value int)");
        setup.ExecuteNonQuery();

        using (var transaction = connection.BeginTransaction())
        {
            var command = connection.CreateCommand(
                "insert into lifecycle_commit values (1), (2), (3) returning value");
            var reader = command.ExecuteReader();
            command.Dispose();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32(0));
            reader.Dispose();
            reader.Dispose();
            Assert.ThrowsExactly<ObjectDisposedException>(() => reader.Read());
            transaction.Commit();
        }

        setup.CommandText = "select count(*)::int from lifecycle_commit";
        Assert.AreEqual(3, setup.ExecuteScalar());
    }

    [TestMethod]
    public async Task ReaderAndTransactionDisposal_RollBackAndLeaveConnectionUsable()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var connection = await dataSource.OpenConnectionAsync(CancellationToken.None);
        await using var command = connection.CreateCommand("create temp table lifecycle_rollback (value int)");
        await command.ExecuteNonQueryAsync(CancellationToken.None);

        await using (var transaction = await connection.BeginTransactionAsync(CancellationToken.None))
        {
            command.CommandText = "insert into lifecycle_rollback values (1), (2), (3) returning value";
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        }

        command.CommandText = "select count(*)::int from lifecycle_rollback";
        Assert.AreEqual(0, await command.ExecuteScalarAsync(CancellationToken.None));
    }

    [TestMethod]
    public async Task FailedTransaction_CanRollBackAndReuseConnection()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var connection = await dataSource.OpenConnectionAsync(CancellationToken.None);
        await using var transaction = await connection.BeginTransactionAsync(CancellationToken.None);
        await using var command = connection.CreateCommand("select * from lifecycle_missing_relation");

        await Assert.ThrowsExactlyAsync<PostgreSqlException>(
            () => command.ExecuteNonQueryAsync(CancellationToken.None));
        await transaction.RollbackAsync(CancellationToken.None);

        command.CommandText = "select 42";
        Assert.AreEqual(42, await command.ExecuteScalarAsync(CancellationToken.None));
    }

    [TestMethod]
    public async Task CloseConnectionBehavior_ClosesWhenReaderIsDisposed()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var connection = await dataSource.OpenConnectionAsync(CancellationToken.None);
        await using var command = connection.CreateCommand("select generate_series(1, 3)");
        var reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, CancellationToken.None);

        Assert.IsTrue(await reader.ReadAsync(CancellationToken.None));
        Assert.AreEqual(ConnectionState.Open, connection.State);
        await reader.DisposeAsync();
        Assert.AreEqual(ConnectionState.Closed, connection.State);

        await connection.OpenAsync(CancellationToken.None);
        command.CommandText = "select 42";
        Assert.AreEqual(42, await command.ExecuteScalarAsync(CancellationToken.None));
    }

    [TestMethod]
    public void CloseConnectionBehavior_ClosesWhenReaderIsDisposedSynchronously()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var connection = dataSource.OpenConnection();
        using var command = connection.CreateCommand("select generate_series(1, 3)");
        var reader = command.ExecuteReader(CommandBehavior.CloseConnection);

        Assert.IsTrue(reader.Read());
        Assert.AreEqual(ConnectionState.Open, connection.State);
        reader.Dispose();
        Assert.AreEqual(ConnectionState.Closed, connection.State);
    }
}
