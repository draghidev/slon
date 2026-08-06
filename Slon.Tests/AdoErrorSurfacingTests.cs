using Slon.Pg.Protocol;

namespace Slon.Tests;

// A failed command stores its ErrorResponse silently on the bare enumerator drain; the error only surfaces
// via GetCommandComplete. ExecuteNonQuery / ExecuteScalar drive the result directly (not via the reader,
// which already surfaces), so they must force that throw - otherwise a failed command silently reports 0
// affected / null. Stateless: runs on the MULTIPLEXED data-source command path; each test then runs a
// follow-up command to prove the wire recovered (an autocommit SQL error rolls back to Idle, no poison).
[TestClass]
public class AdoErrorSurfacingTests
{
    const string Failing = "SELECT slon_no_such_column";

    static SlonCommand Failed() => AdoTestPool.CreateCommand(Failing);

    static void AssertUsable() => Assert.AreEqual(0, AdoTestPool.ExecuteNonQuery("SELECT 1"));

    [TestMethod]
    public void ClientFailureProjection_DoesNotDuplicateInnerMessage()
    {
        const string causeMessage = "synthetic protocol cause";
        var cause = new PgProtocolException(causeMessage);
        var lowLevel = new PgClientException(cause);
        Assert.AreEqual(PgClientException.Summary, lowLevel.Message);
        Assert.AreSame(cause, lowLevel.InnerException);
        var projected = Assert.IsInstanceOfType<SlonException>(
            AdoException.Project(lowLevel));

        Assert.AreEqual(SlonExceptionKind.ClientFailure, projected.Kind);
        Assert.AreEqual(lowLevel.Message, projected.Message);
        Assert.AreSame(cause, projected.InnerException);
        Assert.IsFalse(projected.Message.Contains(causeMessage));
    }

    [TestMethod]
    public void ExecuteNonQuery_FailedCommand_Throws()
    {
        using var cmd = Failed();
        var exception = Assert.ThrowsExactly<PostgresException>(() => cmd.ExecuteNonQuery());
        Assert.AreEqual(SlonExceptionKind.PostgreSqlError, exception.Kind);
        Assert.AreEqual("42703", exception.SqlState);
        StringAssert.Contains(exception.MessageText, "slon_no_such_column");
        Assert.IsNull(exception.InnerException,
            "ADO replaces rather than nests the low-level server-error wrapper");
        AssertUsable();
    }

    [TestMethod]
    public async Task ExecuteNonQueryAsync_FailedCommand_Throws()
    {
        await using var cmd = Failed();
        await Assert.ThrowsExactlyAsync<PostgresException>(async () => await cmd.ExecuteNonQueryAsync(CancellationToken.None));
        AssertUsable();
    }

    [TestMethod]
    public async Task BatchExecuteNonQueryAsync_FailedCommand_Throws()
    {
        await using var connection = await AdoTestPool.OpenConnectionAsync();
        await using var batch = connection.CreateBatch();
        batch.BatchCommands.Add(batch.CreateBatchCommand("SELECT 1"));
        batch.BatchCommands.Add(batch.CreateBatchCommand(Failing));

        await Assert.ThrowsExactlyAsync<PostgresException>(async () => await batch.ExecuteNonQueryAsync(CancellationToken.None));
        await using var command = new SlonCommand(connection, "SELECT 1");
        Assert.AreEqual(0, await command.ExecuteNonQueryAsync());
    }

    [TestMethod]
    public void ExecuteScalar_FailedCommand_Throws()
    {
        using var cmd = Failed();
        Assert.ThrowsExactly<PostgresException>(() => cmd.ExecuteScalar());
        AssertUsable();
    }

    [TestMethod]
    public async Task ExecuteScalarAsync_FailedCommand_Throws()
    {
        await using var cmd = Failed();
        await Assert.ThrowsExactlyAsync<PostgresException>(async () => await cmd.ExecuteScalarAsync(CancellationToken.None));
        AssertUsable();
    }

    [TestMethod]
    public void ReaderRead_FailedSuccessor_Throws()
    {
        using (var connection = AdoTestPool.OpenConnection())
        using (var batch = CreateReaderBatch(connection))
        using (var reader = batch.ExecuteReader())
        {
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32(0));
            Assert.IsTrue(reader.NextResult());
            Assert.ThrowsExactly<PostgresException>(() => reader.Read());
        }

        AssertUsable();
    }

    [TestMethod]
    public async Task ReaderReadAsync_FailedSuccessor_Throws()
    {
        await using (var connection = await AdoTestPool.OpenConnectionAsync())
        await using (var batch = CreateReaderBatch(connection))
        await using (var reader = await batch.ExecuteReaderAsync())
        {
            Assert.IsTrue(await reader.ReadAsync());
            Assert.AreEqual(1, reader.GetInt32(0));
            Assert.IsTrue(await reader.NextResultAsync());
            await Assert.ThrowsExactlyAsync<PostgresException>(() => reader.ReadAsync());
        }

        AssertUsable();
    }

    [TestMethod]
    public void ReaderDispose_FailedSuccessor_Throws()
    {
        using (var connection = AdoTestPool.OpenConnection())
        using (var batch = CreateReaderBatch(connection))
        {
            var reader = batch.ExecuteReader();

            Assert.IsTrue(reader.Read());
            Assert.ThrowsExactly<PostgresException>(() => reader.Dispose());
        }
        AssertUsable();
    }

    [TestMethod]
    public async Task ReaderDisposeAsync_FailedSuccessor_Throws()
    {
        await using (var connection = await AdoTestPool.OpenConnectionAsync())
        await using (var batch = CreateReaderBatch(connection))
        {
            var reader = await batch.ExecuteReaderAsync();

            Assert.IsTrue(await reader.ReadAsync());
            await Assert.ThrowsExactlyAsync<PostgresException>(async () => await reader.DisposeAsync());
        }
        AssertUsable();
    }

    static SlonBatch CreateReaderBatch(SlonConnection connection)
    {
        var batch = connection.CreateBatch();
        batch.BatchCommands.Add(batch.CreateBatchCommand("SELECT 1"));
        batch.BatchCommands.Add(batch.CreateBatchCommand(Failing));
        return batch;
    }
}
