using Slon.Pg.Protocol;

namespace Slon.Tests;

// A failed command stores its ErrorResponse silently on the bare enumerator drain; the error only surfaces
// via GetCommandComplete. ExecuteNonQuery / ExecuteScalar drive the result directly (not via the reader,
// which already surfaces), so they must force that throw - otherwise a failed command silently reports 0
// affected / null. Stateless: runs on the MULTIPLEXED data-source command path; each test then runs a
// follow-up command to prove the wire recovered (an autocommit SQL error rolls back to Idle, no poison).
[TestClass]
[DoNotParallelize]
public class AdoErrorSurfacingTests
{
    const string Failing = "SELECT slon_no_such_column";

    static SlonCommand Failed() => AdoTestPool.CreateCommand(Failing);

    static void AssertUsable() => Assert.AreEqual(0, AdoTestPool.ExecuteNonQuery("SELECT 1"));

    [TestMethod]
    public void ExecuteNonQuery_FailedCommand_Throws()
    {
        using var cmd = Failed();
        Assert.ThrowsExactly<PostgresException>(() => cmd.ExecuteNonQuery());
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
}
