using Slon.Pg.Protocol;

namespace Slon.Tests;

// A failed command stores its ErrorResponse silently on the bare enumerator drain; the error only
// surfaces via GetCommandComplete. ExecuteNonQuery / ExecuteScalar drive the result directly (not via
// the reader, which already surfaces), so they must force that throw - otherwise a failed command
// silently reports 0 affected / null. Each test also runs a follow-up command on the same connection to
// prove the wire still drained to RFQ and the connection stays usable after the error.
[TestClass]
[DoNotParallelize]
public class AdoErrorSurfacingTests
{
    const string Failing = "SELECT slon_no_such_column";

    static async Task AssertUsable(SlonConnection conn)
    {
        await using var cmd = new SlonCommand(conn, "SELECT 1");
        Assert.AreEqual(0, await cmd.ExecuteNonQueryAsync(CancellationToken.None));
    }

    [TestMethod]
    public async Task ExecuteNonQuery_FailedCommand_Throws()
    {
        await using var conn = await AdoTestPool.OpenConnectionAsync();
        using var cmd = new SlonCommand(conn, Failing);
        Assert.ThrowsExactly<PostgresException>(() => cmd.ExecuteNonQuery());
        await AssertUsable(conn);
    }

    [TestMethod]
    public async Task ExecuteNonQueryAsync_FailedCommand_Throws()
    {
        await using var conn = await AdoTestPool.OpenConnectionAsync();
        await using var cmd = new SlonCommand(conn, Failing);
        await Assert.ThrowsExactlyAsync<PostgresException>(async () => await cmd.ExecuteNonQueryAsync(CancellationToken.None));
        await AssertUsable(conn);
    }

    [TestMethod]
    public async Task ExecuteScalar_FailedCommand_Throws()
    {
        await using var conn = await AdoTestPool.OpenConnectionAsync();
        using var cmd = new SlonCommand(conn, Failing);
        Assert.ThrowsExactly<PostgresException>(() => cmd.ExecuteScalar());
        await AssertUsable(conn);
    }

    [TestMethod]
    public async Task ExecuteScalarAsync_FailedCommand_Throws()
    {
        await using var conn = await AdoTestPool.OpenConnectionAsync();
        await using var cmd = new SlonCommand(conn, Failing);
        await Assert.ThrowsExactlyAsync<PostgresException>(async () => await cmd.ExecuteScalarAsync(CancellationToken.None));
        await AssertUsable(conn);
    }
}
