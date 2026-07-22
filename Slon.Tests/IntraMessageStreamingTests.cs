namespace Slon.Tests;

using System.Data;

[TestClass]
[DoNotParallelize]
public class IntraMessageStreamingTests
{
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
    }

}
