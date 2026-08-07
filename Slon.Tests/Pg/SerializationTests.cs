using System.Data;

namespace Slon.Tests.Pg;

[TestClass]
public class FieldSerializationTests
{
    [TestMethod]
    public async Task SequentialAccessStreamsFieldThroughSerializerReader()
    {
        const int length = 256 * 1024;
        await using var command = AdoTestPool.CreateCommand($"select repeat('x', {length}), 42::int4");
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        Assert.IsTrue(await reader.ReadAsync());
        var text = await reader.GetFieldValueAsync<string>(0);
        Assert.AreEqual(length, text.Length);
        Assert.AreEqual('x', text[0]);
        Assert.AreEqual('x', text[^1]);
        Assert.AreEqual(42, await reader.GetFieldValueAsync<int>(1));
    }
}
