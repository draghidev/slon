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

    [TestMethod]
    public async Task SequentialAccess_ReadingLaterColumnRevokesStream()
    {
        const int length = 256 * 1024;
        await using var command = AdoTestPool.CreateCommand(
            $"select decode(repeat('ab', {length}), 'hex'), 42::int4");
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        Assert.IsTrue(await reader.ReadAsync());
        var stream = reader.GetStream(0);
        Assert.AreEqual(0xab, stream.ReadByte());

        Assert.AreEqual(42, await reader.GetFieldValueAsync<int>(1));
        Assert.ThrowsExactly<ObjectDisposedException>(() => stream.ReadByte());
    }

    [TestMethod]
    public async Task SequentialAccess_ReadingLaterColumnRevokesTextReaderBuffer()
    {
        const int length = 256 * 1024;
        await using var command = AdoTestPool.CreateCommand(
            $"select repeat('x', {length}), 42::int4");
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        Assert.IsTrue(await reader.ReadAsync());
        var textReader = reader.GetTextReader(0);
        Assert.AreEqual('x', textReader.Read());

        Assert.AreEqual(42, await reader.GetFieldValueAsync<int>(1));
        Assert.ThrowsExactly<ObjectDisposedException>(() => textReader.Read());
    }

    [TestMethod]
    public async Task SequentialAccess_AdvancingRowRevokesStream()
    {
        const int length = 256 * 1024;
        await using var command = AdoTestPool.CreateCommand(
            $"select decode(repeat('ab', {length}), 'hex') from generate_series(1, 2)");
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        Assert.IsTrue(await reader.ReadAsync());
        var stream = reader.GetStream(0);
        Assert.AreEqual(0xab, stream.ReadByte());

        Assert.IsTrue(await reader.ReadAsync());
        Assert.ThrowsExactly<ObjectDisposedException>(() => stream.ReadByte());
    }
}
