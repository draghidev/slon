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

    [TestMethod]
    public async Task GetBytes_ReusesBufferedColumnLeaseAndSupportsRandomAccess()
    {
        await using var command = AdoTestPool.CreateCommand(
            "select decode('00010203040506070809', 'hex')");
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(10, reader.GetBytes(0, 0, null, 0, 0));
        var buffer = new byte[4];
        Assert.AreEqual(4, reader.GetBytes(0, 4, buffer, 0, buffer.Length));
        CollectionAssert.AreEqual(new byte[] { 4, 5, 6, 7 }, buffer);
        Assert.AreEqual(2, reader.GetBytes(0, 1, buffer, 0, 2));
        CollectionAssert.AreEqual(new byte[] { 1, 2, 6, 7 }, buffer);
    }

    [TestMethod]
    public async Task GetBytes_SequentialLeaseRejectsRewindAndRevokesForLaterColumn()
    {
        await using var command = AdoTestPool.CreateCommand(
            "select decode('00010203040506070809', 'hex'), 42::int4");
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        Assert.IsTrue(await reader.ReadAsync());
        var buffer = new byte[3];
        Assert.AreEqual(3, reader.GetBytes(0, 2, buffer, 0, buffer.Length));
        CollectionAssert.AreEqual(new byte[] { 2, 3, 4 }, buffer);
        Assert.ThrowsExactly<InvalidOperationException>(() =>
            reader.GetBytes(0, 1, buffer, 0, buffer.Length));

        Assert.AreEqual(42, reader.GetInt32(1));
        Assert.ThrowsExactly<InvalidOperationException>(() =>
            reader.GetBytes(0, 5, buffer, 0, buffer.Length));
    }
}
