using System.Data;

namespace Slon.Tests.Ado.Serialization;

[TestClass]
public class FieldSerializationTests
{
    [TestMethod]
    public async Task TypeNamedAdoGettersProjectResolvedValues()
    {
        await using var command = AdoTestPool.CreateCommand(
            "select true, 12::int2, 42::int4, 84::int8, 1.5::float4, 2.5::float8, " +
            "'00112233-4455-6677-8899-aabbccddeeff'::uuid, 'value'::text");
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.IsTrue(reader.GetBoolean(0));
        Assert.AreEqual((short)12, reader.GetInt16(1));
        Assert.AreEqual(42, reader.GetInt32(2));
        Assert.AreEqual(84L, reader.GetInt64(3));
        Assert.AreEqual(1.5f, reader.GetFloat(4));
        Assert.AreEqual(2.5d, reader.GetDouble(5));
        Assert.AreEqual(Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"), reader.GetGuid(6));
        Assert.AreEqual("value", reader.GetString(7));
    }

    [TestMethod]
    public async Task ReaderProjectsFieldMetadataAndNullability()
    {
        await using var command = AdoTestPool.CreateCommand(
            "select 42::int4 as \"ExactName\", null::text as nullable_text, 'value'::text as value_text");
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        Assert.AreEqual("ExactName", reader.GetName(0));
        Assert.AreEqual(0, reader.GetOrdinal("ExactName"));
        Assert.AreEqual(0, reader.GetOrdinal("exactname"));
        Assert.ThrowsExactly<ArgumentException>(() => reader.GetOrdinal(""));
        Assert.ThrowsExactly<IndexOutOfRangeException>(() => reader.GetOrdinal("missing"));
        Assert.AreEqual(typeof(int), reader.GetFieldType(0));
        Assert.AreEqual(typeof(string), reader.GetFieldType(1));
        Assert.AreEqual("integer", reader.GetDataTypeName(0));
        Assert.AreEqual("text", reader.GetDataTypeName(1));

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(42, reader.GetValue(0));
        Assert.IsTrue(reader.IsDBNull(1));
        Assert.AreSame(DBNull.Value, reader.GetValue(1));
        Assert.AreSame(DBNull.Value,
            await reader.GetFieldValueAsync<object>(1, CancellationToken.None));
        Assert.IsFalse(await reader.IsDBNullAsync(2, CancellationToken.None));
        Assert.AreEqual("value", reader.GetString(2));
    }

    [TestMethod]
    public async Task GetValuesProjectsDefaultObjectsAndDbNull()
    {
        await using var command = AdoTestPool.CreateCommand(
            "select 42::int4, null::text, 'value'::text");
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        var values = new object[3];
        Assert.AreEqual(3, reader.GetValues(values));
        CollectionAssert.AreEqual(new object[] { 42, DBNull.Value, "value" }, values);
    }

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
        Assert.IsFalse(await reader.ReadAsync());
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

    [TestMethod]
    public async Task GetChars_ReusesBufferedLeaseAndSupportsRandomCharacterOffsets()
    {
        await using var command = AdoTestPool.CreateCommand("select 'aé日z'::text");
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(4, reader.GetChars(0, 0, null, 0, 0));
        var buffer = new char[2];
        Assert.AreEqual(2, reader.GetChars(0, 1, buffer, 0, buffer.Length));
        CollectionAssert.AreEqual(new[] { 'é', '日' }, buffer);
        Assert.AreEqual(2, reader.GetChars(0, 0, buffer, 0, buffer.Length));
        CollectionAssert.AreEqual(new[] { 'a', 'é' }, buffer);
    }

    [TestMethod]
    public async Task GetChars_SequentialLeaseStreamsAndRejectsCharacterRewind()
    {
        await using var command = AdoTestPool.CreateCommand("select 'aé日z'::text, 42::int4");
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        Assert.IsTrue(await reader.ReadAsync());
        var buffer = new char[2];
        Assert.AreEqual(2, reader.GetChars(0, 1, buffer, 0, buffer.Length));
        CollectionAssert.AreEqual(new[] { 'é', '日' }, buffer);
        Assert.ThrowsExactly<InvalidOperationException>(() =>
            reader.GetChars(0, 1, buffer, 0, buffer.Length));

        Assert.AreEqual(42, reader.GetInt32(1));
        Assert.ThrowsExactly<InvalidOperationException>(() =>
            reader.GetChars(0, 3, buffer, 0, buffer.Length));
    }

    [TestMethod]
    public async Task GetChars_ComposesOverJsonbVersionPrefix()
    {
        await using var command = AdoTestPool.CreateCommand("select '\"hello\"'::jsonb");
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        Assert.IsTrue(await reader.ReadAsync());
        var buffer = new char[5];
        Assert.AreEqual(5, reader.GetChars(0, 1, buffer, 0, buffer.Length));
        CollectionAssert.AreEqual("hello".ToCharArray(), buffer);
    }

    [TestMethod]
    public async Task GetChars_RejectsTypesWithoutCharacterProjection()
    {
        await using var command = AdoTestPool.CreateCommand("select 42::int4");
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        var buffer = new char[2];
        Assert.ThrowsExactly<InvalidCastException>(() =>
            reader.GetChars(0, 0, buffer, 0, buffer.Length));
    }

    [TestMethod]
    public async Task ReaderDisposal_RevokesActiveCharacterLeaseBeforeResultReuse()
    {
        await using var connection = await AdoTestPool.OpenConnectionAsync();
        await using (var command = connection.CreateCommand("select 'hello'::text"))
        await using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess))
        {
            Assert.IsTrue(await reader.ReadAsync());
            var buffer = new char[1];
            Assert.AreEqual(1, reader.GetChars(0, 0, buffer, 0, 1));
        }

        await using var nextCommand = connection.CreateCommand("select 42::int4");
        await using var nextReader = await nextCommand.ExecuteReaderAsync();
        Assert.IsTrue(await nextReader.ReadAsync());
        Assert.AreEqual(42, nextReader.GetInt32(0));
    }

}
