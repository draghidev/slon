namespace Slon.Tests.Ado.Serialization;

[TestClass]
public class ParameterSerializationTests
{
    [TestMethod]
    public async Task ParametersUseCapturedSerializerMappings()
    {
        await using var command = AdoTestPool.CreateCommand(
            "select ($1::int4 = 42 and $2::bool and $3::float8 = 12.5)::bool");
        command.Parameters.Add(42);
        command.Parameters.Add(true);
        command.Parameters.Add(12.5d);

        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.IsTrue(reader.GetBoolean(0));
    }

    [TestMethod]
    public async Task SeekableStreamParameterFlushesWithinBindAsync()
    {
        const int length = 256 * 1024;
        using var stream = CreatePubliclyVisibleStream(length);
        await using var command = AdoTestPool.CreateCommand($"select octet_length($1::bytea) = {length}");
        command.Parameters.Add(stream);

        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.IsTrue(reader.GetBoolean(0));
        Assert.AreEqual(length, stream.Position);
    }

    [TestMethod]
    public void SeekableStreamParameterFlushesWithinResumableSyncBind()
    {
        const int length = 256 * 1024;
        using var stream = CreatePubliclyVisibleStream(length);
        using var command = AdoTestPool.CreateCommand($"select octet_length($1::bytea) = {length}");
        command.Parameters.Add(stream);

        using var reader = command.ExecuteReader();
        Assert.IsTrue(reader.Read());
        Assert.IsTrue(reader.GetBoolean(0));
        Assert.AreEqual(length, stream.Position);
    }

    [TestMethod]
    public async Task MultipleStreamParametersResumeAcrossBindFlushes()
    {
        const int length = 128 * 1024;
        using var first = CreatePubliclyVisibleStream(length);
        using var second = CreatePubliclyVisibleStream(length + 1);
        await using var command = AdoTestPool.CreateCommand(
            $"select octet_length($1::bytea) = {length} and octet_length($2::bytea) = {length + 1}");
        command.Parameters.Add(first);
        command.Parameters.Add(second);

        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.IsTrue(reader.GetBoolean(0));
        Assert.AreEqual(length, first.Position);
        Assert.AreEqual(length + 1, second.Position);
    }

    static byte[] CreatePayload(int length)
    {
        var payload = new byte[length];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)i;
        return payload;
    }

    static MemoryStream CreatePubliclyVisibleStream(int length)
        => new(CreatePayload(length), 0, length, writable: false, publiclyVisible: true);

}
