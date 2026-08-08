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

    [TestMethod]
    public async Task FailedStreamParameterWriteBreaksLeaseButPhysicalConnectionRecovers()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = "select octet_length($1::bytea)";
                command.Parameters.Add(
                    new SlonParameter<Stream>(new ThrowingReadStream(256 * 1024, 64 * 1024)));
                var exception = await Assert.ThrowsExactlyAsync<SlonException>(
                    () => command.ExecuteNonQueryAsync());
                Assert.AreEqual(SlonExceptionKind.ClientFailure, exception.Kind);
            }
            Assert.AreEqual(System.Data.ConnectionState.Broken, connection.State);
        }

        await using var recovered = await dataSource.OpenConnectionAsync();
        await using var next = recovered.CreateCommand();
        next.CommandText = "select 42::int4";
        _ = await next.ExecuteNonQueryAsync();
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

    sealed class ThrowingReadStream(int length, int throwAfter) : Stream
    {
        int _position;

        public override int Read(Span<byte> buffer)
        {
            if (_position >= throwAfter)
                throw new IOException("Synthetic parameter read failure.");
            var count = Math.Min(buffer.Length, throwAfter - _position);
            buffer.Slice(0, count).Clear();
            _position += count;
            return count;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            try { return new(Read(buffer.Span)); }
            catch (Exception ex) { return ValueTask.FromException<int>(ex); }
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count,
            CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override int Read(byte[] buffer, int offset, int count)
            => Read(buffer.AsSpan(offset, count));
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => length;
        public override long Position { get => _position; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

}
