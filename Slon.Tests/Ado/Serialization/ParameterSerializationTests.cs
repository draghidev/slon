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

}
