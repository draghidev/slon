namespace Slon.Tests;

using System.Data;
using System.Reflection;
using Slon.Pg.Protocol.Flows;

[TestClass]
public class ReaderDisposalTests
{
    const BindingFlags AllInstanceFields = BindingFlags.Instance
        | BindingFlags.Public | BindingFlags.NonPublic;

    [TestMethod]
    [DataRow(false, DisplayName = "sync")]
    [DataRow(true, DisplayName = "async")]
    public async Task FullyConsumedFinalResult_DoesNotCreateCancellationState(bool async)
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var command = dataSource.CreateCommand("select generate_series(1, 3)");
        var reader = async
            ? await command.ExecuteReaderAsync(CancellationToken.None)
            : command.ExecuteReader();
        var flow = GetFlow(reader);

        try
        {
            for (var expected = 1; expected <= 3; expected++)
            {
                Assert.IsTrue(async
                    ? await reader.ReadAsync(CancellationToken.None)
                    : reader.Read());
                Assert.AreEqual(expected, reader.GetInt32(0));
            }
            Assert.IsFalse(async
                ? await reader.ReadAsync(CancellationToken.None)
                : reader.Read());
            Assert.IsFalse(HasCancellationState(flow));
        }
        finally
        {
            if (async)
                await reader.DisposeAsync();
            else
                reader.Dispose();
        }
    }

    [TestMethod]
    [DataRow(false, DisplayName = "sync")]
    [DataRow(true, DisplayName = "async")]
    public async Task IncompleteFinalResult_DisposalCreatesCancellationState(bool async)
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var command = dataSource.CreateCommand("select generate_series(1, 100)");
        var reader = async
            ? await command.ExecuteReaderAsync(CancellationToken.None)
            : command.ExecuteReader();
        var flow = GetFlow(reader);

        Assert.IsTrue(async
            ? await reader.ReadAsync(CancellationToken.None)
            : reader.Read());
        if (async)
            await reader.DisposeAsync();
        else
            reader.Dispose();

        Assert.IsTrue(HasCancellationState(flow));
    }

    [TestMethod]
    [DataRow(false, DisplayName = "sync")]
    [DataRow(true, DisplayName = "async")]
    public async Task HiddenLaterResult_DisposalCreatesCancellationState(bool async)
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var batch = dataSource.CreateBatch();
        batch.BatchCommands.Add(batch.CreateBatchCommand("select 1"));
        batch.BatchCommands.Add(batch.CreateBatchCommand("select 2"));
        var reader = async
            ? await batch.ExecuteReaderAsync(CommandBehavior.SingleResult, CancellationToken.None)
            : batch.ExecuteReader(CommandBehavior.SingleResult);
        var flow = GetFlow(reader);

        Assert.IsTrue(async
            ? await reader.ReadAsync(CancellationToken.None)
            : reader.Read());
        Assert.IsFalse(async
            ? await reader.ReadAsync(CancellationToken.None)
            : reader.Read());

        if (async)
            await reader.DisposeAsync();
        else
            reader.Dispose();

        Assert.IsTrue(HasCancellationState(flow));
    }

    static CommandFlow GetFlow(SlonDataReader reader)
    {
        var enumerator = typeof(SlonDataReader).GetField("_enumerator", AllInstanceFields)!
            .GetValue(reader)!;
        var flowField = enumerator.GetType().GetFields(AllInstanceFields)
            .Single(static field => field.FieldType == typeof(CommandFlow));
        return (CommandFlow)flowField.GetValue(enumerator)!;
    }

    static bool HasCancellationState(CommandFlow flow)
        => FindField(flow.GetType(), "_cancellationState").GetValue(flow) is not null;

    static FieldInfo FindField(Type type, string name)
    {
        for (var current = type; current is not null; current = current.BaseType)
        {
            if (current.GetField(name, AllInstanceFields) is { } field)
                return field;
        }
        throw new MissingFieldException(type.FullName, name);
    }
}
