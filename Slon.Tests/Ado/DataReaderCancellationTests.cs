using Slon.Tests.Pg;

namespace Slon.Tests.Ado;

[TestClass]
public class DataReaderCancellationTests : ConnectionCreatingTest
{
    [TestMethod]
    public async Task DisposeAsync_CancelsRemainingBatchWindowsAndReturnsUsableWire()
    {
        await using var blocker = await PgAdvisoryLock.AcquireAsync();
        await using var dataSource = AdoTestPool.NewIsolatedDataSource(o => o with
        {
            MaxPoolSize = 1
        });
        await using var batch = dataSource.CreateBatch();
        batch.EnableErrorBarriers = true;
        batch.BatchCommands.Add(batch.CreateBatchCommand("select pg_backend_pid()"));
        batch.BatchCommands.Add(batch.CreateBatchCommand($"select pg_advisory_xact_lock({blocker.Key})"));
        batch.BatchCommands.Add(batch.CreateBatchCommand($"select pg_advisory_xact_lock({blocker.Key})"));

        var reader = await batch.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        var processId = reader.GetInt32(0);
        await blocker.WaitUntilContendedAsync(processId);

        await reader.DisposeAsync();

        for (var attempt = 0; attempt < 3; attempt++)
        {
            await using var command = new SlonCommand(dataSource, "select 1");
            try
            {
                Assert.AreEqual(0, await command.ExecuteNonQueryAsync());
                return;
            }
            catch (SlonException ex) when (attempt < 2 && ex.IsTransient)
            {
                // A request unspent in its target window can strike two successors. Each RFQ consumes
                // one bounded attribution position; the following window must then be usable.
            }
        }
        Assert.Fail("The wire did not become usable after its bounded collateral cancellation windows.");
    }

    [TestMethod]
    public async Task Dispose_CancelsRemainingBatchWindowsAndReturnsUsableWire()
    {
        var iterations = StressEnv.Iterations(fallback: 1, cap: 5_000);
        if (iterations == 1)
        {
            await RunIteration();
            return;
        }

        await Parallel.ForEachAsync(Enumerable.Range(0, iterations),
            new ParallelOptions { MaxDegreeOfParallelism = 10 },
            static async (_, _) => await RunIteration());

        static async Task RunIteration()
        {
            await using var blocker = await PgAdvisoryLock.AcquireAsync();
            using var dataSource = AdoTestPool.NewIsolatedDataSource(o => o with
            {
                MaxPoolSize = 1
            });
            using var batch = dataSource.CreateBatch();
            batch.EnableErrorBarriers = true;
            batch.BatchCommands.Add(batch.CreateBatchCommand("select pg_backend_pid()"));
            batch.BatchCommands.Add(batch.CreateBatchCommand($"select pg_advisory_xact_lock({blocker.Key})"));
            batch.BatchCommands.Add(batch.CreateBatchCommand($"select pg_advisory_xact_lock({blocker.Key})"));

            var reader = batch.ExecuteReader();
            Assert.IsTrue(reader.Read());
            var processId = reader.GetInt32(0);
            await blocker.WaitUntilContendedAsync(processId);

            reader.Dispose();

            for (var attempt = 0; attempt < 3; attempt++)
            {
                using var command = new SlonCommand(dataSource, "select 1");
                try
                {
                    Assert.AreEqual(0, command.ExecuteNonQuery());
                    return;
                }
                catch (SlonException ex)
                {
                    if (attempt < 2 && ex.IsTransient)
                    {
                        // Each RFQ consumes one of the request's two successor positions.
                        continue;
                    }
                    Assert.Fail($"attempt={attempt}, transient={ex.IsTransient}, collateral={ex.IsCollateral}, " +
                                $"postgresCollateral={ex.PostgreSqlError?.IsCollateralCancellation}, " +
                                $"sqlState={ex.PostgreSqlError?.SqlState}");
                }
            }
            Assert.Fail("The wire did not become usable after its bounded collateral cancellation windows.");
        }
    }
}
