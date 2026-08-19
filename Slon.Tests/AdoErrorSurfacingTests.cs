using System.Buffers;
using System.Text;
using Slon.Pg.Protocol;
using Microsoft.Extensions.Time.Testing;

namespace Slon.Tests;

// A failed command stores its ErrorResponse silently on the bare enumerator drain; the error only surfaces
// via GetCommandComplete. ExecuteNonQuery / ExecuteScalar drive the result directly (not via the reader,
// which already surfaces), so they must force that throw - otherwise a failed command silently reports 0
// affected / null. Stateless: runs on the MULTIPLEXED data-source command path; each test then runs a
// follow-up command to prove the wire recovered (an autocommit SQL error rolls back to Idle, no poison).
[TestClass]
public class AdoErrorSurfacingTests
{
    const string Failing = "SELECT slon_no_such_column";

    static SlonCommand Failed() => AdoTestPool.CreateCommand(Failing);

    static void AssertUsable() => Assert.AreEqual(0, AdoTestPool.ExecuteNonQuery("SELECT 1"));

    [TestMethod]
    public void ClientProjection_DoesNotDuplicateInnerMessage()
    {
        const string causeMessage = "synthetic protocol cause";
        var cause = new PgProtocolException(causeMessage);
        var lowLevel = new PgClientException(cause);
        Assert.AreEqual(PgClientException.Summary, lowLevel.Message);
        Assert.AreSame(cause, lowLevel.InnerException);
        var projected = Assert.IsInstanceOfType<SlonException>(
            AdoException.Project(lowLevel));

        Assert.IsNull(projected.PostgreSqlError);
        Assert.AreEqual(lowLevel.Message, projected.Message);
        Assert.AreSame(cause, projected.InnerException);
        Assert.IsFalse(projected.Message.Contains(causeMessage));
    }

    [TestMethod]
    public void RawProtocolViolationProjection_IsClientAndPreservesOriginalException()
    {
        var lowLevel = new PgProtocolException("synthetic protocol violation");

        var projected = Assert.IsInstanceOfType<SlonException>(AdoException.Project(lowLevel));

        Assert.IsNull(projected.PostgreSqlError);
        Assert.AreSame(lowLevel, projected.InnerException);
        Assert.IsFalse(projected.IsTransient);
    }

    [TestMethod]
    public void ClosedProtocolProjection_IsClient()
    {
        var projected = Assert.IsInstanceOfType<SlonException>(
            AdoException.Project(new PgClientClosedException()));

        Assert.IsNull(projected.PostgreSqlError);
        Assert.IsFalse(projected.IsTransient);
    }

    [TestMethod]
    public void BackendTerminationProjection_IsPostgreSqlErrorAndNonTransient()
    {
        PgError error = ErrorOrNoticeMessage.FromFieldBlock(ErrorBlock(
            ('S', "FATAL"),
            ('V', "FATAL"),
            ('C', PgErrorCodes.AdminShutdown),
            ('M', "terminating connection due to administrator command")));
        var lowLevel = new PgCollateralException(
            PgCollateralSource.BackendTermination,
            new PgErrorException(error));

        var projected = Assert.IsInstanceOfType<SlonException>(AdoException.Project(lowLevel));

        Assert.IsTrue(projected.IsCollateral);
        Assert.IsFalse(projected.IsTransient);
        var backend = Assert.IsInstanceOfType<PostgreSqlException>(projected.InnerException);
        Assert.AreSame(backend, projected.PostgreSqlError);
        Assert.AreEqual(PgErrorCodes.AdminShutdown, backend.SqlState);
    }

    [TestMethod]
    public void ProtocolCondemnationProjection_IsClient()
    {
        var cause = new PgProtocolException("synthetic protocol failure");
        var lowLevel = new PgCollateralException(PgCollateralSource.ProtocolFailure, cause);

        var projected = Assert.IsInstanceOfType<SlonException>(AdoException.Project(lowLevel));

        Assert.IsNull(projected.PostgreSqlError);
        Assert.IsTrue(projected.IsCollateral);
        var projectedCause = Assert.IsInstanceOfType<SlonException>(projected.InnerException);
        Assert.IsNull(projectedCause.PostgreSqlError);
        Assert.AreSame(cause, projectedCause.InnerException);
        Assert.IsFalse(projected.IsTransient);
    }

    [TestMethod]
    public void ExecuteNonQuery_FailedCommand_Throws()
    {
        using var cmd = Failed();
        var exception = Assert.ThrowsExactly<PostgreSqlException>(() => cmd.ExecuteNonQuery());
        Assert.AreSame(exception, exception.PostgreSqlError);
        Assert.IsFalse(exception.IsCollateral);
        Assert.AreEqual("42703", exception.SqlState);
        StringAssert.Contains(exception.MessageText, "slon_no_such_column");
        Assert.IsNull(exception.InnerException,
            "ADO replaces rather than nests the low-level server-error wrapper");
        AssertUsable();
    }

    [TestMethod]
    public async Task ExecuteNonQueryAsync_FailedCommand_Throws()
    {
        await using var cmd = Failed();
        await Assert.ThrowsExactlyAsync<PostgreSqlException>(async () => await cmd.ExecuteNonQueryAsync(CancellationToken.None));
        AssertUsable();
    }

    [TestMethod]
    public async Task BatchExecuteNonQueryAsync_FailedCommand_Throws()
    {
        await using var connection = await AdoTestPool.OpenConnectionAsync();
        await using var batch = connection.CreateBatch();
        batch.BatchCommands.Add(batch.CreateBatchCommand("SELECT 1"));
        batch.BatchCommands.Add(batch.CreateBatchCommand(Failing));

        await Assert.ThrowsExactlyAsync<PostgreSqlException>(async () => await batch.ExecuteNonQueryAsync(CancellationToken.None));
        await using var command = new SlonCommand(connection, "SELECT 1");
        Assert.AreEqual(0, await command.ExecuteNonQueryAsync());
    }

    [TestMethod]
    public void ExecuteScalar_FailedCommand_Throws()
    {
        using var cmd = Failed();
        Assert.ThrowsExactly<PostgreSqlException>(() => cmd.ExecuteScalar());
        AssertUsable();
    }

    [TestMethod]
    public async Task ExecuteScalarAsync_FailedCommand_Throws()
    {
        await using var cmd = Failed();
        await Assert.ThrowsExactlyAsync<PostgreSqlException>(async () => await cmd.ExecuteScalarAsync(CancellationToken.None));
        AssertUsable();
    }

    [TestMethod]
    public void ReaderRead_FailedSuccessor_Throws()
    {
        using (var connection = AdoTestPool.OpenConnection())
        using (var batch = CreateReaderBatch(connection))
        using (var reader = batch.ExecuteReader())
        {
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32(0));
            Assert.IsTrue(reader.NextResult());
            Assert.ThrowsExactly<PostgreSqlException>(() => reader.Read());
        }

        AssertUsable();
    }

    [TestMethod]
    public async Task ReaderReadAsync_FailedSuccessor_Throws()
    {
        await using (var connection = await AdoTestPool.OpenConnectionAsync())
        await using (var batch = CreateReaderBatch(connection))
        await using (var reader = await batch.ExecuteReaderAsync())
        {
            Assert.IsTrue(await reader.ReadAsync());
            Assert.AreEqual(1, reader.GetInt32(0));
            Assert.IsTrue(await reader.NextResultAsync());
            await Assert.ThrowsExactlyAsync<PostgreSqlException>(() => reader.ReadAsync());
        }

        AssertUsable();
    }

    [TestMethod]
    public void ErrorBarrierBatch_CanContinueAfterObservedError()
    {
        using var batch = CreateErrorBarrierBatch();
        using var reader = batch.ExecuteReader();

        Assert.IsTrue(reader.Read());
        Assert.AreEqual(1, reader.GetInt32(0));
        Assert.IsTrue(reader.NextResult());
        Assert.ThrowsExactly<PostgreSqlException>(() => reader.Read());
        Assert.IsTrue(reader.NextResult());
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(3, reader.GetInt32(0));
        Assert.IsFalse(reader.NextResult());
    }

    [TestMethod]
    public async Task ErrorBarrierBatchAsync_CanContinueAfterObservedError()
    {
        await using var batch = CreateErrorBarrierBatch();
        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(1, reader.GetInt32(0));
        Assert.IsTrue(await reader.NextResultAsync());
        await Assert.ThrowsExactlyAsync<PostgreSqlException>(() => reader.ReadAsync());
        Assert.IsTrue(await reader.NextResultAsync());
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(3, reader.GetInt32(0));
        Assert.IsFalse(await reader.NextResultAsync());
    }

    [TestMethod]
    public void ErrorBarrierBatch_CanContinueAfterSkippingFailedResult()
    {
        using var batch = CreateErrorBarrierBatch();
        using var reader = batch.ExecuteReader();

        Assert.IsTrue(reader.Read());
        Assert.IsTrue(reader.NextResult());
        Assert.ThrowsExactly<PostgreSqlException>(() => reader.NextResult());
        Assert.IsTrue(reader.NextResult());
        Assert.IsTrue(reader.Read());
        Assert.AreEqual(3, reader.GetInt32(0));
    }

    [TestMethod]
    public async Task ErrorBarrierBatchAsync_CanContinueAfterSkippingFailedResult()
    {
        await using var batch = CreateErrorBarrierBatch();
        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);

        Assert.IsTrue(await reader.ReadAsync());
        Assert.IsTrue(await reader.NextResultAsync());
        await Assert.ThrowsExactlyAsync<PostgreSqlException>(() => reader.NextResultAsync());
        Assert.IsTrue(await reader.NextResultAsync());
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(3, reader.GetInt32(0));
    }

    [TestMethod]
    public void ReaderDispose_FailedSuccessor_Throws()
    {
        using (var connection = AdoTestPool.OpenConnection())
        using (var batch = CreateReaderBatch(connection))
        {
            var reader = batch.ExecuteReader();

            Assert.IsTrue(reader.Read());
            Assert.ThrowsExactly<PostgreSqlException>(() => reader.Dispose());
        }
        AssertUsable();
    }

    [TestMethod]
    public async Task ReaderDispose_FailedSuccessor_ReleasesScopeBeforeDatasourceSuccessor_Stress()
    {
        var iters = Pg.StressEnv.Iterations(fallback: 8, cap: 8_000);
        var time = new FakeTimeProvider();
        using var dataSource = AdoTestPool.NewIsolatedDataSource(
            options => options with
            {
                MaxPoolSize = 1,
                ConnectionTimeout = Timeout.InfiniteTimeSpan,
                TimeProvider = time
            });
        var workers = Math.Min(4, iters);
        var tasks = new Task[workers];
        for (var worker = 0; worker < workers; worker++)
        {
            var start = worker;
            tasks[worker] = Task.Factory.StartNew(() =>
            {
                for (var i = start; i < iters; i += workers)
                {
                    using (var connection = dataSource.OpenConnection())
                    using (var batch = CreateReaderBatch(connection))
                    {
                        var reader = batch.ExecuteReader();
                        Assert.IsTrue(reader.Read(), $"iter {i}: first row was not delivered");
                        if ((i & 1) == 0)
                        {
                            Assert.ThrowsExactly<PostgreSqlException>(reader.Dispose,
                                $"iter {i}: disposal did not surface the unread successor error");
                        }
                        else
                        {
                            Assert.IsTrue(reader.NextResult(), $"iter {i}: failed successor was not exposed");
                            Assert.ThrowsExactly<PostgreSqlException>(() => reader.Read(),
                                $"iter {i}: reading the failed successor did not surface its error");
                            reader.Dispose();
                        }
                    }

                    using var command = new SlonCommand(dataSource, "select 1")
                    {
                        PendingTimeout = Timeout.InfiniteTimeSpan
                    };
                    Assert.AreEqual(0, command.ExecuteNonQuery(), $"iter {i}: datasource successor failed");
                }
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }
        await Task.WhenAll(tasks);
    }

    [TestMethod]
    public async Task ReaderDisposeAsync_FailedSuccessor_Throws()
    {
        await using (var connection = await AdoTestPool.OpenConnectionAsync())
        await using (var batch = CreateReaderBatch(connection))
        {
            var reader = await batch.ExecuteReaderAsync();

            Assert.IsTrue(await reader.ReadAsync());
            await Assert.ThrowsExactlyAsync<PostgreSqlException>(async () => await reader.DisposeAsync());
        }
        AssertUsable();
    }

    static SlonBatch CreateReaderBatch(SlonConnection connection)
    {
        var batch = connection.CreateBatch();
        batch.BatchCommands.Add(batch.CreateBatchCommand("SELECT 1"));
        batch.BatchCommands.Add(batch.CreateBatchCommand(Failing));
        return batch;
    }

    static SlonBatch CreateErrorBarrierBatch()
    {
        var batch = AdoTestPool.CreateBatch();
        batch.EnableErrorBarriers = true;
        batch.BatchCommands.Add(batch.CreateBatchCommand("SELECT 1"));
        batch.BatchCommands.Add(batch.CreateBatchCommand(Failing));
        batch.BatchCommands.Add(batch.CreateBatchCommand("SELECT 3"));
        return batch;
    }

    static ReadOnlySequence<byte> ErrorBlock(params (char Type, string Value)[] fields)
    {
        using var stream = new MemoryStream();
        foreach (var (type, value) in fields)
        {
            stream.WriteByte((byte)type);
            stream.Write(Encoding.UTF8.GetBytes(value));
            stream.WriteByte(0);
        }
        stream.WriteByte(0);
        return new ReadOnlySequence<byte>(stream.ToArray());
    }
}
