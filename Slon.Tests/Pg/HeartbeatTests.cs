using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Threading;

namespace Slon.Tests.Pg;

[TestClass]
public class HeartbeatTests
{
    [TestMethod]
    public async Task BackloggedFlow_ActivationTimeoutAdvancesBeforeDispatch()
    {
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol, protocol.FlowControl);
        var flow = new CommandFlow(async: true, Command.Create("select 1"));
        var control = flow.GetExecutionControl(protocol.FlowControl);

        source.Enqueue(flow, activationTimeout: TimeSpan.FromSeconds(2));
        var activation = control.GetDecoderTask(CancellationToken.None);

        source.OnActivationHeartbeat(TimeSpan.FromSeconds(1));
        Assert.IsFalse(activation.IsCompleted);
        source.OnActivationHeartbeat(TimeSpan.FromSeconds(1));

        await Assert.ThrowsExactlyAsync<TimeoutException>(async () => await activation);
    }

    [TestMethod]
    public async Task BackloggedFlow_PendingTimeoutOverridesProtocolActivationDefault()
    {
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(PgTestPool.NewOptions()));
        var source = PgClientFlowSource.Create(protocol, protocol.FlowControl);
        var flow = new CommandFlow(async: true, new CommandFlowOptions
        {
            Commands = new(Command.Create("select 1")),
            PendingTimeout = TimeSpan.FromSeconds(2)
        });
        var control = flow.GetExecutionControl(protocol.FlowControl);

        source.Enqueue(flow, activationTimeout: TimeSpan.FromMinutes(1));
        var activation = control.GetDecoderTask(CancellationToken.None);

        source.OnActivationHeartbeat(TimeSpan.FromSeconds(2));

        await Assert.ThrowsExactlyAsync<TimeoutException>(async () => await activation);
    }

    [TestMethod]
    public void RegisterAfterDispose_IsRejected()
    {
        var heartbeat = new Heartbeat(TimeSpan.FromHours(1));
        heartbeat.Dispose();

        Assert.ThrowsExactly<ObjectDisposedException>(
            () => heartbeat.Register(static _ => ValueTask.CompletedTask));
    }

    [TestMethod]
    public async Task ThrowingCallback_IsReported()
    {
        var time = new FakeTimeProvider();
        var logger = new RecordingLogger();
        using var heartbeat = new Heartbeat(TimeSpan.FromSeconds(1), time, logger);
        var calls = 0;
        heartbeat.Register(_ =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                return ValueTask.FromException(new InvalidOperationException("callback failed"));
            return ValueTask.CompletedTask;
        });

        time.Advance(TimeSpan.FromSeconds(1));
        var entry = await logger.Entry.Task;

        Assert.AreEqual(LogLevel.Error, entry.Level);
        Assert.IsInstanceOfType<InvalidOperationException>(entry.Exception);
        StringAssert.Contains(entry.Message, "heartbeat tick");
    }

    sealed class RecordingLogger : ILogger
    {
        public TaskCompletionSource<Entry> Entry { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
            => Entry.TrySetResult(new(logLevel, exception, formatter(state, exception)));
    }

    readonly record struct Entry(LogLevel Level, Exception? Exception, string Message);
}
