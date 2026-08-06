using Microsoft.Extensions.Logging;

namespace Slon.Tests;

[TestClass]
public sealed class LoggingTests
{
    [DataRow("Slon", "Slon")]
    [DataRow("Slon.Pg.Protocol", "Slon.Protocol")]
    [DataRow("Slon.Pool", "Slon.Pool")]
    [TestMethod]
    public void PrimaryComposition_ProjectsSemanticCategory(string category, string expected)
    {
        var inner = new RecordingLoggerFactory();
        var factory = new SlonLoggerFactory(inner);

        factory.CreateLogger(category);

        Assert.AreEqual(expected, inner.Category);
    }

    sealed class RecordingLoggerFactory : ILoggerFactory
    {
        public string? Category { get; private set; }

        public ILogger CreateLogger(string categoryName)
        {
            Category = categoryName;
            return Logger.Instance;
        }

        public void AddProvider(ILoggerProvider provider) { }
        public void Dispose() { }
    }

    sealed class Logger : ILogger
    {
        public static Logger Instance { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => false;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter) { }
    }
}
