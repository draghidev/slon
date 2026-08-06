using Microsoft.Extensions.Logging;

namespace Slon;

// Projects canonical lower-level categories into the primary Slon composition roles. The wrapper
// does not own the application-supplied factory; direct low-level use bypasses it and retains the
// canonical Slon.Pg.*, Slon.Pool, and other component categories.
sealed class SlonLoggerFactory(ILoggerFactory inner) : ILoggerFactory
{
    public ILogger CreateLogger(string categoryName)
    {
        var separator = categoryName.LastIndexOf('.');
        var role = separator < 0 ? categoryName : categoryName[(separator + 1)..];
        return inner.CreateLogger(role is "Slon" ? "Slon" : $"Slon.{role}");
    }

    public void AddProvider(ILoggerProvider provider) => inner.AddProvider(provider);

    // The application owns the supplied factory.
    public void Dispose() { }
}
