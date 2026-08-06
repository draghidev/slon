namespace Slon.Tests;

static class TestTimeout
{
    // Wall-clock backstop only. Tests should use gates or fake time for the schedule itself.
    internal static readonly TimeSpan Hang = TimeSpan.FromSeconds(10);
}
