using System.Runtime.CompilerServices;

[module: SkipLocalsInit]

namespace Slon;

static class Statics
{
    public static bool EnableAssertions { get; } = AppContext.TryGetSwitch("Slon.EnableAssertions", out var enabled) && enabled;
    
}
