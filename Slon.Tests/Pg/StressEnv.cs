namespace Slon.Tests.Pg;

// Centralizes SLON_STRESS_ITERATIONS parsing and the SLON_UNCAPPED override for the stress suites.
//
// Each test passes its fast-default plus a per-test cap reflecting its per-iteration COST:
//   - In-memory replay / pure data-structure loops are ~microseconds an iteration, so a blanket high
//     count is harmless (a generous cap, effectively none).
//   - Tests that open a real connection or run a real query each iteration cost orders of magnitude more,
//     so a cap keeps a blanket SLON_STRESS_ITERATIONS=20000 (or a fat-fingered 2_000_000) from turning the
//     whole suite into a multi-minute mountain that also saturates the Postgres connection pool.
//
// Set SLON_UNCAPPED=1 to bypass the cap and let the raw SLON_STRESS_ITERATIONS through for a DELIBERATE
// deep soak of a single test. (A genuinely non-scalable test - connection churn that leaves lingering
// backends - keeps a HARD cap inline instead of using this, since uncapping only buys "too many clients".)
static class StressEnv
{
    public static bool Uncapped
    {
        get
        {
            var v = Environment.GetEnvironmentVariable("SLON_UNCAPPED");
            return v is { Length: > 0 } && v != "0" && !v.Equals("false", StringComparison.OrdinalIgnoreCase);
        }
    }

    // SLON_STRESS_ITERATIONS if a positive int, else fallback; then clamp to cap unless SLON_UNCAPPED.
    public static int Iterations(int fallback, int cap = int.MaxValue)
    {
        var raw = Environment.GetEnvironmentVariable("SLON_STRESS_ITERATIONS");
        var n = int.TryParse(raw, out var p) && p > 0 ? p : fallback;
        return Uncapped ? n : Math.Min(n, cap);
    }
}
