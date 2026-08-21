using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg;

namespace Slon.Tests;

sealed class BindingProbeContext(string name) : PgClientFlowBindingContext
{
    internal string Name { get; } = name;
}

sealed class BindingProbeFlow(bool fail = false) : CommandFlow(async: true, [])
{
    internal int BindCount { get; private set; }
    internal string? ContextName { get; private set; }

    internal override void Bind(PgClientFlowBindingContext? context)
    {
        BindCount++;
        ContextName = ((BindingProbeContext)context!).Name;
        if (fail)
            throw new InvalidOperationException("binding rejected");
        Initialize(IsAsync, Command.Create("select 1"));
    }
}
