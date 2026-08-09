using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg;

namespace Slon.Tests;

sealed class BindingProbeContext(string name) : PgClientFlowBindingContext
{
    internal string Name { get; } = name;
}

sealed class BindingProbeStrategy(bool fail = false) : CommandFlowBindingStrategy
{
    internal int BindCount { get; private set; }
    internal string? ContextName { get; private set; }

    internal override CommandFlowOptions Bind(
        PgClientFlowBindingContext context, in CommandFlowBinding binding, TimeSpan? pendingTimeout)
    {
        BindCount++;
        ContextName = ((BindingProbeContext)context).Name;
        if (fail)
            throw new InvalidOperationException("binding rejected");
        return new() { Commands = new(Command.Create("select 1")) };
    }
}
