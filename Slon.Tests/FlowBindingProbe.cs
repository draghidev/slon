using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg;

namespace Slon.Tests;

sealed class BindingProbeContext(string name) : PgClientFlowBindingContext
{
    internal string Name { get; } = name;
}

sealed class BindingProbeFlow : PgClientFlow
{
    readonly bool _fail;
    internal int BindCount { get; private set; }
    internal string? ContextName { get; private set; }

    internal BindingProbeFlow(bool fail = false)
        : base(supportsDeferredFlush: true)
    {
        _fail = fail;
        IsAsync = true;
    }

    protected override bool EnableActivationTimeout => true;

    protected override ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        var write = new CommandList(Command.Create("select 1"))
            .WriteCommandsAsync(context.GetEncoder(), appendSync: true);
        return new(new FlowTasks(write, DrainAsync(context)));

        static async ValueTask DrainAsync(Context context)
        {
            var decoder = await context.GetDecoderAsync().ConfigureAwait(false);
            while (context.OutstandingRfqCount is not 0)
                _ = await decoder.GetNextAsync().ConfigureAwait(false);
        }
    }

    internal override void Bind(PgClientFlowBindingContext? context)
    {
        BindCount++;
        ContextName = ((BindingProbeContext)context!).Name;
        if (_fail)
            throw new InvalidOperationException("binding rejected");
    }
}
