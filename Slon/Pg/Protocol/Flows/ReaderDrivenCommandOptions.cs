using System.Diagnostics.CodeAnalysis;

namespace Slon.Pg.Protocol.Flows;

/// The stable part of a reader-driven execution: the command's descriptor, protocol flags, result
/// formats, and timeouts. Parameters are supplied per execution, so one instance can be shared by
/// every flow that executes the same statement, including concurrently.
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed class ReaderDrivenCommandOptions
{
    readonly Command _template;

    public ReaderDrivenCommandOptions(in Command template, TimeSpan? pendingTimeout = null)
    {
        if (template.DescribeForPreparation || template.SuppressEnumeration)
            ThrowHelper.ThrowArgumentException(nameof(template),
                "Preparation and suppressed commands require the general command flow.");
        if (template.Parameters.Count is not 0)
            ThrowHelper.ThrowArgumentException(nameof(template),
                "Parameters are supplied per execution, not on the shared template.");
        _template = template;
        PendingTimeout = pendingTimeout;
    }

    public TimeSpan? PendingTimeout { get; }

    internal ref readonly Command Template => ref _template;

    internal Command CreateCommand(in ParameterSource parameters)
        => parameters.Count is 0 ? _template : _template with { Parameters = parameters };
}
