using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

[TestClass]
public class PublicCommandFlowSurfaceTests
{
    [TestMethod]
    public void ReplacementIsTheSealedPublicCommandFlowSurface()
    {
        var flow = typeof(Slon.Pg.Protocol.Flows.CommandFlow);
        Assert.IsTrue(flow.IsPublic);
        Assert.IsTrue(flow.IsSealed);
        Assert.IsTrue(typeof(Slon.Pg.Protocol.Flows.CommandFlowObserver).IsPublic);
        Assert.IsTrue(typeof(Slon.Pg.Protocol.Flows.CommandFlowOptions).IsPublic);
        Assert.IsTrue(typeof(IEnumerator<CommandResult>)
            .IsAssignableFrom(typeof(Slon.Pg.Protocol.Flows.CommandFlow.Enumerator)));

        Assert.IsNotNull(flow.GetConstructor([
            typeof(bool), typeof(ReadOnlySpan<Command>)
        ]));
        Assert.IsNotNull(flow.GetConstructor([
            typeof(bool), typeof(Slon.Pg.Protocol.Flows.CommandFlowOptions).MakeByRefType()
        ]));
    }

    [TestMethod]
    public void CollectionIsOnTheExperimentalCommandResultSurface()
    {
        var result = typeof(CommandResult);
        var diagnosticId = result.GetCustomAttribute<ExperimentalAttribute>()!.DiagnosticId;
        var collect = result.GetMethods().Single(method =>
            method.Name == nameof(CommandResult.CollectAsync));
        var rowView = result.GetNestedType(
            nameof(CommandResult.RowView), BindingFlags.Public)!;

        Assert.IsTrue(collect.IsPublic);
        Assert.IsTrue(collect.IsGenericMethodDefinition);
        Assert.AreEqual(diagnosticId,
            collect.GetCustomAttribute<ExperimentalAttribute>()!.DiagnosticId);
        Assert.IsTrue(rowView.IsNestedPublic);
        Assert.IsNotNull(rowView.GetMethod(
            nameof(CommandResult.RowView.BorrowFieldMemory), BindingFlags.Public | BindingFlags.Instance));
        Assert.AreEqual(diagnosticId,
            rowView.GetCustomAttribute<ExperimentalAttribute>()!.DiagnosticId);
    }
}
