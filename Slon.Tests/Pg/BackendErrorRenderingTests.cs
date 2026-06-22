using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Live end-to-end: drives invalid SQL through a real connection so the ErrorResponse parser meets
// actual backend wire bytes (the synthetic ErrorResponseParseTests assume the format; this confirms
// it). Command errors are captured into CommandResult and surface on consumption
// (GetCommandComplete -> PostgresException.Throw), NOT on the bare MoveNextAsync drain. That is
// deliberate: it keeps the command->result correspondence 1:1 and ordered across a batch - an
// errored command still yields its own result, which throws only when that result is consumed, so
// the failure stays attributable and the results positioned after it remain readable.
[TestClass]
public class BackendErrorRenderingTests
{
    [TestMethod]
    public async Task BackendSyntaxError_SurfacesRenderedPostgresException()
    {
        // An input-caused syntax error (ErrorResponse + ReadyForQuery) leaves the session fine, so this
        // leases from the shared pool rather than burning an isolated connection.
        await using var lease = await PgTestPool.LeaseAsync();
        var protocol = lease.Protocol;

        var flow = new CommandFlow(async: true, Command.Create("SLECT 1"));
        Assert.IsTrue(protocol.TryQueue(flow));

        PostgresException? thrown = null;
        var e = flow.GetAsyncEnumerator();
        try
        {
            while (await e.MoveNextAsync())
                e.Current.GetCommandComplete(); // consume the result -> surfaces the captured error
        }
        catch (PostgresException ex)
        {
            thrown = ex;
        }
        await e.DisposeAsync();

        Assert.IsNotNull(thrown, "Invalid SQL should surface a PostgresException on result consumption.");
        // Parsed end-to-end from a real backend ErrorResponse: SQLSTATE + human-readable text,
        // not the opaque base "Exception of type ... was thrown".
        Assert.AreEqual(5, thrown!.SqlState.Length, "SQLSTATE is a 5-character code.");
        StringAssert.StartsWith(thrown.SqlState, "42"); // syntax error / access rule violation class
        Assert.IsFalse(string.IsNullOrEmpty(thrown.MessageText), "Message text should be parsed.");
        StringAssert.Contains(thrown.Message, thrown.SqlState);
        StringAssert.Contains(thrown.Message, thrown.MessageText);

        Console.WriteLine($"Rendered: {thrown.Message}");
    }
}
