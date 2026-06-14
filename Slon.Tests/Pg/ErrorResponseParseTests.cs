using System;
using System.Buffers;
using System.IO;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg.Protocol;

namespace Slon.Tests.Pg;

// Connection-free tests for the ErrorResponse/NoticeResponse field parser (ErrorOrNoticeMessage) and
// how PostgresException renders it. Feeds a synthetic field block built to the wire format: a
// sequence of <field-type byte><null-terminated UTF8 string> pairs, terminated by a zero byte.
[TestClass]
public class ErrorResponseParseTests
{
    static byte[] BlockBytes(params (char Type, string Value)[] fields)
    {
        var ms = new MemoryStream();
        foreach (var (type, value) in fields)
        {
            ms.WriteByte((byte)type);
            var bytes = Encoding.UTF8.GetBytes(value);
            ms.Write(bytes, 0, bytes.Length);
            ms.WriteByte(0);
        }
        ms.WriteByte(0); // field-block terminator
        return ms.ToArray();
    }

    static ReadOnlySequence<byte> Block(params (char Type, string Value)[] fields)
        => new(BlockBytes(fields));

    [TestMethod]
    public void ParsesAllStandardFields()
    {
        var msg = ErrorOrNoticeMessage.FromFieldBlock(Block(
            ('S', "FATAL"),
            ('V', "FATAL"),
            ('C', "53300"),
            ('M', "sorry, too many clients already"),
            ('D', "the detail"),
            ('H', "the hint"),
            ('P', "42"),
            ('s', "public"),
            ('t', "users"),
            ('c', "email"),
            ('n', "users_email_key"),
            ('F', "postinit.c"),
            ('L', "858"),
            ('R', "InitPostgres")));

        Assert.AreEqual("FATAL", msg.Severity);
        Assert.AreEqual("FATAL", msg.InvariantSeverity);
        Assert.AreEqual("53300", msg.SqlState);
        Assert.AreEqual("sorry, too many clients already", msg.MessageText);
        Assert.AreEqual("the detail", msg.Detail);
        Assert.AreEqual("the hint", msg.Hint);
        Assert.AreEqual(42, msg.Position);
        Assert.AreEqual("public", msg.SchemaName);
        Assert.AreEqual("users", msg.TableName);
        Assert.AreEqual("email", msg.ColumnName);
        Assert.AreEqual("users_email_key", msg.ConstraintName);
        Assert.AreEqual("postinit.c", msg.File);
        Assert.AreEqual("858", msg.Line);
        Assert.AreEqual("InitPostgres", msg.Routine);
        Assert.IsTrue(msg.IsTransientError, "53300 (too many connections) is transient.");
    }

    [TestMethod]
    public void AbsentFields_AreEmptyOrNull()
    {
        // Only severity + code + message present; everything else absent.
        var msg = ErrorOrNoticeMessage.FromFieldBlock(Block(
            ('S', "ERROR"),
            ('C', "23505"),
            ('M', "duplicate key value violates unique constraint")));

        Assert.AreEqual("23505", msg.SqlState);
        Assert.AreEqual("duplicate key value violates unique constraint", msg.MessageText);
        Assert.IsNull(msg.Detail);
        Assert.IsNull(msg.Hint);
        Assert.AreEqual(0, msg.Position);
        Assert.IsNull(msg.SchemaName);
        // 'V' absent, so InvariantSeverity falls back to 'S'.
        Assert.AreEqual("ERROR", msg.InvariantSeverity);
    }

    [TestMethod]
    public void EmptyBlock_YieldsDefaults()
    {
        var msg = ErrorOrNoticeMessage.FromFieldBlock(new ReadOnlySequence<byte>(new byte[] { 0 }));
        Assert.AreEqual("", msg.SqlState);
        Assert.AreEqual("", msg.MessageText);
        Assert.IsNull(msg.Detail);
        Assert.IsFalse(msg.IsTransientError);
    }

    [TestMethod]
    public void PostgresException_RendersSelfDiagnosingMessage()
    {
        PgError error = ErrorOrNoticeMessage.FromFieldBlock(Block(
            ('S', "FATAL"),
            ('C', "53300"),
            ('M', "sorry, too many clients already")));

        var ex = Assert.Throws<PostgresException>(() => PostgresException.Throw(error));

        // No longer the opaque "Exception of type ... was thrown".
        StringAssert.Contains(ex.Message, "FATAL");
        StringAssert.Contains(ex.Message, "sorry, too many clients already");
        StringAssert.Contains(ex.Message, "53300");
        Assert.AreEqual("53300", ex.SqlState);
        Assert.AreEqual("sorry, too many clients already", ex.MessageText);
        Assert.IsTrue(ex.IsTransient);
    }

    [TestMethod]
    public void Preserve_CopiesBytes_ViewStaysAView_EagerSqlStateSurvives()
    {
        var backing = BlockBytes(('S', "ERROR"), ('C', "23505"), ('M', "duplicate key"));
        var view = ErrorOrNoticeMessage.FromFieldBlock(new ReadOnlySequence<byte>(backing));
        var preserved = view.Preserve();

        // Recycle the wire buffer out from under both.
        Array.Clear(backing);

        // The preserved copy owns its bytes, so it is intact.
        Assert.AreEqual("duplicate key", preserved.MessageText);
        Assert.AreEqual("23505", preserved.SqlState);

        // The view's lazy field reads the now-cleared buffer, proving it was a view, not a copy.
        Assert.AreEqual("", view.MessageText);
        // ...but SqlState was captured eagerly as a string at construction, so it survives the view.
        Assert.AreEqual("23505", view.SqlState);
    }
}
