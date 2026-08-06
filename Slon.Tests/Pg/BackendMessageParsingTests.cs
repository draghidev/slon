using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Pg.Serialization;
using Slon.Pg.Types;

namespace Slon.Tests.Pg;

// Backend-message value parsing after framing: command tags, error/notice fields and exception
// rendering. Synthetic payloads pin parsing and ownership; one live error verifies real wire bytes.
[TestClass]
public class BackendMessageParsingTests
{
    [TestMethod]
    public void RawFieldDecoder_UsesTheSuppliedExecutionEncoding()
    {
        ReadOnlySpan<byte> latin1 = [0x63, 0x61, 0x66, 0xE9];

        Assert.AreEqual("café", RawFieldDecoder.Read<string>(latin1, Encoding.Latin1));
        Assert.AreNotEqual("café", RawFieldDecoder.Read<string>(latin1),
            "the bootstrap decoder must not silently fall back to UTF-8 when an execution snapshot was supplied");
    }

    static BackendMessage Message(PgTypes.BackendType type, ReadOnlySpan<byte> body)
    {
        var length = sizeof(int) + body.Length;
        var bytes = new byte[sizeof(byte) + length];
        bytes[0] = (byte)type;
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(1), length);
        body.CopyTo(bytes.AsSpan(BackendHeader.ByteCount));
        var context = new BackendMessageContext();
        context.SetBatch(new BackendMessageBatch(new ReadOnlySequence<byte>(bytes)));
        Assert.IsTrue(context.TryMoveNext());
        return context.Current;
    }

    static byte[] RowDescriptionBody(
        params (string Name, uint TableOid, short Attribute, uint TypeOid, short Size, int TypeModifier, PgFormat Format)[] fields)
    {
        var encodedNames = fields.Select(static field => Encoding.UTF8.GetBytes(field.Name)).ToArray();
        var body = new byte[sizeof(short) + encodedNames.Sum(static name => name.Length + 1 + 18)];
        var span = body.AsSpan();
        BinaryPrimitives.WriteInt16BigEndian(span, checked((short)fields.Length));
        var offset = sizeof(short);
        for (var i = 0; i < fields.Length; i++)
        {
            var field = fields[i];
            encodedNames[i].CopyTo(span[offset..]); offset += encodedNames[i].Length;
            span[offset++] = 0;
            BinaryPrimitives.WriteUInt32BigEndian(span[offset..], field.TableOid); offset += 4;
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], field.Attribute); offset += 2;
            BinaryPrimitives.WriteUInt32BigEndian(span[offset..], field.TypeOid); offset += 4;
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], field.Size); offset += 2;
            BinaryPrimitives.WriteInt32BigEndian(span[offset..], field.TypeModifier); offset += 4;
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)field.Format); offset += 2;
        }
        return body;
    }

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
    public void UnexpectedMessageType_IsAProtocolFailure()
    {
        var message = Message(PgTypes.BackendType.CommandComplete, "SELECT 1\0"u8);

        Assert.ThrowsExactly<PgProtocolException>(() =>
            message.EnsureExpected(PgTypes.BackendType.ReadyForQuery));
        Assert.ThrowsExactly<PgProtocolException>(() =>
            message.EnsureExpectedOrError(PgTypes.BackendType.ReadyForQuery));
        Assert.ThrowsExactly<PgProtocolException>(() =>
            message.EnsureExpected(PgTypes.BackendType.ReadyForQuery, PgTypes.BackendType.ErrorResponse));
    }

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
    public void PgErrorException_RendersSelfDiagnosingMessage()
    {
        PgError error = ErrorOrNoticeMessage.FromFieldBlock(Block(
            ('S', "FATAL"),
            ('C', "53300"),
            ('M', "sorry, too many clients already")));

        var ex = Assert.Throws<PgErrorException>(() => PgErrorException.Throw(error));

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

    [TestMethod]
    public void NoticeFields_CreateNoticeRatherThanError()
    {
        var message = ErrorOrNoticeMessage.FromFieldBlock(
            Block(('S', "NOTICE"), ('C', "00000"), ('M', "hello")), isNotice: true);

        Assert.IsTrue(message.IsNotice);
        var notice = new PgNotice(message);
        Assert.ThrowsExactly<ArgumentException>(() => new PgError(message));
    }

    [TestMethod]
    public void ReadyForQuery_ParsesEveryWireStatus()
    {
        foreach (var (wire, expected) in new[]
        {
            ((byte)'I', TransactionStatus.Idle),
            ((byte)'T', TransactionStatus.Transaction),
            ((byte)'E', TransactionStatus.Error),
        })
        {
            var parsed = ReadyForQueryMessage.Create(Message(PgTypes.BackendType.ReadyForQuery, [wire]));
            Assert.AreEqual(expected, parsed.TransactionStatus);
        }
    }

    [TestMethod]
    public void ReadyForQuery_RejectsUnknownStatus()
        => Assert.ThrowsExactly<UnreachableException>(() =>
            ReadyForQueryMessage.Create(Message(PgTypes.BackendType.ReadyForQuery, "X"u8)));

    [TestMethod]
    public void RowDescription_ParsesCompleteFramedBackendMessage()
    {
        var message = Message(PgTypes.BackendType.RowDescription, RowDescriptionBody(
            ("value", 42, 3, 23, 4, -1, PgFormat.Binary),
            ("display name", 0, 0, 25, -1, 17, PgFormat.Text)));
        var description = new RowDescription();

        Assert.AreEqual(PgTypes.BackendType.RowDescription, message.Header.Type);
        description.Initialize(message.BodyReader);

        Assert.AreEqual(2, description.FieldCount);
        Assert.AreEqual(new RowDescriptionField("value", (Oid)42, 3, (Oid)23, 4, -1, PgFormat.Binary),
            description[0]);
        Assert.AreEqual(new RowDescriptionField("display name", default, 0, (Oid)25, -1, 17, PgFormat.Text),
            description[1]);
    }

    [TestMethod]
    public void RowDescription_RejectsTruncatedFieldCount()
    {
        var description = new RowDescription();
        var message = Message(PgTypes.BackendType.RowDescription, new byte[1]);
        var exception = Assert.ThrowsExactly<PgProtocolException>(() =>
            description.Initialize(message.BodyReader));
        StringAssert.Contains(exception.Message, "enough RowDescription data");
        Assert.IsNull(exception.InnerException);
    }

    [TestMethod]
    public void RowDescription_RejectsTruncatedAndTrailingFramedBodies()
    {
        var oneField = RowDescriptionBody(("value", 42, 3, 23, 4, -1, PgFormat.Binary));
        BinaryPrimitives.WriteInt16BigEndian(oneField, 2);
        var truncated = Message(PgTypes.BackendType.RowDescription, oneField);
        Assert.ThrowsExactly<PgProtocolException>(() =>
            new RowDescription().Initialize(truncated.BodyReader));

        var valid = RowDescriptionBody(("value", 42, 3, 23, 4, -1, PgFormat.Binary));
        var bodyWithTrailingByte = new byte[valid.Length + 1];
        valid.CopyTo(bodyWithTrailingByte, 0);
        var trailing = Message(PgTypes.BackendType.RowDescription, bodyWithTrailingByte);
        var exception = Assert.ThrowsExactly<PgProtocolException>(() =>
            new RowDescription().Initialize(trailing.BodyReader));
        StringAssert.Contains(exception.Message, "trailing data");
    }

    [TestMethod]
    public void RowDescription_ReusesStorageWhilePreservedCopyRemainsStable()
    {
        var description = new RowDescription();
        description.Initialize(new SequenceReader<byte>(new ReadOnlySequence<byte>(RowDescriptionBody("first", 23))));
        var preserved = description.Preserve();

        description.Initialize(new SequenceReader<byte>(new ReadOnlySequence<byte>(RowDescriptionBody("second", 25))));

        Assert.AreEqual("second", description[0].Name);
        Assert.AreEqual((Oid)25, description[0].TypeOid);
        Assert.AreEqual("first", preserved[0].Name);
        Assert.AreEqual((Oid)23, preserved[0].TypeOid);

        static byte[] RowDescriptionBody(string name, uint typeOid)
        {
            var nameBytes = Encoding.UTF8.GetBytes(name);
            var body = new byte[2 + nameBytes.Length + 1 + 4 + 2 + 4 + 2 + 4 + 2];
            var span = body.AsSpan();
            BinaryPrimitives.WriteInt16BigEndian(span, 1);
            nameBytes.CopyTo(span[2..]);
            var offset = 2 + nameBytes.Length + 1;
            BinaryPrimitives.WriteUInt32BigEndian(span[offset..], 0); offset += 4;
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], 0); offset += 2;
            BinaryPrimitives.WriteUInt32BigEndian(span[offset..], typeOid); offset += 4;
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], -1); offset += 2;
            BinaryPrimitives.WriteInt32BigEndian(span[offset..], -1); offset += 4;
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)PgFormat.Binary);
            return body;
        }
    }

    [TestMethod]
    public void RowDescription_RetirementInvalidatesMetadataAndTrimsOversizedStorage()
    {
        var description = new RowDescription();
        description.Initialize(new SequenceReader<byte>(new ReadOnlySequence<byte>(RowDescriptionBody(256))));
        description.PrepareForReuse();
        Assert.AreEqual(0, description.FieldCount);
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => _ = description[0]);

        description.Initialize(new SequenceReader<byte>(new ReadOnlySequence<byte>(RowDescriptionBody(257))));
        description.PrepareForReuse();
        Assert.AreEqual(0, description.FieldCount);

        static byte[] RowDescriptionBody(short fieldCount)
        {
            const int fieldLength = 1 + 4 + 2 + 4 + 2 + 4 + 2;
            var body = new byte[2 + fieldCount * fieldLength];
            var span = body.AsSpan();
            BinaryPrimitives.WriteInt16BigEndian(span, fieldCount);
            var offset = 2;
            for (var i = 0; i < fieldCount; i++)
            {
                span[offset++] = 0;
                BinaryPrimitives.WriteUInt32BigEndian(span[offset..], 0); offset += 4;
                BinaryPrimitives.WriteInt16BigEndian(span[offset..], 0); offset += 2;
                BinaryPrimitives.WriteUInt32BigEndian(span[offset..], 23); offset += 4;
                BinaryPrimitives.WriteInt16BigEndian(span[offset..], 4); offset += 2;
                BinaryPrimitives.WriteInt32BigEndian(span[offset..], -1); offset += 4;
                BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)PgFormat.Binary); offset += 2;
            }
            return body;
        }
    }

    [TestMethod]
    public void SqlAscii_UsesConfiguredDefaultClientEncoding()
    {
        var configuredDefault = Encoding.Latin1;

        Assert.AreSame(configuredDefault,
            PgClientProtocol.Control.ResolveClientEncoding("SQL_ASCII", configuredDefault));
        Assert.AreEqual(Encoding.UTF8,
            PgClientProtocol.Control.ResolveClientEncoding("UTF-8", configuredDefault));
    }

    [TestMethod]
    public async Task BackendSyntaxError_SurfacesRenderedPgErrorException()
    {
        var protocol = await PgTestPool.GetProtocolAsync();
        var flow = new CommandFlow(async: true, Command.Create("SLECT 1"));
        Assert.IsTrue(protocol.TryQueue(flow));

        PgErrorException? thrown = null;
        var e = flow.GetAsyncEnumerator();
        try
        {
            while (await e.MoveNextAsync())
                e.Current.GetCommandComplete();
        }
        catch (PgErrorException ex)
        {
            thrown = ex;
        }
        await e.DisposeAsync();

        Assert.IsNotNull(thrown);
        Assert.AreEqual(5, thrown.SqlState.Length);
        StringAssert.StartsWith(thrown.SqlState, "42");
        Assert.IsFalse(string.IsNullOrEmpty(thrown.MessageText));
        StringAssert.Contains(thrown.Message, thrown.SqlState);
        StringAssert.Contains(thrown.Message, thrown.MessageText);
    }

    static void AssertTag(string tag, StatementType type, uint oid, ulong rows)
    {
        // The wire tag is null-terminated; include the NUL so the parser exercises the strip.
        var body = new ReadOnlySequence<byte>(Encoding.ASCII.GetBytes(tag + "\0"));
        var msg = CommandCompleteMessage.FromTag(body);
        Assert.AreEqual(type, msg.StatementType, $"StatementType for '{tag}'");
        Assert.AreEqual(oid, msg.Oid, $"Oid for '{tag}'");
        Assert.AreEqual(rows, msg.Rows, $"Rows for '{tag}'");
    }

    [TestMethod]
    public void ParsesRowCountsAndTypes()
    {
        AssertTag("INSERT 0 5", StatementType.Insert, oid: 0, rows: 5);
        AssertTag("INSERT 16384 1", StatementType.Insert, oid: 16384, rows: 1);
        AssertTag("UPDATE 3", StatementType.Update, 0, 3);
        AssertTag("DELETE 2", StatementType.Delete, 0, 2);
        AssertTag("SELECT 42", StatementType.Select, 0, 42);
        AssertTag("MOVE 7", StatementType.Move, 0, 7);
        AssertTag("FETCH 9", StatementType.Fetch, 0, 9);
        AssertTag("COPY 100", StatementType.Copy, 0, 100);
        AssertTag("MERGE 4", StatementType.Merge, 0, 4);
        AssertTag("CREATE TABLE AS 5", StatementType.CreateTableAs, 0, 5);
    }

    [TestMethod]
    public void NoCountTags_AreZero()
    {
        AssertTag("BEGIN", StatementType.Other, 0, 0);
        AssertTag("COMMIT", StatementType.Other, 0, 0);
        AssertTag("CREATE TABLE", StatementType.Other, 0, 0); // distinct from CREATE TABLE AS
        AssertTag("SET", StatementType.Other, 0, 0);          // distinct from SELECT
        AssertTag("DISCARD ALL", StatementType.Other, 0, 0);
        AssertTag("CALL", StatementType.Call, 0, 0);
    }

    [TestMethod]
    public void LargeRowCount_ParsesAsUlong()
    {
        AssertTag("UPDATE 5000000000", StatementType.Update, 0, 5_000_000_000UL); // > int.MaxValue
    }

    [TestMethod]
    public void EmptyQueryResponse_IsEmptyZeroRows()
    {
        var empty = CommandCompleteMessage.FromTag(default);
        // FromTag flags non-empty; EmptyQueryResponse goes through Create's EmptyQueryResponse branch,
        // but a zero-length body parses to Other/0 either way (no keyword, no count).
        Assert.AreEqual(StatementType.Other, empty.StatementType);
        Assert.AreEqual(0UL, empty.Rows);
    }

    [TestMethod]
    public void ParsesAtConstruction_SurvivesBufferReuse()
    {
        var backing = Encoding.ASCII.GetBytes("UPDATE 3\0");
        var msg = CommandCompleteMessage.FromTag(new ReadOnlySequence<byte>(backing));
        // The message holds parsed value scalars, not a buffer view - clobbering the backing buffer
        // must not change them.
        Array.Clear(backing);
        Assert.AreEqual(StatementType.Update, msg.StatementType);
        Assert.AreEqual(3UL, msg.Rows);
    }
}
