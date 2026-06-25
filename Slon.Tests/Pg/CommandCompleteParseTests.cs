using System.Buffers;
using System.Text;
using Slon.Pg.Protocol;

namespace Slon.Tests.Pg;

// Parses the CommandComplete command tag the way npgsql does: anchor on the leading keyword, then
// Utf8Parser the OID (INSERT only) and the trailing row count off the bytes - no string allocation.
[TestClass]
public class CommandCompleteParseTests
{
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
