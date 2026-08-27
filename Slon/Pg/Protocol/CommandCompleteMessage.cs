using System.Buffers;
using System.Buffers.Text;

namespace Slon.Pg.Protocol;

// The command tag of a successfully-executed command. https://www.postgresql.org/docs/current/protocol-message-formats.html
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public enum StatementType : byte
{
    Unknown = 0,
    Empty,          // EmptyQueryResponse - the portal came from an empty query string, no tag at all.
    Select,
    Insert,
    Update,
    Delete,
    Merge,
    Copy,
    Call,
    Move,
    Fetch,
    CreateTableAs,
    Other,          // DDL / BEGIN / SET / ... - no row count.
}

// CommandComplete (or EmptyQueryResponse). The body is a single null-terminated command tag
// ("INSERT oid rows", "UPDATE rows", "SELECT rows", a no-count tag like "BEGIN", ...). Following
// npgsql's parse: anchor on the leading keyword to find where the numeric arguments start, then
// Utf8Parser the OID (INSERT only) and row count straight off the bytes - no string allocation.
//
// Unlike ErrorResponse (many string fields, usually unread, reference types needing the buffer) this
// is three value-type scalars, parsed eagerly into inline fields - so it needs no body view and no
// Preserve: the values survive any buffer recycle for free. (RecordsAffected forces the parse to be
// eager-while-on-message anyway, since it's read after the command when the view could be stale.)
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct CommandCompleteMessage
{
    public StatementType StatementType { get; }
    public uint Oid { get; }
    public ulong Rows { get; }
    public long RecordsAffected => StatementType is
        StatementType.Insert or StatementType.Update or StatementType.Delete or StatementType.Merge
        or StatementType.Copy or StatementType.Move or StatementType.Fetch or StatementType.CreateTableAs
            ? (long)Rows
            : 0;
    internal long BatchRecordsAffected => StatementType is StatementType.Select ? -1 : RecordsAffected;

    CommandCompleteMessage(StatementType statementType, uint oid, ulong rows)
    {
        StatementType = statementType;
        Oid = oid;
        Rows = rows;
    }

    /// EmptyQueryResponse: the portal was created from an empty query string. No tag, no rows.
    public bool IsEmptyQuery => StatementType is StatementType.Empty;

    internal static CommandCompleteMessage Create(in BackendMessage message)
    {
        message.EnsureExpected(PgTypes.BackendType.EmptyQueryResponse, PgTypes.BackendType.CommandComplete);
        message.EnsureBuffered();
        if (message.Header.Type is PgTypes.BackendType.EmptyQueryResponse)
            return new(StatementType.Empty, 0, 0);

        Span<byte> scratch = stackalloc byte[64];
        var bodyLength = message.Header.BodyLength;
        var bytes = message.TryGetFirstSpan(0, out var first) && first.Length >= bodyLength
            ? first[..bodyLength]
            : CopyToScratch(message.GetSequence(), scratch);
        return Parse(bytes);
    }

    // Test seam: build directly from a raw command-tag body (the null-terminated tag bytes), bypassing
    // the BackendMessage wrapper, so the tag parser can be exercised without a live connection. Exposed
    // via InternalsVisibleTo (Slon.Tests).
    internal static CommandCompleteMessage FromTag(ReadOnlySequence<byte> tagBody)
    {
        Span<byte> scratch = stackalloc byte[64];
        return Parse(tagBody.IsSingleSegment ? tagBody.FirstSpan : CopyToScratch(tagBody, scratch));
    }

    static CommandCompleteMessage Parse(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
            return new(StatementType.Other, 0, 0);
        if (bytes[^1] is 0)
            bytes = bytes[..^1];   // strip the null terminator

        var (type, argsStart) = bytes[0] switch
        {
            (byte)'S' when bytes.StartsWith("SELECT "u8) => (StatementType.Select, "SELECT ".Length),
            (byte)'I' when bytes.StartsWith("INSERT "u8) => (StatementType.Insert, "INSERT ".Length),
            (byte)'U' when bytes.StartsWith("UPDATE "u8) => (StatementType.Update, "UPDATE ".Length),
            (byte)'D' when bytes.StartsWith("DELETE "u8) => (StatementType.Delete, "DELETE ".Length),
            (byte)'M' when bytes.StartsWith("MERGE "u8) => (StatementType.Merge, "MERGE ".Length),
            (byte)'C' when bytes.StartsWith("COPY "u8) => (StatementType.Copy, "COPY ".Length),
            (byte)'C' when bytes.StartsWith("CALL"u8) => (StatementType.Call, "CALL".Length),
            (byte)'M' when bytes.StartsWith("MOVE "u8) => (StatementType.Move, "MOVE ".Length),
            (byte)'F' when bytes.StartsWith("FETCH "u8) => (StatementType.Fetch, "FETCH ".Length),
            (byte)'C' when bytes.StartsWith("CREATE TABLE AS "u8) => (StatementType.CreateTableAs, "CREATE TABLE AS ".Length),
            _ => (StatementType.Other, 0),
        };

        // Call and Other carry no numeric arguments.
        if (type is StatementType.Other or StatementType.Call)
            return new(type, 0, 0);

        var args = bytes[argsStart..];
        uint oid = 0;
        if (type is StatementType.Insert)
        {
            // "INSERT oid rows" - oid first, then a space, then the row count.
            Utf8Parser.TryParse(args, out oid, out var consumed);
            args = consumed + 1 <= args.Length ? args[(consumed + 1)..] : default;
        }
        Utf8Parser.TryParse(args, out ulong rows, out _);
        return new(type, oid, rows);
    }

    static ReadOnlySpan<byte> CopyToScratch(ReadOnlySequence<byte> tag, Span<byte> scratch)
    {
        var len = (int)Math.Min(tag.Length, scratch.Length);
        tag.Slice(0, len).CopyTo(scratch);
        return scratch[..len];
    }
}
