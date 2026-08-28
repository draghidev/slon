using System.Buffers.Binary;
using System.Text;
using Slon.Pg.Protocol;
using Slon.Text;

namespace Slon.Tests.Pg;

// The fused prepared-execution write must produce exactly the bytes the message-per-message writers
// produce, for every Describe, Execute, and Sync combination, and must leave the writer's
// per-message framing validation intact.
[TestClass]
public class PgEncoderPreparedExecutionTests
{
    static readonly Encoding Encoding = Encoding.UTF8;

    static (ProtocolDataWriter Writer, BufferOutputWriter Sink) NewWriter()
    {
        var sink = new BufferOutputWriter();
        var writer = new ProtocolDataWriter(sink, Encoding, static _ => { }, default, null!);
        return (writer, sink);
    }

    static byte[] Expected(string commandName, bool describe, bool execute, int syncCount)
    {
        var bytes = new List<byte>();
        var name = Encoding.GetBytes(commandName);

        // Bind: unnamed portal, statement name, no parameter format codes, no parameters, one result
        // format code, binary.
        Message(bytes, (byte)'B', [0, .. name, 0, 0, 0, 0, 0, 0, 1, 0, 1]);
        if (describe)
            Message(bytes, (byte)'D', [(byte)'P', 0]);
        if (execute)
            Message(bytes, (byte)'E', [0, 0, 0, 0, 0]);
        for (var i = 0; i < syncCount; i++)
            Message(bytes, (byte)'S', []);
        return bytes.ToArray();

        static void Message(List<byte> bytes, byte type, ReadOnlySpan<byte> body)
        {
            bytes.Add(type);
            Span<byte> length = stackalloc byte[sizeof(uint)];
            BinaryPrimitives.WriteUInt32BigEndian(length, (uint)(sizeof(uint) + body.Length));
            bytes.AddRange(length);
            bytes.AddRange(body);
        }
    }

    [TestMethod]
    [DataRow(false, false, 0)]
    [DataRow(false, false, 1)]
    [DataRow(false, false, 2)]
    [DataRow(false, true, 0)]
    [DataRow(false, true, 1)]
    [DataRow(false, true, 2)]
    [DataRow(true, false, 0)]
    [DataRow(true, false, 1)]
    [DataRow(true, false, 2)]
    [DataRow(true, true, 0)]
    [DataRow(true, true, 1)]
    [DataRow(true, true, 2)]
    public void WritesExactWireBytes(bool describe, bool execute, int syncCount)
    {
        var (writer, sink) = NewWriter();

        PgEncoder.WritePreparedExecutionCore(writer, Encoding, new EncodedCString("prepared_probe"),
            describe, execute, syncCount);
        writer.Flush();

        CollectionAssert.AreEqual(Expected("prepared_probe", describe, execute, syncCount), sink.ToArray());
    }

    [TestMethod]
    public void UnnamedStatement_WritesEmptyName()
    {
        var (writer, sink) = NewWriter();

        PgEncoder.WritePreparedExecutionCore(writer, Encoding, default, describe: false, execute: true, syncCount: 1);
        writer.Flush();

        CollectionAssert.AreEqual(Expected("", describe: false, execute: true, syncCount: 1), sink.ToArray());
    }

    [TestMethod]
    public void EveryMessageIsFramedForTheDeclaredLengthCheck()
    {
        var (writer, sink) = NewWriter();

        PgEncoder.WritePreparedExecutionCore(writer, Encoding, new EncodedCString("prepared_probe"),
            describe: true, execute: true, syncCount: 2);
        // Arming the next message validates that the previous one was written to its declared length.
        writer.StartMessage(totalLength: 5);
        writer.WriteRaw(new byte[5]);
        writer.Flush();

        Assert.AreEqual(Expected("prepared_probe", describe: true, execute: true, syncCount: 2).Length + 5,
            sink.ToArray().Length);
    }

    [TestMethod]
    [DataRow(-1)]
    [DataRow(3)]
    public void SyncCount_OutsideZeroToTwo_Throws(int syncCount)
    {
        var (writer, _) = NewWriter();

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            PgEncoder.WritePreparedExecutionCore(writer, Encoding, new EncodedCString("prepared_probe"),
                describe: false, execute: true, syncCount));
    }
}
