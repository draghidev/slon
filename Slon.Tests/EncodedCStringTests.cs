using System.Text;
using Slon.Text;

namespace Slon.Tests;

[TestClass]
public class EncodedCStringTests
{
    [TestMethod]
    public void Default_RepresentsUnnamedProtocolString()
    {
        var value = default(EncodedCString);

        Assert.IsTrue(value.IsDefault);
        Assert.IsTrue(value.AsSpan(Encoding.UTF8).IsEmpty);
        var terminated = value.AsNullTerminatedSpan(Encoding.UTF8);
        Assert.AreEqual(1, terminated.Length);
        Assert.AreEqual(0, terminated[0]);
    }

    [TestMethod]
    public void NullAndEmbeddedNul_AreRejected()
    {
        Assert.ThrowsExactly<ArgumentNullException>(() => _ = new EncodedCString(null!));
        Assert.ThrowsExactly<ArgumentException>(() => _ = new EncodedCString("name\0suffix"));
    }

    [TestMethod]
    public void ValueComparison_IsOrdinal()
    {
        EncodedCString first = "name";
        EncodedCString same = "name";
        EncodedCString differentCase = "Name";

        Assert.IsTrue(first.ValueEquals(same));
        Assert.IsFalse(first.ValueEquals(differentCase));
        Assert.IsTrue(default(EncodedCString).ValueEquals(default));
    }

    [TestMethod]
    public void ConcurrentReencoding_PublishesEncodingAndBytesTogether()
    {
        EncodedCString value = "é";
        var utf8 = Encoding.UTF8.GetBytes("é\0");
        var latin1 = Encoding.Latin1.GetBytes("é\0");

        Parallel.For(0, 100_000, i =>
        {
            var encoding = (i & 1) is 0 ? Encoding.UTF8 : Encoding.Latin1;
            var expected = (i & 1) is 0 ? utf8 : latin1;
            Assert.IsTrue(value.AsNullTerminatedSpan(encoding).SequenceEqual(expected));
        });
    }
}
