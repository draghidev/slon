using System.Text;
using Slon.Text;

namespace Slon.Tests;

[TestClass]
public class EncodedStringTests
{
    [TestMethod]
    public void Default_HasStableHashCode()
        => Assert.AreEqual(0, default(EncodedString).GetHashCode());

    [TestMethod]
    public void ConcurrentReencoding_PublishesEncodingAndBytesTogether()
    {
        EncodedString value = "é";
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
