namespace Slon.Tests;

[TestClass]
public class SlonDbTypeTests
{
    [TestMethod]
    public void TypeModifiersCompose()
    {
        var range = SlonDbType.Create("public.measurement_range");

        AssertDisplay(range, "public.measurement_range");
        AssertDisplay(range.MakeArrayType(), "public.measurement_range[]");
        AssertDisplay(range.MakeMultirangeType(), "public.measurement_multirange");
        AssertDisplay(range.MakeMultirangeType().MakeArrayType(), "public.measurement_multirange[]");

        static void AssertDisplay(SlonDbType type, string expected)
            => Assert.AreEqual($@"Case = ""DataTypeName"", Value = ""{expected}""", type.ToString());
    }
}
