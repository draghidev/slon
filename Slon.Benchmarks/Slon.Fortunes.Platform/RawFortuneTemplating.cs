using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Encodings.Web;

namespace Slon.Fortunes.Platform;

internal static class RawFortuneTemplating
{
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal static void Render(
        List<Fortune> fortunes, IBufferWriter<byte> writer, HtmlEncoder encoder)
    {
        const int FortunesTemplateLength = 1232;
        var span = writer.GetSpan(FortunesTemplateLength);
        var startingLength = span.Length;
        span = span.WriteAndSlice(
            "<!DOCTYPE html><html><head><title>Fortunes</title></head><body><table><tr><th>id</th><th>message</th></tr>"u8);
        foreach (var fortune in fortunes)
        {
            var current = span.WriteAndSlice("<tr><td>"u8);
            var success = Utf8Formatter.TryFormat((uint)fortune.Id, current, out var written);
            Debug.Assert(success);
            current = current.Slice(written).WriteAndSlice("</td><td>"u8);
            var status = encoder.EncodeUtf8(
                fortune.Message.Span, current, out _, out written, isFinalBlock: true);
            Debug.Assert(status is OperationStatus.Done);
            span = current.Slice(written).WriteAndSlice("</td></tr>"u8);
        }
        span = span.WriteAndSlice("</table></body></html>"u8);
        writer.Advance(startingLength - span.Length);
    }
}

internal static class SpanExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static Span<T> WriteAndSlice<T>(this Span<T> destination, ReadOnlySpan<T> source)
    {
        source.CopyTo(destination);
        return destination.Slice(source.Length);
    }
}
