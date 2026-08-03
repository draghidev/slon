using System.Buffers.Binary;

namespace Slon.Tests.Pg;

static class StartupTranscript
{
    // Authentication exchanges contain fresh challenges and cannot be replayed. These tests exercise
    // post-startup behavior, so retain the captured startup transcript from AuthenticationOk onward.
    public static byte[] MakeReplayable(ReadOnlySpan<byte> transcript)
    {
        var offset = 0;
        while (offset + 5 <= transcript.Length)
        {
            var length = BinaryPrimitives.ReadInt32BigEndian(transcript.Slice(offset + 1, 4));
            if (length < 4 || offset + 1 + length > transcript.Length)
                break;

            if (transcript[offset] == (byte)'R' && length == 8 &&
                BinaryPrimitives.ReadInt32BigEndian(transcript.Slice(offset + 5, 4)) == 0)
                return transcript[offset..].ToArray();

            offset += 1 + length;
        }

        Assert.Fail("The captured startup transcript did not contain AuthenticationOk.");
        return [];
    }
}
