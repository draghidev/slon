using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Slon.Buffers;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol;

static class PgTypes
{
    public enum BackendType: byte
    {
        // Startup only
        AuthenticationRequest    = (byte)'R',
        BackendKeyData           = (byte)'K',
        NegotiateProtocolVersion = (byte)'v',

        BindComplete             = (byte)'2',
        CloseComplete            = (byte)'3',
        CommandComplete          = (byte)'C',
        CopyData                 = (byte)'d',
        CopyDone                 = (byte)'c',
        CopyBothResponse         = (byte)'W',
        CopyInResponse           = (byte)'G',
        CopyOutResponse          = (byte)'H',
        DataRow                  = (byte)'D',
        EmptyQueryResponse       = (byte)'I',
        ErrorResponse            = (byte)'E',
        FunctionCallResponse     = (byte)'V',
        NoData                   = (byte)'n',
        NoticeResponse           = (byte)'N',
        NotificationResponse     = (byte)'A',
        ParameterDescription     = (byte)'t',
        ParameterStatus          = (byte)'S',
        ParseComplete            = (byte)'1',
        PortalSuspended          = (byte)'s',
        ReadyForQuery            = (byte)'Z',
        RowDescription           = (byte)'T',
    }

    public enum FrontendType: byte
    {
        Describe = (byte) 'D',
        Sync = (byte) 'S',
        Execute = (byte) 'E',
        Parse = (byte) 'P',
        Bind = (byte) 'B',
        Close = (byte) 'C',
        Query = (byte) 'Q',
        FunctionCall = (byte)'F',
        CopyData = (byte) 'd',
        CopyDone = (byte) 'c',
        CopyFail = (byte) 'f',
        Terminate = (byte) 'X',
        // Describes PasswordMessage, GSSResponse, SASLInitialResponse, SASLResponse.
        Authentication = (byte) 'p',
    }

    public readonly struct Header
    {
        public const int ByteCount = sizeof(int) + sizeof(byte);

        public Header(byte tag, int length)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(length, 4);
            Tag = tag;
            Length = length;
        }

        public bool HasBody => Length is not 4;

        public byte Tag { get; }
        // Never negative.
        public int Length { get; }
        public int BodyLength => Length - 4;
        public uint MessageLength => (uint)Length + sizeof(byte);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryParse(ReadOnlySpan<byte> span, out Header header)
        {
            if (span.Length < ByteCount)
            {
                header = default;
                return false;
            }

            ref var first = ref MemoryMarshal.GetReference(span);

            var length = Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref first, 1));
            if (BitConverter.IsLittleEndian)
                length = BinaryPrimitives.ReverseEndianness(length);

            header = new(first, length);
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static bool TryParseMultiSegment(ReadOnlySequence<byte> buffer, out Header header)
        {
            Span<byte> span = stackalloc byte[ByteCount];
            if (!ReadOnlySequenceExtensions.TryCopySlow(buffer, span) || !TryParse(span, out header))
            {
                header = default;
                return false;
            }

            return true;
        }
    }
}

// Can't have extensions nested in another type.
static class PgTypesBackendTypeExtensions
{
    public static byte ToByte(this BackendType value)
    {
        Debug.Assert(typeof(FrontendType).GetEnumUnderlyingType() == typeof(byte));
        return (byte)value;
    }

    public static bool IsDefined(this BackendType value)
        => value switch
        {
            BackendType.AuthenticationRequest or
            BackendType.BackendKeyData or
            BackendType.NegotiateProtocolVersion or
            BackendType.BindComplete or
            BackendType.CloseComplete or
            BackendType.CommandComplete or
            BackendType.CopyData or
            BackendType.CopyDone or
            BackendType.CopyBothResponse or
            BackendType.CopyInResponse or
            BackendType.CopyOutResponse or
            BackendType.DataRow or
            BackendType.EmptyQueryResponse or
            BackendType.ErrorResponse or
            BackendType.FunctionCallResponse or
            BackendType.NoData or
            BackendType.NoticeResponse or
            BackendType.NotificationResponse or
            BackendType.ParameterDescription or
            BackendType.ParameterStatus or
            BackendType.ParseComplete or
            BackendType.PortalSuspended or
            BackendType.ReadyForQuery or
            BackendType.RowDescription => true,
            _ => false
        };
}

// Can't have extensions nested in another type.
static class PgTypesFrontendTypeExtensions
{
    public static byte ToByte(this FrontendType value)
    {
        Debug.Assert(typeof(FrontendType).GetEnumUnderlyingType() == typeof(byte));
        return (byte)value;
    }

    public static bool IsDefined(this FrontendType value)
        => value switch
        {
            FrontendType.Describe or
            FrontendType.Sync or
            FrontendType.Execute or
            FrontendType.Parse or
            FrontendType.Bind or
            FrontendType.Close or
            FrontendType.Query or
            FrontendType.FunctionCall or
            FrontendType.CopyData or
            FrontendType.CopyDone or
            FrontendType.CopyFail or
            FrontendType.Terminate or
            FrontendType.Authentication => true,
            _ => false
        };
}
