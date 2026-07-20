using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
readonly struct BackendMessage
{
    string DebuggerDisplay => $"Type = {Header.Type}, Length = {Header.MessageLength}";

    readonly ReadOnlySequence<byte> _buffer;
    readonly BackendMessageContext _context;

    // Packed to avoid another 8 bytes.
    readonly bool _buffered;
    readonly BackendType _type;
    readonly short _token;
    readonly int _length;

    BackendMessage(BackendHeader header, ReadOnlySequence<byte> buffer, BackendMessageContext context, short token, bool buffered)
    {
        _buffer = buffer;
        _context = context;
        _buffered = buffered;
        _type = header.Type;
        _token = token;
        _length = header.Length;
    }

    public BackendMessage(BackendHeader header, ReadOnlySequence<byte> buffer, BackendMessageContext context, short token)
        : this(header, buffer, context, token, buffer.Length >= header.Length) {}

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryCreateFromBatch(ref BackendMessageBatch batch, BackendMessageContext context, short token, out BackendMessage message)
    {
        if (!batch.TryReadNextInPlace(out var header, out var buffer, out var bufferLength))
        {
            message = default;
            return false;
        }

        Unsafe.SkipInit(out message);
        Initialize(ref message, header, buffer, context, token, bufferLength >= header.Length);
        return true;
    }

    internal static void Initialize(ref BackendMessage destination, BackendHeader header, ReadOnlySequence<byte> buffer,
        BackendMessageContext context, short token, bool buffered)
    {
        var value = new BackendMessage(header, buffer, context, token, buffered);
        WriteGranularly(ref destination, in value, destinationIsZero: false);
    }

    // The JIT should have a phase for picking granular writes (and write barriers) over full struct assignments.
    // This translation is entirely mechanical (even though these implementations need to deviate for external types).
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void WriteGranularly(ref BackendMessage destination, in BackendMessage value, bool destinationIsZero = false)
    {
        if ((destinationIsZero && value._context is not null) || !ReferenceEquals(destination._context, value._context))
            Unsafe.AsRef(in destination._context) = value._context!;

        WriteGranularly(ref Unsafe.AsRef(in destination._buffer), in value._buffer);

        Unsafe.AsRef(in destination._buffered) = value._buffered;
        Unsafe.AsRef(in destination._type) = value._type;
        Unsafe.AsRef(in destination._token) = value._token;
        Unsafe.AsRef(in destination._length) = value._length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void WriteGranularly(ref ReadOnlySequence<byte> destination, in ReadOnlySequence<byte> value)
    {
        ref var source = ref Unsafe.AsRef(in value);
        ref var destinationStartObject = ref StartObject(ref destination);
        var sourceStartObject = StartObject(ref source);
        if (!ReferenceEquals(destinationStartObject, sourceStartObject))
            destinationStartObject = sourceStartObject;

        ref var destinationEndObject = ref EndObject(ref destination);
        var sourceEndObject = EndObject(ref source);
        if (!ReferenceEquals(destinationEndObject, sourceEndObject))
            destinationEndObject = sourceEndObject;

        StartInteger(ref destination) = StartInteger(ref source);
        EndInteger(ref destination) = EndInteger(ref source);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void SetSequence(ref ReadOnlySequence<byte> destination, in ReadOnlySequence<byte> value)
        => WriteGranularly(ref destination, in value);

    BackendType Type => _type;

    public BackendHeader Header
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => BackendHeader.CreateUnchecked(_type, _length);
    }

    public ReadOnlySequence<byte> GetSequence(SequencePosition start)
        => _buffer.Slice(start);

    public ReadOnlySequence<byte> GetSequence(long offset)
        => _buffer.Slice(BackendHeader.ByteCount + offset);

    public ReadOnlySequence<byte> GetSequence()
        => GetSequence(0);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetFirstSpan(int offset, out ReadOnlySpan<byte> span)
    {
        offset += BackendHeader.ByteCount;
        ref var buffer = ref Unsafe.AsRef(in _buffer);
        var startObject = StartObject(ref buffer);
        ReadOnlySpan<byte> firstSpan;
        if (startObject is not null && startObject.GetType() == typeof(byte[]))
        {
            Debug.Assert(buffer.IsSingleSegment);
            var start = StartInteger(ref buffer);
            firstSpan = Unsafe.As<byte[]>(startObject).AsSpan(start, buffer.End.GetInteger() - start);
        }
        else
        {
            firstSpan = buffer.FirstSpan;
        }
        if ((uint)offset <= (uint)firstSpan.Length)
        {
            span = firstSpan.Slice(offset);
            return true;
        }

        span = default;
        return false;
    }

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_startObject")]
    static extern ref object? StartObject(ref ReadOnlySequence<byte> buffer);

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_startInteger")]
    static extern ref int StartInteger(ref ReadOnlySequence<byte> buffer);

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_endObject")]
    static extern ref object? EndObject(ref ReadOnlySequence<byte> buffer);

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_endInteger")]
    static extern ref int EndInteger(ref ReadOnlySequence<byte> buffer);

    public SequenceReader<byte> BodyReader => new(GetSequence());

    public (PgError? Error, BackendType Type) EnsureExpectedOrError(params ReadOnlySpan<BackendType> expected)
        => EnsureExpectedOrError(unhandledError: true, expected);

    // Inlining helps as it's usually run over a few RVA items at most.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    (PgError? Error, BackendType Type) EnsureExpectedOrError(bool unhandledError, ReadOnlySpan<BackendType> expected)
    {
        foreach (var type in expected)
        {
            if (type == Type)
                return (null, type);
        }

        if (Type is BackendType.ErrorResponse)
            return (CreateError(null, unhandledError), BackendType.ErrorResponse);

        Throw(Type, expected);
        return default;

        static void Throw(BackendType actual, ReadOnlySpan<BackendType> expected)
            => throw new InvalidOperationException($"Unexpected backend message: {actual}, expected: {string.Join(" or ", expected.ToArray())}.");
    }

    public Accessor GetAccessor() => new(_context, _token);

    public readonly struct Accessor
    {
        readonly BackendMessageContext _context;
        readonly short _token;

        internal Accessor(BackendMessageContext context, short token)
        {
            _context = context;
            _token = token;
        }

        public BackendMessage Message => _context.GetCurrent(_token);

        // The JIT should have a phase for picking granular writes (and write barriers) over full struct assignments.
        // This translation is entirely mechanical (even though these implementations need to deviate for external types).
        internal static void WriteGranularly(ref Accessor destination, in Accessor value, bool destinationIsZero = false)
        {
            if ((destinationIsZero && value._context is not null) || !ReferenceEquals(destination._context, value._context))
                Unsafe.AsRef(in destination._context) = value._context!;

            Unsafe.AsRef(in destination._token) = value._token;
        }
    }

    public bool TryCreateError([NotNullWhen(true)]out PgError? pgError)
    {
        if (Type is BackendType.ErrorResponse)
        {
            pgError = CreateError(null);
            return true;
        }

        pgError = default;
        return false;
    }

    public PgError? EnsureExpectedOrError(BackendType expected)
        => EnsureExpectedOrError(unhandledError: true, expected);

    PgError? EnsureExpectedOrError(bool unhandledError, BackendType expected)
    {
        if (expected == Type)
            return null;

        if (Type is BackendType.ErrorResponse)
            return CreateError(new(in expected), unhandledError);

        Throw(Type, expected);
        return null;

        static void Throw(BackendType actual, BackendType expected)
            => throw new InvalidOperationException($"Unexpected backend message: {actual}, expected: {expected}.");
    }

    public void EnsureExpected(BackendType expected)
    {
        if (expected != Type)
            Throw(Type, expected);

        static void Throw(BackendType actual, BackendType expected)
            => throw new InvalidOperationException($"Unexpected backend message: {actual}, expected: {expected}.");
    }

    // Inlining helps as it's usually run over a few RVA items at most.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BackendType EnsureExpected(params ReadOnlySpan<BackendType> expected)
    {
        foreach (var type in expected)
        {
            if (type == Type)
            {
                return type;
            }
        }

        Throw(Type, expected);
        return default;

        static void Throw(BackendType actual, ReadOnlySpan<BackendType> expected)
            => throw new InvalidOperationException($"Unexpected backend message: {actual}, expected: {string.Join(" or ", expected.ToArray())}.");
    }

    public void EnsureBuffered()
    {
        if (!Buffered)
            Throw(Type);

        static void Throw(BackendType actual)
            => throw new InvalidOperationException($"Message type: {actual} was expected to be buffered");
    }

    [Conditional("DEBUG")]
    public void DebugEnsureBuffered()
    {
        Debug.Assert(Buffered, $"Message type: {Type} was expected to be buffered.");
    }

    [Conditional("DEBUG")]
    internal void DebugEnsureExpected(params ReadOnlySpan<BackendType> expected)
    {
        if (ToDebugType(Header.Type) is { } debugType)
        {
            foreach (var type in expected)
            {
                if (type == debugType)
                    return;
            }

            Debug.Fail($"Message type: {Type} was not an expected: {string.Join(" or ", expected.ToArray())} ");
        }

        // Filtered type for debug asserts, removing other possible (but implicitly handled backend message types) to keep asserts succinct.
        BackendType? ToDebugType(BackendType type) => type switch
        {
            // Error
            BackendType.ErrorResponse => null,
            // Async
            BackendType.NoticeResponse or BackendType.NotificationResponse or BackendType.ParameterStatus => null,
            _ => type
        };
    }

    PgError CreateError(ReadOnlySpan<BackendType> expected, bool unhandled = true)
    {
        Debug.Assert(Type is BackendType.ErrorResponse);
        return ErrorOrNoticeMessage.Create(this, expected, unhandled);
    }

    // We have no buffer for header only messages.
    public bool Buffered => _buffered;
}

readonly struct BackendHeader
{
    public const int ByteCount = Header.ByteCount;

    public BackendHeader(BackendType type, int length)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(length, 4);
        Debug.Assert(type.IsDefined());
        Type = type;
        Length = length;
    }

    BackendHeader(Header header)
    {
        Debug.Assert(((BackendType)header.Tag).IsDefined());
        Type = (BackendType)header.Tag;
        Length = header.Length;
    }

    internal static BackendHeader CreateUnchecked(BackendType type, int length)
    {
        Debug.Assert(type.IsDefined());
        Debug.Assert(length >= 4);
        return new BackendHeader { Type = type, Length = length };
    }

    public BackendType Type { get; private init; }

    // Never negative.
    public int Length { get; private init; }
    public int BodyLength => Length - 4;
    public uint MessageLength => (uint)Length + sizeof(byte);
    public bool HasBody => Length is not 4;

    // Consistently not inlined for some reason?
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static implicit operator BackendHeader(Header header) => new(header);

    public override string ToString() => $"Type: {Type}, Length: {Length}";
}
