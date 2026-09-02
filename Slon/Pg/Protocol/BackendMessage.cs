using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon.Pg.Protocol;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct BackendMessage
{
    string DebuggerDisplay => $"Type = {Header.Type}, Length = {Header.MessageLength}";

    readonly object? _firstObject;
    readonly object? _contextOrEndObject;

    // Buffered, peeked, type, and token fit in one word so the message remains 32 bytes.
    readonly uint _state;
    readonly int _length;
    readonly int _startIndex;
    readonly int _endIndexOrBufferedLength;

    BackendMessage(BackendHeader header, ReadOnlySequence<byte> buffer,
        BackendMessageContext? context, short token, bool buffered,
        bool independent = false)
    {
        _firstObject = buffer.Start.GetObject();
        _contextOrEndObject = independent ? buffer.End.GetObject() : context;
        _startIndex = buffer.Start.GetInteger() & int.MaxValue;
        _endIndexOrBufferedLength = independent
            ? buffer.End.GetInteger() & int.MaxValue
            : checked((int)buffer.Length);
        _state = (buffered ? 1u : 0)
            | (independent ? 2u : 0)
            | ((uint)(byte)header.Type << 2)
            | ((uint)(ushort)token << 10);
        _length = header.Length;
        if (context is not null)
            context.SetCurrentFallbackBuffer(in buffer,
                _firstObject is ReadOnlySequenceSegment<byte>);
    }

    internal BackendMessage(BackendHeader header, ReadOnlySequence<byte> buffer, BackendMessageContext context, short token)
        : this(header, buffer, context, token, buffer.Length >= header.MessageLength) {}

    internal static BackendMessage CreateIndependent(
        BackendHeader header, ReadOnlySequence<byte> buffer)
    {
        if (buffer.Length < header.MessageLength)
            ThrowHelper.ThrowInvalidOperation(
                "An independent backend message must be fully buffered.");
        return new(header, buffer, context: null, token: 0,
            buffered: true, independent: true);
    }

    internal static void InitializeIndependent(ref BackendMessage destination,
        BackendHeader header, ReadOnlySequence<byte> buffer)
    {
        var value = CreateIndependent(header, buffer);
        WriteGranularly(ref destination, in value);
    }

    internal static void Copy(
        ref BackendMessage destination, in BackendMessage value)
        => WriteGranularly(ref destination, in value);

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
        if ((destinationIsZero && value._contextOrEndObject is not null)
            || !ReferenceEquals(destination._contextOrEndObject, value._contextOrEndObject))
            Unsafe.AsRef(in destination._contextOrEndObject) = value._contextOrEndObject;
        if (!ReferenceEquals(destination._firstObject, value._firstObject))
            Unsafe.AsRef(in destination._firstObject) = value._firstObject;

        Unsafe.AsRef(in destination._state) = value._state;
        Unsafe.AsRef(in destination._length) = value._length;
        Unsafe.AsRef(in destination._startIndex) = value._startIndex;
        Unsafe.AsRef(in destination._endIndexOrBufferedLength) = value._endIndexOrBufferedLength;
    }

    BackendType Type => (BackendType)((_state >> 2) & byte.MaxValue);
    short Token => (short)(_state >> 10);
    bool IsIndependent => (_state & 2) != 0;
    internal bool IsDefault => Type == default;
    BackendMessageContext Context
        => _contextOrEndObject as BackendMessageContext
            ?? throw new InvalidOperationException(
                "The independent backend message has no decoder context.");

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ReadOnlySequence<byte> GetBuffer()
    {
        if (!IsIndependent)
        {
            if (_firstObject is byte[] array)
                return new(array, _startIndex, _endIndexOrBufferedLength);
            if (_firstObject is MemoryManager<byte> manager)
                return new(manager.Memory.Slice(_startIndex, _endIndexOrBufferedLength));
            return Context.GetFallbackBuffer(Token);
        }

        if (_firstObject is byte[] independentArray
            && ReferenceEquals(_firstObject, _contextOrEndObject))
            return new(independentArray, _startIndex,
                _endIndexOrBufferedLength - _startIndex);
        if (_firstObject is MemoryManager<byte> independentManager
            && ReferenceEquals(_firstObject, _contextOrEndObject))
            return new(independentManager.Memory.Slice(
                _startIndex, _endIndexOrBufferedLength - _startIndex));
        return new((ReadOnlySequenceSegment<byte>)_firstObject!, _startIndex,
            (ReadOnlySequenceSegment<byte>)_contextOrEndObject!,
            _endIndexOrBufferedLength);
    }

    public BackendHeader Header
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => BackendHeader.CreateUnchecked(Type, _length);
    }

    public ReadOnlySequence<byte> GetSequence(SequencePosition start)
    {
        EnsureBodyWindowAvailable();
        return GetBuffer().Slice(start);
    }

    public ReadOnlySequence<byte> GetSequence(long offset)
    {
        EnsureBodyWindowAvailable();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        var start = checked(BackendHeader.ByteCount + offset);
        var length = BufferedLength - start;
        if (length < 0 || length > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(offset));

        if (!IsIndependent || ReferenceEquals(_firstObject, _contextOrEndObject))
        {
            if (_firstObject is byte[] array)
                return new(array, checked(_startIndex + (int)start), (int)length);
            if (_firstObject is MemoryManager<byte> manager)
                return new(manager.Memory.Slice(
                    checked(_startIndex + (int)start), (int)length));
        }
        return GetBuffer().Slice(start);
    }

    public ReadOnlySequence<byte> GetSequence()
        => GetSequence(0);

    internal ReadOnlyMemory<byte> GetContiguousMemory(
        ReadOnlyMemory<byte> source)
    {
        EnsureBodyWindowAvailable();
        return IsIndependent
            ? source
            : Context.GetContiguousMemory(Token, source);
    }

    internal ReadOnlyMemory<byte> GetContiguousMemory(
        in ReadOnlySequence<byte> source)
    {
        EnsureBodyWindowAvailable();
        if (IsIndependent)
            return source.IsSingleSegment ? source.First : source.ToArray();
        return Context.GetContiguousMemory(Token, source);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetFirstSpan(int offset, out ReadOnlySpan<byte> span)
    {
        EnsureBodyWindowAvailable();
        return TryGetFirstSpanUnchecked(offset, out span);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetFirstSpanUnchecked(int offset, out ReadOnlySpan<byte> span)
    {
        offset += BackendHeader.ByteCount;
        var firstSpan = GetFirstMemory().Span;
        if ((uint)offset <= (uint)firstSpan.Length)
        {
            span = firstSpan.Slice(offset);
            return true;
        }

        span = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetBufferedFirstMemory(int offset, out ReadOnlyMemory<byte> memory)
    {
        Debug.Assert(Buffered);
        offset += BackendHeader.ByteCount;
        var firstLength = IsIndependent
            ? _endIndexOrBufferedLength - _startIndex
            : _endIndexOrBufferedLength;
        if (_firstObject is byte[] array && (uint)offset <= (uint)firstLength)
        {
            memory = array.AsMemory(_startIndex + offset, firstLength - offset);
            return true;
        }

        var firstMemory = GetFirstMemory();
        if ((uint)offset <= (uint)firstMemory.Length)
        {
            memory = firstMemory.Slice(offset);
            return true;
        }

        memory = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetBufferedArrayMemory(int offset, out ReadOnlyMemory<byte> memory)
    {
        Debug.Assert(Buffered);
        offset += BackendHeader.ByteCount;
        var firstLength = IsIndependent
            ? _endIndexOrBufferedLength - _startIndex
            : _endIndexOrBufferedLength;
        if (_firstObject is byte[] array && (uint)offset <= (uint)firstLength)
        {
            memory = array.AsMemory(_startIndex + offset, firstLength - offset);
            return true;
        }

        memory = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ReadOnlyMemory<byte> GetFirstMemory()
    {
        var length = IsIndependent
            ? _endIndexOrBufferedLength - _startIndex
            : _endIndexOrBufferedLength;
        return _firstObject switch
        {
            byte[] array => array.AsMemory(_startIndex, length),
            MemoryManager<byte> manager => manager.Memory.Slice(_startIndex, length),
            ReadOnlySequenceSegment<byte> segment => segment.Memory.Slice(_startIndex),
            _ => GetBuffer().First
        };
    }

    public SequenceReader<byte> BodyReader => new(GetSequence());

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void EnsureBodyWindowAvailable()
    {
        if (!Buffered)
            Context.EnsureBodyWindowAvailable(Token);
    }

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
            => throw new InvalidOperationException(
                $"Unexpected backend message: {actual}, expected: {string.Join(" or ", expected.ToArray())}.");
    }

    public Accessor GetAccessor() => new(Context, Token, Type, Buffered);

    internal BackendMessageBodyReader OpenBodyReader()
        => new(Context, Token, GetSequence(), Buffered);

    internal void BufferBody()
    {
        if (!Buffered)
            Context.BufferCurrentMessage(Token);
    }

    internal ValueTask BufferBodyAsync(CancellationToken cancellationToken)
        => Buffered ? default : Context.BufferCurrentMessageAsync(Token, cancellationToken);

    public readonly struct Accessor
    {
        readonly BackendMessageContext _context;
        readonly short _token;
        readonly PgTypes.BackendType _type;
        readonly bool _buffered;

        internal Accessor(BackendMessageContext context, short token,
            PgTypes.BackendType type, bool buffered)
        {
            _context = context;
            _token = token;
            _type = type;
            _buffered = buffered;
        }

        public BackendMessage Message => _context.GetCurrent(_token);

        internal PgTypes.BackendType Type => _type;
        internal bool Buffered => _buffered;
        internal BackendMessageBodyReader OpenBodyReader()
            => _context.OpenCurrentBodyReader(_token);
        internal bool TryGetBufferedFirstMemory(int offset, out ReadOnlyMemory<byte> memory)
            => _context.TryGetCurrentBufferedFirstMemory(_token, offset, out memory);
        internal void BufferBody() => _context.BufferCurrentMessage(_token);
        internal ValueTask BufferBodyAsync(CancellationToken cancellationToken)
            => _context.BufferCurrentMessageAsync(_token, cancellationToken);

        // The JIT should have a phase for picking granular writes (and write barriers) over full struct assignments.
        // This translation is entirely mechanical (even though these implementations need to deviate for external types).
        internal static void WriteGranularly(ref Accessor destination, in Accessor value, bool destinationIsZero = false)
        {
            if ((destinationIsZero && value._context is not null) || !ReferenceEquals(destination._context, value._context))
                Unsafe.AsRef(in destination._context) = value._context!;

            Unsafe.AsRef(in destination._token) = value._token;
            Unsafe.AsRef(in destination._type) = value._type;
            Unsafe.AsRef(in destination._buffered) = value._buffered;
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
            => throw new PgProtocolException($"Unexpected backend message: {actual}, expected: {expected}.");
    }

    public void EnsureExpected(BackendType expected)
    {
        if (expected != Type)
            Throw(Type, expected);

        static void Throw(BackendType actual, BackendType expected)
            => throw new PgProtocolException($"Unexpected backend message: {actual}, expected: {expected}.");
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
            => throw new PgProtocolException(
                $"Unexpected backend message: {actual}, expected: {string.Join(" or ", expected.ToArray())}.");
    }

    public void EnsureBuffered()
    {
        if (!Buffered)
            Throw(Type);

        static void Throw(BackendType actual)
            => throw new InvalidOperationException($"Message type: {actual} was expected to be buffered");
    }

    [Conditional("DEBUG")]
    internal void DebugEnsureBuffered()
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
        return new(ErrorOrNoticeMessage.Create(this, expected, unhandled));
    }

    internal void MarkPriorCancellationExposure()
        => Context.MarkPriorCancellationExposure(Token);

    internal bool HasPriorCancellationExposure
        => Context.HasPriorCancellationExposure(Token);

    internal void MarkBackendTermination()
        => Context.MarkBackendTermination(Token);

    internal bool IsBackendTermination
        => Context.IsBackendTermination(Token);

    internal bool TryObserveError()
        => Context.TryObserveError(Token);

    // We have no buffer for header only messages.
    public bool Buffered => (_state & 1) != 0;
    internal long BufferedLength
        => IsDefault ? 0
            : IsIndependent ? Header.MessageLength
            : _endIndexOrBufferedLength;
}

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct BackendHeader
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
    internal static BackendHeader FromHeader(Header header) => new(header);

    public override string ToString() => $"Type: {Type}, Length: {Length}";
}
