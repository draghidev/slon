using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Slon.Pipelines;
using Slon.Buffers;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg.Protocol;

// Bounded view over a partially buffered message body. It releases consumed bytes to the pipe and
// extends only to this message's declared boundary; the normal decoder resumes with its successor.
sealed class BackendMessageBodyReader : IInputReader
{
    readonly BackendMessageContext _context;
    readonly short _token;
    readonly long _segmentOffset;
    ReadOnlySequence<byte> _buffer;
    SequencePosition _consumed;
    long _consumedLength;
    int _continuationOffset;
    bool _initial = true;
    bool _advanced;

    internal BackendMessageBodyReader(BackendMessageContext context, short token,
        ReadOnlySequence<byte> buffer, bool isComplete)
    {
        _context = context;
        _token = token;
        _segmentOffset = isComplete ? 0 : context.GetCurrentMessageOffset(token);
        _buffer = buffer;
        _consumed = buffer.Start;
        IsComplete = isComplete;
    }

    public ReadOnlySequence<byte> Buffer => _buffer;
    public bool IsComplete { get; private set; }
    internal int ContinuationOffset => _continuationOffset;

    public void AdvanceTo(SequencePosition consumed, long consumedLength)
    {
        if (_advanced)
            ThrowHelper.ThrowInvalidOperation();
        if (consumedLength < 0 || consumedLength > _buffer.Length)
            throw new ArgumentOutOfRangeException(nameof(consumedLength));
        Debug.Assert(_buffer.Slice(0, consumed).Length == consumedLength,
            "The consumed position and consumed length must identify the same byte.");

        _consumed = consumed;
        _continuationOffset = checked((int)consumedLength);
        if (IsComplete)
            return;
        _consumedLength = consumedLength + (_initial ? _segmentOffset + BackendHeader.ByteCount : 0);
        _advanced = true;
    }

    public bool TryRead()
    {
        EnsureAdvanced();
        if (!_context.TryContinue(_token, _consumed, _consumedLength, out var result))
            return false;
        Publish(result);
        return true;
    }

    public ValueTask ReadAsync(CancellationToken cancellationToken = default)
    {
        EnsureAdvanced();
        var task = _context.ContinueAsync(_token, _consumed, _consumedLength, cancellationToken);
        if (task.IsCompletedSuccessfully)
        {
            Publish(task.Result);
            return default;
        }
        return Core(task);

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder))]
        async ValueTask Core(ValueTask<CurrentSegmentBuffer> task)
            => Publish(await task.ConfigureAwait(false));
    }

    public void Read()
    {
        EnsureAdvanced();
        Publish(_context.Continue(_token, _consumed, _consumedLength));
    }

    public bool TryExtend()
    {
        EnsureCanExtend();
        if (!_context.TryExtend(_token, out var result))
            return false;
        Publish(result, retained: true);
        return true;
    }

    public ValueTask ExtendAsync(CancellationToken cancellationToken = default)
    {
        EnsureCanExtend();
        var task = _context.ExtendAsync(_token, cancellationToken);
        if (task.IsCompletedSuccessfully)
        {
            Publish(task.Result, retained: true);
            return default;
        }
        return Core(task);

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder))]
        async ValueTask Core(ValueTask<CurrentSegmentBuffer> task)
            => Publish(await task.ConfigureAwait(false), retained: true);
    }

    public void Extend()
    {
        EnsureCanExtend();
        Publish(_context.Extend(_token), retained: true);
    }

    public void BufferAll()
    {
        while (!IsComplete)
            Extend();
    }

    internal void Consume(int offset, int count)
    {
        while (true)
        {
            var consumed = (int)Math.Min(count, _buffer.Length - offset);
            offset += consumed;
            count -= consumed;
            AdvanceTo(_buffer.GetPosition(offset), offset);
            if (!IsComplete)
            {
                Read();
                offset = 0;
            }
            else if (count != 0)
            {
                throw new EndOfStreamException();
            }

            if (count == 0)
                return;
        }
    }

    internal async ValueTask ConsumeAsync(int offset, int count,
        CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var consumed = (int)Math.Min(count, _buffer.Length - offset);
            offset += consumed;
            count -= consumed;
            AdvanceTo(_buffer.GetPosition(offset), offset);
            if (!IsComplete)
            {
                await ReadAsync(cancellationToken).ConfigureAwait(false);
                offset = 0;
            }
            else if (count != 0)
            {
                throw new EndOfStreamException();
            }

            if (count == 0)
                return;
        }
    }

    public ValueTask BufferAllAsync(CancellationToken cancellationToken = default)
    {
        if (IsComplete)
            return default;
        return Core(cancellationToken);

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder))]
        async ValueTask Core(CancellationToken cancellationToken)
        {
            while (!IsComplete)
                await ExtendAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    void EnsureCanExtend()
    {
        if (_advanced || IsComplete)
            ThrowHelper.ThrowInvalidOperation();
    }

    void EnsureAdvanced()
    {
        if (!_advanced)
            ThrowHelper.ThrowInvalidOperation("AdvanceTo must be called before reading more message data.");
    }

    void Publish(CurrentSegmentBuffer result, bool retained = false)
    {
        _buffer = result.Buffer;
        IsComplete = result.IsComplete;
        if (!retained)
        {
            _initial = false;
            _continuationOffset = 0;
        }
        _advanced = false;
    }
}
