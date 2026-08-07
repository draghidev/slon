using Slon.Pg;
using Slon.Pg.Serialization;

namespace Slon;

sealed class ByteColumnLease(PgReader reader, bool sequential) : IColumnLease
{
    bool _revoked;

    internal int Length => reader.FieldSize;

    internal int Read(int offset, Span<byte> destination)
    {
        ObjectDisposedException.ThrowIf(_revoked, this);
        if (sequential && offset < reader.FieldOffset)
            throw new InvalidOperationException(
                "Attempted to read a position in a sequential column which has already been consumed.");
        reader.Seek(offset);
        var count = Math.Min(destination.Length, reader.CurrentRemaining);
        reader.Read(destination.Slice(0, count));
        return count;
    }

    int IColumnLease.Revoke()
    {
        ObjectDisposedException.ThrowIf(_revoked, this);
        _revoked = true;
        return reader.RevokeField();
    }

    async ValueTask<int> IColumnLease.RevokeAsync()
    {
        ObjectDisposedException.ThrowIf(_revoked, this);
        _revoked = true;
        return await reader.RevokeFieldAsync().ConfigureAwait(false);
    }
}
