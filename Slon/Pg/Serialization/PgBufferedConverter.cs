using System;
using System.Threading;
using System.Threading.Tasks;

namespace Slon.Pg.Serialization;

public abstract class PgBufferedConverter<T> : PgConverter<T>
{
    protected PgBufferedConverter()
    {
    }

    protected override Size BindValue(in BindContext context, T value, ref object? writeState)
        => throw new NotSupportedException();

    public sealed override ValueTask<T> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => new(Read(reader));

    internal override ValueTask<object?> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken)
        => new(Read(reader));

    public sealed override ValueTask WriteAsync(PgWriter writer, T value, CancellationToken cancellationToken = default)
    {
        Write(writer, value);
        return new();
    }

    internal override ValueTask WriteAsObject(bool async, PgWriter writer, object? value, CancellationToken cancellationToken)
    {
        Write(writer, (T)value!);
        return new();
    }
}
