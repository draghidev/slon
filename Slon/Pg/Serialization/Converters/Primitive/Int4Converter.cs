using System;
using System.Numerics;
using Slon.Pg.Serialization;

// ReSharper disable once CheckNamespace
namespace Slon.Pg.Serialization.Converters;

sealed class Int4Converter<T> : PgBufferedConverter<T> where T : INumberBase<T>
{
    public Int4Converter() => IsReadViewBased = true;

    public override ConverterDescriptor GetDescriptor(in DescriptorContext context)
        => ConverterDescriptor.Invariant with { BufferRequirements = BufferRequirements.CreateFixedSize(sizeof(int)) };

    public override T Read(PgReader reader)
        => T.CreateChecked(reader.TryGetReadView(out var view) ? view.ReadInt32() : reader.ReadInt32());
    public override void Write(PgWriter writer, T value) => writer.WriteInt32(int.CreateChecked(value));
}
