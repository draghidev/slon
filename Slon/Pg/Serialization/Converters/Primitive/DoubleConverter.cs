using System;
using System.Numerics;
using Slon.Pg.Serialization;

// ReSharper disable once CheckNamespace
namespace Slon.Pg.Serialization.Converters;

sealed class DoubleConverter<T> : PgBufferedConverter<T> where T : INumberBase<T>
{
    public override ConverterDescriptor GetDescriptor(in DescriptorContext context)
        => ConverterDescriptor.Invariant with { BufferRequirements = BufferRequirements.CreateFixedSize(sizeof(double)) };

    public override T Read(PgReader reader) => T.CreateChecked(reader.ReadDouble());
    public override void Write(PgWriter writer, T value) => writer.WriteDouble(double.CreateChecked(value));
}
