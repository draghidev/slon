using System.Buffers;

namespace Slon.Buffers;

interface ICopyableBuffer<T>
{
    void CopyTo<TWriter>(ref TWriter destination) where TWriter: struct, IBufferWriter<T>, allows ref struct;
    void CopyTo(IBufferWriter<T> destination);
}
