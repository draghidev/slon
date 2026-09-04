namespace Slon.Runtime.CompilerServices;

// Provides reusable, allocation-free access to a field retained by another object or struct.
// Generic consumers preserve the concrete implementation type so the ref-returning call can inline.
interface IFieldRef<TOwner, T>
    where TOwner : struct, IFieldRef<TOwner, T>
{
    ref T GetField();
}
